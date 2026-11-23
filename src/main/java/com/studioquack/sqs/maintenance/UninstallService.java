package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.channel.net.NetAgentClient;
import com.studioquack.sqs.channel.wg.WindowsOverlayFirewall;
import com.studioquack.sqs.sandbox.egress.PodmanNetnsTarget;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.secrets.inventory.ClientSecretInventoryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The shared "make this host forget this engine" routine, used by BOTH the pushed
 * {@code uninstall} command and the localhost ops endpoint. Steps (each best-effort and
 * independently logged so one failure never blocks the rest): Purge owned secrets —
 * {@link ClientSecretInventoryService#purgeAllOwned} deletes the OS-store plaintext this daemon
 * wrote, and only that.
 */
@Slf4j
@Service
public class UninstallService {

    /** Provenance labels {@code HardeningArgs}/stateful runs stamp, so a reaper can find leftovers. */
    private static final List<String> SANDBOX_LABELS = List.of("studioquack.sqs", "studioquack.sqs.stateful");
    private static final Duration REAP_TIMEOUT = Duration.ofSeconds(20);

    /** Uninstall marker the platform installers read. */
    static final String UNINSTALL_MARKER = ".sq-uninstalled";

    /**
     * Install-prefix files that must not outlive the engine: the run launcher the guard counts as
     * "installed", and the pid file it would otherwise reuse to kill a process that has since
     * inherited that pid.
     */
    private static final List<String> INSTALL_ARTIFACTS = List.of("run.sh", "run.cmd", "daemon.pid");

    /** Shadow-runtime dir the launchers build to give the JVM an OS-visible process name. */
    private static final String PROCESS_ALIAS_DIR = "runtime";

    /** Renamed launcher path the launchers publish, so an uninstall can remove one placed elsewhere. */
    private static final String PROCESS_ALIAS_ENV = "SQ_PROCESS_ALIAS";

    /** Only a launcher alias of ours may be deleted through {@link #PROCESS_ALIAS_ENV}. */
    private static final String PROCESS_ALIAS_PREFIX = "StudioQuack";

    private final ClientSecretInventoryService inventory;
    private final ContainerRuntime containerRuntime;
    private final Environment environment;
    private final WarmPool warmPool;
    private final NetAgentClient netAgent;

    /** Test / no-mesh wiring. */
    public UninstallService(ClientSecretInventoryService inventory,
                            ContainerRuntime containerRuntime,
                            Environment environment,
                            WarmPool warmPool) {
        this(inventory, containerRuntime, environment, warmPool, null);
    }

    @Autowired
    public UninstallService(ClientSecretInventoryService inventory,
                            ContainerRuntime containerRuntime,
                            Environment environment,
                            WarmPool warmPool,
                            ObjectProvider<NetAgentClient> netAgent) {
        this.inventory = inventory;
        this.containerRuntime = containerRuntime;
        this.environment = environment;
        this.warmPool = warmPool;
        this.netAgent = netAgent == null ? null : netAgent.getIfAvailable();
    }

    /** Immutable outcome of an uninstall purge, echoed back to the BE / ops caller. */
    public record UninstallSummary(int purgedSecrets, boolean dependenciesRemoved, int reapedContainers,
                                   List<String> notes) {
    }

    /** Never throws — failures are captured in {@link UninstallSummary#notes}. */
    public UninstallSummary purge(boolean removeDependencies) {
        List<String> notes = new ArrayList<>();

        int purged = 0;
        try {
            purged = inventory.purgeAllOwned();
        } catch (Exception e) {
            // The purge is the security-critical step; surface but don't abort the rest.
            notes.add("secret purge error: " + safe(e));
            log.warn("uninstall: secret purge failed: {}", safe(e));
        }

        // While we are still the LocalAPI operator — the side-car SIGKILL cannot do this later
        // if the CLI path is gone. Privileged agent uninstall is the side-car / installer.
        withdrawMeshSession(notes);
        removeWindowsOverlayFirewall(notes);

        boolean depsRemoved = false;
        if (removeDependencies) {
            depsRemoved = removeDependencyCache(notes);
        }

        // Close the warm pool BEFORE reaping. It is the only thing on this host that opens containers
        // on a timer, so reaping while it still holds a live target just hands it 30 seconds to warm
        // a fresh reservoir behind the uninstall.
        int stoppedWarm = stopWarmPool(notes);
        int reaped = reapSandboxContainers(notes) + stoppedWarm;

        // Clear enrollment BEFORE the process exits so a relaunch race cannot spam /enroll.
        invalidateEnrollmentState(notes);

        // …and make the host stop advertising itself as installed, or the next install stops to ask.
        clearInstallFootprint(notes);

        log.info("uninstall: purge complete purgedSecrets={} dependenciesRemoved={} reapedContainers={}",
                purged, depsRemoved, reaped);
        return new UninstallSummary(purged, depsRemoved, reaped, notes);
    }

    /**
     * Takes this node off the mesh while the daemon is still the LocalAPI operator.
     * Best-effort; the side-car repeats down/logout and then removes the root service.
     */
    private void withdrawMeshSession(List<String> notes) {
        if (netAgent == null) {
            return;
        }
        try {
            netAgent.down();
            netAgent.logout();
            notes.add("mesh session down+logout");
            log.info("uninstall: mesh session torn down");
        } catch (Exception e) {
            notes.add("mesh session teardown failed: " + safe(e));
            log.warn("uninstall: mesh session teardown failed: {}", safe(e));
        }
    }

    private void removeWindowsOverlayFirewall(List<String> notes) {
        try {
            if (WindowsOverlayFirewall.removeInboundAllow()) {
                notes.add("windows overlay firewall rule removed");
            }
        } catch (Exception e) {
            notes.add("windows overlay firewall rule not removed: " + safe(e));
            log.warn("uninstall: Windows overlay firewall delete failed: {}", safe(e));
        }
    }

    /**
     * Renames/deletes the installer-written enrollment properties and drops channel key material
     * under {@code SQS_DATA_DIR/channel}. Best-effort; failures are notes only.
     */
    private void invalidateEnrollmentState(List<String> notes) {
        java.util.LinkedHashSet<Path> enrollmentCandidates = new java.util.LinkedHashSet<>();
        // Prefer Spring Environment (tests can override; production already mirrors process env).
        String chCfg = firstNonBlank(environment.getProperty("SQ_CHANNEL_CONFIG"),
                System.getenv("SQ_CHANNEL_CONFIG"));
        if (chCfg != null) {
            enrollmentCandidates.add(Path.of(chCfg).toAbsolutePath().normalize());
        }
        Path dataPath = dataDir();
        // Stock layout: <prefix>/data + <prefix>/config/enrollment.properties
        Path parent = dataPath.getParent();
        if (parent != null) {
            enrollmentCandidates.add(parent.resolve("config").resolve("enrollment.properties"));
        }
        enrollmentCandidates.add(dataPath.resolve("config").resolve("enrollment.properties"));

        boolean anyCleared = false;
        for (Path p : enrollmentCandidates) {
            if (p == null || !Files.isRegularFile(p)) {
                continue;
            }
            try {
                Path tombstone = p.resolveSibling(p.getFileName().toString() + ".uninstalled");
                Files.deleteIfExists(tombstone);
                String body = Files.readString(p);
                Files.move(p, tombstone);
                // Marker so operators see why a manual launch will not enroll.
                Files.writeString(tombstone, body
                        + "\n# INVALIDATED by uninstall at " + java.time.Instant.now()
                        + " — env deleted; do not restore without a fresh enrollment bearer.\n");
                anyCleared = true;
                notes.add("enrollment config invalidated: " + p + " -> " + tombstone.getFileName());
                log.info("uninstall: enrollment config invalidated path={}", p);
            } catch (Exception e) {
                // Fall back to delete if rename/annotate fails.
                try {
                    Files.deleteIfExists(p);
                    anyCleared = true;
                    notes.add("enrollment config deleted: " + p);
                } catch (Exception e2) {
                    notes.add("enrollment config clear failed (" + p + "): " + safe(e2));
                    log.warn("uninstall: enrollment clear failed path={}: {}", p, safe(e2));
                }
            }
        }
        if (!anyCleared) {
            notes.add("enrollment config already absent (no SQ_CHANNEL_CONFIG / stock path)");
        }

        // Drop persisted channel identity so a manual jar launch cannot reuse mTLS material.
        Path channelDir = dataPath.resolve("channel");
        if (Files.isSymbolicLink(channelDir)) {
            try {
                Files.deleteIfExists(channelDir);
                notes.add("channel data dir (symlink) unlinked: " + channelDir);
            } catch (IOException e) {
                notes.add("channel data clear error: " + safe(e));
            }
        } else if (Files.isDirectory(channelDir, LinkOption.NOFOLLOW_LINKS)) {
            try {
                deleteJailedTree(channelDir);
                notes.add("channel data dir cleared: " + channelDir);
                log.info("uninstall: channel data cleared dir={}", channelDir);
            } catch (IOException e) {
                notes.add("channel data clear error: " + safe(e));
                log.warn("uninstall: channel data clear failed dir={}: {}", channelDir, safe(e));
            }
        }

        Path wgDir = dataPath.resolve("wg");
        if (Files.isSymbolicLink(wgDir)) {
            try {
                Files.deleteIfExists(wgDir);
                notes.add("legacy wg dir (symlink) unlinked: " + wgDir);
            } catch (IOException e) {
                notes.add("legacy wg dir clear error: " + safe(e));
            }
        } else if (Files.isDirectory(wgDir, LinkOption.NOFOLLOW_LINKS)) {
            try {
                deleteJailedTree(wgDir);
                notes.add("legacy wg dir cleared: " + wgDir);
                log.info("uninstall: leftover wg conf/key dir cleared dir={}", wgDir);
            } catch (IOException e) {
                notes.add("legacy wg dir clear error: " + safe(e));
                log.warn("uninstall: leftover wg dir clear failed dir={}: {}", wgDir, safe(e));
            }
        }
    }

    /**
     * The platform installers refuse to land on top of an existing engine, and they decide that from
     * the install prefix: a bootJar, a {@code run.sh}/{@code run.cmd} launcher, an enrollment config,
     * or a registered supervisor. Purging secrets and unloading the launchd/systemd unit therefore
     * leaves the host half installed — the jar and launcher still there — which is why a
     * delete-then-reinstall on the same machine.
     */
    private void clearInstallFootprint(List<String> notes) {
        Path prefix = dataDir().getParent();
        if (prefix == null || !Files.isDirectory(prefix)) {
            notes.add("install prefix not resolvable from SQS_DATA_DIR — install footprint left as is");
            return;
        }
        // Nothing the guard reads means nothing to clear — and no marker, so a daemon run out of a
        // plain working dir (a dev run, a test) never drops files next to whatever else lives there.
        List<Path> artifacts = installedArtifacts(prefix);
        if (artifacts.isEmpty()) {
            notes.add("no installed artifacts under " + prefix + " — install footprint already clear");
            return;
        }

        Path marker = prefix.resolve(UNINSTALL_MARKER);
        String envId = firstNonBlank(environment.getProperty("sqs.channel.env-id"), "unknown");
        try {
            Files.writeString(marker, """
                    # StudioQuack — this installation was torn down by an uninstall command.
                    # The platform installer reads this file to tell a stale install from a live
                    # engine, so do not delete it by hand: a fresh install removes it itself.
                    uninstalled-at=%s
                    env-id=%s
                    """.formatted(java.time.Instant.now(), envId));
            notes.add("uninstall marker written: " + marker);
        } catch (Exception e) {
            notes.add("uninstall marker write failed (" + marker + "): " + safe(e));
            log.warn("uninstall: marker write failed path={}: {}", marker, safe(e));
        }

        int removed = 0;
        for (Path artifact : artifacts) {
            if (deleteQuietly(artifact, notes)) {
                removed++;
            }
        }
        if (deleteRecursively(prefix.resolve(PROCESS_ALIAS_DIR), notes)) {
            removed++;
        }
        removed += removeProcessAlias(notes);

        log.info("uninstall: install footprint cleared prefix={} removedArtifacts={} marker={}",
                prefix, removed, marker);
        notes.add("install footprint cleared: " + removed + " artifact(s) under " + prefix
                + " (data/, logs/ and the dependency cache kept)");
    }

    /**
     * The install-prefix entries that prove an engine is installed here: the bootJar, the run
     * launcher, its pid file. Keep in step with {@code guard_existing_install} in the install scripts
     * — anything the guard tests for and this does not return keeps the REPLACE prompt alive.
     */
    private static List<Path> installedArtifacts(Path prefix) {
        List<Path> found = new ArrayList<>();
        try (var jars = Files.newDirectoryStream(prefix, "*.jar")) {
            for (Path jar : jars) {
                if (Files.isRegularFile(jar)) {
                    found.add(jar);
                }
            }
        } catch (IOException e) {
            log.debug("uninstall: bootJar scan failed dir={} ({})", prefix, e.getMessage());
        }
        for (String artifact : INSTALL_ARTIFACTS) {
            Path p = prefix.resolve(artifact);
            if (Files.isRegularFile(p)) {
                found.add(p);
            }
        }
        return found;
    }

    /**
     * Removes the renamed launcher that gives the daemon its OS-visible process name when the
     * launcher had to put it outside the prefix (next to a system JDK's {@code java}). The launcher
     * publishes the path it used; the name check keeps a tampered environment from turning this into
     * an arbitrary delete.
     */
    private int removeProcessAlias(List<String> notes) {
        String alias = firstNonBlank(environment.getProperty(PROCESS_ALIAS_ENV),
                System.getenv(PROCESS_ALIAS_ENV));
        if (alias == null) {
            return 0;
        }
        Path aliasPath = Path.of(alias).toAbsolutePath().normalize();
        if (!aliasPath.getFileName().toString().startsWith(PROCESS_ALIAS_PREFIX)) {
            notes.add("ignoring " + PROCESS_ALIAS_ENV + " with an unexpected name: " + aliasPath);
            return 0;
        }
        return deleteQuietly(aliasPath, notes) ? 1 : 0;
    }

    /** Best-effort single-file delete. Reports only what it actually removed. */
    private static boolean deleteQuietly(Path path, List<String> notes) {
        try {
            if (!Files.deleteIfExists(path)) {
                return false;
            }
            notes.add("removed install artifact: " + path);
            return true;
        } catch (Exception e) {
            // Windows keeps the running JVM's own bootJar locked; the marker covers that case.
            notes.add("could not remove " + path + " (" + safe(e) + ") — uninstall marker still applies");
            return false;
        }
    }

    /** Best-effort recursive delete. Symlinks are unlinked, never followed into the real JDK. */
    private static boolean deleteRecursively(Path root, List<String> notes) {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        try {
            if (Files.isSymbolicLink(root)) {
                Files.deleteIfExists(root);
            } else {
                deleteJailedTree(root);
            }
        } catch (IOException e) {
            notes.add("could not remove " + root + ": " + safe(e));
            return false;
        }
        notes.add("removed install artifact: " + root);
        return true;
    }

    /**
     * Depth-first delete that never passes {@code FOLLOW_LINKS}. A path whose real location
     * is outside {@code root} is skipped (symlink-to-elsewhere).
     */
    static void deleteJailedTree(Path root) throws IOException {
        Path rootAbs = root.toAbsolutePath().normalize();
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                if (!mayDelete(rootAbs, p)) {
                    log.warn("uninstall: skipping path outside jail {} (root={})", p, rootAbs);
                    return;
                }
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.debug("uninstall: could not delete {} ({})", p, e.getMessage());
                }
            });
        }
    }

    static boolean mayDelete(Path rootAbs, Path p) {
        Path pAbs = p.toAbsolutePath().normalize();
        if (!pAbs.startsWith(rootAbs)) {
            return false;
        }
        if (Files.isSymbolicLink(p)) {
            return true;
        }
        try {
            Path realRoot = rootAbs.toRealPath();
            Path realP = p.toRealPath();
            return realP.startsWith(realRoot);
        } catch (IOException e) {
            return pAbs.startsWith(rootAbs);
        }
    }

    /** Resolved {@code SQS_DATA_DIR} — the stock layout puts it directly under the install prefix. */
    private Path dataDir() {
        String dataDir = firstNonBlank(environment.getProperty("SQS_DATA_DIR"),
                System.getenv("SQS_DATA_DIR"), "./data");
        return Path.of(dataDir).toAbsolutePath().normalize();
    }

    /** Recursively delete the {@code sqs.deps.root} dependency cache. Best-effort. */
    private boolean removeDependencyCache(List<String> notes) {
        String root = environment.getProperty("sqs.deps.root", "./data/deps");
        Path dir = Path.of(root);
        if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
            notes.add("dependency cache already absent: " + dir);
            return true;
        }
        try {
            if (Files.isSymbolicLink(dir)) {
                Files.deleteIfExists(dir);
            } else {
                deleteJailedTree(dir);
            }
            boolean gone = !Files.exists(dir, LinkOption.NOFOLLOW_LINKS);
            if (!gone) {
                notes.add("dependency cache partially removed: " + dir);
            }
            log.info("uninstall: dependency cache removal dir={} removed={}", dir, gone);
            return gone;
        } catch (IOException e) {
            notes.add("dependency cache removal error: " + safe(e));
            log.warn("uninstall: dependency cache removal failed dir={}: {}", dir, safe(e));
            return false;
        }
    }

    /**
     * Latches the warm pool shut and tears down everything it is holding — including the slots
     * currently leased to a run, which the label reap below would otherwise be the only thing to
     * catch. Uninstall is allowed to be forceful: the engine is being removed, so a job still in
     * flight has nowhere to report back to.
     */
    private int stopWarmPool(List<String> notes) {
        if (warmPool == null) {
            return 0;
        }
        try {
            int stopped = warmPool.shutdown();
            notes.add("warm pool closed: " + stopped + " container(s) stopped");
            return stopped;
        } catch (Exception e) {
            notes.add("warm pool close error: " + safe(e));
            log.warn("uninstall: warm pool close failed: {}", safe(e));
            return 0;
        }
    }

    /**
     * Best-effort {@code podman rm -f} of any container still carrying our provenance labels. Skips
     * silently when no runtime is available (nothing could have been left running).
     */
    private int reapSandboxContainers(List<String> notes) {
        if (!containerRuntime.available() || containerRuntime.cli() == null) {
            return 0;
        }
        String cli = containerRuntime.cli();
        // The egress anchor is a pod: `rm -f` refuses its infra container, so the pod goes first.
        try {
            CliRunner.run(List.of(cli, "pod", "rm", "-f", "--ignore", PodmanNetnsTarget.ANCHOR_POD), REAP_TIMEOUT);
        } catch (Exception e) {
            log.debug("uninstall: anchor pod removal failed: {}", safe(e));
        }
        int reaped = 0;
        for (String label : SANDBOX_LABELS) {
            try {
                CliRunner.Result list = CliRunner.run(
                        List.of(cli, "ps", "-aq", "--filter", "label=" + label), REAP_TIMEOUT);
                if (!list.ok() || list.stdout().isBlank()) {
                    continue;
                }
                List<String> ids = new ArrayList<>();
                for (String line : list.stdout().split("\\r?\\n")) {
                    if (!line.isBlank()) {
                        ids.add(line.trim());
                    }
                }
                if (ids.isEmpty()) {
                    continue;
                }
                List<String> rm = new ArrayList<>(List.of(cli, "rm", "-f"));
                rm.addAll(ids);
                CliRunner.Result removed = CliRunner.run(rm, REAP_TIMEOUT);
                if (removed.ok()) {
                    reaped += ids.size();
                } else {
                    notes.add("container reap (" + label + ") exited " + removed.exitCode());
                }
            } catch (Exception e) {
                notes.add("container reap (" + label + ") error: " + safe(e));
                log.debug("uninstall: container reap failed label={}: {}", label, safe(e));
            }
        }
        return reaped;
    }

    private static String safe(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /** First non-null, non-blank candidate, or {@code null} when all are blank. */
    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v.trim();
            }
        }
        return null;
    }
}
