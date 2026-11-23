package com.studioquack.sqs.deps;

import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.egress.SandboxDns;
import com.studioquack.sqs.sandbox.oci.CgroupCaps;
import com.studioquack.sqs.sandbox.oci.SandboxHostBudget;
import com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;
import com.studioquack.sqs.sandbox.oci.SeccompProfile;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerEnv;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The daemon-side dependency install pipeline. Consumes the live {@link DependencyManifestHolder}
 * and, per entry, runs the correct package manager into a daemon-managed runtime directory,
 * driving each entry through {@code queued → installing → installed/failed} and echoing every.
 */
@Slf4j
@Component
public class DependencyInstaller {

    /** Terminal + transitional states echoed to the BE (match {@code EnvironmentDependencyDto.state}). */
    static final String STATE_INSTALLING = "installing";
    static final String STATE_INSTALLED = "installed";
    static final String STATE_FAILED = "failed";

    /** Bounded failure-message budget (last N chars of stderr) so a chatty build can't bloat the row. */
    private static final int MAX_MESSAGE_CHARS = 4_096;

    /**
     * Anchored, so it rejects a leading {@code -} (option injection), path separators, whitespace and
     * every shell metacharacter. This is the arg-safety gate.
     */
    private static final Pattern NAME =
            Pattern.compile("^(@[A-Za-z0-9._-]+/)?[A-Za-z0-9][A-Za-z0-9._-]*(\\[[A-Za-z0-9,._-]+])?$");

    /** Exact version token (PEP 440-ish / semver); NO range operators, whitespace or metacharacters. */
    private static final Pattern VERSION = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9.+!_-]*$");

    private final DependencyManifestHolder holder;
    private final DependencyStatusReporter reporter;
    private final DepsConfig config;
    private final InstallRunner runner;
    private final ExecutorService executor;
    /**
     * Deps install INSIDE a linux container so wheels match the sandbox platform. Null or unavailable
     * fails closed — there is no host pip/npm path.
     */
    private final ContainerRuntime containerRuntime;
    /** Production ensures {@code sqs-build}. Tests leave this false so they never touch podman. */
    private final boolean ensureBuildNetwork;
    /** Defaults 2048 MiB / 2 CPUs; pids follows the sandbox cap. */
    private int installMemoryMb = 2048;
    private double installCpus = 2.0;
    private int installPids = 1024;
    private List<String> installDns = SandboxDns.defaults();
    /** In-flight install container names by key, so a removed (cancelled) dep can be force-killed. */
    private final ConcurrentHashMap<String, String> inFlightContainer = new ConcurrentHashMap<>();

    /** Tuple keys we have installed this process (key → digest); prevents re-install churn. */
    private final ConcurrentHashMap<String, String> installedKeys = new ConcurrentHashMap<>();
    /** Tuple keys with an in-flight install; prevents duplicate concurrent installs of the same entry. */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    /** One-shot guard for the on-disk cache discovery (runs once, after the channel is up). */
    private final AtomicBoolean discoveryDone = new AtomicBoolean(false);

    @Autowired
    public DependencyInstaller(DependencyManifestHolder holder,
                               DependencyStatusReporter reporter,
                               Environment environment,
                               ContainerRuntime containerRuntime) {
        this(holder, reporter, DepsConfig.fromEnvironment(environment), DependencyInstaller::runCli,
                defaultExecutor(DepsConfig.fromEnvironment(environment).maxParallel()), containerRuntime, true);
        int memory = environment.getProperty("sqs.deps.install-memory-mb", Integer.class, 2048);
        double cpus = environment.getProperty("sqs.deps.install-cpus", Double.class, 2.0);
        this.installMemoryMb = memory > 0 ? memory : 2048;
        this.installCpus = cpus > 0 ? cpus : 2.0;
        this.installPids = SandboxHostBudget.pidsLimit(
                environment.getProperty("sqs.sandbox.pids-limit", Integer.class, 1024));
        this.installDns = SandboxDns.parse(environment.getProperty(SandboxDns.CONFIG_KEY, ""));
    }

    private static CliRunner.Result runCli(List<String> argv, Duration timeout, Map<String, String> env) {
        return CliRunner.run(argv, null, timeout, 256 * 1024, 256 * 1024, env);
    }

    /** Test constructor (host-pip mode: no container runtime). */
    DependencyInstaller(DependencyManifestHolder holder,
                        DependencyStatusReporter reporter,
                        DepsConfig config,
                        InstallRunner runner,
                        ExecutorService executor) {
        this(holder, reporter, config, runner, executor, null, false);
    }

    /** Full constructor: inject a fake {@link InstallRunner} + executor + optional container runtime. */
    DependencyInstaller(DependencyManifestHolder holder,
                        DependencyStatusReporter reporter,
                        DepsConfig config,
                        InstallRunner runner,
                        ExecutorService executor,
                        ContainerRuntime containerRuntime) {
        this(holder, reporter, config, runner, executor, containerRuntime, false);
    }

    private DependencyInstaller(DependencyManifestHolder holder,
                                DependencyStatusReporter reporter,
                                DepsConfig config,
                                InstallRunner runner,
                                ExecutorService executor,
                                ContainerRuntime containerRuntime,
                                boolean ensureBuildNetwork) {
        this.holder = holder;
        this.reporter = reporter;
        this.config = config;
        this.runner = runner;
        this.executor = executor;
        this.containerRuntime = containerRuntime;
        this.ensureBuildNetwork = ensureBuildNetwork;
    }

    /** Reconcile whatever the holder currently knows once the app is up (in-memory manifest at boot). */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        if (!config.installEnabled()) {
            return;
        }
        reconcile(holder == null ? List.of() : holder.current());
    }

    /**
     * Submit installs for every entry in {@code manifest} that is not already installed. Non-blocking:
     * work runs on the background pool, so this is safe to call from the heartbeat command thread (
     * {@link com.studioquack.sqs.command.SetDepsCommandHandler}).
     */
    public void reconcile(List<DependencyEntry> manifest) {
        if (!config.installEnabled()) {
            log.info("deps: install pipeline disabled (sqs.deps.install.enabled=false); manifest not reconciled");
            return;
        }
        // Surface packages already in the on-disk cache (preloaded / from a prior install)
        // once the channel is up, so the FE library isn't empty after a fresh enrol. One-shot and
        // non-blocking (runs on the background pool), so it never stalls the heartbeat command thread.
        maybeDiscoverAsync();
        List<DependencyEntry> entries = manifest == null ? List.of() : manifest;

        Set<String> present = new HashSet<>();
        for (DependencyEntry e : entries) {
            if (e != null && e.name() != null && !e.name().isBlank()) {
                present.add(key(e));
            }
        }
        installedKeys.keySet().retainAll(present);
        // Cancel: a dep removed from the manifest while its install is still running (the operator
        // clicked remove/cancel on an "installing" row) → force-kill its install container.
        for (String key : new HashSet<>(inFlightContainer.keySet())) {
            if (!present.contains(key)) {
                cancelInFlight(key);
            }
        }

        for (DependencyEntry entry : entries) {
            if (entry == null || entry.name() == null || entry.name().isBlank()) {
                continue;
            }
            String key = key(entry);
            // Trust a BE-reported installed row so a manifest re-push doesn't reinstall everything.
            if (STATE_INSTALLED.equalsIgnoreCase(entry.state())) {
                // An installed row often has no digest (the backend only stores one after a successful echo).
                // putIfAbsent(null) aborted the whole set-deps before any queued row was submitted.
                String digest = entry.digest();
                installedKeys.putIfAbsent(key, digest == null || digest.isBlank()
                        ? fallbackDigest(entry.name(), normaliseVersion(entry.version()))
                        : digest);
                continue;
            }
            if (installedKeys.containsKey(key)) {
                continue;
            }
            if (!inFlight.add(key)) {
                continue; // already installing this exact tuple
            }
            try {
                executor.execute(() -> {
                    try {
                        installOne(entry, key);
                    } finally {
                        inFlight.remove(key);
                    }
                });
            } catch (RejectedExecutionException reject) {
                // Pool saturated / shutting down — release the guard and report a clean failure.
                inFlight.remove(key);
                reporter.report(entry, STATE_FAILED, null, null, null,
                        "install queue saturated; will retry on the next manifest push");
            }
        }
    }

    /** The per-entry state machine: installing → (installed | failed). Never throws. */
    private void installOne(DependencyEntry entry, String key) {
        reporter.report(entry, STATE_INSTALLING, null, null, null, null);
        try {
            if (config.egress() == EgressPolicy.NONE) {
                reporter.report(entry, STATE_FAILED, null, null, null,
                        "network egress is disabled for this environment (egress=none); cannot install "
                                + entry.name());
                return;
            }

            String lang = entry.language() == null ? "python" : entry.language().trim().toLowerCase(Locale.ROOT);
            String name = entry.name().trim();
            String version = normaliseVersion(entry.version());

            if (!NAME.matcher(name).matches()) {
                reporter.report(entry, STATE_FAILED, null, null, null,
                        "invalid package name '" + name + "' (rejected by the arg-safety validator)");
                return;
            }
            if (!version.isEmpty() && !VERSION.matcher(version).matches()) {
                reporter.report(entry, STATE_FAILED, null, null, null,
                        "invalid version '" + entry.version() + "' — only an exact version or blank "
                                + "(latest) is installable");
                return;
            }

            switch (lang) {
                case "python", "py", "python3" -> installPython(entry, key, name, version);
                case "javascript", "js", "node", "nodejs", "typescript", "ts" ->
                        installNode(entry, key, name, version);
                default -> reporter.report(entry, STATE_FAILED, null, null, null,
                        "no installer for language '" + lang + "' on this daemon "
                                + "(supported: python, javascript, typescript)");
            }
        } catch (Exception e) {
            log.warn("deps: install failed name={} : {}", entry.name(), e.toString());
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "install error: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
    }

    /* ------------------------------------------------------------------ */
    /* On-disk dependency-cache discovery. */
    /* ------------------------------------------------------------------ */

    /**
     * Submit the one-shot cache discovery to the background pool when (a) it has not run yet and (b)
     * the report channel is live. Called from {@link #reconcile} so the FIRST reconcile after
     * enrolment (the control plane re-pushes {@code set-deps} on enrol) triggers it — never inline on
     * the heartbeat command thread.
     */
    private void maybeDiscoverAsync() {
        if (discoveryDone.get() || !reporter.isChannelReady()) {
            return;
        }
        try {
            executor.execute(this::discoverCachedDeps);
        } catch (RejectedExecutionException ignore) {
            // Pool saturated / shutting down — a later reconcile retries (guard not yet flipped).
        }
    }

    /**
     * Scan {@code sqs.deps.root/<lang>} for packages already present in the cache and report each to
     * the BE as {@code installed} with the discovered marker, so preloaded / pre-provisioned deps show
     * up in the FE library even though the manifest never listed them. Runs at most ONCE per process
     * (guarded) and never throws.
     */
    void discoverCachedDeps() {
        if (!discoveryDone.compareAndSet(false, true)) {
            return;
        }
        try {
            int found = scanPython() + scanNode();
            log.info("deps: cache discovery reported {} cached package(s) from {}", found, config.targetRoot());
        } catch (Exception e) {
            log.debug("deps: cache discovery failed: {}", e.toString());
        }
    }

    /** pip {@code --target} layout: each {@code <name>-<version>.dist-info} dir is one installed package. */
    private int scanPython() {
        Path dir = config.targetRoot().resolve("python");
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        int n = 0;
        try (var stream = Files.list(dir)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                String d = p.getFileName().toString();
                if (!d.endsWith(".dist-info") || !Files.isDirectory(p)) {
                    continue;
                }
                String base = d.substring(0, d.length() - ".dist-info".length());
                int dash = base.lastIndexOf('-');
                if (dash <= 0 || dash == base.length() - 1) {
                    continue;
                }
                String name = pep503(base.substring(0, dash));
                String version = base.substring(dash + 1);
                Path record = p.resolve("RECORD");
                if (!Files.isRegularFile(record)) {
                    // pip writes RECORD last, so a dist-info WITHOUT it is a partial/failed install.
                    // Don't claim it as "installed" — that was the "shows installed but isn't" bug.
                    log.debug("deps: skipping partial cached python package (no RECORD): {}", d);
                    continue;
                }
                String digest = null;
                Long bytes = null;
                try {
                    byte[] rec = Files.readAllBytes(record);
                    digest = sha256Hex(rec);
                    bytes = recordBytes(rec);
                } catch (IOException ignore) {
                    // best-effort metadata only — the RECORD presence already proved a complete install
                }
                if (reportDiscovered("python", name, version, digest, bytes)) {
                    n++;
                }
            }
        } catch (IOException e) {
            log.debug("deps: python cache scan failed dir={}: {}", dir, e.toString());
        }
        return n;
    }

    /** npm {@code --prefix} layout: each {@code node_modules/<pkg>/package.json} (incl. {@code @scope/pkg}). */
    private int scanNode() {
        Path nm = config.targetRoot().resolve("node").resolve("node_modules");
        if (!Files.isDirectory(nm)) {
            return 0;
        }
        int n = 0;
        try (var stream = Files.list(nm)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                String d = p.getFileName().toString();
                if (d.startsWith(".") || !Files.isDirectory(p)) {
                    continue; // .bin, .package-lock.json, etc.
                }
                if (d.startsWith("@")) {
                    try (var scoped = Files.list(p)) {
                        for (Path q : (Iterable<Path>) scoped::iterator) {
                            if (Files.isDirectory(q) && reportNodePackage(q)) {
                                n++;
                            }
                        }
                    }
                } else if (reportNodePackage(p)) {
                    n++;
                }
            }
        } catch (IOException e) {
            log.debug("deps: node cache scan failed dir={}: {}", nm, e.toString());
        }
        return n;
    }

    /** Reads {@code package.json} name+version and reports it discovered; false when unreadable. */
    private boolean reportNodePackage(Path pkgDir) {
        Path pj = pkgDir.resolve("package.json");
        if (!Files.isRegularFile(pj)) {
            return false;
        }
        try {
            String json = Files.readString(pj, StandardCharsets.UTF_8);
            String name = jsonStringField(json, "name");
            String version = jsonStringField(json, "version");
            if (name == null || name.isBlank()) {
                return false;
            }
            return reportDiscovered("javascript", name.trim(), version == null ? "" : version.trim(), null, null);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Report one discovered package: cross-reference the live manifest for a preload flag, mark the
     * tuple installed locally so a later {@code set-deps} push won't reinstall it, and echo it to the BE.
     */
    private boolean reportDiscovered(String lang, String name, String version, String digest, Long bytes) {
        if (name == null || name.isBlank()) {
            return false;
        }
        Boolean preload = preloadFromHolder(lang, name);
        DependencyEntry entry = new DependencyEntry(lang, name, version, STATE_INSTALLED, preload,
                null, digest, null, bytes);
        installedKeys.putIfAbsent(key(entry), digest == null ? fallbackDigest(name, version) : digest);
        return reporter.reportDiscovered(entry, version, digest, bytes, preload);
    }

    /** Best-effort preload flag from the live manifest for a {@code (language,name)} — null when absent. */
    private Boolean preloadFromHolder(String lang, String name) {
        if (holder == null) {
            return null;
        }
        String wantLang = lang == null ? "" : lang.trim().toLowerCase(Locale.ROOT);
        String wantName = pep503(name);
        for (DependencyEntry e : holder.current()) {
            if (e == null || e.name() == null) {
                continue;
            }
            String el = e.language() == null ? "" : e.language().trim().toLowerCase(Locale.ROOT);
            if (el.equals(wantLang) && pep503(e.name()).equals(wantName) && Boolean.TRUE.equals(e.preload())) {
                return Boolean.TRUE;
            }
        }
        return null;
    }

    /** Extracts a top-level JSON string field value via a tolerant regex; null when absent. */
    static String jsonStringField(String json, String field) {
        if (json == null || field == null) {
            return null;
        }
        Matcher m = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"")
                .matcher(json);
        if (m.find()) {
            return m.group(1).replace("\\\"", "\"").replace("\\/", "/").replace("\\\\", "\\");
        }
        return null;
    }

    /* ------------------------------------------------------------------ */
    /* Python (pip)                                                       */
    /* ------------------------------------------------------------------ */

    private void installPython(DependencyEntry entry, String key, String name, String version) {
        Path target = config.targetRoot().resolve("python");
        try {
            Files.createDirectories(target);
        } catch (IOException e) {
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "could not create dep target dir: " + e.getMessage());
            return;
        }
        if (!containerInstallAvailable()) {
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "dependency installation requires the container runtime");
            return;
        }
        if (!ensureInstallNetwork(entry)) {
            return;
        }
        String cname = registerContainerName(key);
        Map<String, String> env = installEnv(true);
        List<String> argv = buildContainerPipArgv(containerRuntime.cli(), imageFor(SandboxLanguage.PYTHON),
                target.toString(), name, version, cname, env);
        log.info("deps: installing python name={} version={} mode=container", name,
                version.isEmpty() ? "latest" : version);
        CliRunner.Result result = runner.run(argv, config.installTimeout(), env);
        inFlightContainer.remove(key);
        finishInstall(entry, key, name, version, target, result, /* python */ true);
    }

    /* ------------------------------------------------------------------ */
    /* JavaScript / TypeScript (npm)                                      */
    /* ------------------------------------------------------------------ */

    private void installNode(DependencyEntry entry, String key, String name, String version) {
        Path target = config.targetRoot().resolve("node");
        try {
            Files.createDirectories(target);
        } catch (IOException e) {
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "could not create dep target dir: " + e.getMessage());
            return;
        }
        if (!containerInstallAvailable()) {
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "dependency installation requires the container runtime");
            return;
        }
        if (!ensureInstallNetwork(entry)) {
            return;
        }
        String cname = registerContainerName(key);
        Map<String, String> env = installEnv(false);
        List<String> argv = buildContainerNpmArgv(containerRuntime.cli(), imageFor(SandboxLanguage.JAVASCRIPT),
                target.toString(), name, version, cname, env);
        log.info("deps: installing node name={} version={} mode=container", name,
                version.isEmpty() ? "latest" : version);
        CliRunner.Result result = runner.run(argv, config.installTimeout(), env);
        inFlightContainer.remove(key);
        finishInstall(entry, key, name, version, target, result, /* python */ false);
    }

    /* ------------------------------------------------------------------ */
    /* Container-mode install (linux wheels; no host toolchain)           */
    /* ------------------------------------------------------------------ */

    private boolean containerInstallAvailable() {
        return containerRuntime != null && containerRuntime.available()
                && containerRuntime.cli() != null && !containerRuntime.cli().isBlank();
    }

    /** Sandbox image for the language (matches the runtime the packages must load in). */
    private String imageFor(SandboxLanguage lang) {
        String override = config.installEnabled()
                ? System.getProperty("sqs.deps.image." + lang.wire()) : null;
        return override != null && !override.isBlank() ? override : lang.defaultImage();
    }

    /** A deterministic, filesystem/CLI-safe container name for a dep key so a cancel can kill it. */
    private String registerContainerName(String key) {
        String safe = key.replaceAll("[^A-Za-z0-9_.-]", "_");
        if (safe.length() > 80) {
            safe = safe.substring(0, 80);
        }
        String name = "sq-depinstall-" + safe + "-" + Long.toHexString(System.nanoTime());
        inFlightContainer.put(key, name);
        return name;
    }

    /**
     * HOME stays {@code /tmp} so pip/npm caches do not land in the image's home. Not
     * {@code — read-only} and not {@code — user}: the {@code /target} bind must be writable by the
     * image's default user.
     */
    private Map<String, String> installEnv(boolean pipCache) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("HOME", "/tmp");
        if (pipCache) {
            env.put("PIP_CACHE_DIR", "/tmp/.pipcache");
        }
        return env;
    }

    private boolean ensureInstallNetwork(DependencyEntry entry) {
        if (!ensureBuildNetwork) {
            return true;
        }
        try {
            SandboxNetworkEnsure.ensure(containerRuntime.cli(), SandboxNetworks.BUILD);
            return true;
        } catch (RuntimeException e) {
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "dependency installation requires the container build network (sqs-build)");
            return false;
        }
    }

    /**
     * Hardened {@code podman run} on {@code sqs-build} (not the default bridge).
     * Caps match the job sandbox shape: cap-drop, no-new-privileges, seccomp under
     * the same required/optional policy, pids, memory with swap disabled, cpus.
     */
    private void appendInstallHardening(List<String> argv, Map<String, String> env) {
        argv.add("--network");
        argv.add(SandboxNetworks.BUILD);
        for (String dns : installDns) {
            argv.add("--dns");
            argv.add(dns);
        }
        argv.add("--cap-drop");
        argv.add("ALL");
        argv.add("--security-opt");
        argv.add("no-new-privileges");
        boolean seccompRequired = containerRuntime != null && containerRuntime.isolationEnforced();
        String seccomp = SeccompProfile.profilePath(seccompRequired);
        if (seccomp != null && !seccomp.isBlank()) {
            argv.add("--security-opt");
            argv.add("seccomp=" + seccomp);
        } else if (seccompRequired) {
            throw new IllegalStateException(
                    "dependency install: seccomp profile required but unavailable");
        }
        // A controller the host proved it cannot apply is left out (CgroupCaps).
        if (CgroupCaps.applies(CgroupCaps.Controller.PIDS)) {
            argv.add("--pids-limit");
            argv.add(Integer.toString(installPids));
        }
        if (CgroupCaps.applies(CgroupCaps.Controller.MEMORY)) {
            argv.add("--memory");
            argv.add(installMemoryMb + "m");
            argv.add("--memory-swap");
            argv.add(installMemoryMb + "m");
        }
        if (CgroupCaps.applies(CgroupCaps.Controller.CPU)) {
            argv.add("--cpus");
            argv.add(installCpus == Math.floor(installCpus)
                    ? Integer.toString((int) installCpus)
                    : Double.toString(installCpus));
        }
        ContainerEnv.appendNameOnly(argv, env);
    }

    /**
     * Pip inside the sandbox image. The spec stays after {@code --} so a hostile
     * name cannot be read as a pip option.
     */
    List<String> buildContainerPipArgv(String cli, String image, String targetHostDir,
                                       String name, String version, String containerName,
                                       Map<String, String> env) {
        List<String> argv = new ArrayList<>();
        argv.add(cli);
        argv.add("run");
        argv.add("--rm");
        argv.add("--name");
        argv.add(containerName);
        appendInstallHardening(argv, env);
        argv.add("-v");
        argv.add(targetHostDir + ":/target:rw");
        argv.add(image);
        argv.add("python");
        argv.add("-m");
        argv.add("pip");
        argv.add("install");
        argv.add("--no-input");
        argv.add("--disable-pip-version-check");
        argv.add("--no-warn-script-location");
        argv.add("--prefer-binary");
        argv.add("--target");
        argv.add("/target");
        if (version == null || version.isEmpty()) {
            argv.add("--upgrade");
        }
        argv.add("--");
        argv.add(spec(name, version, "=="));
        return argv;
    }

    /** npm inside the sandbox image. The spec stays after {@code --}. */
    List<String> buildContainerNpmArgv(String cli, String image, String targetHostDir,
                                       String name, String version, String containerName,
                                       Map<String, String> env) {
        List<String> argv = new ArrayList<>();
        argv.add(cli);
        argv.add("run");
        argv.add("--rm");
        argv.add("--name");
        argv.add(containerName);
        appendInstallHardening(argv, env);
        argv.add("-v");
        argv.add(targetHostDir + ":/target:rw");
        argv.add(image);
        argv.add("npm");
        argv.add("install");
        argv.add("--prefix");
        argv.add("/target");
        argv.add("--no-audit");
        argv.add("--no-fund");
        argv.add("--save=false");
        argv.add("--");
        argv.add(spec(name, version, "@"));
        return argv;
    }

    /** Force-kill an in-flight install container for a removed (cancelled) dep key. Best-effort. */
    private void cancelInFlight(String key) {
        String cname = inFlightContainer.remove(key);
        if (cname == null || !containerInstallAvailable()) {
            return;
        }
        try {
            CliRunner.run(List.of(containerRuntime.cli(), "rm", "-f", cname), Duration.ofSeconds(15));
            log.info("deps: cancelled in-flight install container={} (dep removed from manifest)", cname);
        } catch (Exception e) {
            log.debug("deps: cancel of {} failed (non-fatal): {}", cname, e.getMessage());
        }
    }

    /** Common terminal handling: map a {@link CliRunner.Result} to an installed/failed report. */
    private void finishInstall(DependencyEntry entry, String key, String name, String version,
                               Path target, CliRunner.Result result, boolean python) {
        if (result == null) {
            reporter.report(entry, STATE_FAILED, null, null, null, "installer produced no result");
            return;
        }
        if (result.timedOut()) {
            log.warn("deps: install TIMED OUT name={} version={} after {}ms (raise sqs.deps.install-timeout-ms "
                    + "or remove the dep to cancel)", name, version, config.installTimeout().toMillis());
            reporter.report(entry, STATE_FAILED, null, null, null,
                    "install timed out after " + config.installTimeout().toMillis() + "ms");
            return;
        }
        if (!result.ok()) {
            // Log the full diagnostic on the daemon so operators can see WHY it failed (the BE row only
            // carries a bounded tail). stdout+stderr both, since pip writes resolver errors to stdout.
            log.warn("deps: install FAILED name={} version={} exit={}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    name, version, result.exitCode(), result.stdout(), result.stderr());
            reporter.report(entry, STATE_FAILED, null, null, null, tail(result.stderr(), result.stdout()));
            return;
        }
        // Success — resolve the concrete version + a content digest for the row.
        String resolved = parseInstalledVersion(result.stdout(), name);
        Installed info = python ? resolveInstalledPython(target, name) : null;
        if (info != null && info.version() != null && !info.version().isBlank()) {
            resolved = info.version();
        }
        if ((resolved == null || resolved.isBlank()) && !version.isEmpty()) {
            resolved = version;
        }
        String digest = info != null && info.digest() != null
                ? info.digest()
                : fallbackDigest(name, resolved == null ? version : resolved);
        Long bytes = info == null ? null : info.bytes();
        // The sandbox container runs as uid 65534 (nobody) and mounts this tree read-only. pip/npm write
        // with the daemon umask, so a restrictive umask leaves the packages unreadable to the container
        // user (imports then fail even though the dir is mounted).
        ensureWorldReadable(target);
        // Remember the tuple only after the BE accepts the echo. A dropped POST would otherwise
        // leave the row queued forever: the next set-deps would see installedKeys and skip it.
        boolean echoed = reporter.report(entry, STATE_INSTALLED, resolved, digest, bytes,
                resolved == null || resolved.isBlank() ? null : "resolved " + resolved);
        if (echoed) {
            installedKeys.put(key, digest);
        } else {
            log.warn("deps: {} installed on disk but the status echo was not accepted; "
                    + "the next manifest push will retry", name);
        }
    }

    /**
     * Recursively grants world read + directory-traverse on the installed dep tree so the non-root
     * sandbox container user (uid 65534) can read the RO-mounted packages. POSIX only — a no-op on
     * Windows (no Podman sandbox there).
     */
    private void ensureWorldReadable(Path target) {
        if (target == null) {
            return;
        }
        String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
        if (os.contains("win")) {
            return;
        }
        try {
            CliRunner.Result r = CliRunner.run(
                    List.of("chmod", "-R", "a+rX", target.toString()), Duration.ofSeconds(60));
            if (r != null && !r.ok()) {
                log.debug("deps: chmod a+rX {} exited {} (non-fatal)", target, r.exitCode());
            }
        } catch (Exception e) {
            log.debug("deps: chmod a+rX {} failed (non-fatal): {}", target, e.getMessage());
        }
    }

    /* ------------------------------------------------------------------ */
    /* Pure helpers (unit-tested directly)                                */
    /* ------------------------------------------------------------------ */

    /** {@code name<sep>version} for a pinned install, or bare {@code name} for latest. */
    private static String spec(String name, String version, String sep) {
        return (version == null || version.isEmpty()) ? name : name + sep + version;
    }

    /** Strips a leading {@code ==} convenience prefix and trims; blank means "latest". */
    static String normaliseVersion(String version) {
        if (version == null) {
            return "";
        }
        String v = version.trim();
        if (v.startsWith("==")) {
            v = v.substring(2).trim();
        }
        return v;
    }

    /**
     * Parses pip's {@code Successfully installed name-x.y.z dep-a.b.c} line for the concrete version
     * of {@code name}, tolerant of hyphenated project names (the version is the segment after the
     * LAST hyphen). Returns {@code null} when absent (e.g. a "Requirement already satisfied" re-run).
     */
    static String parseInstalledVersion(String stdout, String name) {
        if (stdout == null || name == null) {
            return null;
        }
        String want = pep503(name);
        for (String line : stdout.split("\\R")) {
            int idx = line.indexOf("Successfully installed");
            if (idx < 0) {
                continue;
            }
            for (String token : line.substring(idx).replace("Successfully installed", "").trim().split("\\s+")) {
                int dash = token.lastIndexOf('-');
                if (dash <= 0 || dash == token.length() - 1) {
                    continue;
                }
                if (pep503(token.substring(0, dash)).equals(want)) {
                    return token.substring(dash + 1);
                }
            }
        }
        return null;
    }

    /** PEP 503 normalisation: lowercase, collapse runs of {@code -_.} to a single {@code -}, drop extras. */
    static String pep503(String name) {
        if (name == null) {
            return "";
        }
        String n = name.trim().toLowerCase(Locale.ROOT);
        int bracket = n.indexOf('[');
        if (bracket >= 0) {
            n = n.substring(0, bracket);
        }
        int slash = n.lastIndexOf('/'); // npm scope
        if (slash >= 0) {
            n = n.substring(slash + 1);
        }
        return n.replaceAll("[-_.]+", "-");
    }

    /** Deterministic fallback digest when no dist-info RECORD is available. */
    static String fallbackDigest(String name, String version) {
        return sha256Hex((pep503(name) + "==" + (version == null ? "" : version)).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Best-effort content digest + size from the installed {@code *.dist-info/RECORD} (which lists
     * every installed file and its own hash — hashing it is effectively a content digest of the
     * install). Returns {@code null} when the dist-info is not found; never throws.
     */
    private Installed resolveInstalledPython(Path target, String name) {
        try {
            if (target == null || !Files.isDirectory(target)) {
                return null;
            }
            String want = pep503(name);
            try (var stream = Files.list(target)) {
                for (Path p : (Iterable<Path>) stream::iterator) {
                    String dir = p.getFileName().toString();
                    if (!dir.endsWith(".dist-info") || !Files.isDirectory(p)) {
                        continue;
                    }
                    String base = dir.substring(0, dir.length() - ".dist-info".length());
                    int dash = base.lastIndexOf('-');
                    if (dash <= 0) {
                        continue;
                    }
                    if (!pep503(base.substring(0, dash)).equals(want)) {
                        continue;
                    }
                    String version = base.substring(dash + 1);
                    Path record = p.resolve("RECORD");
                    if (Files.isRegularFile(record)) {
                        byte[] bytes = Files.readAllBytes(record);
                        return new Installed(version, sha256Hex(bytes), recordBytes(bytes));
                    }
                    return new Installed(version, fallbackDigest(name, version), null);
                }
            }
        } catch (Exception e) {
            log.debug("deps: dist-info resolve failed name={}: {}", name, e.toString());
        }
        return null;
    }

    /** Sums the size column (3rd CSV field) of a pip RECORD file; null when unparseable. */
    private static Long recordBytes(byte[] record) {
        try {
            long total = 0;
            for (String line : new String(record, StandardCharsets.UTF_8).split("\\R")) {
                String[] cols = line.split(",");
                if (cols.length >= 3 && !cols[2].isBlank()) {
                    try {
                        total += Long.parseLong(cols[2].trim());
                    } catch (NumberFormatException ignore) {
                        // header / non-numeric row
                    }
                }
            }
            return total > 0 ? total : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder sb = new StringBuilder("sha256:");
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            return "sha256:unknown";
        }
    }

    /** Last {@link #MAX_MESSAGE_CHARS} chars of stderr (or stdout when stderr is empty). */
    private static String tail(String stderr, String stdout) {
        String src = stderr != null && !stderr.isBlank() ? stderr : (stdout == null ? "" : stdout);
        src = src.strip();
        if (src.isEmpty()) {
            return "install failed (no diagnostic output)";
        }
        return src.length() > MAX_MESSAGE_CHARS ? src.substring(src.length() - MAX_MESSAGE_CHARS) : src;
    }

    private static String key(DependencyEntry e) {
        String l = e.language() == null ? "" : e.language().trim().toLowerCase(Locale.ROOT);
        String n = e.name() == null ? "" : e.name().trim();
        String v = normaliseVersion(e.version());
        return l + "\u0000" + n + "\u0000" + v;
    }

    private static ExecutorService defaultExecutor(int maxParallel) {
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "deps-installer");
            t.setDaemon(true);
            return t;
        };
        return Executors.newFixedThreadPool(Math.max(1, maxParallel), factory);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }

    /** The interpreter/exec seam so tests drive the state machine without spawning a real process. */
    @FunctionalInterface
    interface InstallRunner {
        CliRunner.Result run(List<String> argv, Duration timeout, Map<String, String> processEnv);
    }

    /** Resolved install metadata (version + content digest + size). */
    record Installed(String version, String digest, Long bytes) {
    }

    /** Installer configuration resolved from {@code sqs.deps.*} (Environment-bound, no extra beans). */
    record DepsConfig(boolean installEnabled, String python, String npm, Path targetRoot,
                      Duration installTimeout, EgressPolicy egress, int maxParallel) {

        static DepsConfig fromEnvironment(Environment env) {
            boolean enabled = env.getProperty("sqs.deps.install.enabled", Boolean.class, true);
            String python = env.getProperty("sqs.deps.python", "python3");
            String npm = env.getProperty("sqs.deps.npm", "npm");
            String root = env.getProperty("sqs.deps.root", "./data/deps");
            // Container-mode installs pull prebuilt wheels so they're fast, but a large tree over a slow
            // link can still run long; default to a generous 30min ceiling (an operator can cancel by
            // removing the row, which force-kills the install container). Still overridable.
            long timeoutMs = env.getProperty("sqs.deps.install-timeout-ms", Long.class, 1_800_000L);
            EgressPolicy egress = EgressPolicy.fromWire(env.getProperty("sqs.deps.egress", "open"));
            int parallel = env.getProperty("sqs.deps.max-parallel", Integer.class, 2);
            return new DepsConfig(enabled, python, npm, Path.of(root),
                    Duration.ofMillis(timeoutMs), egress, parallel);
        }

    }
}
