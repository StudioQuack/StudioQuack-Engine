package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Idempotently ensures a dedicated Podman network exists before containers attach to it. Networks
 * are created with netavark {@code isolate=true} so sibling containers on the same bridge cannot
 * reach each other at L2 (callbacks use the SQ_HELPER_SOCKET broker; cross-instance reachability
 * goes through the daemon relay {@code studioquack.connect}).
 */
public final class SandboxNetworkEnsure {

    private static final Logger log = LoggerFactory.getLogger(SandboxNetworkEnsure.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** Rootful CNI conflist directory (the only one podman-as-root writes). */
    private static final String CNI_CONFLIST_DIR_ROOTFUL = "/etc/cni/net.d";

    /**
     * Matches {@code "cniVersion": "1.0.0"} in a conflist. Ubuntu 22.04 ships podman 3.4.x
     * (writes 1.0.0 into new-network conflists) together with containernetworking-plugins 0.9.x
     * whose firewall plugin implements CNI spec ≤ 0.4.0 — every container attach then fails with
     * {@code plugin firewall does not support config version "1.0.0"}.
     */
    private static final Pattern CNI_VERSION_1_0 =
            Pattern.compile("\"cniVersion\"\\s*:\\s*\"1\\.0\\.0\"");

    /** Go-template: first subnet/gateway pair as {@code CIDR;gateway}. */
    static final String SUBNET_FORMAT = "{{range .Subnets}}{{.Subnet}};{{.Gateway}}{{end}}";

    /** Go-template: netavark isolate option ({@code true} when siblings are L2-isolated). */
    static final String ISOLATE_FORMAT = "{{index .Options \"isolate\"}}";

    /** Go-template: the network's dns_enabled flag ({@code false} once created --disable-dns). */
    static final String DNS_ENABLED_FORMAT = "{{.DNSEnabled}}";

    /** Process-local cache of networks confirmed <b>isolated</b> (avoids a CLI round-trip per spawn). */
    private static final Set<String> ENSURED = ConcurrentHashMap.newKeySet();

    /**
     * Process-local cache of networks created WITHOUT {@code isolate}/{@code --disable-dns} because
     * the host podman predates netavark (CNI era). Cached separately so the per-spawn ensure does
     * not re-attempt the unsupported flags on every container start.
     */
    private static final Set<String> ENSURED_DEGRADED = ConcurrentHashMap.newKeySet();

    /**
     * Networks confirmed AND brought under the host firewall ({@link #cover}) — the only ones a
     * spawn may skip straight past. {@link #ENSURED} fills while a network is being confirmed,
     * before it is covered, so a concurrent spawn must never read that cache as "ready".
     */
    private static final Set<String> READY = ConcurrentHashMap.newKeySet();

    /** One ensure per network at a time (one lock per sandbox network name). */
    private static final ConcurrentHashMap<String, Object> LOCKS = new ConcurrentHashMap<>();

    /**
     * Brings a sandbox network under the host firewall before any container attaches to it — the
     * egress manager installs one. Process-wide like {@link #ENSURED}; absent in slice tests and on
     * hosts without the manager.
     */
    @FunctionalInterface
    public interface CoverageHook {
        /**
         * @throws IllegalStateException When {@code networkName} could not be brought under the rules.
         */
        void cover(String networkName);
    }

    private static volatile CoverageHook coverageHook;

    private SandboxNetworkEnsure() {
    }

    /** Installs the process's {@link CoverageHook}; {@code null} removes it. */
    public static void setCoverageHook(CoverageHook hook) {
        coverageHook = hook;
    }

    /**
     * Discovered addressing + isolation/DNS posture for one Podman network.
     *
     * @param name Network name.
     * @param subnetCidr First subnet CIDR, or {@code null} when absent/unparseable.
     * @param gatewayIp First gateway IP, or {@code null} when absent/unparseable.
     */
    public record SandboxNetworkInfo(String name, String subnetCidr, String gatewayIp,
                                     boolean isolated, boolean dnsDisabled) {
    }

    /**
     * Outcome of one CLI invocation. Kept tiny so callers can adapt {@link CliRunner} or
     * {@code OciCli} without this class depending on the stateful package.
     *
     * <p>{@code stdout} carries {@code --format} probe output for {@link #inspect}; adapters that
     * omit it (3-arg constructor) leave it empty.
     */
    public record ExecResult(boolean ok, int exitCode, String stdout, String stderr) {
        /** Compatibility for adapters that only forward exit + stderr. */
        public ExecResult(boolean ok, int exitCode, String stderr) {
            this(ok, exitCode, "", stderr);
        }
    }

    /** Runs {@code argv} under {@code timeout}; never throws. */
    @FunctionalInterface
    public interface Exec {
        ExecResult run(List<String> argv, Duration timeout);
    }

    /** Ensures {@link SandboxNetworks#JOBS} via {@link CliRunner} (warm / cold path). */
    public static void ensureJobs(String cliPath) {
        ensure(cliPath, SandboxNetworks.JOBS);
    }

    /** Ensures {@code networkName} via {@link CliRunner}. */
    public static void ensure(String cliPath, String networkName) {
        ensure(cliPath, networkName, (argv, timeout) -> {
            CliRunner.Result r = CliRunner.run(argv, timeout);
            return new ExecResult(r.ok(), r.exitCode(), r.stdout(), r.stderr());
        });
    }

    /**
     * Ensures {@code networkName} using a caller-supplied exec (stateful path drives {@code OciCli} so
     * unit tests stay on {@code FakeOciCli}). When the network already exists but is not isolated,
     * attempts {@code network rm} then recreate with {@code --opt isolate=true}.
     *
     * @throws IllegalStateException When the network cannot be confirmed or created, or cannot be.
     */
    public static void ensure(String cliPath, String networkName, Exec exec) {
        if (cliPath == null || cliPath.isBlank()) {
            throw new IllegalStateException("cannot ensure podman network: cli path is blank");
        }
        if (networkName == null || networkName.isBlank()) {
            throw new IllegalStateException("cannot ensure podman network: name is blank");
        }
        if (exec == null) {
            throw new IllegalStateException("cannot ensure podman network: exec is null");
        }
        if (READY.contains(networkName)) {
            return;
        }
        synchronized (LOCKS.computeIfAbsent(networkName, name -> new Object())) {
            if (READY.contains(networkName)) {
                return;
            }
            if (!ENSURED.contains(networkName) && !ENSURED_DEGRADED.contains(networkName)) {
                confirm(cliPath, networkName, exec);
            }
            cover(networkName);
            if (ENSURED.contains(networkName) || ENSURED_DEGRADED.contains(networkName)) {
                READY.add(networkName);
            }
        }
    }

    /**
     * Ensures {@code networkName} exists (clearing any stale process-local cache if the runtime
     * was reset) WITHOUT invoking {@link CoverageHook} or marking {@link #READY}. Used by
     * {@code SandboxEgressManager.discoverNetworks} so the Podman anchor pod can attach to
     * {@code sqs-jobs} before the host firewall rules are applied — avoiding recursion into
     * {@code coverSandboxNetwork}.
     */
    public static void ensureWithoutCoverage(String cliPath, String networkName, Exec exec) {
        if (cliPath == null || cliPath.isBlank()) {
            throw new IllegalStateException("cannot ensure podman network: cli path is blank");
        }
        if (networkName == null || networkName.isBlank()) {
            throw new IllegalStateException("cannot ensure podman network: name is blank");
        }
        if (exec == null) {
            throw new IllegalStateException("cannot ensure podman network: exec is null");
        }
        synchronized (LOCKS.computeIfAbsent(networkName, name -> new Object())) {
            READY.remove(networkName);
            ENSURED.remove(networkName);
            ENSURED_DEGRADED.remove(networkName);
            confirm(cliPath, networkName, exec);
        }
    }

    /** Creates or adopts {@code networkName}; caches it once confirmed isolated (or degraded). */
    private static void confirm(String cliPath, String networkName, Exec exec) {
        // podman network exists <name> → exit 0 when present.
        ExecResult exists = exec.run(List.of(cliPath, "network", "exists", networkName), TIMEOUT);
        if (exists.ok()) {
            adoptExisting(cliPath, networkName, exec);
            return;
        }
        ExecResult created = exec.run(
                List.of(cliPath, "network", "create", "--opt", "isolate=true", "--disable-dns",
                        networkName),
                TIMEOUT);
        if (created.ok()) {
            ENSURED.add(networkName);
            return;
        }
        // Race: another process created the network between exists and create.
        if (alreadyExists(created)) {
            adoptExisting(cliPath, networkName, exec);
            return;
        }
        // Pre-netavark podman (CNI, e.g. Ubuntu 22.04's 3.4.x) rejects isolate/--disable-dns —
        // degrade to a plain network rather than killing the whole sandbox/Docling path.
        if (unsupportedOption(created)) {
            createPlain(cliPath, networkName, exec, created);
            return;
        }
        String detail = firstLine(created.stderr());
        if (detail.isEmpty()) {
            detail = "exit=" + created.exitCode();
        }
        throw new IllegalStateException(
                "failed to ensure podman network '" + networkName + "': " + detail);
    }

    /**
     * A network takes containers only once the host firewall filters it: the rules are built from
     * the networks present at overlay bring-up, and this one may have just been created. Throws
     * when the hook could not cover it; the network then stays out of {@link #READY}, so the next
     * ensure tries again.
     */
    private static void cover(String networkName) {
        CoverageHook hook = coverageHook;
        if (hook != null) {
            hook.cover(networkName);
        }
    }

    /**
     * Plain {@code network create} for pre-netavark podman; on success the network is cached in
     * {@link #ENSURED_DEGRADED} (never {@link #ENSURED} — it is NOT isolated). Throws with the
     * plain-create detail when that fails too, falling back to the original error when it is more
     * informative (e.g. the plain create raced another process).
     */
    private static void createPlain(String cliPath, String networkName, Exec exec,
                                    ExecResult originalFailure) {
        ExecResult plain = exec.run(List.of(cliPath, "network", "create", networkName), TIMEOUT);
        if (plain.ok()) {
            // Ubuntu 22.04 ships podman 3.4.x writing cniVersion 1.0.0 conflists while its CNI
            // plugins only speak ≤ 0.4.0 — fix the conflist or no container can ever attach.
            downgradeCniConflistVersion(
                    Path.of(CNI_CONFLIST_DIR_ROOTFUL, networkName + ".conflist"));
            String home = System.getProperty("user.home");
            if (home != null && !home.isBlank()) {
                downgradeCniConflistVersion(
                        Path.of(home, ".config", "cni", "net.d", networkName + ".conflist"));
            }
            ENSURED_DEGRADED.add(networkName);
            log.warn("sandbox-network: '{}' created WITHOUT isolate/--disable-dns — host podman "
                    + "predates netavark ({}); sibling L2 isolation is OFF for this network, "
                    + "T-17 --dns pins still apply (CNI does not hijack resolv.conf)",
                    networkName, firstLine(originalFailure.stderr()));
            return;
        }
        if (alreadyExists(plain)) {
            adoptExisting(cliPath, networkName, exec);
            return;
        }
        String detail = firstLine(plain.stderr());
        if (detail.isEmpty()) {
            detail = "exit=" + plain.exitCode();
        }
        throw new IllegalStateException(
                "failed to ensure podman network '" + networkName + "': " + detail);
    }

    /**
     * Discovers subnet/gateway/isolate for {@code networkName}.
     *
     * @return info record, or {@code null} when the network is absent or output is unparseable
     */
    public static SandboxNetworkInfo inspect(String cliPath, String networkName, Exec exec) {
        if (cliPath == null || cliPath.isBlank() || networkName == null || networkName.isBlank()
                || exec == null) {
            return null;
        }
        ExecResult subnetProbe = exec.run(
                List.of(cliPath, "network", "inspect", networkName, "--format", SUBNET_FORMAT),
                TIMEOUT);
        if (subnetProbe == null || !subnetProbe.ok()) {
            return null;
        }
        String subnetOut = subnetProbe.stdout() == null ? "" : subnetProbe.stdout().trim();
        String subnetCidr = null;
        String gatewayIp = null;
        if (!subnetOut.isEmpty()) {
            int semi = subnetOut.indexOf(';');
            if (semi <= 0) {
                return null;
            }
            subnetCidr = subnetOut.substring(0, semi).trim();
            String rest = subnetOut.substring(semi + 1).trim();
            // First pair only: gateway ends at ';' (glued next pair) or whitespace.
            int end = rest.length();
            for (int i = 0; i < rest.length(); i++) {
                char c = rest.charAt(i);
                if (c == ';' || Character.isWhitespace(c)) {
                    end = i;
                    break;
                }
            }
            gatewayIp = rest.substring(0, end).trim();
            if (subnetCidr.isEmpty() || gatewayIp.isEmpty()) {
                return null;
            }
        }

        boolean isolated = false;
        ExecResult isolateProbe = exec.run(
                List.of(cliPath, "network", "inspect", networkName, "--format", ISOLATE_FORMAT),
                TIMEOUT);
        // Older podman may error on Options index — fall back to isolated=false so migration runs.
        if (isolateProbe != null && isolateProbe.ok()) {
            String iso = isolateProbe.stdout() == null ? "" : isolateProbe.stdout().trim();
            isolated = "true".equals(iso);
        }

        // Same fail-safe default: an unparseable/absent DNSEnabled means not disabled, so the migration
        // path runs rather than silently keeping the aardvark resolver.
        boolean dnsDisabled = false;
        ExecResult dnsProbe = exec.run(
                List.of(cliPath, "network", "inspect", networkName, "--format", DNS_ENABLED_FORMAT),
                TIMEOUT);
        if (dnsProbe != null && dnsProbe.ok()) {
            String dns = dnsProbe.stdout() == null ? "" : dnsProbe.stdout().trim();
            dnsDisabled = "false".equals(dns);
        }

        return new SandboxNetworkInfo(networkName, subnetCidr, gatewayIp, isolated, dnsDisabled);
    }

    /**
     * Existing network: cache if already isolated AND dns-disabled; otherwise try rm+create (a older
     * network has dns_enabled=true and forces the gateway resolver into every container's
     * resolv.conf). Rm failure or unparseable inspect → succeed without caching (retry on next spawn).
     */
    private static void adoptExisting(String cliPath, String networkName, Exec exec) {
        SandboxNetworkInfo info = inspect(cliPath, networkName, exec);
        if (info == null) {
            return;
        }
        if (info.isolated() && info.dnsDisabled()) {
            ENSURED.add(networkName);
            return;
        }
        ExecResult rm = exec.run(List.of(cliPath, "network", "rm", networkName), TIMEOUT);
        if (!rm.ok()) {
            return;
        }
        ExecResult created = exec.run(
                List.of(cliPath, "network", "create", "--opt", "isolate=true", "--disable-dns",
                        networkName),
                TIMEOUT);
        // Pre-netavark podman: the isolated recreate can never succeed — go plain instead.
        if (!created.ok() && !alreadyExists(created) && unsupportedOption(created)) {
            createPlain(cliPath, networkName, exec, created);
            return;
        }
        if (created.ok() || alreadyExists(created)) {
            // After a successful rm+create, the new network is isolated+dns-disabled by
            // construction. alreadyExists after rm is unusual; confirm via inspect when possible.
            if (created.ok()) {
                ENSURED.add(networkName);
                return;
            }
            SandboxNetworkInfo again = inspect(cliPath, networkName, exec);
            if (again != null && again.isolated() && again.dnsDisabled()) {
                ENSURED.add(networkName);
            }
        }
        // Create failed after rm — network may be gone. Fail loud (no default-bridge fallback).
        if (!created.ok() && !alreadyExists(created)) {
            String detail = firstLine(created.stderr());
            if (detail.isEmpty()) {
                detail = "exit=" + created.exitCode();
            }
            throw new IllegalStateException(
                    "failed to ensure podman network '" + networkName + "': " + detail);
        }
    }

    /** Race: another process created the network between exists and create. */
    private static boolean alreadyExists(ExecResult r) {
        if (r == null || r.stderr() == null) {
            return false;
        }
        String s = r.stderr().toLowerCase(Locale.ROOT);
        return s.contains("already exists") || s.contains("already exist");
    }

    /**
     * Downgrades {@code cniVersion "1.0.0"} to {@code "0.4.0"} in a CNI conflist. 0.4.0 is accepted by
     * every CNI plugin generation (spec backward compatibility) and covers podman 3.4's
     * bridge/portmap/firewall/tuning template, so the rewrite is safe even on hosts whose plugins DO
     * support 1.0.0. No-op when the file is absent (netavark hosts write no conflist), already ≤
     * 0.4.0, or unreadable (the attach error then surfaces with its own cause at spawn).
     */
    static void downgradeCniConflistVersion(Path conflist) {
        String json;
        try {
            if (conflist == null || !Files.exists(conflist)) {
                return;
            }
            json = Files.readString(conflist);
        } catch (IOException | RuntimeException ex) {
            log.warn("sandbox-network: cannot read {} to check cniVersion: {}", conflist, ex.getMessage());
            return;
        }
        Matcher m = CNI_VERSION_1_0.matcher(json);
        if (!m.find()) {
            return;
        }
        String fixed = m.replaceFirst(Matcher.quoteReplacement("\"cniVersion\": \"0.4.0\""));
        try {
            Files.writeString(conflist, fixed);
            log.warn("sandbox-network: downgraded cniVersion 1.0.0 -> 0.4.0 in {} (host CNI plugins "
                    + "predate spec 1.0.0 — the Ubuntu 22.04 podman/CNI packaging mismatch)", conflist);
        } catch (IOException ex) {
            throw new IllegalStateException("CNI config " + conflist + " needs cniVersion 0.4.0 "
                    + "(host plugins predate spec 1.0.0) but is not writable: " + ex.getMessage());
        }
    }

    /**
     * Pre-netavark podman (CNI era, e.g. Ubuntu 22.04's podman 3.4.x) rejects the netavark-only create
     * flags: {@code Error: unsupported option isolate} for {@code --opt isolate=true} and {@code
     * unknown flag: --disable-dns} for the DNS switch.
     */
    private static boolean unsupportedOption(ExecResult r) {
        if (r == null || r.stderr() == null) {
            return false;
        }
        String s = r.stderr().toLowerCase(Locale.ROOT);
        return s.contains("unsupported option") || s.contains("unknown flag");
    }

    private static String firstLine(String stderr) {
        if (stderr == null || stderr.isBlank()) {
            return "";
        }
        int nl = stderr.indexOf('\n');
        return (nl < 0 ? stderr : stderr.substring(0, nl)).trim();
    }
}
