package com.studioquack.sqs.channel;

import com.studioquack.sqs.sandbox.queue.DurableJobQueue;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cross-platform host and workload telemetry for heartbeat/status responses. The default host probe uses only
 * JDK APIs: the platform operating-system MXBean for CPU/RAM and {@link FileStore} for the data volume.
 */
@Slf4j
public class DefaultHeartbeatResourceProvider implements HeartbeatResourceProvider {

    static final long MIB = 1024L * 1024L;
    static final long GIB = 1024L * MIB;

    private final HostResourceProbe hostProbe;
    private final WorkloadResourceProbe workloadProbe;
    private final ResourceCapsProbe capsProbe;

    /**
     * Back-compatible fallback used outside Spring wiring. Host values remain real; workload counters are unknown
     * because no queue/pool was supplied.
     */
    public DefaultHeartbeatResourceProvider() {
        this(new JdkHostResourceProbe(defaultDataPath()),
                WorkloadSnapshot::unavailable, ResourceCaps::unlimited);
    }

    /**
     * Production constructor: reads the host containing {@code dataPath} and obtains consistent queue/warm-pool
     * counts on every heartbeat.
     */
    public DefaultHeartbeatResourceProvider(Path dataPath, DurableJobQueue jobQueue, WarmPool warmPool) {
        this(new JdkHostResourceProbe(dataPath),
                () -> workloadSnapshot(jobQueue, warmPool),
                ResourceCaps::unlimited);
    }

    /**
     * Fully injectable seam for deterministic tests.
     */
    public DefaultHeartbeatResourceProvider(HostResourceProbe hostProbe,
                                            WorkloadResourceProbe workloadProbe,
                                            ResourceCapsProbe capsProbe) {
        this.hostProbe = hostProbe == null ? HostSnapshot::unavailable : hostProbe;
        this.workloadProbe = workloadProbe == null ? WorkloadSnapshot::unavailable : workloadProbe;
        this.capsProbe = capsProbe == null ? ResourceCaps::unlimited : capsProbe;
    }

    /**
     * Fast LLM reachability probe.
     */
    @FunctionalInterface
    public interface LlmHealthProbe {
        String snapshot();
    }

    /**
     * Overlay-egress enforcement posture probe. Returns the daemon's current {@code EgressReadiness} as a simple
     * {@code [state, reason, overlayIp]} triple so this provider never imports the egress package.
     */
    @FunctionalInterface
    public interface EgressPostureProbe {
        /**
         * {@code [state, reason, overlayIp]} — state is ENFORCED / DEGRADED / UNAVAILABLE.
         */
        String[] snapshot();
    }

    /**
     * Advisory egress-proxy health probe. Returns {@code [state, reason]} with state UP / DOWN, or {@code null}
     * when no proxy is configured (the {@code egressProxy} block stays out of the payload entirely — "absent"
     * means "nothing to watch", not "unknown").
     */
    @FunctionalInterface
    public interface EgressProxyHealthProbe {
        /**
         * {@code [state, reason]} — state is UP / DOWN; null or blank state omits the block.
         */
        String[] snapshot();
    }

    /**
     * Host NIC rx/tx rate. Default is the OS probe (Linux {@code /proc/net/dev}, macOS {@code netstat -ibn});
     * tests replace it so the heartbeat schema stay deterministic.
     */
    private NetworkRateProbe networkRateProbe = new OsNetworkThroughputProbe();

    /**
     * Live per-GPU stats, one map per GPU in the backend's wire shape; empty when none.
     */
    @FunctionalInterface
    public interface GpuStatsProbe {
        java.util.List<Map<String, Object>> sample();
    }

    /**
     * Default for this host: the Apple Silicon GPU on a Mac with Apple Silicon, else {@code nvidia-smi}.
     */
    private GpuStatsProbe gpuStatsProbe = defaultGpuProbe(System.getProperty("os.name"), System.getProperty("os.arch"));

    /**
     * Apple Silicon has no {@code nvidia-smi}; its GPU is read from the macOS tools instead. Both probes cache.
     */
    static GpuStatsProbe defaultGpuProbe(String osName, String osArch) {
        String os = osName == null ? "" : osName.toLowerCase(Locale.ROOT);
        String arch = osArch == null ? "" : osArch.toLowerCase(Locale.ROOT);
        boolean appleSilicon = os.contains("mac") && (arch.equals("aarch64") || arch.equals("arm64"));
        return appleSilicon ? new AppleSiliconGpuStatsProbe() : new NvidiaSmiGpuStatsProbe();
    }

    private LlmHealthProbe llmHealthProbe = () -> "UNKNOWN";

    private EgressPostureProbe egressPostureProbe = null;

    private EgressProxyHealthProbe egressProxyHealthProbe = null;

    /**
     * Drain-on-read reason-class counters for {@code securityTelemetry}. Absent accumulator → block omitted
     * (channel-less / old wiring stays silent).
     */
    private SecurityTelemetryAccumulator securityTelemetryAccumulator = null;

    public void setLlmHealthProbe(LlmHealthProbe probe) {
        this.llmHealthProbe = probe == null ? () -> "UNKNOWN" : probe;
    }

    public void setEgressPostureProbe(EgressPostureProbe probe) {
        this.egressPostureProbe = probe;
    }

    public void setEgressProxyHealthProbe(EgressProxyHealthProbe probe) {
        this.egressProxyHealthProbe = probe;
    }

    public void setSecurityTelemetryAccumulator(SecurityTelemetryAccumulator accumulator) {
        this.securityTelemetryAccumulator = accumulator;
    }

    /**
     * Host GPU-passthrough capability (the {@code GpuCapabilityProbe}, internally cached, so a beat never pays for
     * a subprocess). Absent probe → the key stays OUT of the payload rather than reporting a guess.
     */
    private java.util.function.BooleanSupplier gpuPassthroughProbe = null;

    public void setGpuPassthroughProbe(java.util.function.BooleanSupplier probe) {
        this.gpuPassthroughProbe = probe;
    }

    /**
     * Test seam: null restores the OS NIC probe.
     */
    void setNetworkRateProbe(NetworkRateProbe probe) {
        this.networkRateProbe = probe == null ? new OsNetworkThroughputProbe() : probe;
    }

    /**
     * Test seam: null restores this host's default probe.
     */
    void setGpuStatsProbe(GpuStatsProbe probe) {
        this.gpuStatsProbe = probe == null
                ? defaultGpuProbe(System.getProperty("os.name"), System.getProperty("os.arch"))
                : probe;
    }

    /**
     * {@code MemAvailable} from a Linux {@code /proc/meminfo}, in bytes; null elsewhere or unreadable.
     */
    static Long linuxMemAvailableBytes(Path meminfo) {
        if (!Files.isReadable(meminfo)) {
            return null;
        }
        try (var lines = Files.lines(meminfo, StandardCharsets.US_ASCII)) {
            return lines.filter(l -> l.startsWith("MemAvailable:"))
                    .findFirst()
                    .map(l -> l.replaceAll("[^0-9]", ""))
                    .filter(kb -> !kb.isEmpty())
                    .map(kb -> Long.parseLong(kb) * 1024L)
                    .orElse(null);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    @Override
    public Map<String, Object> resources() {
        HostSnapshot host = safely(hostProbe::snapshot, HostSnapshot.unavailable());
        WorkloadSnapshot workload = safely(workloadProbe::snapshot, WorkloadSnapshot.unavailable());
        ResourceCaps caps = normaliseCaps(safely(capsProbe::snapshot, ResourceCaps.unlimited()));

        Integer cpuCount = positive(host.cpuCount());
        Double cpuUsagePct = usagePercent(host.cpuLoad());
        Long ramTotalMb = bytesToUnit(positive(host.ramTotalBytes()), MIB);
        Long ramUsedMb = usedInUnit(host.ramTotalBytes(), host.ramFreeBytes(), MIB);
        Long diskTotalGb = bytesToUnit(positive(host.diskTotalBytes()), GIB);
        Long diskUsedGb = usedInUnit(host.diskTotalBytes(), host.diskUsableBytes(), GIB);
        java.util.List<Map<String, Object>> gpus = safely(gpuStatsProbe::sample, java.util.List.of());
        Integer gpuCount = nonNegative(host.gpuCount());
        if (gpuCount == null && gpus != null && !gpus.isEmpty()) {
            gpuCount = gpus.size();
        }

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("cpuCount", cpuCount);
        r.put("cpuUsagePct", cpuUsagePct);
        r.put("ramTotalMb", ramTotalMb);
        r.put("ramUsedMb", ramUsedMb);
        r.put("diskTotalGb", diskTotalGb);
        r.put("diskUsedGb", diskUsedGb);
        r.put("gpuCount", gpuCount);
        r.put("jobsRunning", nonNegative(workload.jobsRunning()));
        r.put("jobsQueued", nonNegative(workload.jobsQueued()));
        r.put("warmPoolBuilt", nonNegative(workload.warmPoolBuilt()));
        r.put("warmPoolTarget", nonNegative(workload.warmPoolTarget()));
        r.put("capCpuPercent", caps.cpuPercent());
        r.put("capRamMb", caps.ramMb());
        r.put("capDiskMb", caps.diskMb());
        r.put("capGpuCount", caps.gpuCount());
        r.put("effectiveCpuCount", effectiveCpu(cpuCount, caps.cpuPercent()));
        r.put("effectiveRamMb", effectiveLong(ramTotalMb, caps.ramMb()));
        r.put("effectiveDiskGb", effectiveDisk(diskTotalGb, caps.diskMb()));
        r.put("effectiveGpuCount", effectiveInteger(gpuCount, caps.gpuCount()));
        if (gpus != null && !gpus.isEmpty()) {
            r.put("gpus", gpus);
        }

        NetworkRate net = safely(networkRateProbe::sample, null);
        if (net != null) {
            if (net.rxMbps() != null) {
                r.put("networkRxMbps", net.rxMbps());
            }
            if (net.txMbps() != null) {
                r.put("networkTxMbps", net.txMbps());
            }
        }

        String llmHealth = safely(llmHealthProbe::snapshot, "UNKNOWN");
        if (llmHealth != null) {
            Map<String, Object> llm = new LinkedHashMap<>();
            llm.put("providerHealth", llmHealth);
            r.put("llm", llm);
        }

        // The sandbox egress posture must be VISIBLE, never implied: a DEGRADED

        // Emitted whenever the probe is wired
        // including ENFORCED, so "no block" unambiguously means "daemon too old to say".
        String[] egress = egressPostureProbe == null ? null : safely(egressPostureProbe::snapshot, null);
        if (egress != null && egress.length > 0 && egress[0] != null && !egress[0].isBlank()) {
            Map<String, Object> sandboxEgress = new LinkedHashMap<>();
            sandboxEgress.put("state", egress[0]);
            if (egress.length > 1 && egress[1] != null && !egress[1].isBlank()) {
                sandboxEgress.put("reason", egress[1]);
            }
            if (egress.length > 2 && egress[2] != null && !egress[2].isBlank()) {
                sandboxEgress.put("overlayIp", egress[2]);
            }
            r.put("sandboxEgress", sandboxEgress);
        }

        // Advisory egress-proxy health. Emitted only when a proxy is configured and
        // the wiring therefore has a verdict to report: "absent" means "no proxy configured", while
        // an explicit DOWN means "runs are going direct" — the two must never be confusable.
        String[] proxy = egressProxyHealthProbe == null ? null : safely(egressProxyHealthProbe::snapshot, null);
        if (proxy != null && proxy.length > 0 && proxy[0] != null && !proxy[0].isBlank()) {
            Map<String, Object> egressProxy = new LinkedHashMap<>();
            egressProxy.put("state", proxy[0]);
            if (proxy.length > 1 && proxy[1] != null && !proxy[1].isBlank()) {
                egressProxy.put("reason", proxy[1]);
            }
            r.put("egressProxy", egressProxy);
        }

        // Emitted only when the probe is wired: "absent" means "daemon too old /
        // channel-less", while explicit false means "probed, no passthrough" (gpu caps refused).
        if (gpuPassthroughProbe != null) {
            r.put("gpuPassthrough", safely(gpuPassthroughProbe::getAsBoolean, false));
        }

        // Corroborating security telemetry (reason-class counts only). Omit entirely
        // when empty so old BE behaviour stays clean; fail-open so a bad drain never breaks the beat.
        putSecurityTelemetry(r);

        return r;
    }

    /**
     * Drain-on-read {@code securityTelemetry}; never throws into the heartbeat path.
     */
    private void putSecurityTelemetry(Map<String, Object> resources) {
        if (securityTelemetryAccumulator == null) {
            return;
        }
        try {
            Map<String, Integer> counts = securityTelemetryAccumulator.drain();
            if (counts == null || counts.isEmpty()) {
                return;
            }
            Map<String, Object> block = new LinkedHashMap<>();
            block.put("schema", SecurityTelemetryAccumulator.SCHEMA);
            block.put("source", SecurityTelemetryAccumulator.SOURCE);
            block.put("counts", counts);
            resources.put("securityTelemetry", block);
        } catch (RuntimeException e) {
            log.debug("resources: securityTelemetry drain failed: {}", e.toString());
        }
    }

    /**
     * Cheap host snapshot seam; CPU load is a 0.1 ratio, not a percentage.
     */
    @FunctionalInterface
    public interface HostResourceProbe {
        HostSnapshot snapshot();
    }

    /**
     * Runtime queue/pool snapshot seam.
     */
    @FunctionalInterface
    public interface WorkloadResourceProbe {
        WorkloadSnapshot snapshot();
    }

    /**
     * Resource-cap seam; zero means unlimited on every axis.
     */
    @FunctionalInterface
    public interface ResourceCapsProbe {
        ResourceCaps snapshot();
    }

    /**
     * Raw host values. Byte units keep conversion deterministic and avoid losing precision inside the platform
     * probe.
     */
    public record HostSnapshot(
            Integer cpuCount,
            Double cpuLoad,
            Long ramTotalBytes,
            Long ramFreeBytes,
            Long diskTotalBytes,
            Long diskUsableBytes,
            Integer gpuCount) {

        public static HostSnapshot unavailable() {
            return new HostSnapshot(null, null, null, null, null, null, null);
        }
    }

    /**
     * Current daemon workload counts.
     */
    public record WorkloadSnapshot(
            Integer jobsRunning,
            Integer jobsQueued,
            Integer warmPoolBuilt,
            Integer warmPoolTarget) {

        public static WorkloadSnapshot unavailable() {
            return new WorkloadSnapshot(null, null, null, null);
        }
    }

    /**
     * Daemon-wide workload caps; zero is the wire-level unlimited sentinel.
     */
    public record ResourceCaps(int cpuPercent, long ramMb, long diskMb, int gpuCount) {

        public static ResourceCaps unlimited() {
            return new ResourceCaps(0, 0L, 0L, 0);
        }
    }

    /**
     * Builds one workload snapshot without fabricating unavailable counters. {@link WarmPool#counts()} sums idle
     * built slots and desired targets over all keys, so it is already image-key-agnostic after (no per-language
     * walk needed here).
     */
    private static WorkloadSnapshot workloadSnapshot(DurableJobQueue jobQueue, WarmPool warmPool) {
        Integer running = null;
        Integer queued = null;
        Integer built = null;
        Integer target = null;

        if (jobQueue != null) {
            DurableJobQueue.Counts counts = jobQueue.counts();
            running = counts.running();
            queued = counts.queued();
        }
        if (warmPool != null) {
            WarmPool.Counts counts = warmPool.counts();
            built = counts.built();
            target = counts.target();
        }
        return new WorkloadSnapshot(running, queued, built, target);
    }

    /**
     * Converts the documented 0.1 MXBean load ratio to a bounded percent.
     */
    private static Double usagePercent(Double load) {
        if (load == null || !Double.isFinite(load) || load < 0.0d) {
            return null;
        }
        return Math.min(100.0d, load * 100.0d);
    }

    private static Long bytesToUnit(Long bytes, long unit) {
        return bytes == null ? null : bytes / unit;
    }

    private static Long usedInUnit(Long totalBytes, Long freeBytes, long unit) {
        if (totalBytes == null || totalBytes <= 0L || freeBytes == null
                || freeBytes < 0L || freeBytes > totalBytes) {
            return null;
        }
        return (totalBytes - freeBytes) / unit;
    }

    private static Integer effectiveCpu(Integer hostCpu, int capPercent) {
        if (hostCpu == null) {
            return null;
        }
        if (capPercent <= 0 || capPercent >= 100) {
            return hostCpu;
        }
        return Math.max(1, hostCpu * capPercent / 100);
    }

    private static Long effectiveLong(Long hostValue, long cap) {
        if (hostValue == null) {
            return null;
        }
        return cap > 0L ? Math.min(hostValue, cap) : hostValue;
    }

    private static Long effectiveDisk(Long hostGb, long capMb) {
        if (hostGb == null) {
            return null;
        }
        long capGb = capMb / 1024L;
        // The wire shape has whole GiB precision; sub-GiB caps cannot be
        // represented and therefore leave this display-only field unchanged.
        return capGb > 0L ? Math.min(hostGb, capGb) : hostGb;
    }

    private static Integer effectiveInteger(Integer hostValue, int cap) {
        if (hostValue == null) {
            return null;
        }
        return cap > 0 ? Math.min(hostValue, cap) : hostValue;
    }

    private static ResourceCaps normaliseCaps(ResourceCaps caps) {
        if (caps == null) {
            return ResourceCaps.unlimited();
        }
        return new ResourceCaps(
                Math.max(0, Math.min(100, caps.cpuPercent())),
                Math.max(0L, caps.ramMb()),
                Math.max(0L, caps.diskMb()),
                Math.max(0, caps.gpuCount()));
    }

    private static Integer positive(Integer value) {
        return value != null && value > 0 ? value : null;
    }

    private static Long positive(Long value) {
        return value != null && value > 0L ? value : null;
    }

    private static Integer nonNegative(Integer value) {
        return value != null && value >= 0 ? value : null;
    }

    private static <T> T safely(ProbeCall<T> probe, T fallback) {
        try {
            T value = probe.get();
            return value == null ? fallback : value;
        } catch (RuntimeException e) {
            log.debug("resources: telemetry probe failed: {}", e.toString());
            return fallback;
        }
    }

    @FunctionalInterface
    private interface ProbeCall<T> {
        T get();
    }

    private static Path defaultDataPath() {
        try {
            return Path.of(System.getenv().getOrDefault("SQS_DATA_DIR", "./data"));
        } catch (RuntimeException e) {
            return Path.of(".");
        }
    }

    /**
     * Host NIC throughput in Mbit/s; either side may be null.
     */
    record NetworkRate(Double rxMbps, Double txMbps) {
    }

    /**
     * Cheap NIC-rate seam; the default implementation is {@link OsNetworkThroughputProbe}.
     */
    @FunctionalInterface
    interface NetworkRateProbe {
        NetworkRate sample();
    }

    /**
     * Linux: read {@code /proc/net/dev} (no subprocess). macOS: {@code netstat -ibn}, spawned at most once per
     * {@link #MACOS_MIN_PROBE_INTERVAL_MS}.
     */
    static final class OsNetworkThroughputProbe implements NetworkRateProbe {

        static final Path PROC_NET_DEV = Path.of("/proc/net/dev");
        static final long MACOS_MIN_PROBE_INTERVAL_MS = 2_000L;
        private static final long NETSTAT_TIMEOUT_MS = 2_000L;

        private volatile Counters prev;
        private volatile NetworkRate lastRate;
        private volatile long lastMacosProbeAtMs;

        @Override
        public NetworkRate sample() {
            Counters counters = readCounters();
            if (counters == null) {
                return lastRate;
            }
            Counters previous = this.prev;
            this.prev = counters;
            if (previous == null || counters.atNanos <= previous.atNanos) {
                return lastRate;
            }
            double dtSec = (counters.atNanos - previous.atNanos) / 1_000_000_000.0d;
            if (dtSec < 0.05d) {
                return lastRate;
            }
            long dRx = counters.rxBytes - previous.rxBytes;
            long dTx = counters.txBytes - previous.txBytes;
            if (dRx < 0L || dTx < 0L) {
                return lastRate;
            }
            double rx = (dRx * 8.0d) / (dtSec * 1_000_000.0d);
            double tx = (dTx * 8.0d) / (dtSec * 1_000_000.0d);
            if (!Double.isFinite(rx) || !Double.isFinite(tx)) {
                return lastRate;
            }
            NetworkRate rate = new NetworkRate(rx, tx);
            this.lastRate = rate;
            return rate;
        }

        private Counters readCounters() {
            if (Files.isRegularFile(PROC_NET_DEV)) {
                return readProcNetDev();
            }
            String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
            if (os.contains("mac") || os.contains("darwin")) {
                long nowMs = System.currentTimeMillis();
                if (lastMacosProbeAtMs > 0L && nowMs - lastMacosProbeAtMs < MACOS_MIN_PROBE_INTERVAL_MS) {
                    return null;
                }
                Counters counters = readNetstatIbn();
                lastMacosProbeAtMs = nowMs;
                return counters;
            }
            return null;
        }

        private Counters readProcNetDev() {
            try {
                return parseProcNetDev(Files.readString(PROC_NET_DEV));
            } catch (IOException | RuntimeException e) {
                log.debug("resources: /proc/net/dev unavailable: {}", e.toString());
                return null;
            }
        }

        private Counters readNetstatIbn() {
            Process process = null;
            try {
                process = new ProcessBuilder("netstat", "-ibn")
                        .redirectErrorStream(true)
                        .start();
                StringBuilder out = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        out.append(line).append('\n');
                    }
                }
                if (!process.waitFor(NETSTAT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    return null;
                }
                if (process.exitValue() != 0) {
                    return null;
                }
                return parseNetstatIbn(out.toString());
            } catch (IOException | InterruptedException | RuntimeException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.debug("resources: netstat -ibn unavailable: {}", e.toString());
                return null;
            } finally {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly();
                }
            }
        }

        /**
         * Sums rx/tx bytes for every interface except loopback. Visible for tests.
         */
        static Counters parseProcNetDev(String text) {
            if (text == null || text.isBlank()) {
                return null;
            }
            long rx = 0L;
            long tx = 0L;
            boolean any = false;
            for (String raw : text.split("\n")) {
                String line = raw.trim();
                int colon = line.indexOf(':');
                if (colon <= 0) {
                    continue;
                }
                String iface = line.substring(0, colon).trim();
                if (isLoopback(iface)) {
                    continue;
                }
                String[] cols = line.substring(colon + 1).trim().split("\\s+");
                if (cols.length < 9) {
                    continue;
                }
                Long rxBytes = parseLong(cols[0]);
                Long txBytes = parseLong(cols[8]);
                if (rxBytes == null || txBytes == null) {
                    continue;
                }
                rx += rxBytes;
                tx += txBytes;
                any = true;
            }
            return any ? new Counters(rx, tx, System.nanoTime()) : null;
        }

        /**
         * Sums the {@code <Link#>} rows only — IPv4/IPv6 rows repeat the same counters. Address is optional on a Link
         * row, so Ibytes is located as the middle of the trailing {@code Ipkts Ierrs Ibytes Opkts Oerrs Obytes}
         * sextet.
         */
        static Counters parseNetstatIbn(String text) {
            if (text == null || text.isBlank()) {
                return null;
            }
            long rx = 0L;
            long tx = 0L;
            boolean any = false;
            for (String raw : text.split("\n")) {
                String line = raw.trim();
                if (line.isEmpty() || line.startsWith("Name")) {
                    continue;
                }
                String[] cols = line.split("\\s+");
                if (cols.length < 9 || isLoopback(cols[0])) {
                    continue;
                }
                if (!cols[2].startsWith("<Link")) {
                    continue;
                }
                int ibytes = ibytesIndex(cols);
                if (ibytes < 0) {
                    continue;
                }
                Long rxBytes = parseLong(cols[ibytes]);
                Long txBytes = parseLong(cols[ibytes + 3]);
                if (rxBytes == null || txBytes == null) {
                    continue;
                }
                rx += rxBytes;
                tx += txBytes;
                any = true;
            }
            return any ? new Counters(rx, tx, System.nanoTime()) : null;
        }

        /**
         * Index of Ibytes. A Link row always ends {@code Ipkts Ierrs Ibytes Opkts Oerrs Obytes Coll}, so Ibytes is
         * {@code length - 5}.
         */
        static int ibytesIndex(String[] cols) {
            int ibytes = cols.length - 5;
            if (ibytes < 3) {
                return -1;
            }
            if (parseLong(cols[ibytes - 2]) == null || parseLong(cols[ibytes - 1]) == null
                    || parseLong(cols[ibytes]) == null || parseLong(cols[ibytes + 1]) == null
                    || parseLong(cols[ibytes + 2]) == null || parseLong(cols[ibytes + 3]) == null) {
                return -1;
            }
            return ibytes;
        }

        static boolean isLoopback(String iface) {
            if (iface == null) {
                return true;
            }
            String name = iface.toLowerCase(Locale.ROOT);
            return "lo".equals(name) || name.matches("lo\\d+");
        }

        private static Long parseLong(String raw) {
            if (raw == null || raw.isBlank() || "-".equals(raw)) {
                return null;
            }
            try {
                return Long.parseLong(raw);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        record Counters(long rxBytes, long txBytes, long atNanos) {
        }
    }

    /**
     * JDK-only production probe. No subprocess is started per heartbeat.
     */
    private static final class JdkHostResourceProbe implements HostResourceProbe {

        private final Path dataPath;

        private JdkHostResourceProbe(Path dataPath) {
            this.dataPath = dataPath == null ? Path.of(".") : dataPath;
        }

        @Override
        public HostSnapshot snapshot() {
            java.lang.management.OperatingSystemMXBean base =
                    ManagementFactory.getOperatingSystemMXBean();
            Integer cpuCount = positive(base.getAvailableProcessors());
            Double cpuLoad = null;
            Long ramTotal = null;
            Long ramFree = null;

            if (base instanceof com.sun.management.OperatingSystemMXBean host) {
                try {
                    double load = host.getCpuLoad();
                    cpuLoad = load < 0.0d ? null : load;
                } catch (RuntimeException e) {
                    log.debug("resources: CPU load probe unavailable: {}", e.toString());
                }
                try {
                    long total = host.getTotalMemorySize();
                    long free = host.getFreeMemorySize();
                    ramTotal = total > 0L ? total : null;
                    ramFree = free >= 0L ? free : null;
                } catch (RuntimeException e) {
                    log.debug("resources: RAM probe unavailable: {}", e.toString());
                }
            }
            // The JDK's "free" is Linux MemFree, which counts reclaimable page cache as used and
            // read several GiB above what free(1) or the host's own dashboard show. MemAvailable
            // is what a new workload can actually get.
            Long available = linuxMemAvailableBytes(Path.of("/proc/meminfo"));
            if (available != null) {
                ramFree = available;
            }

            DiskBytes disk = readDisk(dataPath);
            return new HostSnapshot(cpuCount, cpuLoad, ramTotal, ramFree,
                    disk.total(), disk.usable(), null);
        }

        private static DiskBytes readDisk(Path configuredPath) {
            try {
                Path probe = configuredPath.toAbsolutePath().normalize();
                while (probe != null && !Files.exists(probe)) {
                    probe = probe.getParent();
                }
                if (probe == null) {
                    return DiskBytes.unavailable();
                }
                FileStore store = Files.getFileStore(probe);
                long total = store.getTotalSpace();
                long usable = store.getUsableSpace();
                return new DiskBytes(total > 0L ? total : null, usable >= 0L ? usable : null);
            } catch (IOException | RuntimeException e) {
                log.debug("resources: disk probe unavailable: {}", e.toString());
                return DiskBytes.unavailable();
            }
        }
    }

    private record DiskBytes(Long total, Long usable) {
        private static DiskBytes unavailable() {
            return new DiskBytes(null, null);
        }
    }
}
