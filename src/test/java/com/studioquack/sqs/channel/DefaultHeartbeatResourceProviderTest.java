package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.studioquack.sqs.channel.DefaultHeartbeatResourceProvider.GIB;
import static com.studioquack.sqs.channel.DefaultHeartbeatResourceProvider.MIB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic host-telemetry contract tests. No real MXBean, filesystem, or
 * subprocess is consulted.
 */
class DefaultHeartbeatResourceProviderTest {

    @Test
    void converts_host_units_and_reports_caps_effective_values_and_workload() {
        DefaultHeartbeatResourceProvider.HostSnapshot host =
                new DefaultHeartbeatResourceProvider.HostSnapshot(
                        16,
                        0.625d,
                        16_384L * MIB,
                        7_000L * MIB,
                        500L * GIB,
                        314L * GIB,
                        4);
        DefaultHeartbeatResourceProvider.WorkloadSnapshot workload =
                new DefaultHeartbeatResourceProvider.WorkloadSnapshot(3, 2, 10, 16);
        DefaultHeartbeatResourceProvider.ResourceCaps caps =
                new DefaultHeartbeatResourceProvider.ResourceCaps(50, 8_192L, 204_800L, 1);

        Map<String, Object> resources = provider(host, workload, caps).resources();

        assertEquals(16, resources.get("cpuCount"));
        assertEquals(62.5d, resources.get("cpuUsagePct"));
        assertEquals(16_384L, resources.get("ramTotalMb"));
        assertEquals(9_384L, resources.get("ramUsedMb"));
        assertEquals(500L, resources.get("diskTotalGb"));
        assertEquals(186L, resources.get("diskUsedGb"));
        assertEquals(4, resources.get("gpuCount"));
        assertEquals(3, resources.get("jobsRunning"));
        assertEquals(2, resources.get("jobsQueued"));
        assertEquals(10, resources.get("warmPoolBuilt"));
        assertEquals(16, resources.get("warmPoolTarget"));
        assertEquals(50, resources.get("capCpuPercent"));
        assertEquals(8_192L, resources.get("capRamMb"));
        assertEquals(204_800L, resources.get("capDiskMb"));
        assertEquals(1, resources.get("capGpuCount"));
        assertEquals(8, resources.get("effectiveCpuCount"));
        assertEquals(8_192L, resources.get("effectiveRamMb"));
        assertEquals(200L, resources.get("effectiveDiskGb"));
        assertEquals(1, resources.get("effectiveGpuCount"));
    }

    @Test
    void bounds_cpu_percent_and_normalises_invalid_caps() {
        DefaultHeartbeatResourceProvider.HostSnapshot host =
                new DefaultHeartbeatResourceProvider.HostSnapshot(
                        4, 1.75d, 8_000L * MIB, 2_000L * MIB,
                        100L * GIB, 25L * GIB, 0);
        DefaultHeartbeatResourceProvider.ResourceCaps invalidCaps =
                new DefaultHeartbeatResourceProvider.ResourceCaps(170, -1L, -2L, -3);

        Map<String, Object> resources = provider(
                host,
                new DefaultHeartbeatResourceProvider.WorkloadSnapshot(0, 0, 0, 0),
                invalidCaps).resources();

        assertEquals(100.0d, resources.get("cpuUsagePct"));
        assertEquals(100, resources.get("capCpuPercent"));
        assertEquals(0L, resources.get("capRamMb"));
        assertEquals(0L, resources.get("capDiskMb"));
        assertEquals(0, resources.get("capGpuCount"));
        assertEquals(4, resources.get("effectiveCpuCount"));
    }

    @Test
    void preserves_schema_and_uses_null_for_unsupported_metrics() {
        DefaultHeartbeatResourceProvider.HostSnapshot unsupported =
                new DefaultHeartbeatResourceProvider.HostSnapshot(
                        8, -1.0d, null, null, null, null, null);

        Map<String, Object> resources = provider(
                unsupported,
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited()).resources();

        // Every backend/FE key remains present even when the host cannot supply it. The count is
        // asserted so a new key cannot be added to the heartbeat payload without a deliberate
        // update here — the backend and FE read this schema.
        assertEquals(20, resources.size());
        // `llm` carries the local-LLM reachability the backend validates runs against; with no
        // probe wired the provider reports UNKNOWN rather than omitting the key, so the backend
        // can tell "not reported" from "reported offline".
        assertTrue(resources.containsKey("llm"));
        assertEquals("UNKNOWN", ((Map<?, ?>) resources.get("llm")).get("providerHealth"));
        assertTrue(resources.containsKey("cpuUsagePct"));
        assertNull(resources.get("cpuUsagePct"));
        assertNull(resources.get("ramTotalMb"));
        assertNull(resources.get("ramUsedMb"));
        assertNull(resources.get("diskTotalGb"));
        assertNull(resources.get("diskUsedGb"));
        assertNull(resources.get("gpuCount"));
        assertNull(resources.get("jobsRunning"));
        assertNull(resources.get("jobsQueued"));
        assertNull(resources.get("warmPoolBuilt"));
        assertNull(resources.get("warmPoolTarget"));
        assertEquals(8, resources.get("effectiveCpuCount"));
        assertNull(resources.get("effectiveRamMb"));
        assertNull(resources.get("effectiveDiskGb"));
        assertNull(resources.get("effectiveGpuCount"));
        assertTrue(!resources.containsKey("networkRxMbps") && !resources.containsKey("networkTxMbps"),
                "first OS sample has no delta — keys stay omitted so the schema count stays stable");
    }

    @Test
    void emits_network_rates_when_the_probe_has_a_sample_and_omits_them_when_null() {
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        p.setNetworkRateProbe(() -> new DefaultHeartbeatResourceProvider.NetworkRate(12.5d, 3.25d));
        Map<String, Object> withRates = p.resources();
        assertEquals(12.5d, withRates.get("networkRxMbps"));
        assertEquals(3.25d, withRates.get("networkTxMbps"));

        p.setNetworkRateProbe(() -> null);
        Map<String, Object> omitted = p.resources();
        assertTrue(!omitted.containsKey("networkRxMbps"));
        assertTrue(!omitted.containsKey("networkTxMbps"));
    }

    @Test
    void parses_proc_net_dev_and_netstat_ibn_skipping_loopback() {
        String proc = """
                Inter-|   Receive                                                |  Transmit
                 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo frame compressed multicast
                    lo: 18492  221 0 0 0 0 0 0 18492 221 0 0 0 0 0 0
                  eth0: 1000 1 0 0 0 0 0 0 4000 1 0 0 0 0 0 0
                  wg0: 250 1 0 0 0 0 0 0 750 1 0 0 0 0 0 0
                """;
        DefaultHeartbeatResourceProvider.OsNetworkThroughputProbe.Counters linux =
                DefaultHeartbeatResourceProvider.OsNetworkThroughputProbe.parseProcNetDev(proc);
        assertEquals(1250L, linux.rxBytes());
        assertEquals(4750L, linux.txBytes());

        String netstat = """
                Name       Mtu   Network       Address            Ipkts Ierrs     Ibytes    Opkts Oerrs     Obytes  Coll
                lo0        16384 <Link#1>                      10085300     0 1431661753 10085300     0 1431661753     0
                en1        1500  <Link#12>   86:30:50:8a:dc:51 100     0 8000 50     0 2000     0
                en1        1500  10/24         10.0.0.155      100     - 8000 50     - 2000     -
                """;
        DefaultHeartbeatResourceProvider.OsNetworkThroughputProbe.Counters mac =
                DefaultHeartbeatResourceProvider.OsNetworkThroughputProbe.parseNetstatIbn(netstat);
        assertEquals(8000L, mac.rxBytes());
        assertEquals(2000L, mac.txBytes());
    }

    // The sandboxEgress posture blob

    @Test
    void sandboxEgress_blob_carries_the_wired_posture() {
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        p.setEgressPostureProbe(() -> new String[]{"DEGRADED",
                "overlay-egress requires native Linux + kernel WireGuard (host os=macos)", ""});

        Map<String, Object> resources = p.resources();

        Object blob = resources.get("sandboxEgress");
        assertTrue(blob instanceof Map, "sandboxEgress block must be present once the probe is wired");
        assertEquals("DEGRADED", ((Map<?, ?>) blob).get("state"));
        assertTrue(((String) ((Map<?, ?>) blob).get("reason")).contains("host os=macos"));
        assertNull(((Map<?, ?>) blob).get("overlayIp"), "blank overlayIp is omitted, not empty-stringed");
    }

    @Test
    void sandboxEgress_blob_absent_without_a_probe() {
        // A channel-less boot / old wiring must not claim a posture: absence means "cannot say".
        Map<String, Object> resources = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited()).resources();
        assertNull(resources.get("sandboxEgress"));
    }

    // The egressProxy advisory-proxy health blob

    @Test
    void egressProxy_blob_carries_the_wired_state_and_reason() {
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        p.setEgressProxyHealthProbe(() -> new String[]{"DOWN", "SocketTimeoutException: timed out"});

        Map<String, Object> resources = p.resources();

        Object blob = resources.get("egressProxy");
        assertTrue(blob instanceof Map, "egressProxy block must be present once the probe is wired");
        assertEquals("DOWN", ((Map<?, ?>) blob).get("state"));
        assertEquals("SocketTimeoutException: timed out", ((Map<?, ?>) blob).get("reason"));
    }

    @Test
    void egressProxy_blob_omits_a_blank_reason() {
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        p.setEgressProxyHealthProbe(() -> new String[]{"UP", ""});

        Object blob = p.resources().get("egressProxy");

        assertTrue(blob instanceof Map);
        assertEquals("UP", ((Map<?, ?>) blob).get("state"));
        assertNull(((Map<?, ?>) blob).get("reason"), "a blank reason is omitted, not empty-stringed");
    }

    @Test
    void egressProxy_blob_absent_without_a_probe() {
        // A channel-less boot must not claim proxy health: absence means "cannot say".
        Map<String, Object> resources = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited()).resources();
        assertNull(resources.get("egressProxy"));
    }

    @Test
    void egressProxy_blob_absent_when_no_proxy_is_configured() {
        // The wiring maps NOT_CONFIGURED to a null view: "no proxy configured" must read as
        // "no block", never as a health verdict.
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        p.setEgressProxyHealthProbe(() -> null);

        assertNull(p.resources().get("egressProxy"));
    }

    // SecurityTelemetry reason-class block

    @Test
    void securityTelemetry_emits_bounded_reason_class_counts_and_omits_when_empty() {
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        SecurityTelemetryAccumulator acc = new SecurityTelemetryAccumulator();
        p.setSecurityTelemetryAccumulator(acc);

        assertNull(p.resources().get("securityTelemetry"), "empty drain omits the block");

        acc.record("ingress_tls");
        acc.record("ingress_allowlist");
        acc.record("not_a_real_class");

        Map<String, Object> resources = p.resources();
        Object blob = resources.get("securityTelemetry");
        assertTrue(blob instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> block = (Map<String, Object>) blob;
        assertEquals(1, block.get("schema"));
        assertEquals("daemon", block.get("source"));
        @SuppressWarnings("unchecked")
        Map<String, Integer> counts = (Map<String, Integer>) block.get("counts");
        assertEquals(1, counts.get("ingress_tls"));
        assertEquals(1, counts.get("ingress_allowlist"));
        assertEquals(2, counts.size(), "unknown classes must never appear");
        assertNull(block.get("sourceIp"));
        assertNull(block.get("ip"));
        assertNull(block.get("payload"));
        assertNull(counts.get("sourceIp"));

        // Drain-on-read: the next beat omits again until new increments arrive.
        assertNull(p.resources().get("securityTelemetry"));
    }

    @Test
    void emits_gpu_stats_and_derives_the_gpu_count_from_the_probe() {
        DefaultHeartbeatResourceProvider p = provider(
                DefaultHeartbeatResourceProvider.HostSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.WorkloadSnapshot.unavailable(),
                DefaultHeartbeatResourceProvider.ResourceCaps.unlimited());
        p.setGpuStatsProbe(() -> java.util.List.of(Map.of("index", 0, "name", "NVIDIA GB10", "utilisation_pct", 96)));

        Map<String, Object> resources = p.resources();

        assertEquals(1, resources.get("gpuCount"));
        assertEquals(java.util.List.of(Map.of("index", 0, "name", "NVIDIA GB10", "utilisation_pct", 96)),
                resources.get("gpus"));
    }

    @Test
    void parses_nvidia_smi_csv_and_leaves_unified_memory_na_out() {
        java.util.List<Map<String, Object>> gpus = NvidiaSmiGpuStatsProbe.parse(
                "0, NVIDIA GB10, 580.95.05, 96, [N/A], [N/A]\n"
                        + "1, NVIDIA L4, 550.54.15, 12, 2048, 23034\n"
                        + "garbage line\n");

        assertEquals(2, gpus.size());
        assertEquals(Map.of("index", 0, "name", "NVIDIA GB10", "driver", "580.95.05", "utilisation_pct", 96),
                gpus.get(0));
        assertEquals(2048L, gpus.get(1).get("mem_used_mib"));
        assertEquals(23034L, gpus.get(1).get("mem_total_mib"));
    }

    @Test
    void reads_mem_available_so_page_cache_is_not_counted_as_used(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
            throws java.io.IOException {
        java.nio.file.Path meminfo = dir.resolve("meminfo");
        java.nio.file.Files.writeString(meminfo,
                "MemTotal:       127600000 kB\nMemFree:          2000000 kB\nMemAvailable:    9000000 kB\n");

        assertEquals(9_000_000L * 1024L, DefaultHeartbeatResourceProvider.linuxMemAvailableBytes(meminfo));
        assertNull(DefaultHeartbeatResourceProvider.linuxMemAvailableBytes(dir.resolve("absent")));
    }

    private static DefaultHeartbeatResourceProvider provider(
            DefaultHeartbeatResourceProvider.HostSnapshot host,
            DefaultHeartbeatResourceProvider.WorkloadSnapshot workload,
            DefaultHeartbeatResourceProvider.ResourceCaps caps) {
        DefaultHeartbeatResourceProvider p = new DefaultHeartbeatResourceProvider(() -> host, () -> workload, () -> caps);
        p.setGpuStatsProbe(java.util.List::of);
        return p;
    }
}
