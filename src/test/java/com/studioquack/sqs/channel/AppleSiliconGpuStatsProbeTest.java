package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class AppleSiliconGpuStatsProbeTest {

    private static final String M3_MAX = """
            Graphics/Displays:

                Apple M3 Max:

                  Chipset Model: Apple M3 Max
                  Type: GPU
                  Bus: Built-In
                  Total Number of Cores: 40
                  Vendor: Apple (0x106b)
                  Metal Support: Metal 3
            """;

    private static final String INTEL_MAC_WITH_RADEON = """
            Graphics/Displays:

                AMD Radeon Pro 5500M:

                  Chipset Model: AMD Radeon Pro 5500M
                  Type: GPU
                  Bus: PCIe
                  VRAM (Total): 4 GB
            """;

    @Test
    void readsTheAppleGpuName() {
        assertEquals("Apple M3 Max", AppleSiliconGpuStatsProbe.parseGpuName(M3_MAX));
    }

    @Test
    void anAddInGraphicsCardInAnIntelMacIsNotTheAppleGpu() {
        assertNull(AppleSiliconGpuStatsProbe.parseGpuName(INTEL_MAC_WITH_RADEON));
        assertNull(AppleSiliconGpuStatsProbe.parseGpuName(null));
    }

    @Test
    void readsLiveGpuLoadFromIoreg() {
        String ioreg = "\"PerformanceStatistics\" = {\"In use system memory\"=812384256,"
                + "\"Device Utilization %\"=37,\"Renderer Utilization %\"=35}";
        assertEquals(37, AppleSiliconGpuStatsProbe.parseUtilisation(ioreg));
        assertNull(AppleSiliconGpuStatsProbe.parseUtilisation("no accelerator here"));
    }

    @Test
    void onlyAppleSiliconMacsUseTheAppleProbe() {
        assertInstanceOf(AppleSiliconGpuStatsProbe.class,
                DefaultHeartbeatResourceProvider.defaultGpuProbe("Mac OS X", "aarch64"));
        assertInstanceOf(NvidiaSmiGpuStatsProbe.class,
                DefaultHeartbeatResourceProvider.defaultGpuProbe("Mac OS X", "x86_64"));
        // DGX Spark is Linux on Arm: it keeps nvidia-smi.
        assertInstanceOf(NvidiaSmiGpuStatsProbe.class,
                DefaultHeartbeatResourceProvider.defaultGpuProbe("Linux", "aarch64"));
    }
}
