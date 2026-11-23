package com.studioquack.sqs.channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The GPU of an Apple Silicon Mac, in the same wire shape as {@link NvidiaSmiGpuStatsProbe} (
 * {@code index, name, utilisation_pct}). The GPU shares system memory with the CPU, so no {@code mem_*} fields
 * are sent, exactly as for an NVIDIA GPU without dedicated VRAM.
 */
final class AppleSiliconGpuStatsProbe implements DefaultHeartbeatResourceProvider.GpuStatsProbe {

    static final String[] CHIP_COMMAND = {"system_profiler", "SPDisplaysDataType"};
    static final String[] LOAD_COMMAND = {"ioreg", "-r", "-d", "1", "-w", "0", "-c", "IOAccelerator"};

    private static final Pattern CHIPSET = Pattern.compile("(?m)^\\s*Chipset Model:\\s*(.+?)\\s*$");
    private static final Pattern UTILISATION = Pattern.compile("\"Device Utilization %\"\\s*=\\s*(\\d+)");

    private static final long CACHE_MS = 10_000L;
    private static final long ABSENT_RETRY_MS = 600_000L;
    private static final long CHIP_TIMEOUT_MS = 10_000L;
    private static final long LOAD_TIMEOUT_MS = 3_000L;

    private String gpuName;
    private long nextNameLookupAtMs = 0L;
    private List<Map<String, Object>> last = List.of();
    private long nextRunAtMs = 0L;

    @Override
    public synchronized List<Map<String, Object>> sample() {
        long now = System.currentTimeMillis();
        if (now < nextRunAtMs) {
            return last;
        }
        if (gpuName == null && now >= nextNameLookupAtMs) {
            gpuName = parseGpuName(run(CHIP_COMMAND, CHIP_TIMEOUT_MS));
            nextNameLookupAtMs = now + ABSENT_RETRY_MS;
        }
        if (gpuName == null) {
            return remember(List.of(), now + CACHE_MS);
        }
        Map<String, Object> gpu = new LinkedHashMap<>();
        gpu.put("index", 0);
        gpu.put("name", gpuName);
        Integer utilisation = parseUtilisation(run(LOAD_COMMAND, LOAD_TIMEOUT_MS));
        if (utilisation != null) {
            gpu.put("utilisation_pct", utilisation);
        }
        return remember(List.of(gpu), now + CACHE_MS);
    }

    private List<Map<String, Object>> remember(List<Map<String, Object>> gpus, long nextRunAt) {
        last = gpus;
        nextRunAtMs = nextRunAt;
        return gpus;
    }

    /**
     * Stdout of a short command, or null when it is missing, fails or overruns.
     */
    private static String run(String[] command, long timeoutMs) {
        Process process;
        try {
            process = new ProcessBuilder(command).redirectErrorStream(true).start();
        } catch (IOException notInstalled) {
            return null;
        }
        try {
            if (!process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return null;
            }
            String out = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            return process.exitValue() == 0 ? out : null;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            return null;
        } catch (IOException | RuntimeException failed) {
            return null;
        }
    }

    /**
     * The Apple GPU in {@code system_profiler} output; null for an add-in GPU in an Intel Mac.
     */
    static String parseGpuName(String systemProfiler) {
        if (systemProfiler == null) {
            return null;
        }
        Matcher name = CHIPSET.matcher(systemProfiler);
        return name.find() && name.group(1).startsWith("Apple") ? name.group(1) : null;
    }

    static Integer parseUtilisation(String ioreg) {
        if (ioreg == null) {
            return null;
        }
        Matcher m = UTILISATION.matcher(ioreg);
        return m.find() ? Math.max(0, Math.min(100, Integer.parseInt(m.group(1)))) : null;
    }
}
