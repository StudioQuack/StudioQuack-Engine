package com.studioquack.sqs.channel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Per-GPU utilisation and memory from {@code nvidia-smi}, in the wire shape the backend parses (
 * {@code index, name, driver, utilisation_pct, mem_used_mib, mem_total_mib}). Runs at most once per
 * {@link #CACHE_MS}, so a beat or status poll never pays for a subprocess each time.
 */
final class NvidiaSmiGpuStatsProbe implements DefaultHeartbeatResourceProvider.GpuStatsProbe {

    static final String[] COMMAND = {
            "nvidia-smi",
            "--query-gpu=index,name,driver_version,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits"};

    private static final long CACHE_MS = 10_000L;
    private static final long FAILED_RETRY_MS = 60_000L;
    private static final long ABSENT_RETRY_MS = 600_000L;
    private static final long TIMEOUT_MS = 3_000L;

    private List<Map<String, Object>> last = List.of();
    private long nextRunAtMs = 0L;

    @Override
    public synchronized List<Map<String, Object>> sample() {
        long now = System.currentTimeMillis();
        if (now < nextRunAtMs) {
            return last;
        }
        Process process;
        try {
            process = new ProcessBuilder(COMMAND).redirectErrorStream(true).start();
        } catch (IOException notInstalled) {
            return remember(List.of(), now + ABSENT_RETRY_MS);
        }
        try {
            if (!process.waitFor(TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return remember(List.of(), now + FAILED_RETRY_MS);
            }
            String out = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            List<Map<String, Object>> gpus = process.exitValue() == 0 ? parse(out) : List.of();
            return remember(gpus, now + (gpus.isEmpty() ? FAILED_RETRY_MS : CACHE_MS));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            return last;
        } catch (IOException | RuntimeException failed) {
            return remember(List.of(), now + FAILED_RETRY_MS);
        }
    }

    private List<Map<String, Object>> remember(List<Map<String, Object>> gpus, long nextRunAt) {
        last = gpus;
        nextRunAtMs = nextRunAt;
        return gpus;
    }

    /**
     * One CSV row per GPU; a row that does not carry at least index + name is skipped.
     */
    static List<Map<String, Object>> parse(String csv) {
        List<Map<String, Object>> gpus = new ArrayList<>();
        if (csv == null) {
            return gpus;
        }
        for (String line : csv.split("\\R")) {
            String[] cols = line.split(",");
            if (cols.length < 6) {
                continue;
            }
            Long index = number(cols[0]);
            String name = cols[1].trim();
            if (index == null || name.isEmpty()) {
                continue;
            }
            Map<String, Object> gpu = new LinkedHashMap<>();
            gpu.put("index", index.intValue());
            gpu.put("name", name);
            putIfPresent(gpu, "driver", text(cols[2]));
            Long util = number(cols[3]);
            putIfPresent(gpu, "utilisation_pct", util == null ? null : (int) Math.max(0L, Math.min(100L, util)));
            putIfPresent(gpu, "mem_used_mib", number(cols[4]));
            putIfPresent(gpu, "mem_total_mib", number(cols[5]));
            gpus.add(gpu);
        }
        return gpus;
    }

    private static void putIfPresent(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private static String text(String raw) {
        String v = raw == null ? "" : raw.trim();
        return v.isEmpty() || v.startsWith("[") || v.equalsIgnoreCase("N/A") ? null : v;
    }

    /**
     * {@code [N/A]}, {@code [Not Supported]} and other non-numbers read as absent.
     */
    private static Long number(String raw) {
        String v = text(raw);
        if (v == null) {
            return null;
        }
        try {
            return Math.round(Double.parseDouble(v));
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }
}
