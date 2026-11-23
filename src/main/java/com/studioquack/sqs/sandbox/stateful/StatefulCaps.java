package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Resource caps applied to a stateful instance, decoded from the daemon-command payload's {@code
 * caps:{cpu, ramMb, diskMb, gpu?}} object (built by the control plane {@code
 * StatefulInstanceServiceImpl}). Mirrors {@code StatefulInstanceStartRequest} field names/units so
 * the wire contract lives in one shape.
 */
public record StatefulCaps(int cpu, int ramMb, int diskMb, Integer gpu) {

    /** The defaults mirror {@code StatefulInstanceStartRequest} (cpu 1, 512 MiB, 1024 MiB). */
    public static StatefulCaps defaults() {
        return new StatefulCaps(1, 512, 1024, null);
    }

    /**
     * Zero caps — {@code StatefulRunArgs} omits {@code --cpus}/{@code --memory}/{@code --storage-opt}
     * for non-positive values, so the container is bounded only by the runtime's own machine limit.
     * The built-in node library starts with these (and, as the engine's own runtime, without a pid
     * cap either); never use for user-started instances.
     */
    public static StatefulCaps uncapped() {
        return new StatefulCaps(0, 0, 0, null);
    }

    /** Decodes a {@code caps} JSON object, falling back to {@link #defaults()} per-field. */
    public static StatefulCaps fromJson(JsonNode caps) {
        if (caps == null || !caps.isObject()) {
            return defaults();
        }
        int cpu = caps.path("cpu").asInt(1);
        int ramMb = caps.path("ramMb").asInt(512);
        int diskMb = caps.path("diskMb").asInt(1024);
        JsonNode gpuNode = caps.get("gpu");
        Integer gpu = (gpuNode == null || gpuNode.isNull()) ? null : gpuNode.asInt();
        return new StatefulCaps(Math.max(1, cpu), Math.max(1, ramMb), Math.max(1, diskMb), gpu);
    }

    /** True when a positive GPU count was requested. */
    public boolean hasGpu() {
        return gpu != null && gpu > 0;
    }

    /**
     * User OCI cold start. Each dimension falls back to {@link #defaults()} when unset, so a declared
     * RAM does not drop CPU or disk.
     */
    public static StatefulCaps forUserImage(double cpus, int ramMb, int diskMb) {
        StatefulCaps defaults = defaults();
        int cpu = cpus > 0 ? (int) Math.ceil(cpus) : defaults.cpu();
        int ram = ramMb > 0 ? ramMb : defaults.ramMb();
        int disk = diskMb > 0 ? diskMb : defaults.diskMb();
        return new StatefulCaps(cpu, ram, disk, null);
    }
}
