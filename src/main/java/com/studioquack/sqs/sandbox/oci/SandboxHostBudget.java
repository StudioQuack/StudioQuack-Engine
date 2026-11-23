package com.studioquack.sqs.sandbox.oci;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

/**
 * Host-derived ceiling for container RAM. A node-declared value above this is clamped; platform
 * side-cars with no explicit RAM use the same ceiling so a heavy workload still fits and a runaway
 * cannot take the machine.
 */
public final class SandboxHostBudget {

    /** Floor so a tiny host still gets a usable cap rather than a zero-byte cgroup. */
    public static final int FLOOR_MB = 512;

    private SandboxHostBudget() {
    }

    /**
     * @param configuredMb {@code sqs.sandbox.max-ram-mb}; {@code <= 0} means derive from the host
     */
    public static int maxRamMb(int configuredMb) {
        if (configuredMb > 0) {
            return configuredMb;
        }
        return hostCeilingMb();
    }

    /** 75% of {@link OperatingSystemMXBean#getTotalMemorySize()}, floored at {@link #FLOOR_MB}. */
    public static int hostCeilingMb() {
        long bytes;
        try {
            var os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof OperatingSystemMXBean sun) {
                bytes = sun.getTotalMemorySize();
            } else {
                bytes = (long) FLOOR_MB * 1024L * 1024L;
            }
        } catch (RuntimeException ex) {
            bytes = (long) FLOOR_MB * 1024L * 1024L;
        }
        if (bytes <= 0) {
            return FLOOR_MB;
        }
        long mb = bytes / (1024L * 1024L);
        long ceiling = (mb * 75L) / 100L;
        if (ceiling < FLOOR_MB) {
            ceiling = FLOOR_MB;
        }
        if (ceiling > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) ceiling;
    }

    /** {@code sqs.sandbox.pids-limit}; must be positive. */
    public static int pidsLimit(int configured) {
        if (configured <= 0) {
            throw new IllegalStateException("sqs.sandbox.pids-limit must be > 0");
        }
        return configured;
    }
}
