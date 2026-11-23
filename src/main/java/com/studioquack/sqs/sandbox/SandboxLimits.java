package com.studioquack.sqs.sandbox;

/**
 * Per-job resource ceiling. A zero RAM/CPU field means "use the daemon floor at argv emission"
 * ({@link com.studioquack.sqs.sandbox.oci.HardeningArgs#FLOOR_RAM_MB} / {@link
 * com.studioquack.sqs.sandbox.oci.HardeningArgs#FLOOR_CPUS}).
 *
 * @param cpuSec Unused on the OCI argv path (vCPU is {@code HardeningArgs.cpus}).
 */
public record SandboxLimits(int cpuSec, int ramMb, long wallMs, int stdoutBytes, int stderrBytes) {

    /** No per-node overrides — argv still carries RAM/CPU floors. */
    public static SandboxLimits unbounded() {
        return new SandboxLimits(0, 0, 0L, 0, 0);
    }

    /** Convenience: only a RAM cap + a wall-clock deadline. */
    public static SandboxLimits of(int ramMb, long wallMs) {
        return new SandboxLimits(0, ramMb, wallMs, 0, 0);
    }
}
