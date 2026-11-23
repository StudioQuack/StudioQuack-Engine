package com.studioquack.sqs.sandbox.runtime;

/**
 * One low-level OCI runtime the host's container CLI advertises (runc, runsc/gVisor, kata, crun…).
 *
 * @param name The value passed to {@code --runtime <name>}.
 * @param displayName Human-readable label ("gVisor (runsc)", "runc (kernel-shared)").
 * @param gvisor True when the name matches the gVisor pattern ({@code runsc}/{@code runsc-*}).
 */
public record DetectedOciRuntime(String name, String displayName, boolean gvisor,
                                 boolean isDefault, String source) {

    /** Builds a detected-runtime with a friendly display name derived from the runtime name. */
    public static DetectedOciRuntime of(String name, String defaultName, String source) {
        boolean gvisor = name != null
                && (name.equalsIgnoreCase("runsc") || name.toLowerCase().startsWith("runsc-"));
        String display = name;
        if (gvisor) {
            display = "gVisor (" + name + ")";
        } else if ("runc".equalsIgnoreCase(name)) {
            display = "runc (kernel-shared)";
        } else if ("kata-runtime".equalsIgnoreCase(name) || "kata".equalsIgnoreCase(name)) {
            display = "Kata Containers (" + name + ")";
        } else if ("crun".equalsIgnoreCase(name)) {
            display = "crun (cgroupv2-native)";
        }
        boolean isDefault = name != null && name.equalsIgnoreCase(defaultName);
        return new DetectedOciRuntime(name, display, gvisor, isDefault, source);
    }
}
