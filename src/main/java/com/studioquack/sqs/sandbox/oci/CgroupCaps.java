package com.studioquack.sqs.sandbox.oci;

import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cgroup controllers this host's container runtime cannot apply. Rootless Podman without delegated
 * controllers (a WSL podman machine on Windows, a Linux host without a systemd user session)
 * accepts {@code --memory} / {@code --cpus} / {@code --pids-limit}, then crun refuses the
 * container ({@code memory.max} missing, or "controller `pids` is not available") and no sandbox
 * starts at all.
 */
@Slf4j
public final class CgroupCaps {

    /** Cgroup v2 controllers behind the sandbox caps. */
    public enum Controller { MEMORY, CPU, PIDS }

    private static final Set<Controller> MISSING = ConcurrentHashMap.newKeySet();
    private static final Pattern UNAVAILABLE_CONTROLLER = Pattern.compile("controller `?([a-z]+)`? is not available");

    private CgroupCaps() {
    }

    /** False once a detached start proved the host cannot apply {@code controller}. */
    public static boolean applies(Controller controller) {
        return !MISSING.contains(controller);
    }

    /**
     * Marks the controller whose control file the runtime could not open. Pass the stderr of a
     * detached {@code podman run -d} only.
     *
     * @return true when this call newly marked a controller — the caller retries the start once
     *         without it; false for any other failure or an already-known controller
     */
    public static boolean noteDetachedStartFailure(CharSequence runtimeStderr) {
        Controller controller = missingController(runtimeStderr);
        if (controller == null || !MISSING.add(controller)) {
            return false;
        }
        log.warn("sandbox: the container runtime cannot apply the cgroup {} controller on this host — "
                        + "sandbox containers start without that limit from now on (degraded posture)",
                controller.name().toLowerCase(Locale.ROOT));
        return true;
    }

    /**
     * The controller the runtime reported it cannot use, or null. crun words it two ways:
     * "open `memory.max` for writing: No such file or directory" and "controller `pids` is not
     * available under …/cgroup.controllers" (both seen on a Windows engine, Nov 2026).
     */
    public static Controller missingController(CharSequence output) {
        if (output == null) {
            return null;
        }
        String text = output.toString();
        Matcher unavailable = UNAVAILABLE_CONTROLLER.matcher(text);
        if (unavailable.find()) {
            return switch (unavailable.group(1)) {
                case "memory" -> Controller.MEMORY;
                case "cpu", "cpuset" -> Controller.CPU;
                case "pids" -> Controller.PIDS;
                default -> null;
            };
        }
        if (!text.toLowerCase(Locale.ROOT).contains("no such file or directory")) {
            return null;
        }
        if (text.contains("memory.max") || text.contains("memory.swap.max")) {
            return Controller.MEMORY;
        }
        if (text.contains("cpu.max") || text.contains("cpu.weight")) {
            return Controller.CPU;
        }
        if (text.contains("pids.max")) {
            return Controller.PIDS;
        }
        return null;
    }

    /** Tests only: the flags are process-wide. */
    static void resetForTests() {
        MISSING.clear();
    }
}
