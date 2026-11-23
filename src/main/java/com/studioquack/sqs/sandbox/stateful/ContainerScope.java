package com.studioquack.sqs.sandbox.stateful;

import com.studioquack.sqs.maintenance.HostServiceUninstaller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Starts a stateful container's {@code podman run} in its own transient systemd scope so the
 * container's monitor ({@code conmon}) is not a member of the engine's service cgroup. With the
 * systemd cgroup manager Podman moves the container into {@code machine.slice} but leaves {@code
 * conmon} in the cgroup of whoever ran {@code podman run}.
 */
final class ContainerScope {

    private static final Logger log = LoggerFactory.getLogger(ContainerScope.class);
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(10);

    /** {@code systemd-run} flags before {@code --unit}; {@code --user} is inserted for user units. */
    private final List<String> prefix;

    private ContainerScope(List<String> prefix) {
        this.prefix = prefix;
    }

    /**
     * Resolves the scope mode once: reads the engine's own cgroup, then proves {@code systemd-run}
     * can create a scope here (no user bus, missing polkit rights, or a container without systemd
     * all fail the probe and fall back to the plain launch).
     */
    static ContainerScope detect(String selfCgroup, Predicate<List<String>> probe) {
        List<String> prefix = prefixFor(selfCgroup);
        if (prefix.isEmpty()) {
            return new ContainerScope(List.of());
        }
        List<String> probeArgv = new ArrayList<>(prefix);
        probeArgv.add("true");
        if (!probe.test(probeArgv)) {
            log.warn("stateful: engine runs under systemd but 'systemd-run --scope' failed; stateful "
                    + "containers stay in the engine's cgroup and are restarted in place whenever the "
                    + "engine restarts (gVisor sandboxes then lose their network)");
            return new ContainerScope(List.of());
        }
        log.info("stateful: containers start in their own systemd scope ({} unit)",
                prefix.contains("--user") ? "user" : "system");
        return new ContainerScope(prefix);
    }

    /** Production detection: {@code /proc/self/cgroup} plus a probe through the stateful CLI seam. */
    static ContainerScope detect(OciCli cli) {
        if (!cli.spawnsHostProcesses()) {
            return new ContainerScope(List.of());
        }
        return detect(readSelfCgroup(), argv -> cli.run(argv, PROBE_TIMEOUT).ok());
    }

    /** {@code systemd-run} prefix for a {@code .service} cgroup; empty when not a systemd service. */
    static List<String> prefixFor(String selfCgroup) {
        if (HostServiceUninstaller.parseSystemdUnitFromCgroup(selfCgroup).isEmpty()) {
            return List.of();
        }
        List<String> p = new ArrayList<>(List.of("systemd-run"));
        if (selfCgroup.contains("/user@")) {
            p.add("--user");
        }
        p.addAll(List.of("--scope", "--quiet", "--collect"));
        return List.copyOf(p);
    }

    boolean enabled() {
        return !prefix.isEmpty();
    }

    /**
     * Wraps a {@code podman run} argv. The unit name carries the launch time because a replaced
     * instance reuses its container name while the predecessor's scope may still be draining.
     */
    List<String> wrap(List<String> podmanArgv, String containerName, long nowMillis) {
        if (prefix.isEmpty()) {
            return podmanArgv;
        }
        List<String> a = new ArrayList<>(prefix);
        a.add("--unit=" + unitSafe(containerName) + "-" + nowMillis);
        a.addAll(podmanArgv);
        return a;
    }

    /** systemd unit names allow {@code [A-Za-z0-9:_.\-]}; anything else becomes {@code _}. */
    static String unitSafe(String name) {
        String n = name == null || name.isBlank() ? "sq-stateful" : name;
        return n.replaceAll("[^A-Za-z0-9:_.\\-]", "_");
    }

    private static String readSelfCgroup() {
        Path cg = Path.of("/proc/self/cgroup");
        try {
            return Files.isRegularFile(cg) ? Files.readString(cg) : "";
        } catch (Exception e) {
            return "";
        }
    }
}
