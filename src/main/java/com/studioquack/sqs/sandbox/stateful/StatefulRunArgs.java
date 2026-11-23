package com.studioquack.sqs.sandbox.stateful;

import com.studioquack.sqs.sandbox.egress.SandboxNetworkGuard;
import com.studioquack.sqs.sandbox.oci.CgroupCaps;
import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure, side-effect-free builder of the {@code podman run} argv for a stateful instance (the
 * user's image running the user's long-running app). Kept dependency-free (no Spring, no exec) so
 * the relaxed security posture is asserted exactly in a unit test — the delta from the ephemeral
 * {@link HardeningArgs} default is a documented, tested relaxation, not an accident.
 */
public final class StatefulRunArgs {

    private final Spec spec;

    private StatefulRunArgs(Spec spec) {
        this.spec = spec;
    }

    /** Immutable input to {@link #toArgv()}. */
    public static final class Spec {
        String cliPath;
        String image;
        String containerName;
        String volumeName;
        String volumeMountPath = "/data";
        String restartPolicy = "unless-stopped";
        String runtimeFlag = "";
        String seccompProfilePath;
        /** Same meaning as {@link HardeningArgs}: isolation-enforce refuses an empty path. */
        boolean seccompRequired;
        StatefulCaps caps = StatefulCaps.uncapped();
        boolean dropAllCaps = true;
        String envId = "";
        String imageRef = "";
        String instanceId = "";
        /** Overlay reverse port stamped as a label so a daemon restart can re-publish. */
        Integer reversePort;
        /** The overlay network (interface name) the instance was started on. */
        String networkKey;
        /**
         * When true, stamp {@link #DISPATCH_LABEL}{@code =1} so {@code reconcileWithHost} can rebuild
         * {@code dispatchCapable} from the still-running container after a restart. Default false —
         * unlabeled images stay byte-identical to today's argv.
         */
        boolean dispatchCapable = false;
        List<String> containerCommand = List.of();
        Map<String, String> env = new LinkedHashMap<>();
        Integer publishHostPort;
        Integer publishContainerPort;
        /** Pinned resolvers; same default as the ephemeral path (never the bridge DNS). */
        List<String> dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.defaults();
        /** Pid cgroup cap. Always emitted unless {@link #unlimited}. */
        int pidsLimit = 1024;
        /** The built-in node library: no --memory / --cpus / --storage-opt / --pids-limit at all. */
        boolean unlimited;
        /** A temporary container ({@link StatefulInstance#isTemporary}): {@link #TEMPORARY_LABEL}. */
        boolean temporary;
    }

    /**
     * Marks a temporary container. It replaces {@code studioquack.sqs.stateful=1}, so the host
     * reconcile, container telemetry and every console table leave the container alone.
     */
    public static final String TEMPORARY_LABEL = "studioquack.sqs.temporary";

    /**
     * Label carrying the overlay reverse port this instance was started with. Survives a daemon
     * restart on the still-running container so {@code StatefulInstanceManager} can re-bind the
     * relay without waiting for another start command.
     */
    public static final String REVERSE_PORT_LABEL = "studioquack.sqs.reverse-port";

    /**
     * Label carrying the overlay network (interface name) this instance was started on. Survives a
     * daemon restart on the still-running container so a mid-drain restart can re-publish the reverse
     * port on the RIGHT network and leave the instance's routing pin meaning what it meant.
     */
    public static final String NETWORK_LABEL = "studioquack.sqs.network";

    /**
     * Image/container provenance label for dispatch capability. Copied onto the CONTAINER at {@code
     * podman run} (value always {@code 1} when capable) so {@code reconcileWithHost} can rebuild
     * {@code dispatchCapable} from inspect after a daemon restart.
     */
    public static final String DISPATCH_LABEL = "studioquack.sqs.dispatch";

    /**
     * Truthy image/container label values: "1" or "true" (ignore case, trimmed). Anything else is not capable.
     */
    public static boolean isDispatchLabelTruthy(String raw) {
        if (raw == null) {
            return false;
        }
        String v = raw.trim();
        return "1".equals(v) || "true".equalsIgnoreCase(v);
    }

    public static Builder builder(String cliPath, String image, String containerName) {
        return new Builder(cliPath, image, containerName);
    }

    /** Fluent builder — the argv has many optional dimensions so an all-args ctor is unwieldy. */
    public static final class Builder {
        private final Spec s = new Spec();

        private Builder(String cliPath, String image, String containerName) {
            s.cliPath = cliPath;
            s.image = image;
            s.containerName = containerName;
        }

        public Builder volumeName(String v) { s.volumeName = v; return this; }
        public Builder volumeMountPath(String v) { if (v != null && !v.isBlank()) s.volumeMountPath = v; return this; }
        public Builder restartPolicy(String v) { s.restartPolicy = v == null ? "" : v; return this; }
        public Builder runtimeFlag(String v) { s.runtimeFlag = v == null ? "" : v; return this; }
        public Builder seccompProfilePath(String v) { s.seccompProfilePath = v; return this; }
        public Builder seccompRequired(boolean v) { s.seccompRequired = v; return this; }
        public Builder caps(StatefulCaps v) { s.caps = v == null ? StatefulCaps.uncapped() : v; return this; }
        public Builder envId(String v) { s.envId = v == null ? "" : v; return this; }
        public Builder imageRef(String v) { s.imageRef = v == null ? "" : v; return this; }
        public Builder instanceId(String v) { s.instanceId = v == null ? "" : v; return this; }

        /**
         * Records the overlay reverse port on the container so a daemon restart can re-publish
         * the same posture. {@code null} / out-of-range = no reverse channel (no label).
         */
        public Builder reversePort(Integer v) {
            s.reversePort = (v != null && v >= 1 && v <= 65535) ? v : null;
            return this;
        }

        /**
         * Records the overlay network the instance starts on, so a mid-drain daemon restart re-binds its
         * reverse port and its routing pin on the right network.
         */
        public Builder networkKey(String v) {
            s.networkKey = (v == null || v.isBlank()) ? null : v.trim();
            return this;
        }

        /**
         * When true, stamp {@link #DISPATCH_LABEL}{@code =1} on the container. False / default
         * emits nothing so unlabeled images stay byte-identical.
         */
        public Builder dispatchCapable(boolean v) { s.dispatchCapable = v; return this; }

        public Builder containerCommand(List<String> v) { s.containerCommand = v == null ? List.of() : v; return this; }

        /**
         * The resolvers pinned via {@code --dns} (from {@code sqs.sandbox.dns.servers}). Daemon-owned and
         * re-validated here; null/empty falls back to the public default — a stateful instance must never
         * resolve through the bridge gateway into the host's internal DNS either.
         */
        public Builder dnsServers(List<String> v) {
            s.dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.validate(v);
            return this;
        }

        /**
         * Publishes a LOOPBACK-only host port to a container port ({@code -p 127.0.0.1:host:container}).
         * Opt-in and rare: normal stateful instances reach the daemon via the in-container helper broker
         * UDS; this is for the few daemon-managed services (e.g.
         */
        public Builder publishPort(int hostPort, int containerPort) {
            s.publishHostPort = hostPort;
            s.publishContainerPort = containerPort;
            return this;
        }

        public Builder env(String key, String value) {
            if (key != null && !key.isBlank()) {
                s.env.put(key, value == null ? "" : value);
            }
            return this;
        }

        /** {@code --pids-limit}. Must be {@code > 0}. */
        public Builder pidsLimit(int v) {
            s.pidsLimit = v;
            return this;
        }

        /** True only for the built-in node library ({@link StatefulInstance#isBuiltinNodeLibrary}). */
        public Builder unlimited(boolean v) {
            s.unlimited = v;
            return this;
        }

        /** True only for a temporary container ({@link StatefulInstance#isTemporary}). */
        public Builder temporary(boolean v) {
            s.temporary = v;
            return this;
        }

        public StatefulRunArgs build() {
            return new StatefulRunArgs(s);
        }
    }

    /** Builds the full {@code podman run} argv, starting with the CLI binary path. */
    public List<String> toArgv() {
        List<String> a = new ArrayList<>();
        a.add(spec.cliPath);
        a.add("run");
        // Long-lived: detached, NAMED, NOT --rm, with a sensible restart policy.
        a.add("-d");
        a.add("--name");
        a.add(spec.containerName);
        if (spec.restartPolicy != null && !spec.restartPolicy.isEmpty()) {
            a.add("--restart");
            a.add(spec.restartPolicy);
        }

        // Provenance labels — a reaper / status refresh finds the instance by these.
        a.add("--label");
        a.add(spec.temporary ? TEMPORARY_LABEL + "=1" : "studioquack.sqs.stateful=1");
        a.add("--label");
        a.add("studioquack.sqs.env=" + spec.envId);
        a.add("--label");
        a.add("studioquack.sqs.ref=" + spec.imageRef);
        a.add("--label");
        a.add("studioquack.sqs.instance=" + spec.instanceId);
        if (spec.reversePort != null) {
            a.add("--label");
            a.add(REVERSE_PORT_LABEL + "=" + spec.reversePort);
        }
        if (spec.networkKey != null) {
            a.add("--label");
            a.add(NETWORK_LABEL + "=" + spec.networkKey);
        }
        // Dispatch-capable provenance — only when true; unlabeled argv stays byte-identical.
        if (spec.dispatchCapable) {
            a.add("--label");
            a.add(DISPATCH_LABEL + "=1");
        }

        // gVisor selection on the best tier (kept from the ephemeral posture).
        if (spec.runtimeFlag != null && !spec.runtimeFlag.isEmpty()) {
            a.add("--runtime");
            a.add(spec.runtimeFlag);
        }

        // Egress — segmentation: dedicated stateful network (not Podman default bridge), distinct from
        // SandboxNetworks.JOBS so a job sandbox is not L2-adjacent. This raises the bar but does not
        // enforce RFC1918 / link-local / podman-CIDR / host-loopback deny — that is host nftables/iptables
        // work.
        for (String badHost : HardeningArgs.CLOUD_METADATA_HOSTS) {
            a.add("--add-host");
            a.add(badHost + ":127.0.0.1");
        }
        a.add("--network");
        a.add(SandboxNetworks.STATEFUL);
        // Pin resolvers BEFORE the image (podman: last --dns wins; a tail-injected one is guard-refused
        // below). Stateful always attaches a real network, so the pins always apply.
        for (String dns : spec.dnsServers) {
            a.add("--dns");
            a.add(dns);
        }

        // Daemon→instance port publish (opt-in, loopback-only) — see Builder#publishPort.
        if (spec.publishHostPort != null && spec.publishContainerPort != null) {
            a.add("-p");
            a.add("127.0.0.1:" + spec.publishHostPort + ":" + spec.publishContainerPort);
        }

        // Named persistent (encrypted) volume — the RELAXATION vs tmpfs-only. Rootfs stays
        // writable (no --read-only) so the user's server can run; a small /tmp tmpfs is still
        // provided for scratch that should never persist.
        if (spec.volumeName != null && !spec.volumeName.isEmpty()) {
            a.add("-v");
            a.add(spec.volumeName + ":" + spec.volumeMountPath);
        }
        a.add("--tmpfs");
        a.add("/tmp:rw,nosuid,nodev,size=64m,mode=1777");

        // Privilege drop KEPT (still hardened) — but NOT --read-only and NOT --user 65534.
        if (spec.dropAllCaps) {
            a.add("--cap-drop");
            a.add("ALL");
        }
        a.add("--security-opt");
        a.add("no-new-privileges");
        if (spec.seccompProfilePath != null && !spec.seccompProfilePath.isEmpty()) {
            a.add("--security-opt");
            a.add("seccomp=" + spec.seccompProfilePath);
        } else if (spec.seccompRequired) {
            throw new IllegalStateException(
                    "sandbox: seccomp profile path empty while isolation-enforce requires a profile");
        }

        // Resource caps (kept — "caps always applied" invariant holds for stateful too), except a
        // controller the host proved it cannot apply (CgroupCaps) and the built-in node library, which
        // runs with none (unlimited).
        StatefulCaps caps = spec.unlimited ? StatefulCaps.uncapped() : spec.caps;
        if (caps.ramMb() > 0 && CgroupCaps.applies(CgroupCaps.Controller.MEMORY)) {
            a.add("--memory");
            a.add(caps.ramMb() + "m");
            a.add("--memory-swap");
            a.add(caps.ramMb() + "m");
        }
        if (caps.cpu() > 0 && CgroupCaps.applies(CgroupCaps.Controller.CPU)) {
            a.add("--cpus");
            a.add(Integer.toString(caps.cpu()));
        }
        if (caps.diskMb() > 0) {
            // Writable-layer size cap (overlay). Best-effort: runtimes without the storage
            // driver opt ignore it; the named volume carries the persistent data regardless.
            a.add("--storage-opt");
            a.add("size=" + caps.diskMb() + "m");
        }
        // One shared flag shape with the ephemeral/warm path (HardeningArgs).
        HardeningArgs.appendGpuCap(a, caps.hasGpu() ? caps.gpu() : 0);
        // Every stateful container gets a pid cap, including platform side-cars whose RAM/CPU are
        // otherwise the host ceiling or unset.
        if (!spec.unlimited) {
            if (spec.pidsLimit <= 0) {
                throw new IllegalStateException("stateful: --pids-limit must be > 0");
            }
            if (CgroupCaps.applies(CgroupCaps.Controller.PIDS)) {
                a.add("--pids-limit");
                a.add(Integer.toString(spec.pidsLimit));
            }
        }

        // Name only. The caller puts the values on the podman child process.
        com.studioquack.sqs.sandbox.runtime.ContainerEnv.appendNameOnly(a, spec.env);

        // Image + (rarely) command override — the image entrypoint IS the user's app.
        int imageIndex = a.size();
        a.add(HardeningArgs.requireSafeImageRef(spec.image));
        a.addAll(spec.containerCommand);
        // Hard refusal of host-network / self-selected-network / non-loopback publish, and refusal of a
        // tail-injected --dns override. Scans the full argv including any attacker-controlled
        // containerCommand tail.
        SandboxNetworkGuard.validate(a, imageIndex);
        return a;
    }

    /** Container environment whose values must ride the podman child process, not argv. */
    public Map<String, String> containerEnv() {
        return Map.copyOf(spec.env);
    }

    public String containerName() {
        return spec.containerName;
    }
}
