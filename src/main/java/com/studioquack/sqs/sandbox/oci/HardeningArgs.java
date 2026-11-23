package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.egress.SandboxNetworkGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pure, side-effect-free builder of the per-job container-runtime {@code run} argv. It is kept
 * dependency-free (no Spring, no process exec) so the hardening flags can be asserted exactly in a
 * unit test — the security posture is code that must be tested, not trusted.
 */
public final class HardeningArgs {

    private static final Logger log = LoggerFactory.getLogger(HardeningArgs.class);

    /** In-container sandbox working dir the bootstrap writes the script + {@code .env} into. */
    public static final String SANDBOX_DIR = "/sandbox";
    /** Marks a container as ours at all — the blanket filter an uninstall reaps by. */
    public static final String LABEL_SANDBOX = "studioquack.sqs";
    /** The slot/job language. */
    public static final String LABEL_LANG = "studioquack.sqs.lang";
    /** {@link SandboxOwnership#ownerId()} — which installed engine created this container. */
    public static final String LABEL_OWNER = "studioquack.sqs.owner";
    /** {@link SandboxOwnership#bootId()} — which run of that engine created it. */
    public static final String LABEL_BOOT = "studioquack.sqs.boot";
    /** {@link #ROLE_WARM} or {@link #ROLE_JOB}. */
    public static final String LABEL_ROLE = "studioquack.sqs.role";
    /** A parked warm-pool slot: idle until a dispatch execs into it, and safe to reclaim when unknown. */
    public static final String ROLE_WARM = "warm";
    /** A single-job container: only ever reclaimable once the run that owns it is provably gone. */
    public static final String ROLE_JOB = "job";
    /** Non-root in-container uid:gid (nobody). */
    public static final String SANDBOX_UID = "65534:65534";
    /** Per-container pid cap: ample for interpreters + helpers, far below any host pid_max. */
    public static final int PIDS_LIMIT = 512;
    /** Floor when the node declares no RAM (MiB). */
    public static final int FLOOR_RAM_MB = 512;
    /** Floor when the node / daemon declares no vCPU cap. */
    public static final double FLOOR_CPUS = 1.0;

    /**
     * Cloud-metadata hostnames DNS-nullified under {@link EgressPolicy#OPEN} via {@code --add-host
     * name:127.0.0.1}. This is name-resolution only.
     */
    public static final List<String> CLOUD_METADATA_HOSTS = List.of(
            "metadata.google.internal",
            "metadata.goog",
            "instance-data",
            "instance-data.ec2.internal");

    private final Spec spec;

    private HardeningArgs(Spec spec) {
        this.spec = spec;
    }

    /** Immutable input to {@link #toArgv()}. Use {@link #builder(String, SandboxLanguage, String)}. */
    public static final class Spec {
        String cliPath;
        SandboxLanguage language;
        String image;
        String runtimeFlag = "";
        EgressPolicy egress = EgressPolicy.DEFAULT;
        SandboxLimits limits = SandboxLimits.unbounded();
        double cpus;
        String seccompProfilePath;
        /**
         * When true ({@code sqs.sandbox.isolation-enforce}), an empty seccomp path refuses
         * argv emission instead of omitting the flag.
         */
        boolean seccompRequired;
        String depCacheHostDir;
        boolean detached;
        /**
         * When {@code true}, emit {@code --entrypoint ""} so the image's own ENTRYPOINT is bypassed at
         * create: the idle/parked container's real command stays the hardening {@code sleep}, and the
         * recorded entrypoint is exec'd per job instead.
         */
        boolean clearEntrypoint;
        /** When {@code true} (default) emit {@code --rm}; {@code false} keeps the exited container for inspect. */
        boolean autoRemove = true;
        SandboxOwnership ownership = SandboxOwnership.unowned();
        String role = ROLE_JOB;
        List<String> containerCommand = List.of();
        List<String> env = new ArrayList<>();
        int sandboxTmpfsMb = 64;
        /** Pinned resolvers; defaults so a caller that forgets still never leaks. */
        List<String> dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.defaults();
        /** GPU count cap ({@code 0} = none). Emitted only via {@link #appendGpuCap}. */
        int gpus;
    }

    public static Builder builder(String cliPath, SandboxLanguage language, String image) {
        return new Builder(cliPath, language, image);
    }

    /** Fluent builder — the argv has many optional dimensions so an all-args constructor is unwieldy. */
    public static final class Builder {
        private final Spec s = new Spec();

        private Builder(String cliPath, SandboxLanguage language, String image) {
            s.cliPath = cliPath;
            s.language = language;
            s.image = image;
        }

        public Builder runtimeFlag(String v) { s.runtimeFlag = v == null ? "" : v; return this; }
        public Builder egress(EgressPolicy v) { s.egress = v == null ? EgressPolicy.DEFAULT : v; return this; }
        public Builder limits(SandboxLimits v) { s.limits = v == null ? SandboxLimits.unbounded() : v; return this; }
        public Builder cpus(double v) { s.cpus = v; return this; }

        /**
         * GPU count cap for this container ({@code <= 0} = none). Callers must gate this on {@code
         * GpuCapabilityProbe}: podman resolves {@code --gpus} through CDI, so a host without GPU
         * passthrough fails the whole {@code run} — the cap must never be ATTEMPTED there.
         */
        public Builder gpus(int v) { s.gpus = Math.max(0, v); return this; }
        public Builder seccompProfilePath(String v) { s.seccompProfilePath = v; return this; }
        public Builder seccompRequired(boolean v) { s.seccompRequired = v; return this; }
        public Builder depCacheHostDir(String v) { s.depCacheHostDir = v; return this; }
        public Builder sandboxTmpfsMb(int v) { s.sandboxTmpfsMb = v > 0 ? v : 64; return this; }
        public Builder containerCommand(List<String> v) { s.containerCommand = v == null ? List.of() : v; return this; }

        /**
         * The resolvers pinned via {@code --dns} (from {@code sqs.sandbox.dns.servers}). Daemon-owned: the
         * value comes from operator config, never from the dispatched workload; entries are re-validated
         * as IP literals here (the last line before argv emission) and a null/empty list falls back to the
         * public default — a sandbox must never resolve through the bridge gateway's forwarder into the
         * host's internal DNS.
         */
        public Builder dnsServers(List<String> v) {
            s.dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.validate(v);
            return this;
        }

        /** Warm-pool mode: emit {@code -d} (detached, long-lived) instead of {@code -i} (per-job stdin). */
        public Builder detached(boolean v) { s.detached = v; return this; }

        /**
         * Entrypoint (custom) images: bypass the image ENTRYPOINT at create so the idle container command
         * actually runs. Script-mode language images must not set this: their bootstrap argv is a plain
         * Cmd the entrypoint-free base images already run directly.
         */
        public Builder clearEntrypoint(boolean v) { s.clearEntrypoint = v; return this; }

        /**
         * When {@code false}, omit {@code --rm} so a test run can inspect the exited container.
         * Default {@code true} preserves existing argv.
         */
        public Builder autoRemove(boolean v) { s.autoRemove = v; return this; }

        /** Stamps the owner/boot provenance a reclaim pass matches on. */
        public Builder ownership(SandboxOwnership v) {
            s.ownership = v == null ? SandboxOwnership.unowned() : v;
            return this;
        }

        /** {@link #ROLE_WARM} for a parked warm slot, {@link #ROLE_JOB} for a single-job container. */
        public Builder role(String v) { s.role = (v == null || v.isBlank()) ? ROLE_JOB : v; return this; }

        /** Adds an allow-listed {@code KEY=VALUE} container env var ({@code -e}). */
        public Builder env(String key, String value) {
            s.env.add(key + "=" + (value == null ? "" : value));
            return this;
        }

        public HardeningArgs build() {
            return new HardeningArgs(s);
        }
    }

    /**
     * The on-disk dep-cache <b>bucket</b> for a language — the sub-directory the {@code DependencyInstaller}
     * actually installs into. python → {@code python} (pip {@code --target}); javascript/typescript/node
     * all share {@code node} (npm {@code --prefix}). This closes the "installer writes {@code node/} but
     * the mount looks for {@code javascript/}" gap that silently skipped the JS/TS dep mount.
     */
    public static String depBucket(SandboxLanguage language) {
        String w = language.wire();
        return ("javascript".equals(w) || "typescript".equals(w) || "node".equals(w)) ? "node" : w;
    }

    /** In-container dep-cache mount path for a language, e.g. {@code /opt/sq/deps/python}. */
    public static String depContainerDir(SandboxLanguage language) {
        return "/opt/sq/deps/" + depBucket(language);
    }

    /** Builds the full argv, starting with the CLI binary path. */
    public List<String> toArgv() {
        List<String> a = new ArrayList<>();
        a.add(spec.cliPath);
        a.add("run");
        if (spec.autoRemove) {
            a.add("--rm");
        }
        // Per-job: pipe the script + .env into the tmpfs sandbox via stdin (-i). Warm-pool: run
        // detached (-d) so the hardened container stays alive for a later `exec`.
        a.add(spec.detached ? "-d" : "-i");
        a.add("--init");

        // Provenance labels — a reaper can find leftover sandbox containers by label. Owner + boot +
        // role are what let a LIVE daemon tell its own abandoned container from a co-resident
        // engine's working one; uninstall ignores all three and reaps on the blanket label.
        a.add("--label");
        a.add(LABEL_SANDBOX + "=1");
        a.add("--label");
        a.add(LABEL_LANG + "=" + (spec.language != null ? spec.language.wire() : "custom"));
        a.add("--label");
        a.add(LABEL_OWNER + "=" + spec.ownership.ownerId());
        a.add("--label");
        a.add(LABEL_BOOT + "=" + spec.ownership.bootId());
        a.add("--label");
        a.add(LABEL_ROLE + "=" + spec.role);

        // gVisor selection on the best tier.
        if (spec.runtimeFlag != null && !spec.runtimeFlag.isEmpty()) {
            a.add("--runtime");
            a.add(spec.runtimeFlag);
        }

        // Egress policy.
        applyEgress(a);

        // Read-only rootfs + tmpfs scratch (the ONLY writable paths).
        a.add("--read-only");
        a.add("--tmpfs");
        a.add("/tmp:rw,nosuid,nodev,size=64m,mode=1777");
        a.add("--tmpfs");
        a.add("/run:rw,nosuid,nodev,size=8m,mode=1777");
        // Podman cannot portably quota the overlay rootfs (--storage-opt size= needs an xfs/pquota storage
        // driver, absent under podman-machine); the job's only writable payload path is the /sandbox tmpfs
        // (rootfs is --read-only), so the disc cap is enforced as that tmpfs's size. /tmp and /run stay at
        // fixed sizes (no job payloads land there).
        a.add("--tmpfs");
        a.add(SANDBOX_DIR + ":rw,nosuid,nodev,size=" + spec.sandboxTmpfsMb + "m,mode=1777");

        // Privilege drop.
        a.add("--cap-drop");
        a.add("ALL");
        a.add("--security-opt");
        a.add("no-new-privileges");
        if (spec.seccompProfilePath != null && !spec.seccompProfilePath.isEmpty()) {
            a.add("--security-opt");
            a.add("seccomp=" + spec.seccompProfilePath);
        } else if (spec.seccompRequired) {
            throw new IllegalStateException(
                    "sandbox: seccomp profile path empty while isolation-enforce requires a profile");
        } else {
            log.warn("sandbox: seccomp profile path empty — omitting --security-opt seccomp; runtime default applies");
        }

        // Non-root uid.
        a.add("--user");
        a.add(SANDBOX_UID);

        // Resource caps — silent node still gets a RAM + CPU floor (pids always). A controller the host
        // proved it cannot apply is left out (CgroupCaps).
        int ramMb = spec.limits != null && spec.limits.ramMb() > 0
                ? spec.limits.ramMb() : FLOOR_RAM_MB;
        if (CgroupCaps.applies(CgroupCaps.Controller.MEMORY)) {
            a.add("--memory");
            a.add(ramMb + "m");
            a.add("--memory-swap");   // Disable swap → OOM instead of swap-out (parity).
            a.add(ramMb + "m");
        }
        double cpus = spec.cpus > 0 ? spec.cpus : FLOOR_CPUS;
        if (CgroupCaps.applies(CgroupCaps.Controller.CPU)) {
            a.add("--cpus");
            a.add(trimCpus(cpus));
        }
        // A fork bomb inside the sandbox must exhaust the container's pid cgroup, not the host's.
        if (CgroupCaps.applies(CgroupCaps.Controller.PIDS)) {
            a.add("--pids-limit");
            a.add(Integer.toString(PIDS_LIMIT));
        }
        appendGpuCap(a, spec.gpus);

        // Dep-cache RO bind mount — language-less (custom entrypoint) images have no dep bucket.
        if (spec.language != null && spec.depCacheHostDir != null && !spec.depCacheHostDir.isEmpty()) {
            a.add("-v");
            a.add(spec.depCacheHostDir + ":" + depContainerDir(spec.language) + ":ro");
        }

        // Working dir + env. Name only; values ride the child process.
        a.add("-w");
        a.add(SANDBOX_DIR);
        com.studioquack.sqs.sandbox.runtime.ContainerEnv.appendNameOnly(a, containerEnv());

        // Entrypoint images: reset ENTRYPOINT so the parked/idle container runs the hardening sleep
        // instead of the (possibly one-shot) app, which is exec'd per job. Pre-image, so it sits in the
        // trusted flag region the network guard scans as ours.
        if (spec.clearEntrypoint) {
            a.add("--entrypoint");
            a.add("");
        }

        // Image + command. Capture the image index so the network guard can tell the daemon's
        // own pre-image flags (trusted) from the attacker-controlled command tail (not).
        int imageIndex = a.size();
        a.add(requireSafeImageRef(spec.image));
        a.addAll(spec.containerCommand);
        // Hard refusal of host-network / self-selected-network / non-loopback publish, and refusal of a
        // tail-injected --dns override. Scans the full argv including any attacker-controlled
        // containerCommand tail.
        SandboxNetworkGuard.validate(a, imageIndex);
        return a;
    }

    /** Container environment whose values must ride the podman child process, not argv. */
    public Map<String, String> containerEnv() {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        for (String e : spec.env) {
            if (e == null || e.isBlank()) {
                continue;
            }
            int eq = e.indexOf('=');
            if (eq < 0) {
                out.put(e, "");
            } else {
                out.put(e.substring(0, eq), e.substring(eq + 1));
            }
        }
        return out;
    }

    private void applyEgress(List<String> a) {
        if (spec.egress == EgressPolicy.NONE) {
            a.add("--network");
            a.add("none");
            return;
        }
        // OPEN — segmentation: dedicated jobs network (not Podman default bridge), distinct from
        // SandboxNetworks.STATEFUL so a sandbox is not L2-adjacent to a stateful instance. This raises the
        // bar but does not enforce RFC1918 / link-local / podman-CIDR / host-loopback deny — that is host
        // nftables/iptables work.
        for (String badHost : CLOUD_METADATA_HOSTS) {
            a.add("--add-host");
            a.add(badHost + ":127.0.0.1");
        }
        a.add("--network");
        a.add(SandboxNetworks.JOBS);
        // Pin resolvers BEFORE the image so the workload can never set them (podman: last --dns wins; the
        // tail is guard-refused). Without these the container resolves via the bridge gateway's dnsmasq →
        // the HOST's resolver → internal LAN names leak.
        for (String dns : spec.dnsServers) {
            a.add("--dns");
            a.add(dns);
        }
    }

    /**
     * Fail-closed image-ref check at the last line before argv emission. The CLI parses flags until
     * the first positional, so a ref that is blank or begins with {@code -} would be consumed as a
     * FLAG ({@code --privileged}, {@code --cap-add}, …) instead of as the image — the one argv slot a
     * pullable image reference can reach.
     */
    public static String requireSafeImageRef(String image) {
        if (image == null || image.isBlank() || image.startsWith("-")) {
            throw new IllegalArgumentException(
                    "refused sandbox image ref " + (image == null ? "<null>" : "'" + image + "'")
                            + ": blank or leading '-' would be parsed as a runtime flag, not an image");
        }
        return image;
    }

    /** Renders a cpu-count with no trailing zeros ({@code 1.5}, {@code 2}). */
    private static String trimCpus(double cpus) {
        if (cpus == Math.floor(cpus)) {
            return Integer.toString((int) cpus);
        }
        return java.math.BigDecimal.valueOf(cpus).stripTrailingZeros().toPlainString();
    }

    /**
     * The ONE place the {@code --gpus <count>} flag shape is built, shared by the ephemeral /
     * warm-pool path (here) and the stateful path ({@code StatefulRunArgs}), so a GPU cap can never
     * drift into two divergent spellings. Podman resolves {@code --gpus} through CDI device specs;
     * emitting it on a host without passthrough fails the whole {@code run}, which is why every caller
     * gates on {@code GpuCapabilityProbe} first.
     */
    public static void appendGpuCap(List<String> argv, int gpuCount) {
        if (gpuCount > 0) {
            argv.add("--gpus");
            argv.add(Integer.toString(gpuCount));
        }
    }
}
