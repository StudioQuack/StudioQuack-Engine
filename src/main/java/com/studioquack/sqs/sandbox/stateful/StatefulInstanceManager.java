package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.command.BespokeFilePaths;
import com.studioquack.sqs.sandbox.helperchan.HelperPipeChannel;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.oci.CgroupCaps;
import com.studioquack.sqs.sandbox.oci.HelperBroker;
import com.studioquack.sqs.sandbox.oci.ShExec;
import com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;
import com.studioquack.sqs.sandbox.oci.SeccompProfile;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The daemon-side control plane for stateful custom-OCI instances: the podman flow behind the four
 * lifecycle command handlers. Runs {@code podman} entirely through the {@link OciCli} seam
 * (process exec, never a container Java client) and keeps an in-memory registry keyed by {@code
 * (env, imageRef)} → live instance and {@code instanceId} → live instance, so a per-node dispatch
 * can resolve {@code (env+imageRef) → containerId} without a new container.
 */
@Slf4j
@Component
public class StatefulInstanceManager {

    /** Matches a sha256 content digest anywhere in podman {@code load} output. */
    private static final Pattern SHA256 = Pattern.compile("sha256:[0-9a-f]{12,64}");

    /** Operator-/app-facing note when plant/readiness/launch fails (BYO image missing python3). */
    static final String BROKER_NOTE_NO_RUNTIME =
            "image has no usable broker runtime (python3 is required for the in-container helper "
                    + "broker); helper callbacks unavailable";
    /** Note when a previously-ready broker pipe dies between dispatches. */
    static final String BROKER_NOTE_DIED =
            "helper broker died; restart the instance to restore helper callbacks";
    /** Note when the post-launch frame round-trip probe fails (broker up, pipe wedged). */
    static final String BROKER_NOTE_PROBE_FAILED =
            "helper broker frame round-trip broken (probe failed); restart the instance to "
                    + "restore helper callbacks";

    private final OciCli cli;
    private final ObjectMapper mapper;
    private final ContainerRuntime runtime;
    private final StatefulRuntimeProperties props;
    private final OciImageBlobStore blobStore;
    private final HelperRequestRouter helperRouter;
    private final boolean helperBrokerEnabled;
    private final int helperBrokerReadyTimeoutSec;
    private final com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager;
    /** Reverse-port publisher; null in slice tests that never publish. */
    private final com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay reverseRelay;
    /**
     * Inbound Network Access Control projection. Restart recovery refuses to re-publish a port whose
     * rules were deleted while the daemon was down.
     */
    private final com.studioquack.sqs.sandbox.reverse.NetworkRuleStore networkRuleStore;
    /** Daemon-owned DNS pins for the stateful {@code podman run} argv (validated once). */
    private final java.util.List<String> dnsServers;
    /** Pid cgroup cap on every stateful start. Default 1024; config must be &gt; 0. */
    private int pidsLimit = 1024;
    /** XFS-only {@code --storage-opt size=} gate; probed once from the runtime's own store info. */
    private final OverlaySizeQuotaProbe sizeQuotaProbe;
    /** One WARN per daemon lifetime when the disk cap is dropped on a non-XFS store. */
    private final java.util.concurrent.atomic.AtomicBoolean diskQuotaDropLogged =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** (env|imageRef) → live instance (the dispatch routing key). */
    private final Map<String, StatefulInstance> byKey = new ConcurrentHashMap<>();
    /** instanceId → instance (the stop/status key). */
    private final Map<String, StatefulInstance> byInstanceId = new ConcurrentHashMap<>();
    /**
     * Temporary containers by instance id, each alive for one Create Node trial job. Kept apart
     * from {@link #byKey} and {@link #byInstanceId} so no routing, inventory or reconcile reaches them.
     */
    private final Map<String, StatefulInstance> temporaries = new ConcurrentHashMap<>();
    /** containerId → open helper-broker pipe (closed on stop / replace / failed-start cleanup). */
    private final Map<String, HelperPipeChannel> brokerChannels = new ConcurrentHashMap<>();
    /**
     * Per-(env, imageRef) start locks so two racing starts cannot both pass the live-instance check
     * and both launch. Values are plain monitor objects.
     */
    private final ConcurrentHashMap<String, Object> startLocks = new ConcurrentHashMap<>();
    /**
     * Unary invokes + stream sessions currently in the container. Keyed by container id (not
     * instanceId) so a blue/green replace can wait on the old container while the successor shares the
     * same instanceId.
     */
    private final ConcurrentHashMap<String, AtomicInteger> inFlightByContainer = new ConcurrentHashMap<>();
    private static final int DISPATCH_CLOSED = Integer.MIN_VALUE;
    /**
     * Old containers waiting for in-flight work after {@link #replaceWithCurrentImage} switched
     * routing to the successor. Keyed by container id (same as {@link #inFlightByContainer}).
     */
    private final ConcurrentHashMap<String, StatefulInstance> drainingByContainerId = new ConcurrentHashMap<>();

    /** Back-compat: tests that omit egress readiness skip the gate (null manager). */
    public StatefulInstanceManager(OciCli cli, ObjectMapper mapper, ContainerRuntime runtime,
                                   StatefulRuntimeProperties props, OciImageBlobStore blobStore,
                                   HelperRequestRouter helperRouter,
                                   boolean helperBrokerEnabled,
                                   int helperBrokerReadyTimeoutSec) {
        this(cli, mapper, runtime, props, blobStore, helperRouter, helperBrokerEnabled,
                helperBrokerReadyTimeoutSec, null);
    }

    /** Back-compat: tests that omit the reverse relay never publish a reverse port. */
    public StatefulInstanceManager(OciCli cli, ObjectMapper mapper, ContainerRuntime runtime,
                                   StatefulRuntimeProperties props, OciImageBlobStore blobStore,
                                   HelperRequestRouter helperRouter,
                                   boolean helperBrokerEnabled,
                                   int helperBrokerReadyTimeoutSec,
                                   com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager) {
        // Empty DNS list (not null) so the call can't be confused with the String-raw @Autowired ctor;
        // validate() maps empty to the public default pin set.
        this(cli, mapper, runtime, props, blobStore, helperRouter, helperBrokerEnabled,
                helperBrokerReadyTimeoutSec, egressManager, null, null, java.util.List.of());
    }

    /** Back-compat: tests that publish without restart-recovery skip the rule-store gate. */
    public StatefulInstanceManager(OciCli cli, ObjectMapper mapper, ContainerRuntime runtime,
                                   StatefulRuntimeProperties props, OciImageBlobStore blobStore,
                                   HelperRequestRouter helperRouter,
                                   boolean helperBrokerEnabled,
                                   int helperBrokerReadyTimeoutSec,
                                   com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager,
                                   com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay reverseRelay) {
        this(cli, mapper, runtime, props, blobStore, helperRouter, helperBrokerEnabled,
                helperBrokerReadyTimeoutSec, egressManager, reverseRelay, null, java.util.List.of());
    }

    @org.springframework.beans.factory.annotation.Autowired
    public StatefulInstanceManager(OciCli cli, ObjectMapper mapper, ContainerRuntime runtime,
                                   StatefulRuntimeProperties props, OciImageBlobStore blobStore,
                                   HelperRequestRouter helperRouter,
                                   @Value("${sqs.sandbox.helper-broker.enabled:true}") boolean helperBrokerEnabled,
                                   @Value("${sqs.sandbox.helper-broker.ready-timeout-sec:10}")
                                   int helperBrokerReadyTimeoutSec,
                                   com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager,
                                   com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay reverseRelay,
                                   com.studioquack.sqs.sandbox.reverse.NetworkRuleStore networkRuleStore,
                                   // Daemon-owned DNS pins (blank → the public default set). Raw string here, parsed+validated in the
                                   // full ctor so a malformed value fails the FIRST instance start loudly.
                                   @Value("${sqs.sandbox.dns.servers:}") String dnsServers,
                                   @Value("${sqs.sandbox.pids-limit:1024}") int pidsLimit) {
        this(cli, mapper, runtime, props, blobStore, helperRouter, helperBrokerEnabled,
                helperBrokerReadyTimeoutSec, egressManager, reverseRelay, networkRuleStore,
                com.studioquack.sqs.sandbox.egress.SandboxDns.parse(dnsServers));
        this.pidsLimit = com.studioquack.sqs.sandbox.oci.SandboxHostBudget.pidsLimit(pidsLimit);
    }

    /** Back-compat: older callers without DNS pins get the public default set. */
    public StatefulInstanceManager(OciCli cli, ObjectMapper mapper, ContainerRuntime runtime,
                                   StatefulRuntimeProperties props, OciImageBlobStore blobStore,
                                   HelperRequestRouter helperRouter,
                                   boolean helperBrokerEnabled,
                                   int helperBrokerReadyTimeoutSec,
                                   com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager,
                                   com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay reverseRelay,
                                   com.studioquack.sqs.sandbox.reverse.NetworkRuleStore networkRuleStore) {
        this(cli, mapper, runtime, props, blobStore, helperRouter, helperBrokerEnabled,
                helperBrokerReadyTimeoutSec, egressManager, reverseRelay, networkRuleStore,
                java.util.List.of());
    }

    /**
     * Full signature: the DNS pins are a separate trailing param so the older test ctors above keep
     * compiling unchanged (null → the public default set, validated once here).
     */
    public StatefulInstanceManager(OciCli cli, ObjectMapper mapper, ContainerRuntime runtime,
                                   StatefulRuntimeProperties props, OciImageBlobStore blobStore,
                                   HelperRequestRouter helperRouter,
                                   boolean helperBrokerEnabled,
                                   int helperBrokerReadyTimeoutSec,
                                   com.studioquack.sqs.sandbox.egress.SandboxEgressManager egressManager,
                                   com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay reverseRelay,
                                   com.studioquack.sqs.sandbox.reverse.NetworkRuleStore networkRuleStore,
                                   java.util.List<String> dnsServers) {
        this.cli = cli;
        this.mapper = mapper;
        this.runtime = runtime;
        this.props = props;
        this.blobStore = blobStore;
        this.helperRouter = helperRouter;
        this.helperBrokerEnabled = helperBrokerEnabled;
        this.helperBrokerReadyTimeoutSec = helperBrokerReadyTimeoutSec;
        this.egressManager = egressManager;
        this.reverseRelay = reverseRelay;
        this.networkRuleStore = networkRuleStore;
        this.dnsServers = com.studioquack.sqs.sandbox.egress.SandboxDns.validate(dnsServers);
        this.sizeQuotaProbe = new OverlaySizeQuotaProbe(cli);
    }

    /* ------------------------------------------------------------------ */
    /* Result records — mirror the FIXED daemon-command result wire shape */
    /* ------------------------------------------------------------------ */

    /** {@code load-oci-image} / {@code pull-oci-image} → {@code {ref, digest, status}}. */
    public record LoadResult(String ref, String digest, String status) {
    }

    /** {@code delete-oci-image} → {@code {ref, status}}. */
    public record DeleteResult(String ref, String status) {
    }

    /**
     * {@code start-stateful-instance} → {@code {instanceId, containerId, status, health[, message]}}.
     * {@code message} is set when start is refused (already-live) so handlers can surface it.
     */
    public record StartResult(String instanceId, String containerId, String status, Object health,
                              String message) {
        /** Wire-compat ctor when no operator message is needed. */
        public StartResult(String instanceId, String containerId, String status, Object health) {
            this(instanceId, containerId, status, health, null);
        }
    }

    /** {@code stop-stateful-instance} → {@code {status}}. */
    public record StopResult(String status) {
    }

    /** {@code status-stateful-instance} → {@code {status, containerId, health}}. */
    public record StatusResult(String status, String containerId, Object health) {
    }

    /**
     * Outcome of {@link #recycleForImage}: {@code recycled=true} only when a live user instance
     * was stopped and started again. {@code status} is the {@link StartResult} status on bounce.
     */
    public record RecycleResult(boolean recycled, String instanceId, String status, String message) {
    }

    /** Outcome of {@link #replaceWithCurrentImage}: successor took over routing only when {@code replaced}. */
    public record ImageReplaceResult(boolean replaced, String reason, boolean oldForceStopped) {
    }

    /* ------------------------------------------------------------------ */
    /* load-oci-image                                                     */
    /* ------------------------------------------------------------------ */

    /**
     * {@code podman load} the daemon-local blob-store tarball for {@code ref} and resolve its
     * content digest. The tarball is planted by the upload ingress at {@code <blobStore>/<safeRef>.tar}.
     */
    public LoadResult loadImage(String envId, String ref, String displayName) {
        if (ref == null || ref.isBlank()) {
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        if (!cli.available()) {
            log.warn("[SQ-6] load-oci-image ref={} — no container runtime available; reporting FAILED", ref);
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        Path tar = blobStore.pathFor(ref);
        if (!Files.isRegularFile(tar)) {
            log.warn("[SQ-6] load-oci-image ref={} — no blob at {} (upload it first); reporting FAILED",
                    ref, tar);
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "load", "-i", tar.toString()), lifecycleTimeout());
            if (!r.ok()) {
                log.warn("[SQ-6] load-oci-image ref={} exit={} err={}", ref, r.exitCode(), r.firstErrLine());
                return new LoadResult(ref, null, StatefulStatus.FAILED);
            }
            String digest = parseDigest(r.stdout());
            if (digest == null) {
                // Fall back to inspecting the ref for its image id (digest-equivalent).
                digest = inspectImageDigest(ref);
            }
            log.info("[SQ-6] load-oci-image ref={} LOADED digest={}", ref, digest);
            return new LoadResult(ref, digest, StatefulStatus.LOADED);
        } catch (Exception e) {
            log.warn("[SQ-6] load-oci-image ref={} error: {}", ref, e.toString());
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
    }

    /* ------------------------------------------------------------------ */
    /* pull-oci-image (ITEM 5)                                            */
    /* ------------------------------------------------------------------ */

    /**
     * ITEM 5 — {@code podman pull <ref>} anonymously (no registry credentials, so the anonymous
     * per-user rate limit applies) and resolve the pulled image's content digest. Supersedes the
     * tarball {@link #loadImage} flow for Docker-Hub images.
     */
    public LoadResult pullImage(String envId, String ref, String displayName) {
        return pullImage(envId, ref, displayName, lifecycleTimeout());
    }

    /**
     * As {@link #pullImage(String, String, String)} but with a caller-chosen wall-clock budget.
     * Daemon-managed platform images (Docling: ~4.4 GB) cannot fit the 120s lifecycle default on a
     * slow link — the inline {@code podman run} pull was killed at exactly that ceiling and the
     * side-car never came up (exit=-1 after ~121s, looping per reconcile).
     */
    public LoadResult pullImage(String envId, String ref, String displayName, Duration timeout) {
        if (ref == null || ref.isBlank()) {
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        if (!cli.available()) {
            log.warn("[ITEM5] pull-oci-image ref={} — no container runtime available; reporting FAILED", ref);
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        try {
            // Anonymous pull — we deliberately pass NO --creds / auth so we never
            // exceed an authenticated quota and never store a Docker Hub credential.
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "pull", ref), timeout);
            if (!r.ok()) {
                log.warn("[ITEM5] pull-oci-image ref={} exit={} err={}", ref, r.exitCode(), r.firstErrLine());
                return new LoadResult(ref, null, StatefulStatus.FAILED);
            }
            // podman pull prints the resolved digest; fall back to inspecting the ref.
            String digest = parseDigest(r.stdout());
            if (digest == null) {
                digest = inspectImageDigest(ref);
            }
            log.info("[ITEM5] pull-oci-image ref={} ACTIVE digest={}", ref, digest);
            return new LoadResult(ref, digest, StatefulStatus.ACTIVE);
        } catch (Exception e) {
            log.warn("[ITEM5] pull-oci-image ref={} error: {}", ref, e.toString());
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
    }

    /**
     * {@code podman build} a derived image from a generated Containerfile — used by daemon-managed
     * platform side-cars (the Docling ASR layer) that extend an already-pulled upstream image. {@code
     * --pull=missing}: the base ref is expected in the local store (the caller pulls it first); only
     * the small helper images a {@code COPY --from} references may be fetched.
     */
    public LoadResult buildImage(String ref, Path containerFile, Duration timeout) {
        return buildImage(ref, containerFile, timeout, false);
    }

    /**
     * {@code hostNetwork=true} runs the build's RUN steps on the HOST network namespace. Reserved for
     * daemon-trusted Containerfiles (the Docling ASR layer): a private build namespace's DNS depends
     * on the host's podman/aardvark/systemd-resolved chain, and broken stub-resolver forwarding makes
     * every RUN-time fetch fail with "dns error" while the host itself resolves fine (seen on DGX OS).
     */
    public LoadResult buildImage(String ref, Path containerFile, Duration timeout, boolean hostNetwork) {
        if (ref == null || ref.isBlank() || containerFile == null) {
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        if (!cli.available()) {
            log.warn("[SQ-6] build-image ref={} — no container runtime available; reporting FAILED", ref);
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
        try {
            List<String> argv = new ArrayList<>(List.of(cli.cliPath(), "build", "--pull=missing",
                    "-t", ref, "-f", containerFile.toString()));
            if (hostNetwork) {
                argv.add("--network=host");
            }
            argv.add(containerFile.getParent().toString());
            OciCli.Result r = cli.run(argv, timeout);
            if (!r.ok()) {
                // buildah prints progress and warnings first — the REAL error is at the end of
                // stderr, so log the tail, not the first line.
                log.warn("[SQ-6] build-image ref={} exit={} — build output tail:\n{}",
                        ref, r.exitCode(), stderrTail(r.stderr(), 15));
                return new LoadResult(ref, null, StatefulStatus.FAILED);
            }
            String digest = inspectImageDigest(ref);
            log.info("[SQ-6] build-image ref={} ACTIVE digest={}", ref, digest);
            return new LoadResult(ref, digest, StatefulStatus.ACTIVE);
        } catch (Exception e) {
            log.warn("[SQ-6] build-image ref={} error: {}", ref, e.toString());
            return new LoadResult(ref, null, StatefulStatus.FAILED);
        }
    }

    /** The LAST {@code maxLines} of a failed command's stderr, trimmed — where buildah puts the error. */
    private static String stderrTail(String stderr, int maxLines) {
        if (stderr == null || stderr.isBlank()) {
            return "(no stderr)";
        }
        String[] lines = stderr.trim().split("\n");
        StringBuilder sb = new StringBuilder();
        for (int i = Math.max(0, lines.length - maxLines); i < lines.length; i++) {
            sb.append(lines[i].trim()).append('\n');
        }
        return sb.toString().trim();
    }

    /**
     * {@code podman image exists <ref>} — true when the ref is already in the local store. Lets a
     * daemon-managed side-car (Docling) skip the pull entirely on restart and only pay the long-budget
     * pull when the image is genuinely absent.
     */
    public boolean imageExists(String ref) {
        if (ref == null || ref.isBlank() || !cli.available()) {
            return false;
        }
        try {
            return cli.run(List.of(cli.cliPath(), "image", "exists", ref), lifecycleTimeout()).ok();
        } catch (Exception e) {
            log.debug("[SQ-6] image exists check failed ref={} (treating as absent): {}", ref, e.toString());
            return false;
        }
    }

    /* ------------------------------------------------------------------ */
    /* delete-oci-image (ITEM 5)                                          */
    /* ------------------------------------------------------------------ */

    /**
     * ITEM 5 — {@code podman rmi <ref>} and purge any legacy uploaded tarball blob for the ref.
     * Idempotent + never throws: a missing image / absent runtime still reports {@code STOPPED}
     * (removed) so the control plane can drop its row cleanly.
     */
    public DeleteResult deleteImage(String envId, String ref) {
        if (ref == null || ref.isBlank()) {
            return new DeleteResult(ref, StatefulStatus.FAILED);
        }
        // Best-effort remove of any legacy blob-store tarball planted by the old upload flow.
        try {
            Files.deleteIfExists(blobStore.pathFor(ref));
        } catch (Exception e) {
            log.warn("[ITEM5] delete-oci-image ref={} — legacy blob cleanup failed: {}", ref, e.toString());
        }
        if (cli.available()) {
            try {
                // -f so a dangling/tagged image is removed without a manual untag step.
                cli.run(List.of(cli.cliPath(), "rmi", "-f", ref), lifecycleTimeout());
            } catch (Exception e) {
                log.warn("[ITEM5] delete-oci-image ref={} rmi error (treating as removed): {}", ref, e.toString());
            }
        }
        log.info("[ITEM5] delete-oci-image ref={} removed", ref);
        return new DeleteResult(ref, StatefulStatus.STOPPED);
    }

    /* ------------------------------------------------------------------ */
    /* start-stateful-instance                                            */
    /* ------------------------------------------------------------------ */

    /**
     * Brings up (or re-brings-up) the env's single stateful instance for {@code imageRef}. Reuses the
     * existing (env, ref) instance's identity + persistent volume when present so data survives across
     * restarts; a stale container of the same name is force-removed first.
     */
    public StartResult start(String envId, String imageRef, boolean stateful, StatefulCaps caps) {
        return start(envId, imageRef, stateful, caps, null, null, null, null);
    }

    /**
     * Overload for a daemon-managed instance (e.g. Docling) that needs (a) a deterministic,
     * recognizable identity rather than a random {@code inst_<uuid>} — so it survives lookups across a
     * reconcile without depending on the in-memory registry surviving a daemon restart — and (b) a
     * published host port, because the DAEMON PROCESS itself (not a sandboxed script) is the caller,
     * unlike every other stateful instance which is reached the other way.
     */
    public StartResult start(String envId, String imageRef, boolean stateful, StatefulCaps caps,
                             String instanceIdHint, Integer hostPort, Integer containerPort) {
        return start(envId, imageRef, stateful, caps, instanceIdHint, hostPort, containerPort, null);
    }

    /**
     * As {@link #start(String, String, boolean, StatefulCaps, String, Integer, Integer)} plus
     * container {@code -e} pairs. Docling uses this for {@code DOCLING_NUM_THREADS},
     * {@code DOCLING_SERVE_ENG_LOC_NUM_WORKERS}, {@code DOCLING_DEVICE}, and {@code SQ_APP_PORT}.
     *
     * @param extraEnv additional {@code KEY=VALUE} entries, or {@code null}/empty for none
     */
    public StartResult start(String envId, String imageRef, boolean stateful, StatefulCaps caps,
                             String instanceIdHint, Integer hostPort, Integer containerPort,
                             Map<String, String> extraEnv) {
        return start(envId, imageRef, stateful, caps, instanceIdHint, hostPort, containerPort,
                extraEnv, null);
    }

    /**
     * As {@link #start(String, String, boolean, StatefulCaps, String, Integer, Integer, Map)} plus the
     * reverse channel: when {@code reversePort} is non-null the instance's that-port listener is
     * published on the engine's overlay address once the container is RUNNING, reachable only from
     * same-owner engines holding a Network Access Control rule.
     */
    public StartResult start(String envId, String imageRef, boolean stateful, StatefulCaps caps,
                             String instanceIdHint, Integer hostPort, Integer containerPort,
                             Map<String, String> extraEnv, Integer reversePort) {
        if (imageRef == null || imageRef.isBlank()) {
            return new StartResult(null, null, StatefulStatus.FAILED, null);
        }
        // Serialize starts per (env, imageRef); re-check live inside the lock.
        Object lock = startLocks.computeIfAbsent(StatefulInstance.key(envId, imageRef), k -> new Object());
        synchronized (lock) {
            // Daemon-managed hints (e.g. Docling) keep idempotent restart; a user start that meets a live
            // instance reconciles against the host (join / drop-stale) instead of refusing — a refusal
            // stranded the container behind a FAILED backend row, and a replace would bounce a container a
            // racing dispatch may already be serving.
            StatefulInstance live = resolveRunning(envId, imageRef).orElse(null);
            if (live == null && instanceIdHint != null && !instanceIdHint.isBlank()) {
                live = byInstanceId(instanceIdHint).filter(StatefulInstance::isRunning).orElse(null);
            }
            if (live != null) {
                StartResult joined = joinLiveOrDropStale(envId, imageRef, live);
                if (joined != null) {
                    return joined;
                }
            }
            return startUnderLock(envId, imageRef, stateful, caps, instanceIdHint, hostPort,
                    containerPort, extraEnv, reversePort);
        }
    }

    /**
     * Start-time reconciliation when this registry still tracks a RUNNING instance for {@code (envId,
     * imageRef)}. Memory is not trusted against the host.
     *
     * @return The joined RUNNING result, or {@code null} when the stale record was dropped and th.
     */
    private StartResult joinLiveOrDropStale(String envId, String imageRef, StatefulInstance live) {
        if (containerRunningOnHost(live)) {
            // DEBUG: this join is the designed steady state of the builtin-lib supervisor's ensure-running
            // loop (BuiltinNodeLibraryManager.startSupervisor, backoff up to 5 min), so at INFO it repeats
            // every ~5 minutes per live instance without signalling a problem. The sibling stale-record drop
            // below stays INFO — that one changes routing state.
            log.debug("[SQ-6] start-stateful-instance env={} ref={} — instanceId={} is live on the "
                    + "host; joining it instead of starting a second container",
                    envId, imageRef, live.instanceId());
            rebindToCaller(envId, imageRef, live);
            return new StartResult(live.instanceId(), live.containerId(), StatefulStatus.RUNNING, null);
        }
        log.info("[SQ-6] start-stateful-instance env={} ref={} — tracked instanceId={} is gone on the "
                + "host; dropping the stale record", envId, imageRef, live.instanceId());
        closeBrokerChannel(live.containerId());
        live.setBrokerReady(false);
        live.setContainerId(null);
        live.setStatus(StatefulStatus.STOPPED);
        byKey.remove(live.key(), live);
        return null;
    }

    /**
     * A join by instance id can meet a survivor labelled with a previous enrolment's env (engine
     * re-installed under a new env id). Dispatch resolves by the caller's {@code (env, imageRef)},
     * so the joined instance moves under that key — otherwise it answers to a key nothing asks for.
     */
    private void rebindToCaller(String envId, String imageRef, StatefulInstance live) {
        if (envId == null || envId.isBlank() || envId.equals(live.envId()) || !imageRef.equals(live.imageRef())) {
            return;
        }
        String previousEnv = live.envId();
        byKey.remove(live.key(), live);
        live.rebindEnv(envId);
        byKey.put(live.key(), live);
        log.info("[SQ-6] start-stateful-instance instanceId={} ref={} — rebound from env={} to env={}",
                live.instanceId(), imageRef, previousEnv, envId);
    }

    /**
     * Host-truth probe: is the tracked instance's container actually in state {@code running}?
     * An unreadable/unavailable runtime answers {@code false} so the start proceeds to
     * {@code startUnderLock}, which fails loud on its own runtime probe — never refusing a start
     * on memory alone.
     */
    private boolean containerRunningOnHost(StatefulInstance inst) {
        if (!cli.available()) {
            return false;
        }
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", inst.containerName()),
                    lifecycleTimeout());
            if (!r.ok()) {
                return false;
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode container = root.isArray() && root.size() > 0 ? root.get(0) : root;
            return StatefulStatus.RUNNING.equals(
                    mapPodmanState(container.path("State").path("Status").asText("")));
        } catch (Exception e) {
            log.debug("[SQ-6] start-stateful-instance live-probe failed for {}: {}",
                    inst.containerName(), e.toString());
            return false;
        }
    }

    /** Body of {@link #start} after the per-key lock + live-instance refuse. */
    private StartResult startUnderLock(String envId, String imageRef, boolean stateful, StatefulCaps caps,
                                       String instanceIdHint, Integer hostPort, Integer containerPort,
                                       Map<String, String> extraEnv, Integer reversePort) {
        String refusal = startRefusal(envId, imageRef);
        if (refusal != null) {
            // Record an optimistic FAILED row so a later status() can reconcile.
            StatefulInstance rec = reuseOrCreate(envId, imageRef, stateful, caps, instanceIdHint);
            rec.setStatus(StatefulStatus.FAILED);
            return new StartResult(rec.instanceId(), null, StatefulStatus.FAILED, null,
                    refusal.isEmpty() ? null : refusal);
        }
        StatefulInstance inst = reuseOrCreate(envId, imageRef, stateful, caps, instanceIdHint);
        return launch(inst, envId, imageRef, hostPort, containerPort, extraEnv, reversePort);
    }

    /**
     * Why no container may start now, or {@code null} when one may: no runtime (blank reason),
     * overlay egress down, or an enforced isolation floor this host cannot meet.
     */
    private String startRefusal(String envId, String imageRef) {
        if (!cli.available()) {
            log.warn("[SQ-6] start-stateful-instance env={} ref={} — no runtime; FAILED", envId, imageRef);
            return "";
        }
        // Stateful always attaches sqs-stateful; refuse while overlay egress is UNAVAILABLE.
        if (egressManager != null) {
            var readiness = egressManager.readiness();
            if (!readiness.internetAllowed()) {
                String msg = readiness.refusalMessage();
                log.warn("[SQ-6] start-stateful-instance env={} ref={} — {}", envId, imageRef, msg);
                return msg == null ? "" : msg;
            }
        }
        // The enforced isolation floor binds stateful instances the same way as jobs: a host
        // whose runsc went undetected must not run them on the shared kernel.
        Optional<String> shortfall = runtime == null ? Optional.empty() : runtime.isolationShortfall(null);
        if (shortfall.isPresent()) {
            log.warn("[SQ-6] start-stateful-instance env={} ref={} — {}", envId, imageRef, shortfall.get());
            return shortfall.get();
        }
        return null;
    }

    /**
     * Brings {@code inst}'s container up and makes it reachable: volume, network, run (with the
     * cgroup retry), helper broker, readiness gate, then {@link #register} (a temporary container
     * stays only in {@link #temporaries}), the network pin and the optional reverse port. A
     * failure removes the container and reports FAILED.
     */
    private StartResult launch(StatefulInstance inst, String envId, String imageRef, Integer hostPort,
                               Integer containerPort, Map<String, String> extraEnv, Integer reversePort) {
        // Image-label inspect BEFORE podman run. Failure/absence → not capable (default FIFO).
        inst.setDispatchCapable(inspectImageDispatchCapable(imageRef));
        try {
            // Persistent (encrypted-intent) named volume — created once, reused across restarts.
            ensureVolume(inst.volumeName());
            // Stateful network must exist before attach; fail loud (no silent bridge fallback). Driven through
            // OciCli so FakeOciCli unit tests stay deterministic (no real CliRunner).
            SandboxNetworkEnsure.ensure(cli.cliPath(), SandboxNetworks.STATEFUL, (argv, timeout) -> {
                OciCli.Result r = cli.run(argv, timeout);
                // Stdout required: ensure inspects isolate/subnet before caching or migrating.
                return new SandboxNetworkEnsure.ExecResult(r.ok(), r.exitCode(), r.stdout(), r.stderr());
            });
            // Remove any stale container of this deterministic name before a fresh run.
            closeBrokerChannel(inst.containerId());
            inst.setBrokerReady(false);
            cli.run(List.of(cli.cliPath(), "rm", "-f", inst.containerName()), lifecycleTimeout());

            StatefulRunArgs runArgs = buildRunArgs(inst, imageRef, hostPort, containerPort, extraEnv, reversePort);
            OciCli.Result r = runContainer(runArgs);
            // `run -d` stderr is the runtime's own: a cgroup controller crun cannot open is
            // dropped and the start retried, at most once per controller.
            while (!r.ok() && CgroupCaps.noteDetachedStartFailure(r.stderr())) {
                removeFailedStartContainer(inst);
                runArgs = buildRunArgs(inst, imageRef, hostPort, containerPort, extraEnv, reversePort);
                r = runContainer(runArgs);
            }
            if (!r.ok()) {
                log.warn("[SQ-6] start-stateful-instance env={} ref={} exit={} err={}",
                        envId, imageRef, r.exitCode(), r.firstErrLine());
                // `podman run -d` can leave its deterministic named container in `created` when
                // startup fails after creation. Remove it before reporting FAILED; the persistent
                // volume remains intact for a later retry.
                removeFailedStartContainer(inst);
                inst.setStatus(StatefulStatus.FAILED);
                return new StartResult(inst.instanceId(), null, StatefulStatus.FAILED, null);
            }
            String containerId = r.out();
            if (containerId != null) {
                containerId = containerId.trim();
            }
            inst.setContainerId(containerId);
            // Plant the in-container helper broker AFTER the container id is known. Degrade loudly
            // on failure (BYO images may lack python3) — never fail the start itself.
            plantHelperBroker(inst, containerId);
            // Cold-start readiness gate: the container process is up but the app may still be booting (JVM
            // startup is seconds). Without this wait the first dispatch's launcher races the listener and dies
            // with curl exit 7 (connection refused).
            String readinessFailure = awaitAppListener(inst, containerId);
            if (readinessFailure != null) {
                log.warn("[SQ-6] start-stateful-instance env={} ref={} — {}; FAILED",
                        envId, imageRef, readinessFailure);
                removeFailedStartContainer(inst);
                inst.setStatus(StatefulStatus.FAILED);
                return new StartResult(inst.instanceId(), inst.containerId(), StatefulStatus.FAILED,
                        null, readinessFailure);
            }
            inst.setStatus(StatefulStatus.RUNNING);
            Object health = withBrokerHealth(inspectHealth(containerId), inst);
            inst.setHealthJson(health == null ? null : health.toString());
            if (!inst.isTemporary()) {
                register(inst);
            }
            // Pin the instance to the network it was started on (the current default; mid-drain that is the
            // NEW network, which is how the successor lands there). A long-lived instance keeps this network
            // until it stops; the pin is released in stop().
            assignInstanceNetwork(inst, containerId);
            // Reverse channel: publish on the engine's overlay address only AFTER the instance is live, and
            // fail the start loudly when the publish fails (port conflict names the holder) — never leave a
            // node running unpublished.
            if (reversePort != null) {
                try {
                    publishReversePort(inst, containerId, reversePort);
                } catch (Exception pub) {
                    if (reverseRelay != null && !reverseRelay.anyOverlayUp()) {
                        // Overlay DOWN at start: the reverse channel only serves cross-environment callers — the container
                        // itself is healthy and reachable on its loopback port. Reaping it here broke LOCAL-only use
                        // (Docling's primary path) on every daemon boot that raced the WireGuard bring-up.
                        log.warn("[SQ-6] start-stateful-instance env={} ref={} — reverse publish of "
                                        + "port {} DEFERRED (overlay down: {}); handshake-time republish "
                                        + "will restore it from the container label",
                                envId, imageRef, reversePort, pub.toString());
                    } else {
                        log.warn("[SQ-6] start-stateful-instance env={} ref={} — reverse publish failed: {}",
                                envId, imageRef, pub.toString());
                        removeFailedStartContainer(inst);
                        inst.setStatus(StatefulStatus.FAILED);
                        return new StartResult(inst.instanceId(), inst.containerId(),
                                StatefulStatus.FAILED, null, pub.getMessage());
                    }
                }
            }
            log.info("[SQ-6] start-stateful-instance env={} ref={} instanceId={} containerId={} RUNNING",
                    envId, imageRef, inst.instanceId(), shortId(containerId));
            return new StartResult(inst.instanceId(), containerId, StatefulStatus.RUNNING, health);
        } catch (Exception e) {
            log.warn("[SQ-6] start-stateful-instance env={} ref={} error: {}", envId, imageRef, e.toString());
            removeFailedStartContainer(inst);
            inst.setStatus(StatefulStatus.FAILED);
            return new StartResult(inst.instanceId(), inst.containerId(), StatefulStatus.FAILED, null);
        }
    }

    /* ------------------------------------------------------------------ */
    /* stop-stateful-instance                                             */
    /* ------------------------------------------------------------------ */

    /** Stops + removes the named container (the persistent volume is KEPT). Idempotent. */
    public StopResult stop(String envId, String instanceId) {
        StatefulInstance inst = instanceId == null ? null : byInstanceId.get(instanceId);
        if (inst == null) {
            // Nothing tracked (e.g. the daemon restarted while the container kept running) — the
            // container name is deterministic (prefix + instanceId), so reap the orphan by name
            // instead of reporting STOPPED over a live container the user then cannot reach.
            log.info("[SQ-6] stop-stateful-instance env={} instanceId={} — not tracked; reaping by "
                    + "deterministic name and reporting STOPPED", envId, instanceId);
            if (instanceId != null && !instanceId.isBlank()) {
                stopByContainerName(containerNameFor(instanceId));
            }
            return new StopResult(StatefulStatus.STOPPED);
        }
        try {
            closeBrokerChannel(inst.containerId());
            inst.setBrokerReady(false);
            // Release the reverse port before the container goes away, so the claim is free for the next start
            // and the relay never points at a dead container.
            if (inst.publishedReversePort() != null && reverseRelay != null) {
                try {
                    reverseRelay.unpublish(inst.publishedReversePort(), inst.instanceId());
                } catch (Exception unpub) {
                    log.warn("[SQ-6] stop-stateful-instance instanceId={} — reverse unpublish failed: {}",
                            inst.instanceId(), unpub.toString());
                }
                inst.setPublishedReversePort(null);
            }
            releaseNetworkPin(inst);
            if (cli.available()) {
                cli.run(List.of(cli.cliPath(), "stop", inst.containerName()), lifecycleTimeout());
                cli.run(List.of(cli.cliPath(), "rm", "-f", inst.containerName()), lifecycleTimeout());
            }
        } catch (Exception e) {
            log.warn("[SQ-6] stop-stateful-instance env={} instanceId={} error (marking STOPPED anyway): {}",
                    envId, instanceId, e.toString());
        }
        inst.setStatus(StatefulStatus.STOPPED);
        inst.setContainerId(null);
        // Drop the routing entry so no dispatch routes to a stopped instance; keep the byInstanceId
        // record so a follow-up status() reports STOPPED rather than UNKNOWN.
        byKey.remove(inst.key(), inst);
        log.info("[SQ-6] stop-stateful-instance env={} instanceId={} STOPPED", envId, instanceId);
        return new StopResult(StatefulStatus.STOPPED);
    }

    /**
     * Releases the instance's network pin so a later container reusing the bridge address never
     * inherits it.
     */
    private void releaseNetworkPin(StatefulInstance inst) {
        if (inst.assignedBridgeIp() == null || egressManager == null) {
            return;
        }
        try {
            egressManager.releaseContainerNetwork(inst.assignedBridgeIp());
        } catch (Exception rel) {
            log.debug("[SQ-6] stop-stateful-instance instanceId={} — network release failed: {}",
                    inst.instanceId(), rel.toString());
        }
        inst.setAssignedBridgeIp(null);
    }

    /* ------------------------------------------------------------------ */
    /* temporary containers (Create Node trial runs)                      */
    /* ------------------------------------------------------------------ */

    /** Instance-id prefix of a temporary container; a random suffix follows it. */
    static final String TEMPORARY_INSTANCE_PREFIX = "tmp_";
    /** Container-name prefix of a temporary container; the same random suffix follows it. */
    static final String TEMPORARY_CONTAINER_PREFIX = "sq-tmp-";

    /**
     * Starts a temporary container for one Create Node trial job. The image runs exactly like an
     * operator's instance (hardening, network, helper broker, readiness gate) under the default caps,
     * but nothing outside that job reaches it: it is never registered, no (env, imageRef) lookup
     * resolves it, and it carries {@link StatefulRunArgs#TEMPORARY_LABEL} instead of the stateful
     * label.
     */
    public StartResult startTemporary(String envId, String imageRef) {
        String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String instanceId = TEMPORARY_INSTANCE_PREFIX + suffix;
        String refusal = startRefusal(envId, imageRef);
        if (refusal != null) {
            return new StartResult(instanceId, null, StatefulStatus.FAILED, null,
                    refusal.isEmpty() ? "no container runtime on this engine" : refusal);
        }
        StatefulInstance inst = new StatefulInstance(instanceId, envId, imageRef,
                TEMPORARY_CONTAINER_PREFIX + suffix, props.getVolumeNamePrefix() + instanceId,
                StatefulCaps.defaults(), true, true);
        temporaries.put(instanceId, inst);
        return launch(inst, envId, imageRef, null, null, null, null);
    }

    /** The temporary container's record until {@link #removeTemporary} drops it. */
    public Optional<StatefulInstance> temporary(String instanceId) {
        return Optional.ofNullable(instanceId == null ? null : temporaries.get(instanceId));
    }

    /**
     * Writes every regular file directly under {@code sourceDir} into a running temporary container's
     * bespoke folder ({@code /data/sq-bespoke/<templateId>/<name>}) with the same exec-and-decode
     * write a bespoke upload into an instance uses, so an inject-mode trial finds its workspace where
     * a stateful instance keeps uploaded files.
     */
    public void installTemporaryFiles(String instanceId, String templateId, Path sourceDir) throws IOException {
        StatefulInstance inst = instanceId == null ? null : temporaries.get(instanceId);
        if (inst == null || !inst.isRunning() || !cli.available()) {
            throw new IOException("temporary container " + instanceId + " is not running");
        }
        String tid = BespokeFilePaths.requireTemplateId(templateId);
        List<Path> files;
        try (var listing = Files.list(sourceDir)) {
            files = listing.filter(Files::isRegularFile).sorted().toList();
        }
        for (Path file : files) {
            String name = BespokeFilePaths.requireBasename(file.getFileName().toString());
            String script = "mkdir -p " + BespokeFilePaths.shSingleQuote(BespokeFilePaths.containerDir(tid))
                    + " && base64 -d > " + BespokeFilePaths.shSingleQuote(BespokeFilePaths.containerPath(tid, name));
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "exec", "-i", inst.containerName(), "sh", "-c", script),
                    Base64.getEncoder().encode(Files.readAllBytes(file)), lifecycleTimeout());
            if (r.timedOut() || r.exitCode() != 0) {
                String err = r.timedOut() ? "timed out" : r.firstErrLine();
                throw new IOException("writing " + name + " failed: "
                        + (err == null || err.isBlank() ? "exit=" + r.exitCode() : err));
            }
        }
        log.info("[SQ-6] temporary container {} received {} staged file(s) for template {}",
                instanceId, files.size(), tid);
    }

    /**
     * Kills a temporary container and drops its volume. Idempotent and never throws, so the
     * cleanup after a job can never replace that job's own result.
     */
    public void removeTemporary(String instanceId) {
        StatefulInstance inst = instanceId == null ? null : temporaries.remove(instanceId);
        if (inst == null) {
            return;
        }
        try {
            closeBrokerChannel(inst.containerId());
            inst.setBrokerReady(false);
            releaseNetworkPin(inst);
            if (cli.available()) {
                cli.run(List.of(cli.cliPath(), "rm", "-f", inst.containerName()), lifecycleTimeout());
                cli.run(List.of(cli.cliPath(), "volume", "rm", "-f", inst.volumeName()), lifecycleTimeout());
            }
        } catch (Exception e) {
            log.warn("[SQ-6] temporary container {} cleanup error (best-effort): {}", instanceId, e.toString());
        }
        inst.setStatus(StatefulStatus.STOPPED);
        inst.setContainerId(null);
        log.info("[SQ-6] temporary container {} removed", instanceId);
    }

    /**
     * Removes the temporary containers (and their volumes) an earlier daemon process left behind
     * when it died between starting one and removing it. Returns how many.
     */
    public int reapLeftoverTemporaries() {
        if (!cli.available()) {
            return 0;
        }
        OciCli.Result listed = cli.run(List.of(cli.cliPath(), "ps", "-a",
                "--filter", "label=" + StatefulRunArgs.TEMPORARY_LABEL + "=1",
                "--format", "{{.Names}}"), lifecycleTimeout());
        if (!listed.ok()) {
            log.warn("[SQ-6] temporary container reap: podman ps failed: {}", listed.firstErrLine());
            return 0;
        }
        int reaped = 0;
        for (String line : listed.out().split("\\R+")) {
            String name = line.trim();
            if (!name.startsWith(TEMPORARY_CONTAINER_PREFIX)) {
                continue;
            }
            String instanceId = TEMPORARY_INSTANCE_PREFIX + name.substring(TEMPORARY_CONTAINER_PREFIX.length());
            if (temporaries.containsKey(instanceId)) {
                continue;
            }
            cli.run(List.of(cli.cliPath(), "rm", "-f", name), lifecycleTimeout());
            cli.run(List.of(cli.cliPath(), "volume", "rm", "-f", props.getVolumeNamePrefix() + instanceId),
                    lifecycleTimeout());
            reaped++;
        }
        return reaped;
    }

    /* ------------------------------------------------------------------ */
    /* recycle after OCI image rebuild                                    */
    /* ------------------------------------------------------------------ */

    /**
     * After a successful rebuild of {@code imageRef}, bounce the live user instance for {@code (envId,
     * imageRef)} onto the new image id with identical settings. Never throws.
     */
    public RecycleResult recycleForImage(String envId, String imageRef) {
        return recycleForImage(envId, imageRef, null);
    }

    /**
     * As {@link #recycleForImage(String, String)}, but only while routing still points at
     * {@code expectedContainerId} (null = whatever is routed). A dead-broker recovery names the
     * container it found dead: by the time it holds the lock, a library update or an earlier
     * recovery may already have routed a healthy successor, and bouncing that one would kill
     * every request running in it.
     */
    public RecycleResult recycleForImage(String envId, String imageRef, String expectedContainerId) {
        String instanceId = null;
        try {
            // Same per-(env, imageRef) monitor start() uses; taken BEFORE reading live state
            // so this cannot race a dispatch/start. Reentrant — stop/start while holding is OK.
            Object lock = startLocks.computeIfAbsent(StatefulInstance.key(envId, imageRef), k -> new Object());
            synchronized (lock) {
                StatefulInstance live = resolveRunning(envId, imageRef).orElse(null);
                if (live == null) {
                    return new RecycleResult(false, null, null, null);
                }
                instanceId = live.instanceId();
                if (expectedContainerId != null && !expectedContainerId.equals(live.containerId())) {
                    log.info("[SQ-6] recycleForImage env={} ref={} instanceId={} — routing already moved"
                                    + " to {}; not bouncing it", envId, imageRef, instanceId,
                            shortId(live.containerId()));
                    return new RecycleResult(false, instanceId, live.status(), "already replaced");
                }
                if (live.isPlatformManaged()) {
                    log.info("[SQ-6] recycleForImage env={} ref={} instanceId={} — skip platform-managed",
                            envId, imageRef, instanceId);
                    return new RecycleResult(false, instanceId, live.status(), null);
                }
                StatefulCaps caps = live.caps();
                boolean stateful = live.stateful();
                Integer reversePort = live.publishedReversePort();
                // Stop first so start() does not hit the already-live refuse path. Volume is KEPT.
                stop(envId, instanceId);
                // extraEnv/hostPort/containerPort are Docling-only — we skip those instances.
                StartResult started = start(envId, imageRef, stateful, caps, null, null, null, null,
                        reversePort);
                if (!StatefulStatus.RUNNING.equals(started.status())) {
                    log.warn("[SQ-6] recycleForImage env={} ref={} instanceId={} — restart status={}",
                            envId, imageRef, instanceId, started.status());
                    return new RecycleResult(false, started.instanceId(), started.status(),
                            started.message());
                }
                log.info("[SQ-6] recycleForImage env={} ref={} instanceId={} recycled",
                        envId, imageRef, started.instanceId());
                return new RecycleResult(true, started.instanceId(), started.status(), started.message());
            }
        } catch (Exception e) {
            log.warn("[SQ-6] recycleForImage env={} ref={} instanceId={} error={}",
                    envId, imageRef, instanceId, e.getClass().getSimpleName());
            return new RecycleResult(false, instanceId, StatefulStatus.FAILED, e.getClass().getSimpleName());
        }
    }

    /* ------------------------------------------------------------------ */
    /* status-stateful-instance                                           */
    /* ------------------------------------------------------------------ */

    /** Inspects the container state + healthcheck and reconciles the tracked status. */
    public StatusResult status(String envId, String instanceId) {
        StatefulInstance inst = instanceId == null ? null : byInstanceId.get(instanceId);
        if (inst == null) {
            return new StatusResult(StatefulStatus.UNKNOWN, null, null);
        }
        if (!cli.available()) {
            return new StatusResult(inst.status(), inst.containerId(), withBrokerHealth(null, inst));
        }
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", inst.containerName()), lifecycleTimeout());
            if (!r.ok()) {
                // Container gone → STOPPED; broker is unavailable with the instance.
                closeBrokerChannel(inst.containerId());
                inst.setBrokerReady(false);
                inst.setStatus(StatefulStatus.STOPPED);
                byKey.remove(inst.key(), inst);
                return new StatusResult(StatefulStatus.STOPPED, inst.containerId(), withBrokerHealth(null, inst));
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode container = root.isArray() && root.size() > 0 ? root.get(0) : root;
            JsonNode state = container.path("State");
            String podmanStatus = state.path("Status").asText("");
            String status = mapPodmanState(podmanStatus);
            JsonNode health = state.get("Health");
            String containerId = container.path("Id").asText(inst.containerId());
            inst.setStatus(status);
            inst.setContainerId(containerId);
            if (StatefulStatus.RUNNING.equals(status)) {
                byKey.putIfAbsent(inst.key(), inst);
                // Reconcile broker liveness so a dead channel shows up on the next status poll.
                brokerAlive(inst);
            } else {
                closeBrokerChannel(containerId);
                inst.setBrokerReady(false);
                byKey.remove(inst.key(), inst);
            }
            Object healthObj = (health == null || health.isNull()) ? null : mapper.convertValue(health, Object.class);
            Object reported = withBrokerHealth(healthObj, inst);
            inst.setHealthJson(reported == null ? null : reported.toString());
            return new StatusResult(status, containerId, reported);
        } catch (Exception e) {
            log.warn("[SQ-6] status-stateful-instance env={} instanceId={} error: {}", envId, instanceId, e.toString());
            return new StatusResult(inst.status() == null ? StatefulStatus.UNKNOWN : inst.status(),
                    inst.containerId(), withBrokerHealth(null, inst));
        }
    }

    /* ------------------------------------------------------------------ */
    /* Dispatch routing + accessors                                       */
    /* ------------------------------------------------------------------ */

    /** Resolves the LIVE (RUNNING) instance for a dispatch's (env + imageRef), or empty. */
    public Optional<StatefulInstance> resolveRunning(String envId, String imageRef) {
        StatefulInstance inst = byKey.get(StatefulInstance.key(envId, imageRef));
        return (inst != null && inst.isRunning() && inst.containerId() != null)
                ? Optional.of(inst) : Optional.empty();
    }

    /** Lightweight live-instance predicate for callers that do not need the resolved record. */
    public boolean hasLiveInstance(String envId, String imageRef) {
        return resolveRunning(envId, imageRef).isPresent();
    }

    /**
     * True when the instance's helper broker is marked ready and its pipe channel is still alive.
     * A dead channel is closed, cleared, and logged — a dead broker must not leave a healthy-looking
     * instance (stateful analogue of the warm pool's liveness tracking).
     */
    public boolean brokerAlive(StatefulInstance inst) {
        if (inst == null || !inst.brokerReady() || inst.containerId() == null) {
            return false;
        }
        HelperPipeChannel channel = brokerChannels.get(inst.containerId());
        if (channel == null) {
            inst.setBrokerReady(false);
            inst.setBrokerNote(BROKER_NOTE_DIED);
            log.warn("[SQ-6] stateful helper broker missing for instance={} container={} — helper callbacks "
                            + "(studioquack.execute/llm/connect) unavailable until the instance is restarted",
                    inst.instanceId(), shortId(inst.containerId()));
            return false;
        }
        if (!channel.alive()) {
            closeBrokerChannel(inst.containerId());
            inst.setBrokerReady(false);
            inst.setBrokerNote(BROKER_NOTE_DIED);
            log.warn("[SQ-6] stateful helper broker died for instance={} container={} — helper callbacks "
                            + "(studioquack.execute/llm/connect) unavailable until the instance is restarted",
                    inst.instanceId(), shortId(inst.containerId()));
            return false;
        }
        return true;
    }

    /** The instance for an id (any status), for tests/diagnostics. */
    public Optional<StatefulInstance> byInstanceId(String instanceId) {
        return Optional.ofNullable(byInstanceId.get(instanceId));
    }

    /**
     * True when a running gVisor instance on a container network sees no interface but loopback. That
     * is what an in-place restart (Podman's restart policy reusing the network namespace) leaves
     * behind: runsc's first boot moved {@code eth0}'s address into the sandbox, so the next boot has
     * none to take.
     */
    public boolean sandboxNetworkLost(StatefulInstance inst) {
        String cid = inst == null ? null : inst.containerId();
        if (cid == null || !inst.isRunning() || cli.cliPath() == null) {
            return false;
        }
        OciCli.Result info = cli.run(List.of(cli.cliPath(), "inspect", "--format",
                "{{.OCIRuntime}} {{.HostConfig.NetworkMode}}", cid), Duration.ofSeconds(15));
        String[] f = info.ok() ? info.out().split("\\s+") : new String[0];
        if (f.length < 2 || !f[0].contains("runsc") || !isContainerNetwork(f[1])) {
            return false;
        }
        OciCli.Result dev = cli.run(List.of(cli.cliPath(), "exec", cid, "cat", "/proc/net/dev"),
                Duration.ofSeconds(15));
        return dev.ok() && onlyLoopback(dev.out());
    }

    private static boolean isContainerNetwork(String networkMode) {
        return !networkMode.isBlank() && !"none".equals(networkMode) && !"host".equals(networkMode)
                && !networkMode.startsWith("container:") && !networkMode.startsWith("ns:");
    }

    /** {@code /proc/net/dev} lists at least one interface and every one of them is {@code lo}. */
    static boolean onlyLoopback(String procNetDev) {
        boolean any = false;
        for (String line : procNetDev.split("\n")) {
            int colon = line.indexOf(':');
            if (colon < 0 || line.contains("|")) {
                continue; // the two header lines
            }
            any = true;
            if (!"lo".equals(line.substring(0, colon).trim())) {
                return false;
            }
        }
        return any;
    }

    /**
     * Network watchdog pass for every instance this daemon does not leave to an owner: a user instance
     * is recycled (same identity and volume, fresh network namespace); a daemon-managed sidecar such
     * as Docling is stopped and its owner's {@code ensureRunning()} starts it fresh on the next use.
     * The built-in node library is skipped — its supervisor replaces it drained ({@code
     * BuiltinNodeLibraryManager}).
     */
    public int healLostSandboxNetworks() {
        int healed = 0;
        for (StatefulInstance inst : List.copyOf(byInstanceId.values())) {
            if (StatefulInstance.isBuiltinNodeLibrary(inst.instanceId()) || !sandboxNetworkLost(inst)) {
                continue;
            }
            String cid = inst.containerId();
            log.warn("[SQ-6] instance {} container {} has no network besides loopback (restarted in place "
                    + "under gVisor) — {}", inst.instanceId(), shortId(cid),
                    inst.isPlatformManaged() ? "stopping it for a fresh start" : "recycling it");
            if (inst.isPlatformManaged()) {
                stop(inst.envId(), inst.instanceId());
            } else {
                recycleForImage(inst.envId(), inst.imageRef(), cid);
            }
            healed++;
        }
        return healed;
    }

    /**
     * Read-only snapshot of every instance this daemon has registered (any status, not just RUNNING)
     * plus any {@link StatefulStatus#DRAINING} predecessor still serving in-flight work after {@link
     * #replaceWithCurrentImage}. This JVM's own in-memory registry only, so it never carries another
     * installation's data.
     */
    public List<StatefulInstance> inventory() {
        List<StatefulInstance> out = new ArrayList<>(byInstanceId.values());
        for (StatefulInstance draining : drainingByContainerId.values()) {
            boolean seen = false;
            for (StatefulInstance existing : out) {
                if (existing == draining) {
                    seen = true;
                    break;
                }
                String dcid = draining.containerId();
                if (dcid != null && dcid.equals(existing.containerId())) {
                    seen = true;
                    break;
                }
            }
            if (!seen) {
                out.add(draining);
            }
        }
        return List.copyOf(out);
    }

    /**
     * One host-truth row per stateful-labeled container (any state) — the same set {@link
     * #reconcileWithHost()} scans. Unlike {@link #inventory()} this is not the in-memory registry: it
     * lists what podman actually has, so it also sees containers a previous installation left behind
     * (an offline uninstall never got its teardown).
     */
    public List<StatefulContainerView> listStatefulContainers() {
        if (!cli.available()) {
            return List.of();
        }
        OciCli.Result listed = cli.run(List.of(cli.cliPath(), "ps", "-a", "--no-trunc",
                "--filter", "label=" + StatefulEndpointResolver.STATEFUL_LABEL + "=1",
                "--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.State}}"), lifecycleTimeout());
        if (!listed.ok()) {
            log.warn("[SQ-6] listStatefulContainers: podman ps failed: {}", listed.firstErrLine());
            return List.of();
        }
        List<StatefulContainerView> out = new java.util.ArrayList<>();
        for (String line : listed.out().split("\\R+")) {
            String[] f = line.trim().split("\t", -1);
            if (f.length < 4 || f[0].isBlank()) {
                continue;
            }
            out.add(new StatefulContainerView(f[0], f[1], f[2],
                    "running".equalsIgnoreCase(f[3].trim())));
        }
        return out;
    }

    /** Host-truth view of one stateful-labeled container, for platform-singleton stray sweeps. */
    public record StatefulContainerView(String containerId, String name, String image, boolean running) {
    }

    /** Test/diagnostic seam: register an already-built instance directly. */
    void register(StatefulInstance inst) {
        byInstanceId.put(inst.instanceId(), inst);
        if (inst.isRunning() && inst.containerId() != null) {
            byKey.put(inst.key(), inst);
        }
    }

    /**
     * Admits one dispatch (unary invoke or stream session) into this container and counts it
     * in-flight. {@code false} — nothing counted — once a drain closed the container (the caller
     * re-resolves routing) or for a null/blank id.
     */
    public boolean beginDispatch(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return false;
        }
        boolean[] admitted = {false};
        inFlightByContainer.compute(containerId, (k, n) -> {
            if (n == null) {
                admitted[0] = true;
                return new AtomicInteger(1);
            }
            if (n.get() == DISPATCH_CLOSED) {
                return n;
            }
            n.incrementAndGet();
            admitted[0] = true;
            return n;
        });
        return admitted[0];
    }

    /** Ends one in-flight dispatch. Never goes negative; missing key or closed → no-op. */
    public void endDispatch(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return;
        }
        inFlightByContainer.compute(containerId, (k, n) -> {
            if (n == null || n.get() == DISPATCH_CLOSED) {
                return n;
            }
            return n.decrementAndGet() <= 0 ? null : n;
        });
    }

    /** Closes the container for new dispatches iff nothing is in flight; {@code true} when closed. */
    private boolean closeForDispatchIfIdle(String containerId) {
        boolean[] closed = {false};
        inFlightByContainer.compute(containerId, (k, n) -> {
            if (n == null || n.get() <= 0) {
                closed[0] = true;
                return new AtomicInteger(DISPATCH_CLOSED);
            }
            return n;
        });
        return closed[0];
    }

    /** Drain deadline: closed regardless of what is still in flight (that work dies with the container). */
    private void forceCloseForDispatch(String containerId) {
        inFlightByContainer.put(containerId, new AtomicInteger(DISPATCH_CLOSED));
    }

    /** Current in-flight count for this container id (0 if unknown/null). */
    public int inFlight(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return 0;
        }
        AtomicInteger n = inFlightByContainer.get(containerId);
        if (n == null) {
            return 0;
        }
        int v = n.get();
        return v < 0 ? 0 : v;
    }

    /** How long the quiesce waits for a reverse port's live connections to drain. */
    private static final long DRAIN_QUIESCE_WAIT_MS = 30_000L;

    /**
     * In-place image upgrade: start a successor on the current image, switch routing only after
     * it is healthy, then drain in-flight work on the old container before stopping it.
     * {@code drainMaxWait} null or negative is {@link Duration#ZERO} (immediate timeout when
     * {@link #inFlight} is still positive).
     */
    public ImageReplaceResult replaceWithCurrentImage(String instanceId, Duration drainMaxWait) {
        return replaceWithCurrentImage(instanceId, drainMaxWait, null);
    }

    /**
     * As {@link #replaceWithCurrentImage(String, Duration)}. {@code onPhase} is invoked with
     * {@code "DRAINING"} exactly once after the routing switch and before the drain poll
     * (a listener failure cannot abort the drain).
     */
    public ImageReplaceResult replaceWithCurrentImage(String instanceId, Duration drainMaxWait,
            java.util.function.Consumer<String> onPhase) {
        Duration wait = (drainMaxWait == null || drainMaxWait.isNegative()) ? Duration.ZERO : drainMaxWait;
        StatefulInstance old = instanceId == null ? null : byInstanceId.get(instanceId);
        if (old == null || !old.isRunning() || old.containerId() == null) {
            return new ImageReplaceResult(false, "not running", false);
        }
        Object lock = startLocks.computeIfAbsent(old.key(), k -> new Object());
        String oldCid;
        StatefulInstance successor;
        synchronized (lock) {
            old = byInstanceId.get(instanceId);
            if (old == null || !old.isRunning() || old.containerId() == null) {
                return new ImageReplaceResult(false, "not running", false);
            }
            // The drain below runs outside this lock, so a second replace could otherwise
            // `rm -f` the alternate name a predecessor is still draining under.
            for (StatefulInstance d : drainingByContainerId.values()) {
                if (instanceId.equals(d.instanceId())) {
                    return new ImageReplaceResult(false, "previous replacement still draining", false);
                }
            }
            String base = containerNameFor(instanceId);
            String successorName = old.containerName().equals(base) ? base + "-b" : base;
            successor = new StatefulInstance(old.instanceId(), old.envId(),
                    old.imageRef(), successorName, old.volumeName(), old.caps(), old.stateful());
            successor.setNetworkKey(old.networkKey());
            successor.setPublishedReversePort(old.publishedReversePort());
            successor.setDispatchCapable(inspectImageDispatchCapable(old.imageRef()));
            // A previous rollout may have left the alternate name behind.
            cli.run(List.of(cli.cliPath(), "rm", "-f", successorName), lifecycleTimeout());
            String successorId = null;
            try {
                ensureVolume(successor.volumeName());
                SandboxNetworkEnsure.ensure(cli.cliPath(), SandboxNetworks.STATEFUL, (argv, timeout) -> {
                    OciCli.Result r = cli.run(argv, timeout);
                    return new SandboxNetworkEnsure.ExecResult(r.ok(), r.exitCode(), r.stdout(), r.stderr());
                });
                StatefulRunArgs runArgs = buildRunArgs(successor, old.imageRef(), null, null, null,
                        old.publishedReversePort());
                OciCli.Result r = runContainer(runArgs);
                if (!r.ok()) {
                    String reason = r.firstErrLine();
                    if (reason == null || reason.isBlank()) {
                        reason = "successor start failed";
                    }
                    log.warn("[SQ-6] image-replace instanceId={} — successor start failed exit={}",
                            instanceId, r.exitCode());
                    cli.run(List.of(cli.cliPath(), "rm", "-f", successorName), lifecycleTimeout());
                    return new ImageReplaceResult(false, reason, false);
                }
                successorId = r.out() == null ? null : r.out().trim();
                successor.setContainerId(successorId);
                plantHelperBroker(successor, successorId);
                String successorReadinessFailure = awaitAppListener(successor, successorId);
                if (successorReadinessFailure != null) {
                    log.warn("[SQ-6] image-replace instanceId={} — successor not serving; reaped",
                            instanceId);
                    closeBrokerChannel(successorId);
                    cli.run(List.of(cli.cliPath(), "rm", "-f", successorName), lifecycleTimeout());
                    return new ImageReplaceResult(false, successorReadinessFailure, false);
                }
                if (brokerAlive(old) && !brokerAlive(successor)) {
                    log.warn("[SQ-6] image-replace instanceId={} — successor helper broker is not alive; reaped",
                            instanceId);
                    closeBrokerChannel(successorId);
                    cli.run(List.of(cli.cliPath(), "rm", "-f", successorName), lifecycleTimeout());
                    return new ImageReplaceResult(false, "successor helper broker is not alive", false);
                }
                // Health gate passed — successor may take over routing. Still do not stop old.
                successor.setStatus(StatefulStatus.RUNNING);
                Object health = withBrokerHealth(inspectHealth(successorId), successor);
                successor.setHealthJson(health == null ? null : health.toString());
                assignInstanceNetwork(successor, successorId);
                if (old.publishedReversePort() != null && reverseRelay != null) {
                    reverseRelay.quiescePort(old.publishedReversePort(), DRAIN_QUIESCE_WAIT_MS);
                    publishReversePort(successor, successorId, old.publishedReversePort());
                }
            } catch (Exception e) {
                log.warn("[SQ-6] image-replace instanceId={} — successor failed ({}); old keeps serving",
                        instanceId, e.toString());
                closeBrokerChannel(successorId);
                cli.run(List.of(cli.cliPath(), "rm", "-f", successorName), lifecycleTimeout());
                return new ImageReplaceResult(false, e.toString(), false);
            }
            // SWITCH: old stays up (containerId / broker / pin kept) until in-flight work drains.
            oldCid = old.containerId();
            drainingByContainerId.put(oldCid, old);
            old.setStatus(StatefulStatus.DRAINING);
            register(successor);
        }
        // The drain waits OUTSIDE the start lock: it can last drain-max-wait (30 min), and a
        // dead-broker recovery or cold start for this key must not stall behind it.
        if (onPhase != null) {
            try {
                onPhase.accept("DRAINING");
            } catch (Exception listener) {
                log.warn("[SQ-6] image-replace instanceId={} — onPhase listener failed: {}",
                        instanceId, listener.toString());
            }
        }
        boolean oldForceStopped = false;
        long deadlineNanos = System.nanoTime() + wait.toNanos();
        while (!closeForDispatchIfIdle(oldCid)) {
            if (System.nanoTime() >= deadlineNanos) {
                log.warn("[SQ-6] image-replace instanceId={} container={} — drain timed out; "
                        + "force-stopping old", instanceId, shortId(oldCid));
                forceCloseForDispatch(oldCid);
                oldForceStopped = true;
                break;
            }
            try {
                Thread.sleep(1_000L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("[SQ-6] image-replace instanceId={} container={} — drain interrupted; "
                        + "force-stopping old", instanceId, shortId(oldCid));
                forceCloseForDispatch(oldCid);
                oldForceStopped = true;
                break;
            }
        }
        closeBrokerChannel(oldCid);
        old.setBrokerReady(false);
        if (old.assignedBridgeIp() != null && egressManager != null) {
            try {
                egressManager.releaseContainerNetwork(old.assignedBridgeIp());
            } catch (Exception rel) {
                log.debug("[SQ-6] image-replace instanceId={} — old pin release failed: {}",
                        instanceId, rel.toString());
            }
        }
        if (cli.available()) {
            cli.run(List.of(cli.cliPath(), "stop", old.containerName()), lifecycleTimeout());
            cli.run(List.of(cli.cliPath(), "rm", "-f", old.containerName()), lifecycleTimeout());
        }
        old.setStatus(StatefulStatus.STOPPED);
        old.setContainerId(null);
        drainingByContainerId.remove(oldCid);
        log.info("[SQ-6] image-replace instanceId={} — successor {} RUNNING, old {} stopped",
                instanceId, shortId(successor.containerId()), shortId(oldCid));
        return new ImageReplaceResult(true, null, oldForceStopped);
    }

    /* ------------------------------------------------------------------ */
    /* internals                                                          */
    /* ------------------------------------------------------------------ */

    private StatefulInstance reuseOrCreate(String envId, String imageRef, boolean stateful, StatefulCaps caps) {
        return reuseOrCreate(envId, imageRef, stateful, caps, null);
    }

    private StatefulInstance reuseOrCreate(String envId, String imageRef, boolean stateful, StatefulCaps caps,
                                           String instanceIdHint) {
        StatefulInstance existing = byInstanceId.values().stream()
                .filter(i -> i.key().equals(StatefulInstance.key(envId, imageRef)))
                .findFirst().orElse(null);
        if (existing != null) {
            // Reuse identity + persistent volume so data survives a restart; refresh caps by rebuild.
            StatefulInstance refreshed = new StatefulInstance(existing.instanceId(), envId, imageRef,
                    existing.containerName(), existing.volumeName(), caps, stateful);
            byInstanceId.put(refreshed.instanceId(), refreshed);
            return refreshed;
        }
        // A caller-supplied hint (e.g. "docling") gives a daemon-managed instance a deterministic,
        // recognizable identity instead of an opaque random one — same naming scheme, just a fixed
        // suffix — so operators can find it by name in `podman ps` / the stateful-instance inventory.
        String instanceId = (instanceIdHint != null && !instanceIdHint.isBlank())
                ? instanceIdHint
                : "inst_" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
        String containerName = props.getContainerNamePrefix() + instanceId;
        String volumeName = props.getVolumeNamePrefix() + instanceId;
        StatefulInstance inst = new StatefulInstance(instanceId, envId, imageRef, containerName,
                volumeName, caps, stateful);
        byInstanceId.put(instanceId, inst);
        return inst;
    }

    /** The deterministic container name a given instance-id hint (e.g. {@code "docling"}) resolves to. */
    public String containerNameFor(String instanceIdHint) {
        return props.getContainerNamePrefix() + instanceIdHint;
    }

    /**
     * Stops + force-removes a stateful container BY NAME, idempotently, without requiring a tracked
     * registry entry. Used for deterministic-hint instances (e.g.
     */
    public void stopByContainerName(String containerName) {
        if (containerName == null || containerName.isBlank() || !cli.available()) {
            return;
        }
        try {
            cli.run(List.of(cli.cliPath(), "stop", containerName), lifecycleTimeout());
            cli.run(List.of(cli.cliPath(), "rm", "-f", containerName), lifecycleTimeout());
        } catch (Exception e) {
            log.warn("[SQ-6] stopByContainerName {} error (best-effort): {}", containerName, e.toString());
        }
    }

    /**
     * Brings the host back to exactly ONE container for a daemon-owned singleton with a fixed {@code
     * instanceId} (the built-in node library). Its container carries the canonical name, the
     * image-replace alternate ({@code -b}) or the network-drain successor ({@code -drain}), so {@link
     * #start}'s force-remove of the canonical name alone cannot prevent a second container.
     */
    public int reconcilePlatformSingleton(String envId, String imageRef, String instanceId) {
        if (!cli.available() || envId == null || envId.isBlank() || imageRef == null || imageRef.isBlank()
                || instanceId == null || instanceId.isBlank()) {
            return 0;
        }
        String base = containerNameFor(instanceId);
        Object lock = startLocks.computeIfAbsent(StatefulInstance.key(envId, imageRef), k -> new Object());
        synchronized (lock) {
            try {
                StatefulInstance routed = resolveRunning(envId, imageRef)
                        .or(() -> byInstanceId(instanceId)
                                .filter(i -> i.isRunning() && i.containerId() != null))
                        .orElse(null);
                if (routed != null && !containerRunningOnHost(routed)) {
                    log.info("[SQ-6] singleton reconcile instanceId={} — routed container {} is not running "
                            + "on the host; dropping it", instanceId, shortId(routed.containerId()));
                    closeBrokerChannel(routed.containerId());
                    routed.setBrokerReady(false);
                    routed.setContainerId(null);
                    routed.setStatus(StatefulStatus.STOPPED);
                    byKey.remove(routed.key(), routed);
                    routed = null;
                }
                List<StatefulContainerView> family = listStatefulContainers().stream()
                        .filter(c -> c.name() != null
                                && (c.name().equals(base) || c.name().startsWith(base + "-")))
                        .toList();
                String keep = routed == null ? null : routed.containerId();
                if (keep == null) {
                    StatefulContainerView survivor = family.stream()
                            .filter(StatefulContainerView::running)
                            .min(java.util.Comparator.comparing(c -> !base.equals(c.name())))
                            .orElse(null);
                    if (survivor != null) {
                        adoptSurvivor(survivor.containerId());
                        keep = survivor.containerId();
                    }
                }
                int removed = 0;
                for (StatefulContainerView c : family) {
                    if ((keep != null && sameContainerId(keep, c.containerId())) || isDraining(c.containerId())) {
                        continue;
                    }
                    log.warn("[SQ-6] singleton reconcile instanceId={} — removing extra container {} ({}, "
                            + "running={}); one container per platform singleton", instanceId, c.name(),
                            shortId(c.containerId()), c.running());
                    stopByContainerName(c.name());
                    removed++;
                }
                return removed;
            } catch (Exception e) {
                log.warn("[SQ-6] singleton reconcile instanceId={} failed (best-effort): {}",
                        instanceId, e.toString());
                return 0;
            }
        }
    }

    /** Adopts one running container through the boot reconcile's own decision; best-effort. */
    private void adoptSurvivor(String containerId) {
        OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerId), lifecycleTimeout());
        if (!r.ok()) {
            return;
        }
        try {
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode c = root.isArray() && !root.isEmpty() ? root.get(0) : root;
            if (c != null && !c.isMissingNode()) {
                reconcileOne(containerId, c);
            }
        } catch (Exception e) {
            log.warn("[SQ-6] singleton reconcile: adopting {} failed: {}", shortId(containerId), e.toString());
        }
    }

    private boolean isDraining(String containerId) {
        return drainingByContainerId.keySet().stream().anyMatch(d -> sameContainerId(d, containerId));
    }

    /** Full vs short container ids compare by prefix in either direction. */
    private static boolean sameContainerId(String a, String b) {
        return a != null && b != null && !a.isBlank() && !b.isBlank()
                && (a.equals(b) || a.startsWith(b) || b.startsWith(a));
    }

    /**
     * Creates the named persistent volume if absent. Encryption-at-rest is delegated to the host
     * volume root (operator points podman's volume path at an encrypted mount); where a future runtime
     * exposes a per-volume encryption opt it can be added here.
     */
    private void ensureVolume(String volumeName) {
        if (volumeName == null || volumeName.isBlank()) {
            return;
        }
        // `volume exists` returns non-zero when absent; only create then (create-on-existing errors).
        OciCli.Result exists = cli.run(List.of(cli.cliPath(), "volume", "exists", volumeName), lifecycleTimeout());
        if (exists.ok()) {
            return;
        }
        String encryptedLabel = "studioquack.sqs.encrypted=" + (props.isEncryptVolume() ? "host-root" : "off");
        OciCli.Result created = cli.run(List.of(cli.cliPath(), "volume", "create",
                "--label", "studioquack.sqs.stateful=1", "--label", encryptedLabel, volumeName), lifecycleTimeout());
        if (!created.ok()) {
            log.warn("[SQ-6] stateful volume create {} failed: {} (run may still succeed if it exists)",
                    volumeName, created.firstErrLine());
        }
    }

    /**
     * Argv is name-only; the container env map is applied to the podman child (the {@code systemd-run
     * --scope} wrapper passes it through to podman unchanged).
     */
    private OciCli.Result runContainer(StatefulRunArgs args) {
        List<String> argv = containerScope().wrap(args.toArgv(), args.containerName(),
                System.currentTimeMillis());
        return cli.run(argv, null, lifecycleTimeout(), args.containerEnv());
    }

    private volatile ContainerScope containerScope;

    private ContainerScope containerScope() {
        ContainerScope s = containerScope;
        if (s == null) {
            synchronized (this) {
                s = containerScope;
                if (s == null) {
                    s = ContainerScope.detect(cli);
                    containerScope = s;
                }
            }
        }
        return s;
    }

    /** Builds the {@code podman run} argv for the instance, wiring the launcher/helper env. */
    private StatefulRunArgs buildRunArgs(StatefulInstance inst, String imageRef) {
        return buildRunArgs(inst, imageRef, null, null, null, null);
    }

    /** As above, optionally publishing a loopback host port and extra env pairs. */
    private StatefulRunArgs buildRunArgs(StatefulInstance inst, String imageRef, Integer hostPort,
                                      Integer containerPort, Map<String, String> extraEnv,
                                      Integer reversePort) {
        StatefulRunArgs.Builder b = StatefulRunArgs.builder(cli.cliPath(), imageRef, inst.containerName())
                .volumeName(inst.volumeName())
                .volumeMountPath(props.getVolumeMountPath())
                // A temporary container serves one job: a crash ends that job, never restarts it.
                .restartPolicy(inst.isTemporary() ? null : props.getRestartPolicy())
                .temporary(inst.isTemporary())
                .caps(effectiveCaps(inst.caps()))
                .envId(inst.envId())
                .imageRef(imageRef)
                .instanceId(inst.instanceId())
                // Persist reverse posture on the container — the in-memory claim dies with the JVM.
                .reversePort(reversePort)
                // Dispatch-capable provenance — so reconcile rebuilds the flag from the still-running container
                // after a daemon restart. Unlabeled = omit (FIFO).
                .dispatchCapable(inst.dispatchCapable())
                // And the network it starts on, so a mid-drain restart re-binds the reverse port and the routing
                // pin on the RIGHT network.
                .networkKey(egressManager == null ? null : egressManager.defaultNetworkKey())
                // The relaxed profile still applies the seccomp baseline + runsc when available.
                .runtimeFlag(runtime == null ? "" : runtime.runtimeFlagFor(null))
                .seccompProfilePath(SeccompProfile.profilePath(
                        runtime != null && runtime.isolationEnforced()))
                .seccompRequired(runtime != null && runtime.isolationEnforced())
                // Daemon-owned DNS pins: a stateful instance never resolves via the bridge gateway's forwarder
                // into the host's internal DNS.
                .dnsServers(dnsServers)
                .pidsLimit(pidsLimit)
                .unlimited(StatefulInstance.isBuiltinNodeLibrary(inst.instanceId()))
                // Signals to the in-instance app / helper SDK that it is a stateful runner.
                .env("SQ_STATEFUL_RUNNER", "1")
                .env("SQ_APP_PORT", Integer.toString(props.getAppPort()))
                .env("SQ_APP_PATH", props.getAppPath())
                // BYO contract prefers SQ_HELPER_SOCKET when set — honour that name exactly.
                .env("SQ_HELPER_SOCKET", HelperBroker.SOCKET_PATH);
        if (extraEnv != null) {
            for (Map.Entry<String, String> e : extraEnv.entrySet()) {
                if (e.getKey() != null && !e.getKey().isBlank()) {
                    b.env(e.getKey(), e.getValue() == null ? "" : e.getValue());
                }
            }
        }
        if (hostPort != null && containerPort != null) {
            b.publishPort(hostPort, containerPort);
        }
        return b.build();
    }

    /**
     * {@code --storage-opt size=} is honoured only on an XFS-backed podman store; on ext4 the run
     * hard-fails at container create (exit 125). The disk cap is documented best-effort ({@link
     * StatefulCaps}), so where the store cannot enforce it the cap is DROPPED — the instance still
     * starts, bounded by the ram/cpu caps.
     */
    private StatefulCaps effectiveCaps(StatefulCaps caps) {
        if (caps == null || caps.diskMb() <= 0 || sizeQuotaProbe.sizeQuotaSupported()) {
            return caps;
        }
        if (diskQuotaDropLogged.compareAndSet(false, true)) {
            log.warn("[SQ-6] stateful disk cap unsupported on this host (podman store backing FS is "
                    + "not XFS) — starting instances WITHOUT --storage-opt size= "
                    + "(ram/cpu caps still apply)");
        }
        return new StatefulCaps(caps.cpu(), caps.ramMb(), 0, caps.gpu());
    }

    /**
     * After a WireGuard handshake has delivered the current Network Access Control rules and the
     * reverse relay is bound, re-publish reverse ports of stateful instances that survived the daemon
     * restart. Discovers posture from the container label (not the empty in-memory registry).
     */
    public void republishSurvivingReversePorts() {
        if (reverseRelay == null || !reverseRelay.anyOverlayUp() || networkRuleStore == null
                || !cli.available()) {
            return;
        }
        OciCli.Result listed = cli.run(List.of(cli.cliPath(), "ps", "-q",
                "--filter", "label=" + StatefulEndpointResolver.STATEFUL_LABEL + "=1"),
                lifecycleTimeout());
        if (!listed.ok()) {
            log.warn("[SQ-6] reverse republish: podman ps failed: {}", listed.firstErrLine());
            return;
        }
        String out = listed.out() == null ? "" : listed.out().trim();
        if (out.isEmpty()) {
            return;
        }
        int republished = 0;
        for (String id : out.split("\\R+")) {
            String containerId = id == null ? "" : id.trim();
            if (containerId.isEmpty()) {
                continue;
            }
            if (republishOneSurviving(containerId)) {
                republished++;
            }
        }
        if (republished > 0) {
            log.info("[SQ-6] reverse republish: restored {} surviving reverse port(s)", republished);
        }
    }

    /**
     * @return true when this container's reverse port was (re)published
     */
    private boolean republishOneSurviving(String containerId) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerId), lifecycleTimeout());
            if (!r.ok()) {
                return false;
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode c = root.isArray() && !root.isEmpty() ? root.get(0) : root;
            if (c == null || c.isMissingNode()) {
                return false;
            }
            // `podman ps` already filters to running, but a race to stop between list and inspect
            // must not resurrect a dead container's listener.
            if (!c.path("State").path("Running").asBoolean(false)) {
                return false;
            }
            JsonNode labels = c.path("Config").path("Labels");
            String portRaw = labels.path(StatefulRunArgs.REVERSE_PORT_LABEL).asText("").trim();
            if (portRaw.isEmpty()) {
                return false;
            }
            int port;
            try {
                port = Integer.parseInt(portRaw);
            } catch (NumberFormatException bad) {
                return false;
            }
            if (port < 1 || port > 65535) {
                return false;
            }
            // Wholesale handshake projection: a rule deleted while we were down is already gone.
            if (networkRuleStore.allowedSourceEngineIds(port).isEmpty()) {
                log.info("[SQ-6] reverse republish: skipping port {} — no inbound Network Access "
                        + "Control rule after handshake", port);
                return false;
            }
            String instanceId = labels.path(StatefulEndpointResolver.INSTANCE_LABEL).asText("").trim();
            String envId = labels.path(StatefulEndpointResolver.ENV_LABEL).asText("").trim();
            String imageRef = labels.path("studioquack.sqs.ref").asText("").trim();
            if (instanceId.isEmpty()) {
                instanceId = "inst_" + containerId.substring(0, Math.min(12, containerId.length()));
            }
            String claimant = instanceId;
            String ip = containerBridgeIp(containerId);
            if (ip == null) {
                log.warn("[SQ-6] reverse republish: container {} has no bridge address — skipped",
                        shortId(containerId));
                return false;
            }
            // The container label says which network it was started on; re-publish on THAT network (a
            // mid-drain survivor may belong to the old one) and re-assert its routing pin, so the restart
            // changes nothing about how its packets leave.
            String networkKey = labels.path(StatefulRunArgs.NETWORK_LABEL).asText("").trim();
            if (egressManager != null && !networkKey.isEmpty()) {
                try {
                    egressManager.assignContainerNetworkTo(ip, networkKey);
                } catch (Exception assign) {
                    log.warn("[SQ-6] reverse republish: network re-pin of container {} failed: {}",
                            shortId(containerId), assign.toString());
                }
            }
            reverseRelay.publish(port, ip, port, claimant, networkKey.isEmpty() ? null : networkKey);
            // Re-adopt into the in-memory registry so a later stop unpublishes the same claim.
            StatefulInstance existing = byInstanceId.get(instanceId);
            if (existing == null) {
                String containerName = c.path("Name").asText("").trim();
                if (containerName.startsWith("/")) {
                    containerName = containerName.substring(1);
                }
                if (containerName.isEmpty()) {
                    containerName = props.getContainerNamePrefix() + instanceId;
                }
                StatefulInstance adopted = new StatefulInstance(instanceId, envId, imageRef,
                        containerName, props.getVolumeNamePrefix() + instanceId,
                        StatefulCaps.uncapped(), true);
                adopted.setContainerId(containerId);
                adopted.setStatus(StatefulStatus.RUNNING);
                adopted.setPublishedReversePort(port);
                adopted.setNetworkKey(networkKey.isEmpty() ? null : networkKey);
                adopted.setAssignedBridgeIp(ip);
                adopted.setDispatchCapable(dispatchCapableFromLabels(labels));
                register(adopted);
            } else {
                existing.setContainerId(containerId);
                existing.setStatus(StatefulStatus.RUNNING);
                existing.setPublishedReversePort(port);
                existing.setNetworkKey(networkKey.isEmpty() ? null : networkKey);
                existing.setAssignedBridgeIp(ip);
                existing.setDispatchCapable(dispatchCapableFromLabels(labels));
                register(existing);
            }
            return true;
        } catch (Exception e) {
            log.warn("[SQ-6] reverse republish: container {} failed: {}", shortId(containerId), e.toString());
            return false;
        }
    }

    /**
     * Best-effort, idempotent reverse-port assertion for a container that is ALREADY running. Two gaps
     * exist between the start-time publish and the handshake-time republish: {@link #start}'s join
     * path returns before the publish step, and a start that raced the overlay bring-up defers the
     * publish.
     */
    public void ensureReversePortPublished(String envId, String instanceId, int port) {
        if (reverseRelay == null || !reverseRelay.anyOverlayUp() || !cli.available()) {
            return;
        }
        StatefulInstance inst = instanceId == null ? null : byInstanceId.get(instanceId);
        if (inst != null && inst.publishedReversePort() != null) {
            return;
        }
        try {
            String containerName = inst != null ? inst.containerName() : containerNameFor(instanceId);
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerName), lifecycleTimeout());
            if (!r.ok()) {
                return;
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode c = root.isArray() && !root.isEmpty() ? root.get(0) : root;
            if (c == null || c.isMissingNode() || !c.path("State").path("Running").asBoolean(false)) {
                return;
            }
            String ip = containerBridgeIp(containerName);
            if (ip == null) {
                return;
            }
            // Publish with the caller-known port (works for labelled AND legacy unlabelled containers alike —
            // the label only matters to the handshake-time republish, which has no caller to ask). Start-time
            // semantics, not the republish's rule gate: the relay's accept-time ACL still default-denies every
            // source without an inbound Network Access Control rule, so publishing here grants reachability to
            // nobody the operator has not allowed.
            JsonNode labels = c.path("Config").path("Labels");
            String networkKey = labels.path(StatefulRunArgs.NETWORK_LABEL).asText("").trim();
            reverseRelay.publish(port, ip, port, instanceId, networkKey.isEmpty() ? null : networkKey);
            if (inst != null) {
                inst.setPublishedReversePort(port);
                inst.setAssignedBridgeIp(ip);
                register(inst);
            }
            log.info("[SQ-6] reverse ensure: published overlay port {} for container {} "
                    + "(instanceId={})", port, containerName, instanceId);
        } catch (Exception e) {
            log.warn("[SQ-6] reverse ensure: instanceId={} port={} failed (handshake republish "
                    + "remains the fallback): {}", instanceId, port, e.toString());
        }
    }

    /* ------------------------------------------------------------------ */
    /* Host reconcile at daemon (re-)registration. */
    /* ------------------------------------------------------------------ */

    /** The most recent {@link #reconcileWithHost()} outcome (null before the first pass). */
    private volatile StatefulReconcileReport lastReconcile;

    /** The last host-reconcile outcome, for the {@code status} payload / telemetry. Null pre-boot. */
    public StatefulReconcileReport lastReconcile() {
        return lastReconcile;
    }

    /**
     * Reconciles the in-memory instance registry against the containers actually present on the host
     * ({@code studioquack.sqs.stateful=1}, any state). Runs once per daemon boot — the registry is
     * JVM-memory only, so without this pass a restart left every surviving container orphaned: the
     * refuse-second-start guard ({@link #hasLiveInstance}) went blind, and {@code stop} answered "not
     * tracked" while the customer's container kept running.
     */
    public StatefulReconcileReport reconcileWithHost() {
        if (!cli.available()) {
            StatefulReconcileReport report = StatefulReconcileReport.noRuntime(java.time.Instant.now().toString());
            lastReconcile = report;
            log.info("[SQ-6] stateful reconcile skipped — no container runtime at boot");
            return report;
        }
        List<StatefulReconcileReport.Entry> entries = new java.util.ArrayList<>();
        OciCli.Result listed = cli.run(List.of(cli.cliPath(), "ps", "-a", "--no-trunc",
                "--filter", "label=" + StatefulEndpointResolver.STATEFUL_LABEL + "=1",
                "--format", "{{.ID}}"), lifecycleTimeout());
        if (!listed.ok()) {
            log.warn("[SQ-6] stateful reconcile: podman ps failed: {}", listed.firstErrLine());
            StatefulReconcileReport report = StatefulReconcileReport.of(java.time.Instant.now().toString(),
                    entries);
            lastReconcile = report;
            return report;
        }
        // Oldest first: when a restart-during-start left TWO containers for one (env, imageRef),
        // the elder is the one the pre-restart control-plane row was written for — it is adopted,
        // the younger is the duplicate.
        record Survivor(String id, JsonNode inspect, java.time.Instant created) {
        }
        List<Survivor> survivors = new java.util.ArrayList<>();
        String out = listed.out();
        for (String line : out.split("\\R+")) {
            String containerId = line.trim();
            if (containerId.isEmpty()) {
                continue;
            }
            try {
                OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerId), lifecycleTimeout());
                if (!r.ok()) {
                    continue; // raced a removal — nothing to decide
                }
                JsonNode root = mapper.readTree(r.stdout());
                JsonNode c = root.isArray() && !root.isEmpty() ? root.get(0) : root;
                if (c == null || c.isMissingNode()) {
                    continue;
                }
                java.time.Instant created = null;
                try {
                    created = java.time.Instant.parse(c.path("Created").asText(""));
                } catch (Exception ignored) {
                    // Unparseable creation time sorts last (nulls-last below).
                }
                survivors.add(new Survivor(containerId, c, created));
            } catch (Exception e) {
                log.warn("[SQ-6] stateful reconcile: inspect {} failed: {}", shortId(containerId), e.toString());
            }
        }
        survivors.sort(java.util.Comparator.comparing(Survivor::created,
                java.util.Comparator.nullsLast(java.util.Comparator.naturalOrder())));

        for (Survivor s : survivors) {
            entries.add(reconcileOne(s.id(), s.inspect()));
        }
        StatefulReconcileReport report = StatefulReconcileReport.of(java.time.Instant.now().toString(), entries);
        lastReconcile = report;
        log.info("[SQ-6] stateful reconcile: adopted={} reaped={} reapedDuplicate={} ignored={} (containers={})",
                report.adopted(), report.reaped(), report.reapedDuplicate(), report.ignored(), entries.size());
        return report;
    }

    /** Decides and executes one container's reconcile action; never throws. */
    private StatefulReconcileReport.Entry reconcileOne(String containerId, JsonNode c) {
        JsonNode labels = c.path("Config").path("Labels");
        String envId = labels.path(StatefulEndpointResolver.ENV_LABEL).asText("").trim();
        String imageRef = labels.path("studioquack.sqs.ref").asText("").trim();
        String instanceId = labels.path(StatefulEndpointResolver.INSTANCE_LABEL).asText("").trim();
        String state = c.path("State").path("Status").asText("").trim().toLowerCase(java.util.Locale.ROOT);
        String containerName = c.path("Name").asText("").trim();
        if (containerName.startsWith("/")) {
            containerName = containerName.substring(1);
        }
        if (instanceId.isEmpty()) {
            instanceId = "inst_" + containerId.substring(0, Math.min(12, containerId.length()));
        }
        if (drainingByContainerId.containsKey(containerId)) {
            return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                    state, StatefulReconcileReport.ACTION_IGNORED,
                    "draining successor-replace — left running");
        }

        // Dead residue (a failed/interrupted start, a crashed instance): removing it kills no
        // workload. The named volume is NOT touched — a later start of the same instance id
        // re-attaches its data.
        boolean running = c.path("State").path("Running").asBoolean(false);
        if (!running) {
            if ("exited".equals(state) || "created".equals(state) || "dead".equals(state)
                    || "stopped".equals(state) || "removing".equals(state)) {
                cli.run(List.of(cli.cliPath(), "rm", "-f", containerId), lifecycleTimeout());
                log.info("[SQ-6] stateful reconcile: reaped non-running container {} ({}) instance={}",
                        shortId(containerId), state, instanceId);
                return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                        state, StatefulReconcileReport.ACTION_REAPED, "container not running — residue removed (volume kept)");
            }
            // paused / unknown live-ish state: not ours to kill on a guess.
            log.info("[SQ-6] stateful reconcile: ignoring container {} in unhandled state '{}'", shortId(containerId), state);
            return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                    state, StatefulReconcileReport.ACTION_IGNORED, "unhandled state '" + state + "' — left untouched");
        }
        if (envId.isEmpty() || imageRef.isEmpty()) {
            // Running but unidentifiable — a running workload is never reaped on a guess.
            log.warn("[SQ-6] stateful reconcile: running container {} lacks env/ref provenance labels — left running, reported",
                    shortId(containerId));
            return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                    state, StatefulReconcileReport.ACTION_IGNORED, "missing env/ref provenance labels");
        }

        // Already tracked under a rebound key (a start joined it for the current env): the label env
        // is the previous enrolment's, and keying by it would adopt the same container twice.
        StatefulInstance tracked = byInstanceId.get(instanceId);
        if (tracked != null && tracked.isRunning() && containerId.equals(tracked.containerId())) {
            return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                    state, StatefulReconcileReport.ACTION_ADOPTED, "already tracked");
        }

        // Serialize with any in-flight start for the same key (same lock start() takes).
        Object lock = startLocks.computeIfAbsent(StatefulInstance.key(envId, imageRef), k -> new Object());
        synchronized (lock) {
            StatefulInstance existing = byKey.get(StatefulInstance.key(envId, imageRef));
            if (existing != null && existing.isRunning()) {
                if (containerId.equals(existing.containerId())) {
                    // Idempotent re-run (e.g. overlay bounce re-triggered a reconcile).
                    return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                            state, StatefulReconcileReport.ACTION_ADOPTED, "already tracked");
                }
                if (drainingByContainerId.containsKey(containerId)) {
                    // Image-replace drain window: the old container is still serving in-flight work. Reaping it as a
                    // duplicate would kill that work.
                    return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                            state, StatefulReconcileReport.ACTION_IGNORED,
                            "draining successor-replace — left running");
                }
                // The ambiguity bans: two live containers for one (env, imageRef). The tracked / elder one stays;
                // this one goes (container only — its volume survives).
                cli.run(List.of(cli.cliPath(), "stop", containerId), lifecycleTimeout());
                cli.run(List.of(cli.cliPath(), "rm", "-f", containerId), lifecycleTimeout());
                log.warn("[SQ-6] stateful reconcile: REAPED DUPLICATE container {} instance={} — one live "
                                + "instance per (env, image) is the hard invariant; kept {}",
                        shortId(containerId), instanceId, shortId(existing.containerId()));
                return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                        state, StatefulReconcileReport.ACTION_REAPED_DUPLICATE,
                        "second live container for (env, imageRef) — removed to restore the one-instance invariant");
            }

            // ADOPT: rebuild the registry record from container labels so stop/status/dispatch and
            // the refuse-second-start guard work again. Caps re-apply on the NEXT start; the live
            // container keeps whatever it was created with.
            StatefulInstance adopted = new StatefulInstance(instanceId, envId, imageRef,
                    containerName.isEmpty() ? props.getContainerNamePrefix() + instanceId : containerName,
                    props.getVolumeNamePrefix() + instanceId, StatefulCaps.uncapped(), true);
            adopted.setContainerId(containerId);
            adopted.setStatus(StatefulStatus.RUNNING);
            adopted.setDispatchCapable(dispatchCapableFromLabels(labels));
            String networkKey = labels.path(StatefulRunArgs.NETWORK_LABEL).asText("").trim();
            if (!networkKey.isEmpty()) {
                adopted.setNetworkKey(networkKey);
                String ip = containerBridgeIp(containerId);
                if (ip != null) {
                    adopted.setAssignedBridgeIp(ip);
                    // Re-assert the routing pin (mid-drain correctness), mirroring the reverse-port
                    // republish path. Best-effort: a failed pin is reported, adoption stands.
                    if (egressManager != null) {
                        try {
                            egressManager.assignContainerNetworkTo(ip, networkKey);
                        } catch (Exception assign) {
                            log.warn("[SQ-6] stateful reconcile: network re-pin of {} failed: {}",
                                    shortId(containerId), assign.toString());
                        }
                    }
                }
            }
            register(adopted);
            // The helper-broker pipe died with the previous JVM — re-attach best-effort so
            // studioquack.execute/llm callbacks on the adopted instance resume. Degrades loudly
            // (brokerReady=false + note), never fails the adoption.
            plantHelperBroker(adopted, containerId);
            log.info("[SQ-6] stateful reconcile: adopted running container {} instance={} ref={}",
                    shortId(containerId), instanceId, imageRef);
            return new StatefulReconcileReport.Entry(instanceId, containerId, containerName, imageRef,
                    state, StatefulReconcileReport.ACTION_ADOPTED, "adopted into the daemon registry");
        }
    }

    private Object inspectHealth(String containerRef) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerRef), lifecycleTimeout());
            if (!r.ok()) {
                return null;
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode container = root.isArray() && root.size() > 0 ? root.get(0) : root;
            JsonNode health = container.path("State").get("Health");
            return (health == null || health.isNull()) ? null : mapper.convertValue(health, Object.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Pins the instance to the current default overlay network and records both the network and the
     * bridge address on the instance (the pin is released at stop). A pin failure fails the start
     * loudly — an instance that cannot be steered to its network must not run on the wrong one.
     */
    private void assignInstanceNetwork(StatefulInstance inst, String containerId) {
        if (egressManager == null) {
            return;
        }
        String ip = containerBridgeIp(containerId);
        if (ip == null) {
            log.debug("[SQ-6] instance={} container={} — no bridge address resolved; network "
                    + "assignment skipped", inst.instanceId(),
                    shortId(containerId));
            return;
        }
        String key = egressManager.assignContainerNetwork(ip);
        inst.setAssignedBridgeIp(ip);
        inst.setNetworkKey(key);
    }

    /**
     * Claim + publish the instance's port on the engine's overlay address. The claim is per engine and
     * loud on conflict (the message names the holding node); the relay binds only the overlay address,
     * so nothing here is diallable from the internet.
     */
    private void publishReversePort(StatefulInstance inst, String containerId, int port) {
        if (reverseRelay == null) {
            throw new IllegalStateException("reverse channel is unavailable on this daemon");
        }
        String ip = containerBridgeIp(containerId);
        if (ip == null) {
            throw new IllegalStateException("container " + shortId(containerId)
                    + " has no bridge address to publish to");
        }
        // Publish on the instance's own network: a mid-drain old instance keeps its audience on the old
        // address; its successor publishes the same port on the new one.
        reverseRelay.publish(port, ip, port, inst.instanceId(), inst.networkKey());
        inst.setPublishedReversePort(port);
    }

    /** The container's address on its sandbox network — the relay's dial target. */
    private String containerBridgeIp(String containerRef) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerRef), lifecycleTimeout());
            if (!r.ok()) {
                return null;
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode c = root.isArray() && root.size() > 0 ? root.get(0) : root;
            // Per-network address first (the flat IPAddress stays empty off the default bridge).
            JsonNode networks = c.path("NetworkSettings").path("Networks");
            if (networks.isObject()) {
                for (JsonNode net : networks) {
                    String ip = net.path("IPAddress").asText("").trim();
                    if (!ip.isEmpty()) {
                        return ip;
                    }
                }
            }
            String flat = c.path("NetworkSettings").path("IPAddress").asText("").trim();
            return flat.isEmpty() ? null : flat;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Plants + launches {@code sq_helper_broker.py} (same plant→launch→readiness→HelperPipeChannel
     * pattern as {@code OciWarmer.startHelperBroker}), but degrades loudly instead of failing start:
     * BYO stateful images may ship {@code curl} without {@code python3}.
     */
    private void plantHelperBroker(StatefulInstance inst, String containerId) {
        if (!helperBrokerEnabled || helperRouter == null || containerId == null || containerId.isBlank()) {
            inst.setBrokerReady(false);
            return;
        }
        Process process = null;
        try {
            OciCli.Result plant = cli.run(List.of(cli.cliPath(), "exec", containerId, "sh", "-c",
                    HelperBroker.plantScript()), lifecycleTimeout());
            if (!plant.ok()) {
                String detail = plant.firstErrLine();
                String note = (detail == null || detail.isBlank())
                        ? BROKER_NOTE_NO_RUNTIME
                        : BROKER_NOTE_NO_RUNTIME + " — " + detail;
                log.warn("[SQ-6] helper-broker plant failed for instance={} container={} (exit={}): {} — "
                                + "helper callbacks (studioquack.execute/llm/connect) unavailable for this "
                                + "instance — the image needs python3",
                        inst.instanceId(), shortId(containerId), plant.exitCode(), detail);
                markBrokerUnavailable(inst, note);
                return;
            }
            process = HelperBroker.launch(cli.cliPath(), containerId).start();
            OciCli.Result ready = cli.run(List.of(cli.cliPath(), "exec", containerId, "sh", "-c",
                            HelperBroker.readinessScript(helperBrokerReadyTimeoutSec)),
                    Duration.ofSeconds(helperBrokerReadyTimeoutSec + 5L));
            if (!ready.ok()) {
                process.destroyForcibly();
                String detail = ready.firstErrLine();
                String note = (detail == null || detail.isBlank())
                        ? BROKER_NOTE_NO_RUNTIME
                        : BROKER_NOTE_NO_RUNTIME + " — " + detail;
                log.warn("[SQ-6] helper-broker readiness failed for instance={} container={} within {}s — "
                                + "helper callbacks (studioquack.execute/llm/connect) unavailable for this "
                                + "instance — the image needs python3",
                        inst.instanceId(), shortId(containerId), helperBrokerReadyTimeoutSec);
                markBrokerUnavailable(inst, note);
                return;
            }
            HelperPipeChannel channel = new HelperPipeChannel(process, helperRouter);
            brokerChannels.put(containerId, channel);
            // End-to-end probe: the readiness poll above only proves the UDS FILE exists. The probe pushes a
            // frame through broker → pipe → daemon router → pipe → broker and requires the daemon's 401 back.
            // 2026-08-28 incident: a broker was alive with the socket present while its stdout pipe to the
            // daemon never delivered a frame — every helper call parked until its curl budget and nodes died
            // on the 300s sandbox deadline with zero daemon-side trace.
            OciCli.Result probe = cli.run(List.of(cli.cliPath(), "exec", containerId, "sh", "-c",
                            HelperBroker.probeScript()),
                    Duration.ofSeconds(20));
            if (!probe.ok()) {
                brokerChannels.remove(containerId);
                channel.close();
                String detail = probe.firstErrLine();
                log.warn("[SQ-6] helper-broker probe FAILED for instance={} container={} (exit={}): {} — "
                                + "helper callbacks unavailable for this instance — the broker bound "
                                + "the socket but a frame round-trip to the daemon did not complete",
                        inst.instanceId(), shortId(containerId), probe.exitCode(), detail);
                markBrokerUnavailable(inst, BROKER_NOTE_PROBE_FAILED
                        + (detail == null || detail.isBlank() ? "" : " — " + detail));
                return;
            }
            inst.setBrokerReady(true);
            inst.setBrokerNote(null);
            log.info("[SQ-6] stateful helper-broker ready instance={} container={} (probe ok)",
                    inst.instanceId(), shortId(containerId));
        } catch (IOException e) {
            if (process != null) {
                process.destroyForcibly();
            }
            log.warn("[SQ-6] helper-broker launch failed for instance={} container={}: {} — "
                            + "helper callbacks (studioquack.execute/llm/connect) unavailable for this "
                            + "instance — the image needs python3",
                    inst.instanceId(), shortId(containerId), e.toString());
            markBrokerUnavailable(inst, BROKER_NOTE_NO_RUNTIME + " — " + e.toString());
        } catch (Exception e) {
            if (process != null) {
                process.destroyForcibly();
            }
            log.warn("[SQ-6] helper-broker setup failed for instance={} container={}: {} — "
                            + "helper callbacks (studioquack.execute/llm/connect) unavailable for this "
                            + "instance — the image needs python3",
                    inst.instanceId(), shortId(containerId), e.toString());
            markBrokerUnavailable(inst, BROKER_NOTE_NO_RUNTIME + " — " + e.toString());
        }
    }

    private static void markBrokerUnavailable(StatefulInstance inst, String note) {
        inst.setBrokerReady(false);
        inst.setBrokerNote(note);
    }

    /**
     * Cold-start readiness gate: poll the in-container app listener until it accepts TCP on {@code
     * 127.0.0.1:$SQ_APP_PORT} or {@code props.appReadyTimeoutSec} elapses. Returns {@code null} when
     * the listener is up — and also when the image ships no probe tool (python3/curl/wget), because
     * then the gate cannot probe and the thin launcher surfaces that case on its own ({@code
     * NO_HTTP_CLIENT_EXIT}).
     */
    private String awaitAppListener(StatefulInstance inst, String containerId) {
        int timeoutSec = Math.max(1, props.getAppReadyTimeoutSec());
        try {
            // The probe script rides stdin (sh -s), never a sh -c argv element: on Windows hosts the Win32
            // command-line round-trip shatters a quote-bearing argv script before it reaches the container,
            // which used to surface here as a bogus "not ready" cold-start failure (exit=2, stderr = a
            // fragment of the script).
            OciCli.Result probe = cli.run(
                    ShExec.stdinArgv(cli.cliPath(), containerId),
                    ShExec.utf8(appReadinessScript(timeoutSec)),
                    Duration.ofSeconds(timeoutSec + 10L));
            if (probe.ok()) {
                return null;
            }
            String dead = containerDeathNote(inst, containerId);
            if (dead != null) {
                return dead;
            }
            if (probe.exitCode() != 1) {
                // Exit 1 is the script's own "deadline reached" — the only clean not-ready. Anything else (syntax
                // error, missing sh, wedged exec, wall-clock timeout) is a BROKEN probe, and a broken probe must
                // not brick an otherwise healthy start: the launcher request is the real test (same fail-open
                // posture as the catch).
                log.warn("[SQ-6] app-readiness probe unusable for instance={} container={} exit={} "
                        + "err={} — skipping the gate", inst.instanceId(), shortId(containerId),
                        probe.exitCode(), probe.firstErrLine());
                return null;
            }
            log.warn("[SQ-6] app-readiness probe instance={} container={} exit={} err={}",
                    inst.instanceId(), shortId(containerId), probe.exitCode(), probe.firstErrLine());
            return "app listener not ready within " + timeoutSec + "s";
        } catch (Exception e) {
            // A probe ERROR (exec failed, runtime hiccup) must not brick an otherwise healthy
            // start — the launcher request is the real test. Only a clean non-zero probe
            // (timeout, listener never came up) fails the gate.
            log.warn("[SQ-6] app-readiness probe errored for instance={} container={}: {} — skipping gate",
                    inst.instanceId(), shortId(containerId), e.toString());
            return null;
        }
    }

    /**
     * When the container is no longer running, returns a short operator-facing note with the exit code
     * and OOM-kill flag; null when the container is still up (a genuine listener timeout) or the state
     * cannot be read. The captured output is kept on {@code inst} so {@code stateful-instance-logs}
     * can still serve it after the container is removed.
     */
    private String containerDeathNote(StatefulInstance inst, String containerId) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerId), lifecycleTimeout());
            if (!r.ok()) {
                return null;
            }
            JsonNode root = mapper.readTree(r.stdout());
            JsonNode container = root.isArray() && root.size() > 0 ? root.get(0) : root;
            JsonNode state = container.path("State");
            String status = state.path("Status").asText("");
            // --restart unless-stopped re-runs a crashing app at once, so mid-boot the state can
            // read "running" again; a restart that already happened is still a boot death.
            int restarts = container.path("RestartCount").asInt(0);
            boolean restarting = state.path("Restarting").asBoolean(false);
            if (status.isBlank() || ("running".equalsIgnoreCase(status) && restarts == 0 && !restarting)) {
                return null;
            }
            int exitCode = state.path("ExitCode").asInt(-1);
            boolean oom = state.path("OOMKilled").asBoolean(false);
            // Which runtime ran it decides where a host-policy denial can come from: under runsc
            // the app's syscalls never reach host LSMs, under crun/runc they do.
            String ociRuntime = container.path("OCIRuntime").asText("");
            String note = "container exited during boot (status=" + status + ", exitCode=" + exitCode
                    + (restarts > 0 ? ", restarted " + restarts + "x" : "")
                    + (ociRuntime.isBlank() ? "" : ", runtime=" + ociRuntime)
                    + (oom ? ", OOM-killed — raise the instance RAM cap" : "") + ")";
            // The failed container is removed right after this returns, so its output is the
            // only record of WHY the app died — read it now or it is gone for good.
            String tail = bootOutputTail(containerId);
            if (tail == null) {
                return note;
            }
            inst.setLastBootOutput(tail);
            log.warn("[SQ-6] container={} boot output (last {} lines) before removal:\n{}",
                    shortId(containerId), BOOT_OUTPUT_TAIL_LINES, tail);
            String last = ContainerLogMerge.stripTimestamp(
                    tail.substring(tail.lastIndexOf('\n') + 1)).trim();
            return last.isEmpty() ? note : note + " — " + truncate(last, BOOT_OUTPUT_NOTE_MAX_CHARS);
        } catch (Exception e) {
            return null;
        }
    }

    /** Lines of a dead container's output kept in the daemon log. */
    private static final int BOOT_OUTPUT_TAIL_LINES = 25;
    /** Cap on the single output line appended to the operator-facing failure note. */
    private static final int BOOT_OUTPUT_NOTE_MAX_CHARS = 300;

    /**
     * Last {@link #BOOT_OUTPUT_TAIL_LINES} lines of a stopped container's stdout+stderr, or null
     * when the runtime returns nothing. Same data the operator already reads through
     * {@code stateful-instance-logs}; captured here because that command finds no container
     * once a failed start has removed it.
     */
    private String bootOutputTail(String containerId) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "logs", "--timestamps", "--tail",
                    Integer.toString(BOOT_OUTPUT_TAIL_LINES), containerId), lifecycleTimeout());
            String combined = ContainerLogMerge.merge(r.stdout(), r.stderr()).trim();
            return combined.isEmpty() ? null : stderrTail(combined, BOOT_OUTPUT_TAIL_LINES);
        } catch (Exception e) {
            log.debug("[SQ-6] boot output read failed for container={}: {}", shortId(containerId),
                    e.toString());
            return null;
        }
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }

    /**
     * The in-container readiness probe (pure + unit-testable). Prefers a python3 raw-socket
     * connect (proves the port accepts TCP without needing a well-formed HTTP response),
     * falls back to curl/wget (any HTTP response counts as up), and exits 0 immediately when
     * the image ships none of them so the gate degrades to a no-op instead of bricking BYO images.
     */
    static String appReadinessScript(int timeoutSec) {
        return String.join("\n",
                "SQ_PORT=\"${SQ_APP_PORT:-8080}\"",
                "SQ_END=$(( $(date +%s) + " + timeoutSec + " ))",
                "while :; do",
                "  if command -v python3 >/dev/null 2>&1; then",
                "    python3 -c 'import socket,sys",
                "s=socket.socket()",
                "s.settimeout(2)",
                "try:",
                "    s.connect((\"127.0.0.1\", int(sys.argv[1])))",
                "except OSError:",
                "    sys.exit(1)",
                "s.close()' \"$SQ_PORT\" && exit 0",
                "  elif command -v curl >/dev/null 2>&1; then",
                "    curl -sS -o /dev/null -m 2 \"http://127.0.0.1:${SQ_PORT}/\" && exit 0",
                "  elif command -v wget >/dev/null 2>&1; then",
                "    wget -q -O /dev/null -T 2 \"http://127.0.0.1:${SQ_PORT}/\" && exit 0",
                "  else",
                "    exit 0",
                "  fi",
                "  [ \"$(date +%s)\" -ge \"$SQ_END\" ] && exit 1",
                "  sleep 1",
                "done");
    }

    /**
     * Additive broker readiness keys on the podman health Object the Engine Settings surface renders.
     * Never strips existing health content; returns null only when health is null and the broker is ready.
     */
    private Object withBrokerHealth(Object health, StatefulInstance inst) {
        boolean ready = inst != null && inst.brokerReady();
        String note = inst == null ? null : inst.brokerNote();
        boolean hasNote = note != null && !note.isBlank();
        if (health instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (e.getKey() != null) {
                    copy.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
            copy.put("sqHelperBroker", ready ? "ready" : "unavailable");
            if (hasNote) {
                copy.put("sqHelperBrokerNote", note);
            }
            return copy;
        }
        if (health == null && !ready) {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("sqHelperBroker", "unavailable");
            out.put("sqHelperBrokerNote", hasNote ? note : BROKER_NOTE_NO_RUNTIME);
            return out;
        }
        return health;
    }

    private void closeBrokerChannel(String containerId) {
        if (containerId == null || containerId.isBlank()) {
            return;
        }
        HelperPipeChannel channel = brokerChannels.remove(containerId);
        if (channel != null) {
            channel.close();
        }
    }

    /** Best-effort cleanup of a named stateful container left behind by a failed `podman run`. */
    private void removeFailedStartContainer(StatefulInstance inst) {
        closeBrokerChannel(inst.containerId());
        inst.setBrokerReady(false);
        try {
            cli.run(List.of(cli.cliPath(), "rm", "-f", inst.containerName()), lifecycleTimeout());
        } catch (Exception cleanupError) {
            log.warn("[SQ-6] failed-start cleanup instance={} error: {}", inst.instanceId(), cleanupError.toString());
        }
    }

    private String inspectImageDigest(String ref) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "image", "inspect",
                    "--format", "{{.Id}}", ref), lifecycleTimeout());
            String id = r.ok() ? r.out() : "";
            return id.isEmpty() ? null : id;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Reads {@code LABEL studioquack.sqs.dispatch} from the image via {@code podman image inspect}.
     * Never throws — a start must not fail over an advisory label.
     */
    private boolean inspectImageDispatchCapable(String ref) {
        try {
            OciCli.Result r = cli.run(List.of(cli.cliPath(), "image", "inspect",
                    "--format", "{{json .Config.Labels}}", ref), lifecycleTimeout());
            if (!r.ok()) {
                return false;
            }
            String out = r.out();
            if (out.isEmpty() || "null".equals(out)) {
                return false;
            }
            JsonNode labels = mapper.readTree(out);
            return dispatchCapableFromLabels(labels);
        } catch (Exception e) {
            log.debug("[SQ-6] image dispatch-label inspect failed for {}: {}", ref, e.toString());
            return false;
        }
    }

    /** Rebuilds {@code dispatchCapable} from container (or image) labels. Missing → false. */
    private static boolean dispatchCapableFromLabels(JsonNode labels) {
        if (labels == null || labels.isNull() || !labels.isObject()) {
            return false;
        }
        return StatefulRunArgs.isDispatchLabelTruthy(
                labels.path(StatefulRunArgs.DISPATCH_LABEL).asText(""));
    }

    private static String parseDigest(String loadStdout) {
        if (loadStdout == null) {
            return null;
        }
        Matcher m = SHA256.matcher(loadStdout);
        return m.find() ? m.group() : null;
    }

    /** Maps podman {@code State.Status} ({@code running}/{@code exited}/…) to our wire status. */
    private static String mapPodmanState(String podmanStatus) {
        if (podmanStatus == null) {
            return StatefulStatus.UNKNOWN;
        }
        return switch (podmanStatus.trim().toLowerCase()) {
            case "running" -> StatefulStatus.RUNNING;
            case "created", "restarting", "paused" -> StatefulStatus.STARTING;
            case "exited", "dead", "removing", "stopped" -> StatefulStatus.STOPPED;
            default -> StatefulStatus.UNKNOWN;
        };
    }

    private Duration lifecycleTimeout() {
        return Duration.ofMillis(props.getLifecycleTimeoutMs());
    }

    private static String shortId(String id) {
        return id == null ? "" : id.substring(0, Math.min(12, id.length()));
    }
}
