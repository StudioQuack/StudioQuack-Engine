package com.studioquack.sqs.sandbox.stateful;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration surface for the client daemon's stateful custom-OCI runtime ({@code
 * com.studioquack.sqs.sandbox.stateful}). Binds the {@code sqs.sandbox.oci.*} tree that drives the
 * daemon-local blob store, the upload ingress credential, the stateful container/volume naming +
 * hardening deltas, and the in-instance app convention the thin launcher dials.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "sqs.sandbox.oci")
public class StatefulRuntimeProperties {

    /**
     * Daemon-local directory the upload ingress streams image tarballs into and {@code podman load}
     * reads from ({@code <blobStore>/<safeRef>.tar}). On-host blob layout; created on demand.
     */
    private String blobStore = "./data/oci-blobs";

    /**
     * Shared bearer the upload ingress verifies constant-time (the credential IS the request,
     * mirroring the log-download posture). Blank = the endpoint is DISABLED (fail-closed): no token
     * configured, no upload accepted.
     */
    private String uploadToken = "";

    /** Prefix for the named, persistent stateful container ({@code <prefix><instanceId>}). */
    private String containerNamePrefix = "sq-stateful-";

    /** Prefix for the named, persistent (encrypted) volume ({@code <prefix><instanceId>}). */
    private String volumeNamePrefix = "sq-stateful-vol-";

    /** In-container mount point for the named persistent volume. */
    private String volumeMountPath = "/data";

    /**
     * {@code --restart} policy for the long-lived stateful container. {@code unless-stopped} survives
     * a daemon/host restart but honours an explicit stop.
     */
    private String restartPolicy = "unless-stopped";

    /**
     * Default TCP port the user's in-instance app listens on (localhost). The thin
     * launcher dials {@code http://127.0.0.1:<appPort><appPath>}; overridable per image
     * via the {@code SQ_APP_PORT} container env the image author sets.
     */
    private int appPort = 8080;

    /** Default HTTP path the launcher POSTs each request's context to (user app convention). */
    private String appPath = "/invoke";

    /**
     * When {@code true}, request a named volume created with an encrypted driver opt
     * (documented relaxed-profile requirement — the persistent disk is encrypted at rest).
     * Best-effort: runtimes that reject the opt fall back to a plain named volume + a WARN.
     */
    private boolean encryptVolume = true;

    /** Wall-clock budget (ms) for a single stateful launcher request against the live instance. */
    private long requestTimeoutMs = 300_000L;

    /**
     * Idle reaper (ms) for a stateful launcher invoke while helper-channel frames are still flowing
     * ({@code stream=true} nodes). The launcher deadline slides on helper activity and only fires
     * after this zero-activity window — never mid-stream.
     */
    private long streamIdleTimeoutMs =
            com.studioquack.sqs.transport.StreamIdleLimits.DEFAULT_STREAM_IDLE_TIMEOUT_MS;

    /**
     * Transport cap (bytes) on the stateful launcher's captured app RESPONSE. The launcher echoes the
     * in-container app's HTTP response on stdout, so this stream is the result TRANSPORT, not a log —
     * a response larger than the cap is truncated mid-JSON and parses as EMPTY outputs (the silent
     * "provider produced files but the caller saw nothing" defect).
     */
    private long statefulResponseMaxBytes = 2_147_483_648L;

    /** Wall-clock budget (ms) for a lifecycle podman op (load/run/stop/inspect). */
    private long lifecycleTimeoutMs = 120_000L;

    /**
     * Cold-start readiness gate (seconds): after {@code podman run -d}, how long the daemon waits for
     * the in-container app to accept TCP on {@code 127.0.0.1:<appPort>} before the instance is marked
     * RUNNING and dispatch reaches it. Without the gate the first launcher request races the app boot
     * and dies with curl exit 7 (connection refused).
     */
    private int appReadyTimeoutSec = 60;

    /**
     * Max waiters already queued on a per-instance FIFO before a new dispatch is rejected immediately
     * with {@code SANDBOX_OVERLOADED} (never silently dropped; never parks the dispatch executor
     * indefinitely). Bound under {@code sqs.sandbox.oci.stateful-queue.max-depth}.
     */
    private int statefulQueueMaxDepth = 64;

    /**
     * How long a dispatch may wait for its turn on the per-instance FIFO before being rejected with
     * {@code SANDBOX_OVERLOADED}. Bound under {@code sqs.sandbox.oci.stateful-queue.wait-timeout-ms}.
     */
    private long statefulQueueWaitTimeoutMs = 30_000L;

    /**
     * Concurrent in-flight dispatches permitted against ONE dispatch-capable stateful instance ({@code
     * LABEL studioquack.sqs.dispatch=1}). Unlabeled instances stay at 1 (FIFO).
     */
    private int statefulDispatchParallelism = 8;
}
