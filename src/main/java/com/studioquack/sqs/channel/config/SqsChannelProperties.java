package com.studioquack.sqs.channel.config;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Configuration surface for the daemon secure-channel client ({@code com.studioquack.sqs.channel}). Binds the
 * {@code sqs.channel.*} property tree that drives enrolment, heartbeat, the exec-channel and the WireGuard
 * overlay handshake against the control plane.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "sqs.channel")
public class SqsChannelProperties {

    /**
     * Master switch. When {@code false} (the default for now) the channel beans are constructed but the background
     * loops (heartbeat / exec-channel / WireGuard) do NOT auto-start — so the 91-test baseline and any slice test
     * boots without trying to dial a BE.
     */
    private boolean enabled = false;

    /**
     * The single environment this daemon serves (single-tenant). Path variable in every BE call.
     */
    private String envId = "";

    /**
     * Installer / bootstrap bearer from config (plaintext). Authenticates first-boot {@code /enroll} +
     * {@code /enrol-hybrid}.
     */
    @Getter(AccessLevel.NONE)
    @Setter
    private String bearer = "";

    /**
     * Post-hybrid channel credential ({@code sqikm1.…}). Installed by {@code SqsChannelBootstrap} after hybrid
     * success or after loading persisted kemIkm.
     */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private volatile String channelCredential;

    /**
     * Credential presented as {@code Authorization: Bearer} on daemon→BE calls (heartbeat, WG handshake,
     * exec-channel, cross-env authorize, drain, reporters). Prefers the kemIkm proof when
     * {@link #installChannelCredential(String)} has run; otherwise the installer bearer.
     */
    public String getBearer() {
        String c = channelCredential;
        if (c != null && !c.isBlank()) {
            return c;
        }
        return bearer;
    }

    /**
     * Raw installer/bootstrap bearer from config — never overwritten by hybrid.
     */
    public String getInstallerBearer() {
        return bearer;
    }

    /**
     * Installs the kemIkm-derived channel credential (or clears when blank).
     */
    public void installChannelCredential(String credential) {
        this.channelCredential = (credential == null || credential.isBlank()) ? null : credential;
    }

    /**
     * {@code true} when bootstrap has installed a post-hybrid channel credential.
     */
    public boolean hasChannelCredential() {
        String c = channelCredential;
        return c != null && !c.isBlank();
    }

    /**
     * Reported to the BE on enrol/heartbeat/handshake for audit + telemetry.
     */
    private String agentVersion = "0.0.1-SNAPSHOT";

    /**
     * Stable per-boot instance id echoed on heartbeat + exec-channel (the instance-id header).
     */
    private String instanceId = "";

    /**
     * Monotonic generation counter reported on heartbeat (bumped on restart).
     */
    private long generation = 0L;

    /**
     * Local directory the client persists its mTLS keypair + BE-signed leaf/CA, WireGuard private key, and hybrid
     * kemIkm under ({@code <data-dir>/mtls}, {@code <data-dir>/wg}, {@code <data-dir>/kem/ikm.b64}).
     */
    private String dataDir = "./data/channel";

    /**
     * How long a first-boot enroll may keep retrying when the control plane is unreachable (minutes). Default 360
     * = 6 h, matching the provisioned-bearer TTL.
     */
    private long enrollRetryMinutes = 360L;

    @NestedConfigurationProperty
    private Heartbeat heartbeat = new Heartbeat();

    @NestedConfigurationProperty
    private ExecChannel execChannel = new ExecChannel();

    @NestedConfigurationProperty
    private Net net = new Net();


    @Getter
    @Setter
    public static class Heartbeat {
        /**
         * Start the 2s heartbeat loop when the channel is enabled.
         */
        private boolean enabled = true;
        /**
         * Beat interval while the daemon is RESPONSIVE — a run is in flight, so a cancel command and a command result
         * must not wait. BE ceiling is 25s.
         */
        private long periodMs = 2_000L;
        /**
         * Long-poll budget appended as {@code ?waitMs=} (ms). BE caps at 25s; 0 disables.
         */
        private long longPollWaitMs = 20_000L;
        /**
         * Beat interval once the WireGuard reverse channel is healthy and the backend has confirmed inbound overlay
         * reachability: liveness keepalive ONLY. Local healthy alone is never enough.
         */
        private long wgKeepaliveSeconds = 60L;
        /**
         * Post-activity grace (seconds) before an idle, overlay-healthy daemon may drop from RESPONSIVE to the
         * keepalive beat. Interactive sessions (Duckbot chat, designer runs) are bursty — the next trigger usually
         * lands within a minute or two of the last run, and an instant demotion makes it wait out a full keepalive
         * whenever the overlay push is missed.
         */
        private long wgKeepaliveGraceSeconds = 120L;
        /**
         * Inbound-overlay freshness window (seconds) for the OVERLAY_SILENT mode: while the reverse listener has seen
         * an authenticated backend request within this window — and the backend advertised {@code overlayLiveness} on
         * the beat response — the daemon sends NO heartbeat at all. Commands arrive over the reverse channel, run
         * status/logs ride their direct posts, and the backend's status polls carry telemetry home.
         */
        private long wgSilentFreshnessSeconds = 30L;
        /**
         * How often (seconds) the beat loop re-resolves while OVERLAY_SILENT. No traffic leaves the host — this is
         * only the local clock that notices the freshness window has lapsed.
         */
        private long wgSilentCheckSeconds = 10L;
        /**
         * Ceiling of the fallback ladder used when there is NO confirmed reverse channel. Prolonged idleness may relax
         * upward to this; the ladder starts at {@link #fallbackMinSeconds} and returns there on activity.
         */
        private long fallbackStartSeconds = 300L;
        /**
         * Floor of the fallback ladder (starts here; doubles each idle cycle up to {@link #fallbackStartSeconds};
         * resets here on activity). With no reverse channel the interval IS the pickup latency for queued work, so the
         * floor must stay responsive — but no tighter than this, because a shorter poll multiplies across every
         * environment on every backend replica to buy latency for an engine that is idle.
         */
        private long fallbackMinSeconds = 30L;
    }

    /**
     * Exec-channel (SSE + tiered poll fallback) tuning (mirrors {@code internal/exectunnel}).
     */
    @Getter
    @Setter
    public static class ExecChannel {
        /**
         * Start the exec-channel loop when the channel is enabled.
         */
        private boolean enabled = true;
        /**
         * SSE idle window before dropping to the poll tiers (ms).
         */
        private long sseIdleWindowMs = 900_000L;
        /**
         * Pull-fallback long-poll budget appended as {@code ?waitMs=} (ms).
         */
        private long pullWaitMs = 15_000L;
        /**
         * Per-dispatchId dedup retention (ms).
         */
        private long dedupTtlMs = 604_800_000L;
    }

    /**
     * SQNet mesh agent + handshake v2. Replaces the retired fabric config tree (PSK / drain / interface
     * programming deleted).
     */
    @Getter
    @Setter
    public static class Net {
        /**
         * Attempt the net handshake + agent bring-up when the channel is enabled. Default {@code true}: the daemon
         * prefers the overlay for backend traffic once the agent is Running and a hub answers HTTPS.
         */
        private boolean enabled = true;
        /**
         * {@code sq-net} CLI path (installer PATH or absolute).
         */
        private String cliPath = "sq-net";
        /**
         * LocalAPI socket / named-pipe path passed as {@code sq-net --socket=}. Blank uses the CLI default.
         */
        private String socketPath = "";
        /**
         * Fallback overlay interface name when {@code sq-net status} does not expose one. Default {@code sqnet0}; the
         * real name is read from status when present.
         */
        private String interfaceName = "sqnet0";
        /**
         * Port the inbound overlay reverse listener binds. Matches the BE dispatcher's daemon-api-port default (9876).
         */
        private int daemonApiPort = 9876;
        @NestedConfigurationProperty
        private OverlayListener overlayListener = new OverlayListener();
        @NestedConfigurationProperty
        private Watchdog watchdog = new Watchdog();
        /**
         * Repoint outbound heartbeat / exec-channel / BeCall onto a hub after a confirmed agent Running + HTTPS hub
         * probe. Set {@code false} to keep public HTTPS.
         */
        private boolean outboundOverOverlay = true;
        /**
         * Consecutive overlay-transport failures tolerated before
         * {@link com.studioquack.sqs.channel.BackendBaseResolver} falls back to public.
         */
        private int outboundFailureThreshold = 3;

        /**
         * The inbound overlay reverse listener. An embedded mTLS HTTPS server the daemon binds to its OWN overlay IP
         * (never {@code 0.0.0.0}) so it is reachable EXCLUSIVELY through the decrypted WireGuard tunnel.
         */
        @Getter
        @Setter
        public static class OverlayListener {
            /**
             * Start the inbound overlay listener after a successful WG bring-up. Default on.
             */
            private boolean enabled = true;
            /**
             * Optional bind-address override. Blank (default) binds the overlay IP derived from the handshake assignment's
             * {@code overlayCidr} (host part).
             */
            private String bindAddress = "";
        }

        /**
         * Periodic overlay watchdog (WG reconnection fix). After a host reboot or a transient network outage the
         * initial bootstrap handshake can fail, and once the outbound base has fallen back to public it would
         * otherwise never return to the overlay.
         */
        @Getter
        @Setter
        public static class Watchdog {
            /**
             * Run the periodic overlay watchdog once the channel + net overlay are enabled.
             */
            private boolean enabled = true;
            /**
             * Cadence between watchdog ticks (seconds). Each tick probes agent BackendState and hub HTTPS; recovery
             * backoff never exceeds {@code NetWatchdog}'s cap.
             */
            private long intervalSeconds = 15L;
        }
    }


    public String environmentBase(String backendBaseUrl) {
        String base = backendBaseUrl == null ? "" : backendBaseUrl.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/be/v1/sqvm/environments/" + envId;
    }
}
