package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.channel.BackendBaseResolver;
import com.studioquack.sqs.channel.OverlayHealthProbe;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Overlay watchdog — agent status instead of {@code wg} probes. Re-handshake / {@code up} when BackendState ≠
 * Running or every hub is dark.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class NetWatchdog {

    private static final int MAX_BACKOFF_SHIFT = 6;
    private static final long PROBE_BUDGET_MS = 15_000L;
    private static final long PROBE_RETRY_DELAY_MS = 1_000L;
    private static final long MAX_BACKOFF_SECONDS = 60L;

    private final SqsChannelProperties props;

    private volatile Context context;
    private volatile ScheduledExecutorService scheduler;
    private volatile boolean started;
    private long intervalSeconds;
    private long nextAttemptNanos;
    private int consecutiveFailures;
    private int attemptsThisOutage;
    private boolean downLogged;
    private final java.util.concurrent.atomic.AtomicBoolean forceAttempt =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile String lastError;

    public NetWatchdog(SqsChannelProperties props) {
        this.props = props;
    }

    public void recoverNow() {
        forceAttempt.set(true);
    }

    public String lastError() {
        return lastError;
    }

    /**
     * @param ensureInbound idempotent hook run on EVERY tick while the agent is Running (before any hub logic):
     * binds the inbound overlay listeners if they are missing. Inbound reachability (BE → daemon:9876, peers
     * →:8443) must never depend on the daemon's own OUTBOUND hub probe — it used to, via {@code onRecovered} only,
     * so a daemon on a cell without reachable hubs (or one whose boot raced the agent coming up) stayed
     * Unreachable forever while headscale showed it Online.
     */
    public record Context(BackendBaseResolver resolver,
                          NetAgentClient agent,
                          Supplier<NetHandshakeClient> handshakeSupplier,
                          HubList hubs,
                          SSLContext hubSsl,
                          String overlayBaseFor,
                          Consumer<NetHandshakeResult> onRecovered,
                          Runnable ensureInbound) {
    }

    public synchronized void start(Context ctx) {
        if (started) {
            return;
        }
        SqsChannelProperties.Net.Watchdog cfg = props.getNet().getWatchdog();
        if (!cfg.isEnabled()) {
            log.info("channel/net/watchdog: disabled (sqs.channel.net.watchdog.enabled=false)");
            return;
        }
        this.context = ctx;
        long interval = cfg.getIntervalSeconds();
        this.intervalSeconds = interval > 0 ? interval : 15L;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sqs-net-watchdog");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleWithFixedDelay(this::tickSafely, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        this.started = true;
        log.info("channel/net/watchdog: started (interval={}s)", intervalSeconds);
    }

    private void tickSafely() {
        try {
            tick();
        } catch (Exception ex) {
            log.debug("channel/net/watchdog: tick failed ({})", ex.toString());
        }
    }

    private void tick() {
        Context ctx = this.context;
        if (ctx == null || !props.getNet().isEnabled()) {
            return;
        }
        if (ctx.agent() == null) {
            return;
        }
        boolean forced = forceAttempt.getAndSet(false);
        boolean running = agentRunning(ctx);
        if (running && ctx.ensureInbound() != null) {
            // Inbound first, independent of the hub probe below (see Context.ensureInbound).
            try {
                ctx.ensureInbound().run();
            } catch (Exception ex) {
                log.debug("channel/net/watchdog: ensureInbound failed ({})", ex.toString());
            }
        }
        boolean hubOk = probeHubs(ctx, false);
        if (ctx.resolver().overlayActive() && running && hubOk && !forced) {
            NetHandshakeClient hs = ctx.handshakeSupplier() == null ? null : ctx.handshakeSupplier().get();
            if (hs != null && hs.shouldRefresh()) {
                attemptRecovery(ctx, "key-refresh");
                return;
            }
            resetOutageState();
            return;
        }
        if (ctx.resolver().overlayActive() && (!running || !hubOk)) {
            log.info("channel/net/watchdog: overlay claimed up but agentRunning={} hubOk={} — falling back",
                    running, hubOk);
            ctx.resolver().deactivateOverlay();
        }
        if (!forced && System.nanoTime() < nextAttemptNanos) {
            return;
        }
        attemptRecovery(ctx, forced ? "reconnect-requested" : "down");
    }

    private void attemptRecovery(Context ctx, String reason) {
        attemptsThisOutage++;
        NetHandshakeClient client = ctx.handshakeSupplier() == null ? null : ctx.handshakeSupplier().get();
        if (client == null) {
            failed("no handshake client (" + reason + ")");
            return;
        }
        try {
            NetHandshakeResult result = client.handshake();
            if (!agentRunning(ctx)) {
                failed("agent not Running after handshake");
                return;
            }
            if (!probeHubs(ctx, true)) {
                failed("no hub reachable over the mesh");
                return;
            }
            HubAddress hub = ctx.hubs() == null ? null : ctx.hubs().inUse();
            if (hub != null && ctx.resolver() != null) {
                String origin = "https://" + hub.address() + ":" + hub.port();
                ctx.resolver().tryReactivateOverlay(ctx.overlayBaseFor() == null
                        ? origin : props.environmentBase(origin));
            }
            if (ctx.onRecovered() != null) {
                try {
                    ctx.onRecovered().accept(result);
                } catch (Exception hookEx) {
                    log.warn("channel/net/watchdog: onRecovered failed ({})", hookEx.toString());
                }
            }
            log.info("channel/net/watchdog: overlay RECONNECTED after {} attempt(s) ({})",
                    attemptsThisOutage, reason);
            resetOutageState();
        } catch (NetHandshakeException ex) {
            if (ex.envNotFound()) {
                failed(ex.getMessage());
                return;
            }
            failed(ex.getMessage());
        } catch (Exception ex) {
            failed("bring-up failed (" + ex + ")");
        }
    }

    private boolean agentRunning(Context ctx) {
        try {
            NetAgentStatus st = ctx.agent().status();
            return st != null && st.running();
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * Walks {@link HubList} in order. {@code untilHandshaked} retries within the probe budget.
     */
    private boolean probeHubs(Context ctx, boolean untilHandshaked) {
        List<HubAddress> list = ctx.hubs() == null ? List.of() : ctx.hubs().snapshot();
        if (list.isEmpty()) {
            return false;
        }
        long deadline = untilHandshaked
                ? System.nanoTime() + PROBE_BUDGET_MS * 1_000_000L
                : System.nanoTime();
        do {
            for (HubAddress hub : list) {
                if (OverlayHealthProbe.httpsReachable(hub.address(), hub.port(), ctx.hubSsl(),
                        OverlayHealthProbe.DEFAULT_TIMEOUT_MS)) {
                    ctx.hubs().markInUse(hub);
                    return true;
                }
            }
            if (!untilHandshaked || System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(PROBE_RETRY_DELAY_MS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        } while (System.nanoTime() < deadline);
        return false;
    }

    private void failed(String reason) {
        lastError = reason;
        consecutiveFailures++;
        if (!downLogged) {
            log.info("channel/net/watchdog: overlay DOWN — {} (attempt {})", reason, attemptsThisOutage);
            downLogged = true;
        } else {
            log.debug("channel/net/watchdog: overlay still down — {} (attempt {})", reason, attemptsThisOutage);
        }
        int shift = Math.min(Math.max(consecutiveFailures - 1, 0), MAX_BACKOFF_SHIFT);
        long delaySeconds = Math.min(intervalSeconds << shift, MAX_BACKOFF_SECONDS);
        nextAttemptNanos = System.nanoTime() + delaySeconds * 1_000_000_000L;
    }

    private void resetOutageState() {
        consecutiveFailures = 0;
        attemptsThisOutage = 0;
        downLogged = false;
        nextAttemptNanos = 0L;
        lastError = null;
    }

    @PreDestroy
    public synchronized void stop() {
        ScheduledExecutorService s = this.scheduler;
        if (s != null) {
            s.shutdownNow();
        }
        started = false;
    }
}
