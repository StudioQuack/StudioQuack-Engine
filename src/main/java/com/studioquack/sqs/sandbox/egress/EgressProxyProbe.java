package com.studioquack.sqs.sandbox.egress;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Liveness probe for the ADVISORY sandbox egress proxy ({@code sqs.sandbox.egress-proxy.url}). */
@Slf4j
@Component
public class EgressProxyProbe {

    /**
     * Micrometer counter (rendered {@code sqs_sandbox_egress_proxy_unreachable_total} on
     * Prometheus): incremented once per sandbox dispatch that had to skip proxy injection because
     * the probe reported DOWN — i.e. the blast radius of the outage in runs-gone-direct.
     */
    public static final String UNREACHABLE_METRIC = "sqs.sandbox.egress.proxy.unreachable";

    public enum State {
        /** No {@code sqs.sandbox.egress-proxy.url} configured — nothing to watch, no heartbeat block. */
        NOT_CONFIGURED,
        /** Reachable (or the bounded startup window before the first check lands — see class doc). */
        UP,
        /** Configured but the daemon could not TCP-connect within the timeout. */
        DOWN
    }

    /** Immutable point-in-time verdict; {@code since} is when the CURRENT state was entered. */
    public record Snapshot(State state, String reason, Instant since) {
    }

    /**
     * Test seam for the actual network check.
     *
     * @return {@code null} on connect success, or a short operator-safe reason on failure.
     *         Implementations must never throw and must honour the timeout.
     */
    @FunctionalInterface
    public interface TcpConnector {
        String connect(String host, int port, Duration timeout);
    }

    private final String host;
    private final int port;
    private final Duration connectTimeout;
    private final long ttlMs;
    private final TcpConnector connector;
    /** Null when no proxy is configured or no registry exists — an unconfigured proxy has no counter. */
    private final Counter unreachableSkips;
    /** Null whenever there is nothing to watch (NOT_CONFIGURED or an unparseable URL). */
    private final ScheduledExecutorService scheduler;

    private volatile Snapshot current;

    /** Spring production ctor: real TCP connect, daemon-thread scheduler, Micrometer counter. */
    @org.springframework.beans.factory.annotation.Autowired
    public EgressProxyProbe(@Value("${sqs.sandbox.egress-proxy.url:}") String proxyUrl,
                            @Value("${sqs.sandbox.egress-proxy.probe-ttl-ms:30000}") long ttlMs,
                            @Value("${sqs.sandbox.egress-proxy.probe-timeout-ms:500}") long timeoutMs,
                            ObjectProvider<MeterRegistry> meterRegistry) {
        this(proxyUrl, ttlMs, timeoutMs, EgressProxyProbe::tcpConnect,
                meterRegistry == null ? null : meterRegistry.getIfAvailable(), true);
    }

    /**
     * Full seam ctor. {@code startBackground} is false in deterministic tests, which then drive
     * {@link #refreshNow()} by hand; production always runs the TTL loop.
     */
    public EgressProxyProbe(String proxyUrl, long ttlMs, long timeoutMs,
                            TcpConnector connector, MeterRegistry meterRegistry,
                            boolean startBackground) {
        this.ttlMs = ttlMs > 0 ? ttlMs : 30_000L;
        // Bounded well under any dispatch budget: this caps a background thread, never a dispatch.
        this.connectTimeout = Duration.ofMillis(Math.max(50L, Math.min(timeoutMs, 500L)));
        this.connector = connector == null ? EgressProxyProbe::tcpConnect : connector;

        String url = proxyUrl == null ? "" : proxyUrl.trim();
        URI uri = url.isEmpty() ? null : parse(url);
        String parsedHost = uri == null ? null : uri.getHost();
        boolean configured = !url.isEmpty();
        // The outage counter exists only when there IS a configured proxy to be down.
        this.unreachableSkips = !configured || meterRegistry == null ? null
                : Counter.builder(UNREACHABLE_METRIC)
                        .description("Sandbox dispatches that skipped egress-proxy injection "
                                + "because the configured proxy was unreachable (D67 degrade)")
                        .register(meterRegistry);
        if (!configured) {
            this.host = null;
            this.port = -1;
            this.scheduler = null;
            this.current = new Snapshot(State.NOT_CONFIGURED, "no egress proxy configured", Instant.now());
            return;
        }
        if (parsedHost == null || parsedHost.isBlank()) {
            // Unparseable can never become reachable without a config edit + restart: pin DOWN so
            // injection is skipped and the outage is alerted, and do not burn a thread watching it.
            this.host = null;
            this.port = -1;
            this.scheduler = null;
            this.current = new Snapshot(State.DOWN,
                    "unparseable egress-proxy url (need scheme://host[:port])", Instant.now());
            log.warn("sandbox.egress.proxy: DOWN — unparseable egress-proxy url; runs will start "
                    + "with DIRECT egress and no proxy injection until the config is fixed");
            return;
        }
        this.host = parsedHost;
        this.port = uri.getPort() > 0 ? uri.getPort() : defaultPort(uri.getScheme());
        // Startup window: optimistic UP until the first scheduled check lands (initial delay 0).
        this.current = new Snapshot(State.UP, "startup probe pending", Instant.now());
        this.scheduler = startBackground ? Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "egress-proxy-probe");
            t.setDaemon(true);
            return t;
        }) : null;
        if (this.scheduler != null) {
            this.scheduler.scheduleWithFixedDelay(this::refreshSafely, 0, this.ttlMs, TimeUnit.MILLISECONDS);
        }
    }

    /** Test factory: no background loop; drive {@link #refreshNow()} explicitly. */
    public static EgressProxyProbe forTest(String proxyUrl, TcpConnector connector,
                                           MeterRegistry meterRegistry) {
        return new EgressProxyProbe(proxyUrl, 30_000L, 500L, connector, meterRegistry, false);
    }

    /** Hot-path read: one volatile load. */
    public Snapshot snapshot() {
        return current;
    }

    /**
     * Dispatch-path gate: everything except a confirmed DOWN injects exactly as configured. The
     * NOT_CONFIGURED arm is unreachable in practice (the caller's blank-URL check fires first) but is
     * deliberately fail-toward-inject: "silent direct" is the one direction forbids.
     */
    public boolean shouldInject() {
        return current.state() != State.DOWN;
    }

    /** Accounting for a dispatch that went direct because the proxy was DOWN (never on UP). */
    public void noteInjectionSkipped() {
        if (unreachableSkips != null) {
            unreachableSkips.increment();
        }
    }

    /**
     * Runs one check NOW, synchronously, on the caller's thread. This is the scheduler tick; tests
     * call it directly for determinism.
     */
    public void refreshNow() {
        if (host == null) {
            return; // NOT_CONFIGURED or pinned-DOWN: nothing to re-check until restart.
        }
        String failure;
        try {
            failure = connector.connect(host, port, connectTimeout);
        } catch (RuntimeException e) {
            // A misbehaving connector must degrade to DOWN (fail toward the alert), never crash
            // the scheduler thread silently — scheduleWithFixedDelay would stop rescheduling.
            failure = "probe error: " + e.getClass().getSimpleName();
        }
        applyVerdict(failure == null ? State.UP : State.DOWN,
                failure == null ? "reachable" : failure);
    }

    /** Transition-only logging: WARN on entering DOWN, INFO on recovering; never per dispatch. */
    private void applyVerdict(State next, String reason) {
        Snapshot prev = current;
        if (prev.state() == next) {
            if (!prev.reason().equals(reason)) {
                // Same state, sharper reason: keep the original `since` (state age stays honest).
                current = new Snapshot(next, reason, prev.since());
            }
            return;
        }
        current = new Snapshot(next, reason, Instant.now());
        if (next == State.DOWN) {
            log.warn("sandbox.egress.proxy: DOWN — {}:{} unreachable ({}); runs will start with "
                    + "DIRECT egress and no proxy injection until it recovers", host, port, reason);
        } else if (prev.state() == State.DOWN) {
            log.info("sandbox.egress.proxy: UP — {}:{} reachable again; resuming proxy injection",
                    host, port);
        }
    }

    private void refreshSafely() {
        try {
            refreshNow();
        } catch (RuntimeException e) {
            log.debug("sandbox.egress.proxy: refresh failed: {}", e.toString());
        }
    }

    @PreDestroy
    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /** The real check: bare TCP connect; a live proxy port accepts, a dead one refuses/times out. */
    private static String tcpConnect(String host, int port, Duration timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
            return null;
        } catch (Exception e) {
            // Operator-safe: exception class + message only (e.g. "Connection refused") — the URL,
            // which may embed credentials, is never echoed.
            String msg = e.getMessage();
            return e.getClass().getSimpleName() + (msg == null || msg.isBlank() ? "" : ": " + msg);
        }
    }

    private static URI parse(String url) {
        try {
            return new URI(url);
        } catch (Exception e) {
            return null;
        }
    }

    private static int defaultPort(String scheme) {
        String s = scheme == null ? "" : scheme.toLowerCase(Locale.ROOT);
        return switch (s) {
            case "https" -> 443;
            case "socks5", "socks" -> 1080;
            default -> 80; // http and anything else: the conventional proxy port default
        };
    }

    /** Heartbeat seam helper: {@code [state, reason]} or null when NOT_CONFIGURED. */
    public static Supplier<String[]> heartbeatView(EgressProxyProbe probe) {
        return () -> {
            Snapshot s = probe.snapshot();
            if (s.state() == State.NOT_CONFIGURED) {
                return null;
            }
            return new String[]{s.state().name(), s.reason()};
        };
    }
}
