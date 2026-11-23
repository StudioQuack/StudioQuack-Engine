package com.studioquack.sqs.sandbox.egress;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The advisory egress-proxy probe. The decided degrade direction under test: a
 * configured-but-unreachable proxy degrades the run (start anyway, direct egress) AND raises an
 * alert (transition WARN + counter + heartbeat state) — never silent, never run-breaking.
 */
class EgressProxyProbeTest {

    private static final String PROXY = "http://10.20.0.9:3128";

    private static EgressProxyProbe.TcpConnector refusing(String reason) {
        return (host, port, timeout) -> reason;
    }

    private static EgressProxyProbe.TcpConnector reachable() {
        return (host, port, timeout) -> null;
    }

    @Test
    void notConfigured_reportsNotConfiguredAndWatchesNothing() {
        EgressProxyProbe probe = EgressProxyProbe.forTest("", reachable(), new SimpleMeterRegistry());

        assertEquals(EgressProxyProbe.State.NOT_CONFIGURED, probe.snapshot().state());
        // Fail toward "inject as configured": the caller's blank-URL check fires first anyway, so
        // this arm must never be the thing that silently strips a configured proxy.
        assertTrue(probe.shouldInject());
        // NOT_CONFIGURED maps to a null heartbeat view: the block stays out of the payload.
        assertNull(EgressProxyProbe.heartbeatView(probe).get());
    }

    @Test
    void startupWindow_injectsAsConfiguredUntilTheFirstCheckLands() {
        // No refreshNow() yet: the bounded startup window must fail toward injecting the
        // configured proxy (the opposite — silent direct — is the forbidden direction).
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY, refusing("Connection refused"),
                new SimpleMeterRegistry());

        assertEquals(EgressProxyProbe.State.UP, probe.snapshot().state());
        assertEquals("startup probe pending", probe.snapshot().reason());
        assertTrue(probe.shouldInject(), "the startup window must inject as configured");
    }

    @Test
    void reachableProxy_reportsUp() {
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY, reachable(), new SimpleMeterRegistry());

        probe.refreshNow();

        assertEquals(EgressProxyProbe.State.UP, probe.snapshot().state());
        assertEquals("reachable", probe.snapshot().reason());
        assertTrue(probe.shouldInject());
        assertNotNull(probe.snapshot().since());
    }

    @Test
    void unreachableProxy_reportsDownWithReasonAndBlocksInjection() {
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY, refusing("Connection refused"),
                new SimpleMeterRegistry());

        probe.refreshNow();

        assertEquals(EgressProxyProbe.State.DOWN, probe.snapshot().state());
        assertEquals("Connection refused", probe.snapshot().reason());
        assertFalse(probe.shouldInject(), "a DOWN proxy must not be injected — the run goes direct");
        String[] view = EgressProxyProbe.heartbeatView(probe).get();
        assertEquals("DOWN", view[0]);
        assertEquals("Connection refused", view[1]);
    }

    @Test
    void repeatedFailuresKeepTheOriginalSinceTimestamp() throws Exception {
        AtomicReference<String> failure = new AtomicReference<>("Connection refused");
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> failure.get(), new SimpleMeterRegistry());

        probe.refreshNow();
        var since = probe.snapshot().since();
        Thread.sleep(5); // ensure a replaced snapshot would carry a measurably later timestamp
        failure.set("Read timed out");
        probe.refreshNow();

        assertEquals(EgressProxyProbe.State.DOWN, probe.snapshot().state());
        assertEquals("Read timed out", probe.snapshot().reason(), "the sharper reason replaces");
        assertEquals(since, probe.snapshot().since(), "state age survives a reason-only change");
    }

    @Test
    void recovery_transitionsDownToUpAndResumesInjection() {
        AtomicReference<String> failure = new AtomicReference<>("Connection refused");
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> failure.get(), new SimpleMeterRegistry());

        probe.refreshNow();
        assertFalse(probe.shouldInject());
        failure.set(null); // proxy is back
        probe.refreshNow();

        assertEquals(EgressProxyProbe.State.UP, probe.snapshot().state());
        assertTrue(probe.shouldInject(), "recovery resumes injection on the next dispatch");
    }

    @Test
    void transitionLogging_warnsOncePerDownTransitionAndNeverPerCheck() {
        Logger logger = (Logger) LoggerFactory.getLogger(EgressProxyProbe.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        Level previous = logger.getLevel();
        logger.setLevel(Level.DEBUG);
        try {
            AtomicReference<String> failure = new AtomicReference<>("Connection refused");
            EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                    (host, port, timeout) -> failure.get(), new SimpleMeterRegistry());

            probe.refreshNow(); // UP(pending) -> DOWN: one WARN
            probe.refreshNow(); // DOWN -> DOWN: silence
            probe.refreshNow(); // DOWN -> DOWN: silence
            failure.set(null);
            probe.refreshNow(); // DOWN -> UP: one INFO (recovery)
            failure.set("Connection refused");
            probe.refreshNow(); // UP -> DOWN: a second WARN

            long warns = appender.list.stream().filter(e -> e.getLevel() == Level.WARN).count();
            long recoveryInfos = appender.list.stream()
                    .filter(e -> e.getLevel() == Level.INFO
                            && e.getFormattedMessage().contains("resuming proxy injection"))
                    .count();
            assertEquals(2, warns, "one WARN per UP->DOWN transition, never per check");
            assertEquals(1, recoveryInfos, "the recovery is logged once");
            assertTrue(appender.list.stream()
                            .filter(e -> e.getLevel() == Level.WARN)
                            .allMatch(e -> e.getFormattedMessage().contains("DIRECT egress")),
                    "the WARN must state the degrade direction so the alert is actionable");
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previous);
        }
    }

    @Test
    void unparseableUrl_isPinnedDownWithoutEverDialling() {
        AtomicInteger dials = new AtomicInteger();
        EgressProxyProbe probe = EgressProxyProbe.forTest("not-a-url",
                (host, port, timeout) -> {
                    dials.incrementAndGet();
                    return null;
                }, new SimpleMeterRegistry());

        assertEquals(EgressProxyProbe.State.DOWN, probe.snapshot().state());
        assertTrue(probe.snapshot().reason().contains("unparseable"));
        assertFalse(probe.shouldInject());
        probe.refreshNow(); // must be a no-op: nothing to watch until a config edit + restart
        assertEquals(0, dials.get(), "an unparseable URL must never be dialled");
    }

    @Test
    void defaultPorts_followTheScheme() {
        AtomicReference<String> dialled = new AtomicReference<>();
        EgressProxyProbe.TcpConnector capture = (host, port, timeout) -> {
            dialled.set(host + ":" + port);
            return null;
        };

        EgressProxyProbe.forTest("http://proxy.local", capture, new SimpleMeterRegistry()).refreshNow();
        assertEquals("proxy.local:80", dialled.get());
        EgressProxyProbe.forTest("https://proxy.local", capture, new SimpleMeterRegistry()).refreshNow();
        assertEquals("proxy.local:443", dialled.get());
        EgressProxyProbe.forTest("http://proxy.local:8888", capture, new SimpleMeterRegistry()).refreshNow();
        assertEquals("proxy.local:8888", dialled.get(), "an explicit port always wins");
    }

    @Test
    void credentialsInTheUrlNeverLeakIntoStateOrReason() {
        EgressProxyProbe probe = EgressProxyProbe.forTest("http://operator:s3cret@proxy.local:3128",
                refusing("Connection refused"), new SimpleMeterRegistry());

        probe.refreshNow();

        assertEquals(EgressProxyProbe.State.DOWN, probe.snapshot().state());
        String rendered = probe.snapshot().reason();
        assertFalse(rendered.contains("operator"), "userinfo must never be rendered");
        assertFalse(rendered.contains("s3cret"), "credentials must never be rendered");
    }

    @Test
    void aThrowingConnectorDegradesToDownInsteadOfCrashingTheScheduler() {
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> {
                    throw new IllegalStateException("boom");
                }, new SimpleMeterRegistry());

        probe.refreshNow();

        assertEquals(EgressProxyProbe.State.DOWN, probe.snapshot().state());
        assertTrue(probe.snapshot().reason().contains("probe error"));
    }

    @Test
    void unreachableCounter_countsOnlySkippedDispatches() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY, refusing("Connection refused"), registry);

        probe.noteInjectionSkipped();
        probe.noteInjectionSkipped();

        assertEquals(2.0, registry.get(EgressProxyProbe.UNREACHABLE_METRIC).counter().count());
    }

    @Test
    void timeoutIsCappedAtHalfASecond() {
        AtomicReference<Duration> seen = new AtomicReference<>();
        // Even if an operator configures a larger timeout, the connect budget stays ≤500 ms.
        EgressProxyProbe probe = new EgressProxyProbe(PROXY, 30_000L, 10_000L,
                (host, port, timeout) -> {
                    seen.set(timeout);
                    return null;
                }, new SimpleMeterRegistry(), false);

        probe.refreshNow();

        assertEquals(Duration.ofMillis(500), seen.get());
    }
}
