package com.studioquack.sqs.channel.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.studioquack.sqs.channel.BackendBaseResolver;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * The two seams that used to leave a restarted daemon "Online in headscale, Unreachable on
 * :9876": the status watcher swallowed the first-IP observation, and the only other listener
 * start was the watchdog's FULL recovery, gated on the daemon's outbound hub probe.
 */
class InboundListenerRecoveryTest {

    @SuppressWarnings("unchecked")
    private static <T> ObjectProvider<T> providerOf(T value) {
        ObjectProvider<T> p = org.mockito.Mockito.mock(ObjectProvider.class);
        org.mockito.Mockito.when(p.getIfAvailable()).thenReturn(value);
        return p;
    }

    @Test
    void statusWatcher_publishesTheFirstObservedIp_notOnlyChanges() {
        // Restart shape: bootstrap ran while the agent was still coming up, so nothing seeded
        // lastIp; the IP appears on the first poll and MUST produce a rebind event.
        FakeNetAgentClient agent = new FakeNetAgentClient().withBackendState("Running")
                .withSelfIp("100.67.229.153");
        AtomicReference<ApplicationEvent> published = new AtomicReference<>();
        ApplicationEventPublisher events = new ApplicationEventPublisher() {
            @Override
            public void publishEvent(ApplicationEvent event) {
                published.set(event);
            }

            @Override
            public void publishEvent(Object event) {
                if (event instanceof ApplicationEvent ae) {
                    published.set(ae);
                }
            }
        };
        NetStatusWatcher watcher = new NetStatusWatcher(providerOf(agent),
                new PeerAddressResolver(agent), new NetTelemetry(), events);

        watcher.tick();

        assertTrue(published.get() instanceof OverlayAddressChangedEvent, "first IP must publish");
        OverlayAddressChangedEvent ev = (OverlayAddressChangedEvent) published.get();
        assertNull(ev.previousIp());
        assertEquals("100.67.229.153", ev.currentIp());
        assertEquals("100.67.229.153", watcher.lastIp());

        // Same IP again → silent (no rebind storm).
        published.set(null);
        watcher.tick();
        assertNull(published.get());
    }

    @Test
    void watchdog_ensuresInboundOnEveryTickWhileRunning_independentOfHubs() {
        // No hubs at all (a dev cell without the hub-address hook) — the recovery path can never
        // complete, yet inbound listeners must still be (re)bound every tick.
        FakeNetAgentClient agent = new FakeNetAgentClient().withBackendState("Running")
                .withSelfIp("100.67.229.153");
        SqsChannelProperties props = new SqsChannelProperties();
        props.getNet().getWatchdog().setIntervalSeconds(15);
        NetWatchdog wd = new NetWatchdog(props);
        AtomicInteger ensured = new AtomicInteger();
        HubList hubs = new HubList();
        hubs.replace(List.of());
        BackendBaseResolver resolver = new BackendBaseResolver("https://be.example/env/e", 3);
        NetWatchdog.Context ctx = new NetWatchdog.Context(
                resolver, agent, () -> null, hubs, null, resolver.publicBase(),
                r -> { }, ensured::incrementAndGet);
        ReflectionTestUtils.setField(wd, "context", ctx);
        ReflectionTestUtils.setField(wd, "intervalSeconds", 15L);

        ReflectionTestUtils.invokeMethod(wd, "tick");
        ReflectionTestUtils.invokeMethod(wd, "tick");
        assertEquals(2, ensured.get(), "ensureInbound runs on every tick while the agent is Running");

        // Agent not Running → nothing to bind; the hook is not invoked.
        agent.withBackendState("Starting");
        ReflectionTestUtils.invokeMethod(wd, "tick");
        assertEquals(2, ensured.get());
    }
}
