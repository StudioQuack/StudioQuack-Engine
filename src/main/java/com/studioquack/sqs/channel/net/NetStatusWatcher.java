package com.studioquack.sqs.channel.net;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Polls {@link NetAgentClient#status()} and publishes {@link OverlayAddressChangedEvent} when the agent's own
 * overlay IP changes so listeners rebind.
 */
@Slf4j
@Component
public class NetStatusWatcher {

    private final ObjectProvider<NetAgentClient> agent;
    private final PeerAddressResolver resolver;
    private final NetTelemetry telemetry;
    private final ApplicationEventPublisher events;

    private volatile ScheduledExecutorService scheduler;
    private volatile String lastIp;
    private volatile boolean started;

    public NetStatusWatcher(ObjectProvider<NetAgentClient> agent,
                            PeerAddressResolver resolver,
                            NetTelemetry telemetry,
                            ApplicationEventPublisher events) {
        this.agent = agent;
        this.resolver = resolver;
        this.telemetry = telemetry;
        this.events = events;
    }

    public synchronized void start() {
        if (started) {
            return;
        }
        if (agent.getIfAvailable() == null) {
            return;
        }
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sqs-net-status");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleWithFixedDelay(this::tick, 2, 2, TimeUnit.SECONDS);
        this.started = true;
    }

    public synchronized void stop() {
        ScheduledExecutorService s = this.scheduler;
        if (s != null) {
            s.shutdownNow();
        }
        started = false;
    }

    void tick() {
        NetAgentClient client = agent.getIfAvailable();
        if (client == null) {
            return;
        }
        try {
            resolver.invalidate();
            NetAgentStatus st = client.status();
            String ip = st.primaryIp().orElse(null);
            telemetry.updateAgent(client.version(), st.backendState(), ip, st.peers().size());
            String prev = lastIp;
            if (ip != null && !ip.equals(prev)) {
                lastIp = ip;
                // Publish on the FIRST observation too (prev == null). Right after a daemon restart
                // the agent is still coming up when bootstrap runs, so the listeners' initial start
                // is skipped ("wg-not-up" / no bind address) and nothing seeded lastIp; the IP then
                // appears here a few seconds later. Swallowing that transition left the reverse
                // listener unstarted for good — the BE saw the node Online in headscale yet
                // unreachable on:9876, and every command fell back to the heartbeat queue.
                log.info("channel/net: overlay IP {} → {} — publishing rebind",
                        prev == null ? "(none)" : prev, ip);
                events.publishEvent(new OverlayAddressChangedEvent(this, prev, ip));
            }
        } catch (Exception ex) {
            log.debug("channel/net: status watch failed ({})", ex.toString());
        }
    }

    public String lastIp() {
        return lastIp;
    }

    public void seedIp(String ip) {
        this.lastIp = ip;
    }
}
