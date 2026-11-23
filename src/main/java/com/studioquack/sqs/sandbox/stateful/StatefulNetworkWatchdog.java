package com.studioquack.sqs.sandbox.stateful;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically heals stateful instances whose gVisor sandbox lost its network after an in-place
 * restart ({@link StatefulInstanceManager#healLostSandboxNetworks()}). Runs on its own thread: a
 * recycle is a full stop + start, which must not hold up the shared Spring scheduler (egress
 * verification, disk guard).
 */
@Slf4j
@Component
public class StatefulNetworkWatchdog {

    private final StatefulInstanceManager instances;
    private final Duration interval;
    private final Duration initialDelay;
    private ScheduledExecutorService executor;

    public StatefulNetworkWatchdog(StatefulInstanceManager instances,
                                   @Value("${sqs.stateful.network-watchdog.interval:PT60S}") Duration interval,
                                   @Value("${sqs.stateful.network-watchdog.initial-delay:PT90S}") Duration initialDelay) {
        this.instances = instances;
        this.interval = interval;
        this.initialDelay = initialDelay;
    }

    /** A zero or negative interval disables the watchdog. */
    @PostConstruct
    void start() {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            log.info("[SQ-6] stateful network watchdog disabled");
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stateful-network-watchdog");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::pass, Math.max(0, initialDelay.toMillis()),
                interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    void pass() {
        try {
            int healed = instances.healLostSandboxNetworks();
            if (healed > 0) {
                log.info("[SQ-6] network watchdog healed {} stateful instance(s)", healed);
            }
        } catch (Exception e) {
            log.warn("[SQ-6] network watchdog pass failed: {}", e.toString());
        }
    }

    @PreDestroy
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
