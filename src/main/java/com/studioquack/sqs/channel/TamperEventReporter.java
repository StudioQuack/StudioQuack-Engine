package com.studioquack.sqs.channel;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Daemon-side tamper-event feed. Collects security-relevant violations detected on this engine (secret-binding
 * mismatches, bearer/keystore anomalies, …) and ships them on the next heartbeat's {@code tamperEvents[]} —
 * the backend persists them into the environment's recent-results audit ring and emits an
 * {@code AUDIT tamper.<type>} record (the control plane), so no new daemon→BE channel is needed.
 */
@Component
public class TamperEventReporter {

    /**
     * Bounded backlog; the BE ring itself is capped, so flooding buys an attacker nothing.
     */
    static final int MAX_QUEUED = 100;
    /**
     * Keeps one event well inside the BE audit-ring column budget.
     */
    static final int MAX_DETAIL_LENGTH = 500;

    /**
     * One tamper event — wire shape mirrors the BE {@code HeartbeatBody.TamperEvent}.
     */
    public record TamperEvent(String type, String detail, String observedAt) {
    }

    private final ConcurrentLinkedQueue<TamperEvent> queue = new ConcurrentLinkedQueue<>();

    /**
     * Queues a tamper event for the next heartbeat. {@code type} is a stable dotted code.
     */
    public void report(String type, String detail) {
        if (type == null || type.isBlank()) {
            return;
        }
        String safeDetail = detail == null ? null
                : detail.length() <= MAX_DETAIL_LENGTH ? detail
                : detail.substring(0, MAX_DETAIL_LENGTH);
        queue.offer(new TamperEvent(type, safeDetail, Instant.now().toString()));
        while (queue.size() > MAX_QUEUED) {
            queue.poll();
        }
    }

    /**
     * Drains everything queued so far; the heartbeat calls this once per beat.
     */
    public List<TamperEvent> drain() {
        List<TamperEvent> out = new ArrayList<>();
        TamperEvent e;
        while ((e = queue.poll()) != null) {
            out.add(e);
        }
        return out;
    }

    /**
     * Current backlog size (tests + diagnostics).
     */
    public int queued() {
        return queue.size();
    }
}
