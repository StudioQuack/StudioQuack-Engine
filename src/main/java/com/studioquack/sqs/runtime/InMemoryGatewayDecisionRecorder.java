package com.studioquack.sqs.runtime;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded in-memory {@link GatewayDecisionRecorder}: one latest decision per
 * {@code (workflowExecutionId, gatewayNodeId)}. Hand-rolled TTL + max-size eviction of the
 * oldest write (no Guava / Caffeine). Stores the decision as given — authored expression
 * text and booleans only; never logs resolved payload values.
 */
@Component
public class InMemoryGatewayDecisionRecorder implements GatewayDecisionRecorder {

    /** Composite map key; blank ids are rejected before a key is built. */
    record Key(String workflowExecutionId, String gatewayNodeId) {
    }

    /** Held row: the decision plus write-time metadata used for TTL and oldest-write eviction. */
    private record Held(GatewayDecision decision, long writtenAtMs, long writeSeq) {
    }

    private final ConcurrentHashMap<Key, Held> byKey = new ConcurrentHashMap<>();
    private final AtomicLong writeSeq = new AtomicLong();
    private final int maxEntries;
    private final long ttlMs;

    public InMemoryGatewayDecisionRecorder(
            @Value("${sqs.gateway.decision-max-entries:256}") int maxEntries,
            @Value("${sqs.gateway.decision-ttl-ms:1800000}") long ttlMs) {
        this.maxEntries = Math.max(1, maxEntries);
        this.ttlMs = Math.max(1L, ttlMs);
    }

    @Override
    public void record(GatewayDecision decision) {
        try {
            if (decision == null) {
                return;
            }
            if (blank(decision.workflowExecutionId()) || blank(decision.gatewayNodeId())) {
                return;
            }
            Key key = new Key(decision.workflowExecutionId(), decision.gatewayNodeId());
            long now = System.currentTimeMillis();
            byKey.put(key, new Held(decision, now, writeSeq.incrementAndGet()));
            evictExpired(now);
            evictOldestWritesOverCap();
        } catch (RuntimeException ignored) {
            // never throw into the engine
        }
    }

    @Override
    public GatewayDecision latest(String workflowExecutionId, String gatewayNodeId) {
        try {
            if (blank(workflowExecutionId) || blank(gatewayNodeId)) {
                return null;
            }
            Key key = new Key(workflowExecutionId, gatewayNodeId);
            Held held = byKey.get(key);
            if (held == null) {
                return null;
            }
            if (expired(held, System.currentTimeMillis())) {
                byKey.remove(key, held); // lazy TTL expire on read
                return null;
            }
            return held.decision();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    /** Periodic backstop so an unread expired row is still reclaimed. */
    @Scheduled(fixedDelayString = "${sqs.gateway.decision-sweep-ms:300000}",
            initialDelayString = "${sqs.gateway.decision-sweep-ms:300000}")
    public void sweep() {
        try {
            evictExpired(System.currentTimeMillis());
        } catch (RuntimeException ignored) {
            // never throw
        }
    }

    /** Current live rows (diagnostics / test assertion). */
    int size() {
        return byKey.size();
    }

    private void evictExpired(long nowMs) {
        byKey.entrySet().removeIf(e -> expired(e.getValue(), nowMs));
    }

    private void evictOldestWritesOverCap() {
        while (byKey.size() > maxEntries) {
            Key oldest = null;
            long oldestSeq = Long.MAX_VALUE;
            for (Map.Entry<Key, Held> e : byKey.entrySet()) {
                long seq = e.getValue().writeSeq();
                if (seq < oldestSeq) {
                    oldestSeq = seq;
                    oldest = e.getKey();
                }
            }
            if (oldest == null || byKey.remove(oldest) == null) {
                break;
            }
        }
    }

    private boolean expired(Held held, long nowMs) {
        return nowMs - held.writtenAtMs() >= ttlMs;
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
