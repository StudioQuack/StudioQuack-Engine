package com.studioquack.sqs.channel;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded, reason-class-only counters for the heartbeat {@code securityTelemetry} block. Corroborating
 * telemetry per Q7 — never audit rows, never payloads / secrets / customer IPs / env content.
 */
public final class SecurityTelemetryAccumulator {

    public static final int SCHEMA = 1;
    public static final String SOURCE = "daemon";

    /**
     * Hard bound on distinct classes in one drain window (catalogue is smaller today).
     */
    public static final int MAX_CLASSES = 16;

    /**
     * Fixed reason-class catalogue. Unknown keys are ignored so a mis-wired hook cannot enlarge the wire shape or
     * smuggle free-form labels.
     */
    public static final Set<String> ALLOWED_CLASSES = Set.of(
            "ingress_tls",
            "ingress_allowlist",
            "ingress_rate",
            "ingress_replay",
            "ingress_auth",
            "ingress_body",
            "sandbox_egress_degraded",
            "local_auth_fail",
            "webhook_key_auth"
    );

    private final ConcurrentHashMap<String, AtomicLong> counts = new ConcurrentHashMap<>();

    /**
     * Increments {@code reasonClass} when it is allowlisted. Unknown classes and overflow past
     * {@link #MAX_CLASSES} distinct keys are ignored.
     */
    public void record(String reasonClass) {
        if (reasonClass == null || !ALLOWED_CLASSES.contains(reasonClass)) {
            return;
        }
        // Bound distinct keys even if the catalogue grows later; ignore newcomers past the cap.
        if (counts.size() >= MAX_CLASSES && !counts.containsKey(reasonClass)) {
            return;
        }
        counts.computeIfAbsent(reasonClass, ignored -> new AtomicLong())
                .updateAndGet(v -> v >= Integer.MAX_VALUE ? Integer.MAX_VALUE : v + 1L);
    }

    /**
     * Test seam — seeds a counter so clamp-at-MAX can be asserted without a billion increments.
     */
    void forceCountForTest(String reasonClass, long value) {
        if (reasonClass == null || !ALLOWED_CLASSES.contains(reasonClass)) {
            return;
        }
        counts.computeIfAbsent(reasonClass, ignored -> new AtomicLong()).set(value);
    }

    /**
     * Snapshot + reset. Returns only positive counts, in catalogue order.
     */
    public Map<String, Integer> drain() {
        if (counts.isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> out = new LinkedHashMap<>();
        for (String cls : ALLOWED_CLASSES) {
            AtomicLong counter = counts.get(cls);
            if (counter == null) {
                continue;
            }
            long value = counter.getAndSet(0L);
            if (value > 0L) {
                out.put(cls, (int) Math.min(value, Integer.MAX_VALUE));
            }
        }
        return out.isEmpty() ? Map.of() : Collections.unmodifiableMap(out);
    }
}
