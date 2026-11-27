package com.studioquack.sqs.webhook;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded in-memory idempotency deduplication store for inbound webhook events.
 *
 * <p>Enforces a 24-hour TTL and a hard cap of {@value #MAX_ENTRIES} entries (evicting expired and
 * oldest entries so memory stays strictly bounded even under sustained webhook traffic).
 */
@Component
public class WebhookIdempotencyStore {

    public static final Duration DEFAULT_TTL = Duration.ofHours(24);
    public static final int MAX_ENTRIES = 100_000;

    private final long ttlMillis;
    private final int maxEntries;
    private final Clock clock;

    private final Map<String, Long> seenExpiresAt;

    public WebhookIdempotencyStore() {
        this(DEFAULT_TTL, MAX_ENTRIES, Clock.systemUTC());
    }

    public WebhookIdempotencyStore(Duration ttl, int maxEntries, Clock clock) {
        this.ttlMillis = ttl == null ? DEFAULT_TTL.toMillis() : Math.max(1L, ttl.toMillis());
        this.maxEntries = Math.max(1, maxEntries);
        this.clock = clock == null ? Clock.systemUTC() : clock;
        this.seenExpiresAt = new LinkedHashMap<>(256, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > WebhookIdempotencyStore.this.maxEntries;
            }
        };
    }

    /**
     * Records {@code (webhookKeyRef, idempotencyKey)} if not already active within the TTL window.
     *
     * @return {@code true} if this event is new (or {@code idempotencyKey} is blank),
     *         {@code false} if it is a duplicate within the TTL window.
     */
    public synchronized boolean recordIfNew(String webhookKeyRef, String idempotencyKey) {
        if (StringUtils.isBlank(idempotencyKey)) {
            return true;
        }
        long now = clock.millis();
        evictExpiredHead(now);
        String key = (webhookKeyRef == null ? "" : webhookKeyRef.trim()) + "\0" + idempotencyKey.trim();
        Long existingExpiry = seenExpiresAt.get(key);
        if (existingExpiry != null && existingExpiry > now) {
            return false;
        }
        seenExpiresAt.remove(key); // refresh insertion order at tail
        seenExpiresAt.put(key, now + ttlMillis);
        return true;
    }

    public synchronized int size() {
        evictExpiredHead(clock.millis());
        return seenExpiresAt.size();
    }

    private void evictExpiredHead(long now) {
        Iterator<Map.Entry<String, Long>> it = seenExpiresAt.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (entry.getValue() <= now) {
                it.remove();
            } else {
                // Insertion-ordered by constant TTL: once we hit a live entry, all later ones are live.
                break;
            }
        }
    }
}
