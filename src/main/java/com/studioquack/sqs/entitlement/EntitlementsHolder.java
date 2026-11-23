package com.studioquack.sqs.entitlement;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The single, live source of truth for this daemon's effective {@link EnvironmentEntitlements}.
 * Retention consumers (and, later, feature gates) read the CURRENT snapshot from here instead of a
 * static {@code @Value("${sqs.log-retention-days}")}, so a pushed {@code reload-config} snapshot
 * takes effect live without a restart.
 */
@Slf4j
@Component
public class EntitlementsHolder {


    private final int defaultLogRetentionDays;

    /**
     * A value above the ceiling is clamped. {@code -1} means no ceiling, so unlimited retention is
     * left unchanged.
     */
    private final int maxLogRetentionDays;

    /** Never null after construction; swapped atomically on each apply. */
    private final AtomicReference<EnvironmentEntitlements> current;

    /**
     * The control plane {@code issuedAt} stamp of the last applied push (null until the first stamped
     * push lands). Both stamps are produced by the control plane clock at enqueue, so the comparison
     * never involves the daemon's clock.
     */
    private final AtomicReference<java.time.Instant> lastAppliedIssuedAt = new AtomicReference<>();


    private final EntitlementSnapshotStore snapshotStore;

    /** Test convenience: no tier ceiling, i.e. the pre-cap behaviour. Production always injects both. */
    public EntitlementsHolder(int defaultLogRetentionDays) {
        this(defaultLogRetentionDays, EnvironmentEntitlements.UNLIMITED_RETENTION - 1);
    }

    /** Test convenience: no sealed store (in-memory only — existing call sites unchanged). */
    public EntitlementsHolder(int defaultLogRetentionDays, int maxLogRetentionDays) {
        this(defaultLogRetentionDays, maxLogRetentionDays, (EntitlementSnapshotStore) null);
    }

    /**
     * Test / explicit wiring: inject a concrete store (null = no persist/restore).
     */
    public EntitlementsHolder(int defaultLogRetentionDays, int maxLogRetentionDays,
                              EntitlementSnapshotStore snapshotStore) {
        this.defaultLogRetentionDays = defaultLogRetentionDays;
        this.maxLogRetentionDays = maxLogRetentionDays;
        this.snapshotStore = snapshotStore;
        this.current = new AtomicReference<>(
                EnvironmentEntitlements.seededDefault(clampRetention(defaultLogRetentionDays)));
        log.info("entitlements: seeded from YAML default logRetentionDays={} (unlimited={}, cap={})",
                defaultLogRetentionDays, current.get().retentionUnlimited(),
                maxLogRetentionDays <= 0 ? "none" : maxLogRetentionDays);
    }

    /**
     * Spring entry point. {@link ObjectProvider} keeps the store optional so tests and
     * trimmed contexts without {@link EntitlementSnapshotStore} still construct the holder;
     * {@code getIfAvailable()} yields null when the bean is absent.
     */
    @org.springframework.beans.factory.annotation.Autowired
    public EntitlementsHolder(
            @Value("${sqs.log-retention-days:" + EnvironmentEntitlements.DEFAULT_LOG_RETENTION_DAYS + "}")
            int defaultLogRetentionDays,
            @Value("${sqs.log-retention-days-max:30}") int maxLogRetentionDays,
            ObjectProvider<EntitlementSnapshotStore> snapshotStoreProvider) {
        this(defaultLogRetentionDays, maxLogRetentionDays,
                snapshotStoreProvider == null ? null : snapshotStoreProvider.getIfAvailable());
    }

    /**
     * Applies the tier ceiling. {@code <= 0} means "unlimited" on BOTH sides of this comparison, so an
     * unlimited cap never clamps, and an unlimited request is only clamped when a finite cap exists —
     * which is exactly the base-tier case this guards.
     */
    private int clampRetention(int requestedDays) {
        if (maxLogRetentionDays <= EnvironmentEntitlements.UNLIMITED_RETENTION) {
            return requestedDays;
        }
        if (requestedDays <= EnvironmentEntitlements.UNLIMITED_RETENTION || requestedDays > maxLogRetentionDays) {
            return maxLogRetentionDays;
        }
        return requestedDays;
    }

    /** The live entitlement snapshot (never null). */
    public EnvironmentEntitlements current() {
        return current.get();
    }

    /**
     * {@code <= 0} means unlimited (the retention reaper skips deletion). Reads through the live
     * holder, falling back to the YAML-seeded default when nothing was pushed.
     */
    public int logRetentionDays() {
        return current.get().logRetentionDays();
    }

    /**
     * Whether the inbound webhook relay add-on is entitled on the LIVE snapshot. The message-event
     * ingress reads this to gate the endpoint.
     */
    public boolean webhookRelay() {
        return current.get().webhookRelay();
    }

    /**
     * Whether this environment may run Docling locally. Server-resolved and pushed with the
     * entitlement snapshot, so it is the same value the operator sees in Engine Settings — not a
     * boot-time property the daemon decides for itself.
     */
    public boolean doclingEnabled() {
        return current.get().doclingEnabled();
    }


    public boolean webhookMessageEventsEnabled() {
        return current.get().webhookMessageEventsEnabled();
    }

    /**
     * The bcrypt hashes of the env's active API keys on the LIVE snapshot (never {@code null}; empty
     * unless the control plane pushed key material under an active webhook-relay entitlement).
     */
    public List<String> apiKeyHashes() {
        return current.get().apiKeyHashes();
    }

    /**
     * A {@code null} snapshot is ignored (defensive — the caller already validates the payload) so the
     * holder never drops to an unseeded state. On success, seals the effective snapshot via
     * {@link EntitlementSnapshotStore} when custody is wired.
     */
    public void apply(EnvironmentEntitlements entitlements) {
        applySealing(entitlements, null);
    }

    /**
     * Applies a pushed snapshot only when it is not strictly OLDER than the last applied push.
     * Comparing the control plane-issued {@code issuedAt} stamps makes application order-independent
     * of delivery order.
     *
     * @return {@code true} when the snapshot was applied, {@code false} when it was rejected as stale
     * (nothing mutated, nothing re-sealed)
     */
    public synchronized boolean applyIfFresh(EnvironmentEntitlements entitlements,
                                             java.time.Instant issuedAt) {
        if (issuedAt != null) {
            java.time.Instant last = lastAppliedIssuedAt.get();
            if (last != null && issuedAt.isBefore(last)) {
                log.warn("entitlements: ignoring STALE snapshot issuedAt={} — older than the last "
                        + "applied push ({}); keeping docling={} logRetentionDays={}",
                        issuedAt, last, current.get().doclingEnabled(), current.get().logRetentionDays());
                return false;
            }
        }
        applySealing(entitlements, issuedAt);
        if (issuedAt != null) {
            lastAppliedIssuedAt.set(issuedAt);
        }
        return true;
    }

    private void applySealing(EnvironmentEntitlements entitlements, java.time.Instant issuedAt) {
        EnvironmentEntitlements effective = applyInMemory(entitlements, true);
        if (effective != null && snapshotStore != null) {
            snapshotStore.save(effective, issuedAt);
        }
    }

    /**
     * Boot restore: load the sealed snapshot (if any) into memory without re-saving. Invoked by Spring
     * after construction so ingress auth material is available before the webhook answers.
     */
    @PostConstruct
    void restorePersistedSnapshot() {
        restoreFromStore();
    }

    /**
     * Restores a sealed snapshot into memory (no re-persist). Returns {@code true}
     * when a snapshot was loaded and applied — used by tests to prove the boot
     * ordering contract (restore before reading {@link #apiKeyHashes()}).
     */
    public boolean restoreFromStore() {
        if (snapshotStore == null) {
            return false;
        }
        Optional<EntitlementSnapshotStore.SealedSnapshot> loaded = snapshotStore.loadSealed();
        if (loaded.isEmpty()) {
            return false;
        }
        EnvironmentEntitlements restored = applyInMemory(loaded.get().snapshot(), false);
        if (restored != null && loaded.get().appliedAt() != null) {
            lastAppliedIssuedAt.set(loaded.get().appliedAt());
        }
        return restored != null;
    }

    /**
     * {@code fromPush=true} uses the full apply INFO line; restore logs the apiKeyHashes COUNT only
     * (never hash values).
     *
     * @return The effective snapshot that was set, or {@code null} when the input was ignored
     */
    private EnvironmentEntitlements applyInMemory(EnvironmentEntitlements entitlements, boolean fromPush) {
        if (entitlements == null) {
            log.warn("entitlements: ignoring null snapshot; keeping current logRetentionDays={}",
                    current.get().logRetentionDays());
            return null;
        }
        int requested = entitlements.logRetentionDays();
        int effectiveDays = clampRetention(requested);
        if (effectiveDays != requested) {
            log.warn("entitlements: BE pushed logRetentionDays={} but this tier is capped at {} — clamping. "
                            + "A base daemon is an ephemeral store; sq-backend holds the durable copy.",
                    requested, maxLogRetentionDays);
            entitlements = entitlements.withLogRetentionDays(effectiveDays);
        }
        EnvironmentEntitlements previous = current.getAndSet(entitlements);
        if (fromPush) {
            // ApiKeyHashes: log the COUNT only — the hashes are secret-adjacent bcrypt material and must never
            // be echoed to logs. The count is enough to confirm the push landed.
            log.info("entitlements: applied snapshot logRetentionDays {} -> {} (monitoring={} webhookRelay={} docling={} webhookMessageEvents={} apiKeyHashes={})",
                    previous.logRetentionDays(), entitlements.logRetentionDays(),
                    entitlements.monitoringEnabled(),
                    entitlements.webhookRelay(), entitlements.doclingEnabled(),
                    entitlements.webhookMessageEventsEnabled(),
                    entitlements.apiKeyHashes().size());
        } else {
            // Boot restore fallback — count only (: never log hash values).
            log.info("entitlements: restored sealed snapshot apiKeyHashes={}",
                    entitlements.apiKeyHashes().size());
        }
        return entitlements;
    }

    /** The YAML-seeded retention default (diagnostics / tests). */
    public int defaultLogRetentionDays() {
        return defaultLogRetentionDays;
    }
}
