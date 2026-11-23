package com.studioquack.sqs.entitlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The client's own copy of the backend's effective entitlement snapshot for this (single-tenant)
 * environment. Received as the OPTIONAL payload of the {@code reload-config} daemon command and
 * applied to the live {@link EntitlementsHolder}.
 *
 * @param logRetentionDays Days of on-client execution history/logs to retain.
 * @param monitoringEnabled Whether advanced monitoring is entitled
 * @param webhookRelay Whether the inbound webhook relay is entitled
 * @param doclingEnabled Whether this env forwards Docling conversions to its own client-side
 * side-car
 * @param webhookMessageEventsEnabled
 * @param apiKeyHashes Bcrypt hashes of the env's active API keys, pushed ADDITIVELY by the control
 * plane only when {@code webhookRelay} is entitled; never a raw key.
 * @param crossEnvInbound The INBOUND cross-environment grant set (who may call THIS daemon's
 * overlay peer endpoints, and for which scopes), pushed on every payload-bearing
 * {@code reload-config}.
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public record EnvironmentEntitlements(
        int logRetentionDays,
        boolean monitoringEnabled,
        boolean webhookRelay,
        boolean doclingEnabled,
        boolean webhookMessageEventsEnabled,
        List<String> apiKeyHashes,
        List<CrossEnvInboundGrant> crossEnvInbound
) {

    /**
     * Free-tier log-retention floor, in days. Matches the BE
     * {@code EnvironmentEntitlements.DEFAULT_LOG_RETENTION_DAYS} and the base
     * tier's {@code sqs.log-retention-days} YAML default.
     */
    public static final int DEFAULT_LOG_RETENTION_DAYS = 1;

    /**
     * Sentinel floor for the "unlimited retention" convention: any
     * {@code logRetentionDays <= 0} means the retention reaper never deletes
     * ({@code -1} means unlimited). Kept as a named constant so both the
     * holder seeding and the enforcer reason about the same boundary.
     */
    public static final int UNLIMITED_RETENTION = 0;

    /**
     * Canonical constructor — normalises {@code apiKeyHashes} and {@code crossEnvInbound} to
     * immutable, {@code null}-free lists so consumers never NPE and a malformed BE push cannot
     * break local matching. Jackson deserialises through this constructor; an absent JSON field
     * arrives as {@code null} and becomes an empty list.
     */
    public EnvironmentEntitlements {
        apiKeyHashes = normalizeHashes(apiKeyHashes);
        crossEnvInbound = crossEnvInbound == null ? List.of() : List.copyOf(crossEnvInbound);
    }

    /**
     * Backward-compatible constructor: message-events flag and key hashes, no inbound cross-env grant
     * set.
     */
    public EnvironmentEntitlements(int logRetentionDays,
                                   boolean monitoringEnabled, boolean webhookRelay, boolean doclingEnabled,
                                   boolean webhookMessageEventsEnabled, List<String> apiKeyHashes) {
        this(logRetentionDays, monitoringEnabled, webhookRelay, doclingEnabled,
                webhookMessageEventsEnabled, apiKeyHashes, List.of());
    }

    /** Retained
so every call site (and the base tier, which never carries key
hashes). */
    public EnvironmentEntitlements(int logRetentionDays,
                                   boolean monitoringEnabled, boolean webhookRelay, boolean doclingEnabled) {
        this(logRetentionDays, monitoringEnabled, webhookRelay, doclingEnabled, false, List.of());
    }

    /** Backward-compatible pre-message-event constructor retaining optional key hashes. */
    public EnvironmentEntitlements(int logRetentionDays,
                                   boolean monitoringEnabled, boolean webhookRelay, boolean doclingEnabled,
                                   List<String> apiKeyHashes) {
        this(logRetentionDays, monitoringEnabled, webhookRelay, doclingEnabled, false, apiKeyHashes);
    }

    /**
     * The safe, everything-off baseline seeded from a YAML retention default at boot (before any BE
     * push). Mirrors the control plane {@code safeDefaults} shape but lets the caller supply the
     * tier's configured retention floor.
     *
     * @param logRetentionDays
     */
    public static EnvironmentEntitlements seededDefault(int logRetentionDays) {
        return new EnvironmentEntitlements(logRetentionDays, false, false, false, false, List.of());
    }

    /** True when retention is unlimited (never reap); see {@link #UNLIMITED_RETENTION}. */
    public boolean retentionUnlimited() {
        return logRetentionDays <= UNLIMITED_RETENTION;
    }

    /**
     * A copy with a different retention window, leaving every other entitlement untouched. Used by
     * {@code EntitlementsHolder} to apply the tier ceiling to a BE-pushed snapshot without discarding
     * the rest of it.
     */
    public EnvironmentEntitlements withLogRetentionDays(int days) {
        return new EnvironmentEntitlements(
                days, monitoringEnabled, webhookRelay, doclingEnabled,
                webhookMessageEventsEnabled, apiKeyHashes, crossEnvInbound);
    }

    /**
     * True when at least one API key hash was pushed, so the message-event ingress can
     * authenticate a presented key locally.
     */
    public boolean hasApiKeyHashes() {
        return !apiKeyHashes.isEmpty();
    }

    /**
     * Normalises the pushed hash list: {@code null}/empty → an immutable empty
     * list; otherwise an immutable copy with {@code null}/blank entries dropped.
     * Kept lenient (never throws) so a forward/looser BE push is tolerated rather
     * than failing the whole {@code reload-config} apply.
     */
    private static List<String> normalizeHashes(List<String> hashes) {
        if (hashes == null || hashes.isEmpty()) {
            return List.of();
        }
        List<String> cleaned = new ArrayList<>(hashes.size());
        for (String hash : hashes) {
            if (hash != null && !hash.isBlank()) {
                cleaned.add(hash);
            }
        }
        return Collections.unmodifiableList(cleaned);
    }
}
