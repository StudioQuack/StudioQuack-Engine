package com.studioquack.common.dto.webhook;

import java.util.Locale;

/**
 * Scope of a daemon-custodied engine API key ({@code sqk_<keyId>_<secret>}).
 *
 * <ul>
 *   <li>{@link #ENGINE_WIDE}: authenticates classic engine message-event correlation
 *       ({@code correlate-message}) against this environment.</li>
 *   <li>{@link #NODE_SCOPED}: authenticates parameter-driven webhook ingress
 *       ({@code ingest-webhook-key-event}) bound to workflow nodes via
 *       {@code {"$webhookKeyRef": ...}}.</li>
 * </ul>
 */
public enum EngineApiKeyScope {
    ENGINE_WIDE,
    NODE_SCOPED;

    /**
     * Parses a scope string case-insensitively, returning {@code fallback} when {@code raw} is
     * {@code null} or blank. Rejects unknown non-blank values with {@link IllegalArgumentException}.
     */
    public static EngineApiKeyScope fromOrDefault(String raw, EngineApiKeyScope fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        return EngineApiKeyScope.valueOf(raw.trim().toUpperCase(Locale.ROOT));
    }

    /**
     * Parses a scope string case-insensitively, returning {@code null} when {@code raw} is
     * {@code null}, blank, or unrecognized.
     */
    public static EngineApiKeyScope fromStringOrNull(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String normalized = raw.trim().toUpperCase(Locale.ROOT);
        for (EngineApiKeyScope scope : values()) {
            if (scope.name().equals(normalized)) {
                return scope;
            }
        }
        return null;
    }
}
