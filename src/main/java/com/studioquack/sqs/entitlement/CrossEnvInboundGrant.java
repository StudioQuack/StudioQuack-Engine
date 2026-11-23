package com.studioquack.sqs.entitlement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One row of the INBOUND cross-environment grant set pushed to this daemon inside the
 * {@code reload-config} entitlement snapshot (the {@code crossEnvInbound} field): WHO may call
 * this daemon's overlay peer endpoints, and for which scopes. The backend collapses wildcard scope
 * rows into the concrete scopes when it builds the snapshot — the wildcard is still honoured
 * defensively at check time so a looser future push cannot fail open by accident.
 *
 * @param callerEnvId The environment id allowed to call THIS daemon, or {@value #WILDCARD_CALLER}
 * @param scopes The granted scopes ({@code execute} / {@code llm} / {@value #WILDCARD_SCOPE});
 * never {@code null} (an absent field normalises to an immutable empty list, which grants nothing)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CrossEnvInboundGrant(String callerEnvId, List<String> scopes) {

    /** Wildcard caller id: any same-owner environment may call (still subject to the scope check). */
    public static final String WILDCARD_CALLER = "*";
    /** Wildcard scope: the grant covers every scope the daemon serves. */
    public static final String WILDCARD_SCOPE = "*";

    /**
     * Canonical constructor — normalises {@code scopes} to an immutable, {@code null}/blank-free
     * list so the grant store never NPEs on a malformed push. Jackson deserialises through this
     * constructor; an absent JSON field arrives as {@code null} and becomes an empty list.
     */
    public CrossEnvInboundGrant {
        scopes = normalizeScopes(scopes);
    }

    /** {@code null}/empty → an immutable empty list; otherwise an immutable copy without blanks. */
    private static List<String> normalizeScopes(List<String> in) {
        if (in == null || in.isEmpty()) {
            return List.of();
        }
        List<String> cleaned = new ArrayList<>(in.size());
        for (String s : in) {
            if (s != null && !s.isBlank()) {
                cleaned.add(s.trim());
            }
        }
        return Collections.unmodifiableList(cleaned);
    }
}
