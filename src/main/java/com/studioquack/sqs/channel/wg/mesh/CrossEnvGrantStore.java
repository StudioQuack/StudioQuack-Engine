package com.studioquack.sqs.channel.wg.mesh;

import com.studioquack.sqs.entitlement.CrossEnvInboundGrant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * This daemon's INBOUND cross-environment grant set: who may call THIS daemon's overlay peer endpoints, and
 * for which scope. Pushed by the control plane as the {@code crossEnvInbound} field of the
 * {@code reload-config} entitlement snapshot and replaced wholesale on every payload-bearing push.
 */
@Slf4j
@Component
public class CrossEnvGrantStore {

    /**
     * CallerEnvId (or {@code "*"}) → granted scopes (trimmed, lower-cased; {@code "*"} = any).
     */
    private volatile Map<String, Set<String>> grants = Map.of();

    /**
     * Whether {@code callerEnvId} holds an inbound grant for {@code scope} on THIS daemon. Exact-caller rows win
     * over the wildcard caller; either may grant the scope directly or vithe wildcard scope.
     */
    public boolean check(String callerEnvId, String scope) {
        if (callerEnvId == null || callerEnvId.isBlank() || scope == null || scope.isBlank()) {
            return false;
        }
        Map<String, Set<String>> current = this.grants;
        String needed = scope.trim().toLowerCase(Locale.ROOT);
        return grantsScope(current.get(callerEnvId.trim()), needed)
                || grantsScope(current.get(CrossEnvInboundGrant.WILDCARD_CALLER), needed);
    }

    /**
     * Replaces the WHOLE set from a reload-config snapshot — the push is the authoritative view, not a delta, so a
     * revoked grant disappears the moment the backend says so. A {@code null} list clears the set (deny-all).
     */
    public void replace(List<CrossEnvInboundGrant> inbound) {
        Map<String, Set<String>> next = new LinkedHashMap<>();
        if (inbound != null) {
            for (CrossEnvInboundGrant grant : inbound) {
                if (grant == null || grant.callerEnvId() == null || grant.callerEnvId().isBlank()) {
                    continue;
                }
                Set<String> scopes = next.computeIfAbsent(grant.callerEnvId().trim(),
                        k -> new LinkedHashSet<>());
                for (String s : grant.scopes()) {
                    if (s != null && !s.isBlank()) {
                        scopes.add(s.trim().toLowerCase(Locale.ROOT));
                    }
                }
            }
        }
        // Freeze both levels: the swapped-in snapshot must be immutable for lock-free reads.
        Map<String, Set<String>> frozen = new LinkedHashMap<>();
        next.forEach((caller, scopes) -> frozen.put(caller, Set.copyOf(scopes)));
        this.grants = Map.copyOf(frozen);
        log.info("channel/mesh: inbound cross-env grant set replaced ({} caller entr{})",
                frozen.size(), frozen.size() == 1 ? "y" : "ies");
    }


    public int size() {
        return grants.size();
    }

    private static boolean grantsScope(Set<String> scopes, String needed) {
        return scopes != null
                && (scopes.contains(needed) || scopes.contains(CrossEnvInboundGrant.WILDCARD_SCOPE));
    }
}
