package com.studioquack.sqs.execution.runtime;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Enforces the REF-ONLY invariant (Mapping Invariant 15): only the encrypted
 * {@code studioquackRef} handle plus a small, fixed set of non-PHI routing/error/epoch
 * tokens may ever enter the engine's {@code ACT_RU_VARIABLE} / {@code ACT_HI_VARINST}
 * tables. Every other key is PHI-bearing payload that must stay sealed in the
 * {@code execution_context} store.
 *
 * <p>The daemon runs at {@code historyLevel=FULL}, so this guard also protects the history
 * tables — a plaintext value that slips into a process variable would be persisted to
 * {@code ACT_HI_VARINST} forever. This is the client-side counterpart of the control plane
 * whitelist audit that wires onto the trigger/complete path.
 *
 * <p><b>:</b> the authoritative permitted-key set now lives on
 * {@link EngineVariableNames#WHITELIST_FOR_PERSIST} (plus the {@code route__} prefix rule) so the
 * generator, the trigger/complete audit, and the durable {@code sub_pi_pending} result-variable
 * filter all agree on one source of truth. This class is the runtime enforcement seam over it.
 */
public final class RefOnlyVariableGuard {

    private RefOnlyVariableGuard() {
    }

    /** @return true when {@code name} is a permitted REF-only / routing / error token. */
    public static boolean isAllowed(String name) {
        return EngineVariableNames.isPersistWhitelisted(name);
    }

    /**
     * Fails fast if {@code variables} carries any key that is not REF-only-safe. Called by
     * {@link RefOnlyVarBuilder} before variables cross onto the engine so a payload leak is
     * caught at the source rather than discovered later in {@code ACT_HI_VARINST}.
     *
     * @throws IllegalStateException listing the offending non-whitelisted keys.
     */
    public static void assertRefOnly(Map<String, Object> variables) {
        if (variables == null || variables.isEmpty()) {
            return;
        }
        var offending = variables.keySet().stream().filter(k -> !isAllowed(k)).sorted().toList();
        if (!offending.isEmpty()) {
            throw new IllegalStateException(
                    "REF-ONLY invariant violated: engine variable map carries non-whitelisted "
                            + "key(s) " + offending + " — only studioquackRef + route__*/error/epoch "
                            + "tokens may reach ACT_*_VARIABLE. PHI must stay in the sealed "
                            + "execution_context store.");
        }
    }

    /**
     * Returns a copy of {@code variables} containing ONLY whitelisted keys — the projection the
     * durable {@code sub_pi_pending.result_variables} column persists. Unlike {@link #assertRefOnly}
     * this never throws: a completing sub-process may legitimately carry PHI in its raw result map,
     * and this drops it so only non-PHI routing/ref tokens are recorded (REF-ONLY holds even in the
     * client-owned recovery store, not just {@code ACT_*}).
     */
    public static Map<String, Object> filterToWhitelist(Map<String, Object> variables) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        if (variables == null || variables.isEmpty()) {
            return filtered;
        }
        for (Map.Entry<String, Object> e : variables.entrySet()) {
            if (isAllowed(e.getKey())) {
                filtered.put(e.getKey(), e.getValue());
            }
        }
        return filtered;
    }
}
