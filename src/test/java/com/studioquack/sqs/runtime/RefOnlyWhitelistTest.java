package com.studioquack.sqs.runtime;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The REF-ONLY persist whitelist ({@link EngineVariableNames#WHITELIST_FOR_PERSIST}) is the single
 * source of truth for what may reach {@code ACT_*_VARIABLE}/{@code ACT_HI_VARINST} (holds at {@code
 * historyLevel=FULL}) and what may be recorded in {@code sub_pi_pending}. A PHI key is rejected on
 * the trigger/complete path and filtered out of the sub-PI result store.
 */
class RefOnlyWhitelistTest {

    @Test
    void whitelist_contains_only_ref_error_epoch_tokens() {
        // Asserted by exact membership rather than a count: the point of the invariant is WHICH names
        // may persist, and a bare size check fails on every legitimate addition without saying what
        // changed — which is how it drifted when the typed error kind/exit-code sentinels were added.
        assertEquals(
                java.util.Set.of(
                        EngineVariableNames.STUDIOQUACK_REF,
                        EngineVariableNames.ERROR_CODE,
                        EngineVariableNames.ERROR_MESSAGE,
                        EngineVariableNames.ERROR_KIND,
                        EngineVariableNames.ERROR_EXIT_CODE,
                        EngineVariableNames.BE_EPOCH,
                        EngineVariableNames.START_CORRELATION_KEY,
                        EngineVariableNames.SKIP_NODE),
                EngineVariableNames.WHITELIST_FOR_PERSIST,
                "the exact set stays small + closed: a ref handle, the typed error sentinels, "
                        + "the boot epoch, the message-start correlation key, the skip pin");
    }

    @Test
    void fresh_instance_scrub_extends_the_error_sentinels_with_the_skip_pin() {
        // A call.activity child inherits every parent variable: the skip pin must be scrubbed
        // alongside the error sentinels, but it must NOT join ERROR_SENTINELS itself (the hold
        // requeue scrubs that family off the scope chain and would wipe a pin it must deliver).
        assertFalse(EngineVariableNames.ERROR_SENTINELS.contains(EngineVariableNames.SKIP_NODE));
        assertEquals(
                java.util.Set.of(
                        EngineVariableNames.ERROR_CODE,
                        EngineVariableNames.ERROR_MESSAGE,
                        EngineVariableNames.ERROR_KIND,
                        EngineVariableNames.ERROR_EXIT_CODE,
                        EngineVariableNames.SKIP_NODE),
                EngineVariableNames.FRESH_INSTANCE_SCRUB,
                "a fresh instance starts with neither error state nor a skip pin");
    }

    @Test
    void isPersistWhitelisted_allows_route_prefix_but_not_phi() {
        assertTrue(EngineVariableNames.isPersistWhitelisted("route__gateway1"));
        assertTrue(EngineVariableNames.isPersistWhitelisted(EngineVariableNames.STUDIOQUACK_REF));
        assertFalse(EngineVariableNames.isPersistWhitelisted("prompt"));
        assertFalse(EngineVariableNames.isPersistWhitelisted("patientName"));
        assertFalse(EngineVariableNames.isPersistWhitelisted(null));
    }

    @Test
    void isPersistWhitelisted_allows_msgcorr_prefix_but_not_lookalikes() {
        assertTrue(EngineVariableNames.isPersistWhitelisted(
                        EngineVariableNames.messageCorrelationVariableName("order.approved")),
                "the receiver's correlation pin is a legal non-PHI routing token");
        assertTrue(EngineVariableNames.isPersistWhitelisted(
                EngineVariableNames.MSG_CORR_PREFIX + "orderApproved"));
        assertFalse(EngineVariableNames.isPersistWhitelisted("__sq_msgcorrx_evade"),
                "a prefix lookalike without the exact reserved prefix is still rejected");
    }

    @Test
    void assertRefOnly_rejects_phi_bearing_variable_map() {
        Map<String, Object> phi = new LinkedHashMap<>();
        phi.put(EngineVariableNames.STUDIOQUACK_REF, "ref");
        phi.put("diagnosis", "PHI leak");
        assertThrows(IllegalStateException.class, () -> RefOnlyVariableGuard.assertRefOnly(phi),
                "a PHI-bearing engine variable must be rejected on the trigger/complete path");
    }

    @Test
    void filterToWhitelist_drops_phi_keeps_routing_tokens() {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put(EngineVariableNames.STUDIOQUACK_REF, "ref-123");
        raw.put("route__gw", "APPROVE");
        raw.put("ssn", "000-00-0000");       // PHI — must be dropped
        raw.put("freeText", "patient note"); // PHI — must be dropped

        Map<String, Object> filtered = RefOnlyVariableGuard.filterToWhitelist(raw);

        assertEquals(2, filtered.size(), "only whitelisted keys survive into sub_pi_pending.result_variables");
        assertTrue(filtered.containsKey(EngineVariableNames.STUDIOQUACK_REF));
        assertTrue(filtered.containsKey("route__gw"));
        assertFalse(filtered.containsKey("ssn"));
        assertFalse(filtered.containsKey("freeText"));
    }

    @Test
    void filterToWhitelist_null_and_empty_are_safe() {
        assertTrue(RefOnlyVariableGuard.filterToWhitelist(null).isEmpty());
        assertTrue(RefOnlyVariableGuard.filterToWhitelist(Map.of()).isEmpty());
    }
}
