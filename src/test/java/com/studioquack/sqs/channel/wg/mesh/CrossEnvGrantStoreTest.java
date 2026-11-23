package com.studioquack.sqs.channel.wg.mesh;

import com.studioquack.sqs.entitlement.CrossEnvInboundGrant;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CrossEnvGrantStore}: the inbound grant set starts EMPTY (deny-all until the first push),
 * matches exact-caller then wildcard-caller and scope then wildcard-scope, and honours scope
 * separation (the acceptance: an {@code llm} grant does NOT permit {@code execute}).
 */
class CrossEnvGrantStoreTest {

    private static CrossEnvInboundGrant grant(String caller, String... scopes) {
        return new CrossEnvInboundGrant(caller, List.of(scopes));
    }

    @Test
    void startsEmpty_deniesEverything() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();

        assertFalse(store.check("env-a", "execute"), "no push yet → deny-all (fail closed)");
        assertFalse(store.check("env-a", "llm"));
        assertEquals(0, store.size());
    }

    @Test
    void exactCallerGrant_matchesOnlyTheGrantedScope() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "execute")));

        assertTrue(store.check("env-a", "execute"));
        // Scope honoured: an execute grant says nothing about llm.
        assertFalse(store.check("env-a", "llm"), "execute grant ≠ llm");
        assertFalse(store.check("env-b", "execute"), "a grant names exactly one caller");
    }

    @Test
    void llmGrant_doesNotPermitExecute() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "llm")));

        assertTrue(store.check("env-a", "llm"));
        assertFalse(store.check("env-a", "execute"), "§9: llm grant ≠ execute");
    }

    @Test
    void wildcardCaller_coversAnyCaller_forTheGrantedScopeOnly() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("*", "llm")));

        assertTrue(store.check("env-a", "llm"));
        assertTrue(store.check("env-anything", "llm"));
        assertFalse(store.check("env-a", "execute"), "the wildcard is still scope-bound");
    }

    @Test
    void wildcardScope_coversEveryScope_forThatCaller() {
        // The server collapses "*" scope rows into the concrete scopes when building the snapshot;
        // the store still honours a raw "*" defensively.
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "*")));

        assertTrue(store.check("env-a", "execute"));
        assertTrue(store.check("env-a", "llm"));
        assertFalse(store.check("env-b", "execute"), "wildcard scope does not widen the caller");
    }

    @Test
    void exactCallerAndWildcardCaller_compose() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "execute"), grant("*", "llm")));

        assertTrue(store.check("env-a", "execute"), "exact row");
        assertTrue(store.check("env-a", "llm"), "wildcard row also covers env-a");
        assertFalse(store.check("env-b", "execute"));
        assertTrue(store.check("env-b", "llm"));
    }

    @Test
    void replace_isWholesale_revokedRowsDisappear() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "execute"), grant("env-b", "llm")));
        assertEquals(2, store.size());

        store.replace(List.of(grant("env-c", "execute")));

        assertFalse(store.check("env-a", "execute"), "revoked by the wholesale replace");
        assertFalse(store.check("env-b", "llm"), "revoked by the wholesale replace");
        assertTrue(store.check("env-c", "execute"));
        assertEquals(1, store.size());
    }

    @Test
    void replaceWithNullOrEmpty_clearsToDenyAll() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "execute")));
        store.replace(null);
        assertFalse(store.check("env-a", "execute"));
        assertEquals(0, store.size());

        store.replace(List.of(grant("env-a", "execute")));
        store.replace(List.of());
        assertFalse(store.check("env-a", "execute"), "an empty push is an explicit deny-all");
    }

    @Test
    void blankInputs_neverMatch_andMalformedRowsAreSkipped() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "execute")));

        assertFalse(store.check(null, "execute"));
        assertFalse(store.check("  ", "execute"));
        assertFalse(store.check("env-a", null));
        assertFalse(store.check("env-a", " "));

        store.replace(List.of(grant("  ", "execute"), grant("env-b", "llm")));
        assertEquals(1, store.size(), "a blank caller row is skipped, not stored");
        assertTrue(store.check("env-b", "llm"));
    }

    @Test
    void scopeMatching_isCaseInsensitiveOnIngest() {
        CrossEnvGrantStore store = new CrossEnvGrantStore();
        store.replace(List.of(grant("env-a", "Execute")));

        assertTrue(store.check("env-a", "execute"), "scopes normalise to lower-case on ingest");
        assertTrue(store.check("env-a", "EXECUTE"));
    }
}
