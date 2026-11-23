package com.studioquack.sqs.execution.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.execution.crypto.ContextKeyProvider;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sub-PI invocation index against a real embedded database datasource: record/query order, parent
 * isolation, blank/null no-ops, retention delete-by-parent-or-child, and the 3-arg store (repo
 * null) no-op. Uses {@code @DataJpaTest} so Hibernate creates {@code sub_pi_invocation} and both
 * repositories are real.
 */
@DataJpaTest(properties = {"spring.flyway.enabled=false"})
class SubPiInvocationStoreH2Test {

    /** 32-byte AES-256 key (Base64) — same crypto setup as {@link ExecutionContextStoreH2Test}. */
    private static final String KEY_B64 = Base64.getEncoder().encodeToString(new byte[32]);

    @Autowired
    private ExecutionContextRepository contextRepository;

    @Autowired
    private SubPiInvocationRepository subPiInvocationRepository;

    private final ObjectMapper mapper = new ObjectMapper();
    private H2ExecutionContextStore store;
    private H2ExecutionContextStore storeWithoutIndex;

    @BeforeEach
    void setUp() {
        ExecutionContextCrypto crypto = new ExecutionContextCrypto(mapper, keyProvider(KEY_B64, "test"));
        crypto.init();
        store = new H2ExecutionContextStore(
                contextRepository, mapper, crypto, subPiInvocationRepository);
        storeWithoutIndex = new H2ExecutionContextStore(contextRepository, mapper, crypto);
    }

    private ContextKeyProvider keyProvider(String keyB64, String source) {
        return new ContextKeyProvider() {
            @Override
            public String contextEncryptionKeyBase64() {
                return keyB64;
            }

            @Override
            public String source() {
                return source;
            }
        };
    }

    @Test
    void records_three_invocations_in_id_asc_insert_order() {
        store.recordSubPiInvocation("P", "C1", "caller-a", "root-1");
        store.recordSubPiInvocation("P", "C2", "caller-b", "root-2");
        store.recordSubPiInvocation("P", "C3", "caller-a", "root-3");

        List<SubPiInvocation> rows = store.subPiInvocationsOf("P");
        assertEquals(3, rows.size(), "record three invocations under parent P → query returns all three");
        assertEquals(new SubPiInvocation("C1", "caller-a", "root-1"), rows.get(0),
                "subPiInvocationsOf is ordered by id asc (insert order): first child/caller/root");
        assertEquals(new SubPiInvocation("C2", "caller-b", "root-2"), rows.get(1),
                "subPiInvocationsOf is ordered by id asc (insert order): second child/caller/root");
        assertEquals(new SubPiInvocation("C3", "caller-a", "root-3"), rows.get(2),
                "subPiInvocationsOf is ordered by id asc (insert order): third child/caller/root");
    }

    @Test
    void query_returns_all_callers_parent_isolation_is_store_side() {
        store.recordSubPiInvocation("P", "C1", "caller-a", "root-1");
        store.recordSubPiInvocation("P", "C2", "caller-b", "root-2");
        store.recordSubPiInvocation("Q", "Cq", "caller-q", "root-q");

        List<SubPiInvocation> ofP = store.subPiInvocationsOf("P");
        assertEquals(2, ofP.size(), "caller filter is resolver-side: store returns ALL callers for P");
        assertEquals("C1", ofP.get(0).childExecutionId());
        assertEquals("caller-a", ofP.get(0).callerNodeId());
        assertEquals("C2", ofP.get(1).childExecutionId());
        assertEquals("caller-b", ofP.get(1).callerNodeId());
        assertTrue(ofP.stream().noneMatch(row -> "Cq".equals(row.childExecutionId())),
                "parent Q is isolated: Q's rows do not appear in P's query");

        List<SubPiInvocation> ofQ = store.subPiInvocationsOf("Q");
        assertEquals(1, ofQ.size(), "parent Q is isolated: query of Q returns only Q's row");
        assertEquals(new SubPiInvocation("Cq", "caller-q", "root-q"), ofQ.get(0));
    }

    @Test
    void blank_or_null_record_and_query_args_are_noops() {
        store.recordSubPiInvocation(null, "C", "caller", "root");
        store.recordSubPiInvocation("P", "  ", "caller", "root");
        store.recordSubPiInvocation("P", "C", "  ", "root");
        store.recordSubPiInvocation("P", "C", "caller", "  ");

        assertTrue(store.subPiInvocationsOf("P").isEmpty(),
                "blank/null ANY record arg → no-op (query of a real parent stays empty)");
        assertTrue(store.subPiInvocationsOf(null).isEmpty(),
                "subPiInvocationsOf(null) returns empty, no exception");
        assertTrue(store.subPiInvocationsOf("  ").isEmpty(),
                "subPiInvocationsOf(\"  \") returns empty, no exception");
    }

    @Test
    void delete_by_parent_or_child_execution_id_in_purges_nested_and_keeps_others() {
        store.recordSubPiInvocation("P", "C1", "caller-a", "root-a");
        store.recordSubPiInvocation("C1", "C2", "caller-b", "root-b");
        store.recordSubPiInvocation("KEEP_P", "KEEP_C", "caller-keep", "root-keep");
        subPiInvocationRepository.flush();

        int removedAsParent = subPiInvocationRepository.deleteByParentOrChildExecutionIdIn(List.of("P"));
        assertEquals(1, removedAsParent,
                "deleteByParentOrChildExecutionIdIn(P) removes the P→C1 row");
        assertTrue(store.subPiInvocationsOf("P").isEmpty(),
                "deleteByParentOrChildExecutionIdIn(P) removes the P→C1 row");
        List<SubPiInvocation> nested = store.subPiInvocationsOf("C1");
        assertEquals(1, nested.size(), "nested C1→C2 survives a parent-P purge");
        assertEquals(new SubPiInvocation("C2", "caller-b", "root-b"), nested.get(0));

        int removedAsChild = subPiInvocationRepository.deleteByParentOrChildExecutionIdIn(List.of("C1"));
        assertEquals(1, removedAsChild,
                "deleteByParentOrChildExecutionIdIn(C1) removes the C1→C2 row (child-id match)");
        assertTrue(store.subPiInvocationsOf("C1").isEmpty(),
                "deleteByParentOrChildExecutionIdIn(C1) removes the C1→C2 row (child-id match)");

        List<SubPiInvocation> survivor = store.subPiInvocationsOf("KEEP_P");
        assertEquals(1, survivor.size(),
                "a row whose parent AND child are other ids survives");
        assertEquals(new SubPiInvocation("KEEP_C", "caller-keep", "root-keep"), survivor.get(0),
                "a row whose parent AND child are other ids survives");
    }

    @Test
    void three_arg_store_record_and_query_are_noops_when_repo_null() {
        storeWithoutIndex.recordSubPiInvocation("P", "C1", "caller-a", "root-1");
        assertTrue(storeWithoutIndex.subPiInvocationsOf("P").isEmpty(),
                "3-arg store (repo null): query is empty");
        assertTrue(store.subPiInvocationsOf("P").isEmpty(),
                "3-arg store (repo null): record is no-op (4-arg store sees nothing written)");
    }
}
