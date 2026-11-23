package com.studioquack.sqs.execution.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.crypto.ContextKeyProvider;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AES-GCM execution-context round-trip against a real embedded database datasource, plus the
 * sealing (AAD binding, ciphertext-at-rest), REF-only shape, and disabled-mode pass-through. Also
 * encodes Mapping Invariant 16 (cross-env sealed over the channel + same-env loopback keeps
 * REF-only).
 */
@DataJpaTest(properties = {"spring.flyway.enabled=false"})
class ExecutionContextStoreH2Test {

    /** Distinctive PHI marker so we can assert it never appears as plaintext in a sealed row. */
    private static final String PHI = "PATIENT-SECRET-9f3a";
    /** 32-byte AES-256 key (Base64). */
    private static final String KEY_B64 = Base64.getEncoder().encodeToString(new byte[32]);

    @Autowired
    private ExecutionContextRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();
    private ExecutionContextCrypto enabledCrypto;
    private H2ExecutionContextStore encryptedStore;

    @BeforeEach
    void setUp() {
        enabledCrypto = new ExecutionContextCrypto(mapper, keyProvider(KEY_B64, "test"));
        enabledCrypto.init();
        encryptedStore = new H2ExecutionContextStore(repository, mapper, enabledCrypto);
    }

    @Test
    void seal_algorithm_reflects_the_actual_key_size() {
        assertEquals("AES-256-GCM", enabledCrypto.sealAlgorithm(),
                "32-byte key -> AES-256-GCM (never a hardcoded claim)");
        ExecutionContextCrypto aes128 = new ExecutionContextCrypto(mapper,
                keyProvider(Base64.getEncoder().encodeToString(new byte[16]), "test"));
        aes128.init();
        assertEquals("AES-128-GCM", aes128.sealAlgorithm());
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

    private ExecutionContext ctxWithPhi(String executionId, String nodeId) {
        NodeDto node = new NodeDto();
        node.setId(nodeId);
        node.setData(new TemplateDto());
        List<InOutValueDto> inputs = new ArrayList<>();
        inputs.add(TransformFixtures.input("up", "result", nodeId, "prompt",
                TransformFixtures.map("patient", PHI)));
        node.setInputList(inputs);

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(executionId);
        ctx.setWorkflowId("wf-1");
        ctx.setEnvId("env-1");
        ctx.setUserId("user-1");
        ctx.setCurrentNodeId(nodeId);
        ctx.setCurrentNode(node);
        return ctx;
    }

    @Test
    void aes_gcm_context_round_trips_through_h2() {
        ExecutionContext ctx = ctxWithPhi("exec-rt", "cur");
        ProcessContextRef ref = encryptedStore.snapshot(ctx, 99L);

        assertTrue(ref.getCurrentNodeRefId().startsWith("node_"));
        assertEquals("exec-rt", ref.getExecutionId());
        assertEquals("env-1", ref.getEnvId());
        assertEquals(2, ref.getSchemaVersion());

        ExecutionContext loaded = encryptedStore.loadByRef(ref).orElseThrow();
        assertEquals("wf-1", loaded.getWorkflowId());
        assertEquals("env-1", loaded.getEnvId());
        assertEquals(1, loaded.getNextNodeInputList().size());
        Object value = loaded.getNextNodeInputList().get(0).getValue();
        assertEquals(Map.of("patient", PHI), value, "decrypted PHI must round-trip intact");
    }

    /** At-rest: the stored columns are ciphertext envelopes; the PHI plaintext never appears. */
    @Test
    void snapshot_seals_payload_columns_at_rest() {
        ProcessContextRef ref = encryptedStore.snapshot(ctxWithPhi("exec-seal", "cur"), null);
        ExecutionContextEntity row = repository.findByNodeRefId(ref.getCurrentNodeRefId()).orElseThrow();

        assertTrue(row.getContextBlobJson().contains(ExecutionContextCrypto.MARKER), "blob is sealed");
        assertTrue(row.getInputsJson().contains(ExecutionContextCrypto.MARKER), "inputs are sealed");
        assertFalse(row.getContextBlobJson().contains(PHI), "PHI must NOT be plaintext in the blob column");
        assertFalse(row.getInputsJson().contains(PHI), "PHI must NOT be plaintext in the inputs column");
    }

    /** AAD binding: a ciphertext sealed under one nodeRefId refuses to open under another. */
    @Test
    void aad_binding_prevents_cross_row_ciphertext_splice() {
        Map<String, Object> sealed = enabledCrypto.sealMap(TransformFixtures.map("patient", PHI), "node_A");
        assertTrue(sealed.containsKey(ExecutionContextCrypto.MARKER));

        assertEquals(Map.of("patient", PHI), enabledCrypto.openMap(sealed, "node_A"), "opens with correct AAD");
        assertThrows(IllegalStateException.class, () -> enabledCrypto.openMap(sealed, "node_B"),
                "GCM AAD mismatch (spliced onto another row) must fail to open");
    }

    /** Retry-idempotent REF-path: upstream outputs are collected for the downstream node, deduped. */
    @Test
    void ref_paths_collect_and_dedup_upstream_outputs() {
        ProcessContextRef upRef = encryptedStore.snapshot(ctxWithPhi("exec-io", "up"), null);
        InOutValueDto out = TransformFixtures.input("up", "result", "cur", "prompt",
                TransformFixtures.map("answer", 1));
        out.setPortId("p1");
        out.setTargetPortId("tp1");
        encryptedStore.saveOutputs(upRef, new ArrayList<>(List.of(out)));

        // A ref carrying just the executionId is enough to resolve inbound wires to "cur".
        ProcessContextRef exec = ProcessContextRef.builder().executionId("exec-io").build();
        List<InOutValueDto> inbound = encryptedStore.loadInputsTargetingNode(exec, "cur");
        assertEquals(1, inbound.size());
        assertEquals("cur", inbound.get(0).getTargetNodeId());

        Map<String, List<InOutValueDto>> byNode = encryptedStore.collectOutputsByNode("exec-io");
        assertTrue(byNode.containsKey("up"));
    }

    /**
     * The dispatch chain mints the NEXT hop's row at the END of a node ({@code RefOnlyVarBuilder} →
     * {@code snapshot(ctx)} while {@code currentNode} is still the node that just finished), and
     * the next node then saves ITS outputs onto that row. So the row labelled "A" carries B's
     * outputs, the row labelled "B" carries C's, and so on.
     */
    @Test
    void chain_rows_attribute_outputs_to_their_producer_not_to_the_hop_that_minted_the_row() {
        // Seed row (label A) → A saves onto it; A's success path mints B's row (label A);
        // B saves onto it; B's success path mints C's row (label B); C saves onto it.
        ProcessContextRef seed = encryptedStore.snapshot(ctxWithPhi("exec-chain", "A"), null);
        encryptedStore.saveOutputs(seed, new ArrayList<>(List.of(
                TransformFixtures.input("A", "out", "B", "in", TransformFixtures.map("files", "from-A")))));
        ProcessContextRef rowForB = encryptedStore.snapshot(ctxWithPhi("exec-chain", "A"), null);
        encryptedStore.saveOutputs(rowForB, new ArrayList<>(List.of(
                TransformFixtures.input("B", "out", "C", "in", TransformFixtures.map("issue", "from-B")))));
        ProcessContextRef rowForC = encryptedStore.snapshot(ctxWithPhi("exec-chain", "B"), null);
        encryptedStore.saveOutputs(rowForC, new ArrayList<>(List.of(
                TransformFixtures.input("C", "out", "D", "in", TransformFixtures.map("x", "from-C")),
                // A self-loop wire C→C: the only legitimate way a node's own output can target it.
                TransformFixtures.input("C", "out", "C", "in", TransformFixtures.map("x", "loop")))));

        Map<String, List<InOutValueDto>> byNode = encryptedStore.collectOutputsByNode("exec-chain");
        assertEquals(Set.of("A", "B", "C"), byNode.keySet(), "one bucket per PRODUCER");
        assertEquals(1, byNode.get("A").size(), "A's bucket must never carry B's payload");
        assertEquals("from-A", ((Map<?, ?>) byNode.get("A").get(0).getValue()).get("files"));
        assertEquals("from-B", ((Map<?, ?>) byNode.get("B").get(0).getValue()).get("issue"));
        assertEquals(2, byNode.get("C").size());

        Map<String, List<InOutValueDto>> onlyA = encryptedStore.collectOutputsByNode("exec-chain", Set.of("A"));
        assertEquals(Set.of("A"), onlyA.keySet(), "the filtered scan must key on the producer too");
        assertEquals("from-A", ((Map<?, ?>) onlyA.get("A").get(0).getValue()).get("files"));

        // Inbound resolution: C receives B's wire, never its own fan-out (which sits on a row
        // labelled "B"); D receives C's wire even though it sits on a row labelled "B".
        ProcessContextRef exec = ProcessContextRef.builder().executionId("exec-chain").build();
        List<InOutValueDto> intoC = encryptedStore.loadInputsTargetingNode(exec, "C");
        assertEquals(1, intoC.size(), "C's own self-loop output is not an input of C");
        assertEquals("B", intoC.get(0).getNodeId());
        List<InOutValueDto> intoD = encryptedStore.loadInputsTargetingNode(exec, "D");
        assertEquals(1, intoD.size());
        assertEquals("C", intoD.get(0).getNodeId());
    }

    /**
     * The reported break: a Duckbot start node wired to a Docling node on a DEDICATED files wire
     * (no msg attribute) must still be readable by Docling's loadInputsTargetingNode. The bridge
     * publishes {files:[…]} with targetNodeId=docling; the store must persist and return it.
     */
    @Test
    void files_only_wire_to_docling_is_persisted_and_loaded() {
        // Bridge output for a files-only wire to the Docling node (mirrors the live run's
        // "output->66e9767e…[files]" line): no msg attribute, targetNodeId = the Docling node.
        ProcessContextRef startRef = encryptedStore.snapshot(ctxWithPhi("exec-doc", "duck-start"), null);
        InOutValueDto filesOnly = new InOutValueDto();
        filesOnly.setNodeId("duck-start");
        filesOnly.setPortId("port-out");
        filesOnly.setPortValue("output");
        filesOnly.setTargetNodeId("docling-node");
        filesOnly.setTargetPortId("port-in");
        filesOnly.setTargetPortValue("input");
        filesOnly.setValue(Map.of("files", List.of(Map.of(
                "kind", "image", "mime_type", "image/gif", "source", "sq-file",
                "name", "a.gif", "fileId", "sq-file:a1"))));
        encryptedStore.saveOutputs(startRef, new ArrayList<>(List.of(filesOnly)));

        ProcessContextRef exec = ProcessContextRef.builder().executionId("exec-doc").build();
        List<InOutValueDto> inbound = encryptedStore.loadInputsTargetingNode(exec, "docling-node");
        assertEquals(1, inbound.size(),
                "the files-only wire must reach Docling — the live run saw inbound=none");
        assertEquals("docling-node", inbound.get(0).getTargetNodeId());
        assertTrue(inbound.get(0).getValue() instanceof Map<?, ?> m && m.containsKey("files"),
                "the files payload must survive the seal/open round-trip");
    }

    /** Disabled mode (no key configured): rows persist as plaintext (backend parity), still round-trips. */
    @Test
    void disabled_mode_persists_plaintext_and_round_trips() {
        ExecutionContextCrypto disabled = new ExecutionContextCrypto(mapper, keyProvider("", "disabled"));
        disabled.init();
        assertFalse(disabled.enabled());
        assertNull(disabled.sealAlgorithm(), "no algorithm claim when encryption is disabled");
        H2ExecutionContextStore plainStore = new H2ExecutionContextStore(repository, mapper, disabled);

        ProcessContextRef ref = plainStore.snapshot(ctxWithPhi("exec-plain", "cur"), null);
        ExecutionContextEntity row = repository.findByNodeRefId(ref.getCurrentNodeRefId()).orElseThrow();
        assertFalse(row.getInputsJson().contains(ExecutionContextCrypto.MARKER), "no seal marker when disabled");
        assertTrue(row.getInputsJson().contains(PHI), "plaintext pass-through when encryption is disabled");

        ExecutionContext loaded = plainStore.loadByRef(ref).orElseThrow();
        assertEquals(Map.of("patient", PHI), loaded.getNextNodeInputList().get(0).getValue());
    }

    /**
     * Invariant 16: the sealed blob is what crosses a cross-env channel (ciphertext, no PHI),
     * while the same-env loopback var map stays REF-only ({@code studioquackRef} alone). Both
     * hold with the same store/crypto — proving the invariants survive either path.
     */
    @Test
    void invariant16_cross_env_sealed_and_loopback_ref_only() {
        ExecutionContext ctx = ctxWithPhi("exec-16", "cur");

        // Cross-env: what would traverse the remote channel is the sealed row + the tiny ref.
        RefOnlyVarBuilder builder = new RefOnlyVarBuilder(encryptedStore);
        Map<String, Object> engineVars = builder.toEngineVariables(ctx, null);
        assertEquals(1, engineVars.size());
        assertTrue(engineVars.containsKey(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME));
        RefOnlyVariableGuard.assertRefOnly(engineVars); // loopback keeps REF-only

        ProcessContextRef ref = (ProcessContextRef) engineVars.get(ProcessContextRef.CONTEXT_REF_VARIABLE_NAME);
        ExecutionContextEntity row = repository.findByNodeRefId(ref.getCurrentNodeRefId()).orElseThrow();
        assertTrue(row.getContextBlobJson().contains(ExecutionContextCrypto.MARKER));
        assertFalse(row.getContextBlobJson().contains(PHI), "cross-env payload is sealed, never plaintext PHI");
    }

    /**
     * Regression for the live SANDBOX_FAILED: an agentic prompt carrying node-library search
     * results amplifies across the blob past Jackson's stock 20 MB string cap, and rehydrate died
     * with "execution_context JSON parse (map) failed". The store must round-trip past that cap,
     * while the mapper it was handed keeps the stock constraint (the raised cap stays local).
     */
    @Test
    void context_blob_round_trips_strings_beyond_jacksons_stock_20mb_cap() {
        String big = "x".repeat(21_000_000);
        ExecutionContext ctx = ctxWithPhi("exec-big", "cur");
        ctx.getCurrentNode().getInputList().add(TransformFixtures.input(
                "up", "result", "cur", "prompt", big));

        ProcessContextRef ref = encryptedStore.snapshot(ctx, null);
        ExecutionContext loaded = encryptedStore.loadByRef(ref).orElseThrow();
        assertTrue(loaded.getNextNodeInputList().stream().anyMatch(dto -> big.equals(dto.getValue())),
                "a >20 MB string must survive the seal/store/open round-trip");

        assertThrows(com.fasterxml.jackson.core.exc.StreamConstraintsException.class,
                () -> mapper.readValue('"' + big + '"', String.class),
                "the handed-in mapper must keep Jackson's stock 20 MB cap");
    }

    /**
     * The replay seed lookup: the run's FIRST row — minted at start-run — wins; a later dispatch
     * row of the same run (e.g. the failed node's attempt a held run points at) must never be taken
     * for the seed. A run the store has never seen has no identifiable seed.
     */
    @Test
    void seedRefOf_returns_the_runs_first_row() {
        ProcessContextRef seedRef = encryptedStore.snapshot(ctxWithPhi("exec-seed", "trigger"), null);
        ProcessContextRef laterRef = encryptedStore.snapshot(ctxWithPhi("exec-seed", "nodeB"), null);

        Optional<ProcessContextRef> found = encryptedStore.seedRefOf("exec-seed");

        assertTrue(found.isPresent());
        assertEquals(seedRef.getCurrentNodeRefId(), found.get().getCurrentNodeRefId(),
                "the seed is the run's first execution_context row");
        assertNotEquals(laterRef.getCurrentNodeRefId(), found.get().getCurrentNodeRefId());
        assertEquals("exec-seed", found.get().getExecutionId());
        assertEquals("env-1", found.get().getEnvId());
        assertTrue(encryptedStore.seedRefOf("exec-unknown").isEmpty());
        assertTrue(encryptedStore.seedRefOf(" ").isEmpty(), "blank execution id → no seed");
    }
}
