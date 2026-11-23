package com.studioquack.sqs.execution.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import com.studioquack.sqs.execution.crypto.OsKeychainContextKeyProvider;
import com.studioquack.sqs.secrets.os.InMemorySecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hardening test — end-to-end proof that with a key sourced from the real {@link
 * OsKeychainContextKeyProvider} (backed here by the in-memory fake so it runs on any host), the
 * {@code execution_context} store writes ciphertext at rest and round-trips. Asserts the two
 * guarantees for at-rest encryption.
 */
@DataJpaTest(properties = {"spring.flyway.enabled=false"})
class AtRestContextEncryptionRoundTripTest {

    private static final String PHI = "PATIENT-SECRET-ssn-123-45-6789";

    @Autowired
    private ExecutionContextRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();
    private final OsSecretManagerProperties props = new OsSecretManagerProperties();

    /** Builds a provider→crypto→store stack over a shared in-memory OS manager (the key survives in it). */
    private H2ExecutionContextStore storeOver(InMemorySecretManager mgr) {
        OsKeychainContextKeyProvider provider =
                new OsKeychainContextKeyProvider(mgr, props);
        ExecutionContextCrypto crypto = new ExecutionContextCrypto(mapper, provider);
        crypto.init();
        assertTrue(crypto.enabled(), "crypto is ENABLED once the provider mints a key from the OS store");
        return new H2ExecutionContextStore(repository, mapper, crypto);
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
        ctx.setWorkflowId("wf-atrest");
        ctx.setEnvId("env-atrest");
        ctx.setUserId("user-atrest");
        ctx.setCurrentNodeId(nodeId);
        ctx.setCurrentNode(node);
        return ctx;
    }

    @Test
    void columns_are_ciphertext_at_rest_and_decrypt_round_trips() {
        InMemorySecretManager mgr = new InMemorySecretManager();
        H2ExecutionContextStore store = storeOver(mgr);

        ProcessContextRef ref = store.snapshot(ctxWithPhi("exec-atrest", "cur"), 7L);

        // (1) Raw embedded database columns are the sealed envelope, and the PHI never appears as plaintext.
        ExecutionContextEntity row = repository.findByNodeRefId(ref.getCurrentNodeRefId()).orElseThrow();
        assertTrue(row.getContextBlobJson().contains(ExecutionContextCrypto.MARKER),
                "context blob column carries the {\"$sqenc\":…} ciphertext marker");
        assertTrue(row.getInputsJson().contains(ExecutionContextCrypto.MARKER),
                "inputs column carries the ciphertext marker");
        assertFalse(row.getContextBlobJson().contains(PHI), "PHI is NOT plaintext in the blob column");
        assertFalse(row.getInputsJson().contains(PHI), "PHI is NOT plaintext in the inputs column");
        assertFalse(row.getContextBlobJson().contains("123-45-6789"), "no PHI SSN token in the blob column");

        // (2) The decrypt path returns the payload intact.
        ExecutionContext loaded = store.loadByRef(ref).orElseThrow();
        assertEquals("wf-atrest", loaded.getWorkflowId());
        assertEquals(1, loaded.getNextNodeInputList().size());
        assertEquals(Map.of("patient", PHI), loaded.getNextNodeInputList().get(0).getValue(),
                "decrypted PHI round-trips intact");
    }

    @Test
    void a_restart_with_the_persisted_key_still_opens_sealed_rows() {
        InMemorySecretManager mgr = new InMemorySecretManager();
        ProcessContextRef ref = storeOver(mgr).snapshot(ctxWithPhi("exec-restart", "cur"), null);

        // Simulate a daemon restart: a brand-new provider/crypto/store over the SAME OS store must
        // read back the SAME key and successfully decrypt the previously sealed row.
        H2ExecutionContextStore afterRestart = storeOver(mgr);
        ExecutionContext loaded = afterRestart.loadByRef(ref).orElseThrow();
        assertEquals(Map.of("patient", PHI), loaded.getNextNodeInputList().get(0).getValue(),
                "the persisted OS-managed key decrypts rows sealed before the restart");
    }

    @Test
    void ref_only_addressing_survives_sealing() {
        InMemorySecretManager mgr = new InMemorySecretManager();
        H2ExecutionContextStore store = storeOver(mgr);

        ProcessContextRef upRef = store.snapshot(ctxWithPhi("exec-refonly", "up"), null);
        InOutValueDto out = TransformFixtures.input("up", "result", "cur", "prompt",
                TransformFixtures.map("answer", PHI));
        out.setPortId("p1");
        out.setTargetPortId("tp1");
        store.saveOutputs(upRef, new ArrayList<>(List.of(out)));

        // Sealed at rest…
        ExecutionContextEntity row = repository.findByNodeRefId(upRef.getCurrentNodeRefId()).orElseThrow();
        assertTrue(row.getOutputsJson().contains(ExecutionContextCrypto.MARKER), "outputs sealed at rest");
        assertFalse(row.getOutputsJson().contains(PHI), "no plaintext PHI in the outputs column");

        // …yet the REF-ONLY collection (addressed only by executionId) still resolves downstream wires.
        ProcessContextRef execRef = ProcessContextRef.builder().executionId("exec-refonly").build();
        List<InOutValueDto> inbound = store.loadInputsTargetingNode(execRef, "cur");
        assertEquals(1, inbound.size(), "the downstream node's inbound wire is recovered via the ref");
        assertEquals(Map.of("answer", PHI), inbound.get(0).getValue(), "decrypted downstream value intact");
    }
}
