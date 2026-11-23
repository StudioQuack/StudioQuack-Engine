package com.studioquack.sqs.execution.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.crypto.ContextKeyProvider;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Operator input-override store round-trip against a real embedded database datasource. */
@DataJpaTest(properties = {"spring.flyway.enabled=false"})
class OperatorInputOverrideStoreH2Test {

    /** Distinctive PHI marker so we can assert it never appears as plaintext in a sealed row. */
    private static final String PHI = "PATIENT-SECRET-override-7c2b";
    /** 32-byte AES-256 key (Base64). */
    private static final String KEY_B64 = Base64.getEncoder().encodeToString(new byte[32]);

    @Autowired
    private OperatorInputOverrideRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();
    private OperatorInputOverrideStore store;

    @BeforeEach
    void setUp() {
        ExecutionContextCrypto crypto = new ExecutionContextCrypto(mapper, keyProvider(KEY_B64));
        crypto.init();
        store = new OperatorInputOverrideStore(repository, mapper, crypto);
    }

    private ContextKeyProvider keyProvider(String keyB64) {
        return new ContextKeyProvider() {
            @Override
            public String contextEncryptionKeyBase64() {
                return keyB64;
            }

            @Override
            public String source() {
                return "test";
            }
        };
    }

    private static InOutValueDto edited(String portValue, Object value) {
        return TransformFixtures.input("W", portValue, "X", "in", value);
    }

    @Test
    void override_round_trips_sealed_and_never_stores_plaintext_phi() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", TransformFixtures.map("patient", PHI))));

        Optional<List<InOutValueDto>> loaded = store.loadOverride("run-1", "X");
        assertTrue(loaded.isPresent());
        assertEquals(1, loaded.get().size());
        assertEquals(Map.of("patient", PHI), loaded.get().get(0).getValue(),
                "the sealed override must open back to the operator's exact edit");

        OperatorInputOverrideEntity row = repository
                .findById(new OperatorInputOverrideKey("run-1", "X")).orElseThrow();
        assertTrue(row.getInputsJson().contains(ExecutionContextCrypto.MARKER), "override is sealed at rest");
        assertFalse(row.getInputsJson().contains(PHI), "PHI must NOT be plaintext in the override column");
    }

    @Test
    void latest_override_wins_on_the_same_run_node_key() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", "first")));
        store.saveOverride("run-1", "X", List.of(edited("prompt", "second"), edited("extra", "added")));

        Optional<List<InOutValueDto>> loaded = store.loadOverride("run-1", "X");
        assertTrue(loaded.isPresent());
        assertEquals(2, loaded.get().size(), "the second apply-inputs replaces the first, row count stays 1");
        assertEquals("second", loaded.get().get(0).getValue());
        assertEquals(1, repository.count(), "one row per (run, node) — latest override wins");
    }

    @Test
    void empty_override_is_present_and_distinct_from_absent() {
        assertTrue(store.loadOverride("run-1", "X").isEmpty(), "no override stored → empty Optional");

        store.saveOverride("run-1", "X", List.of());

        Optional<List<InOutValueDto>> loaded = store.loadOverride("run-1", "X");
        assertTrue(loaded.isPresent(), "an explicit clear must survive as a PRESENT override");
        assertTrue(loaded.get().isEmpty(), "the cleared inbound list loads back empty");
    }

    @Test
    void overrides_are_isolated_per_run_and_node() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", "for-X")));

        assertTrue(store.loadOverride("run-1", "Y").isEmpty(), "another node of the same run");
        assertTrue(store.loadOverride("run-2", "X").isEmpty(), "same node of another run");
        assertEquals("for-X", store.loadOverride("run-1", "X").orElseThrow().get(0).getValue());
    }

    @Test
    void output_override_round_trips_sealed_and_never_stores_plaintext_phi() {
        store.saveOutputsOverride("run-1", "X", List.of(edited("result", TransformFixtures.map("patient", PHI))));

        Optional<List<InOutValueDto>> loaded = store.loadOutputOverride("run-1", "X");
        assertTrue(loaded.isPresent());
        assertEquals(Map.of("patient", PHI), loaded.get().get(0).getValue(),
                "the sealed output override must open back to the operator's exact edit");

        OperatorInputOverrideEntity row = repository
                .findById(new OperatorInputOverrideKey("run-1", "X")).orElseThrow();
        assertTrue(row.getOutputsJson().contains(ExecutionContextCrypto.MARKER),
                "output override is sealed at rest");
        assertFalse(row.getOutputsJson().contains(PHI),
                "PHI must NOT be plaintext in the outputs column");
    }

    @Test
    void latest_wins_merges_at_the_row_level_across_lanes() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", "in-1")));
        store.saveOutputsOverride("run-1", "X", List.of(edited("result", "out-1")));

        // A call carrying one lane keeps the other's stored value (merge at the ROW level).
        store.saveOverride("run-1", "X", List.of(edited("prompt", "in-2")));

        assertEquals("in-2", store.loadOverride("run-1", "X").orElseThrow().get(0).getValue());
        assertEquals("out-1", store.loadOutputOverride("run-1", "X").orElseThrow().get(0).getValue(),
                "rewriting inputs must not clear the stored outputs");
        assertEquals(1, repository.count(), "still ONE row per (run, node)");

        store.saveOutputsOverride("run-1", "X", List.of(edited("result", "out-2")));

        assertEquals("in-2", store.loadOverride("run-1", "X").orElseThrow().get(0).getValue(),
                "rewriting outputs must not clear the stored inputs");
        assertEquals("out-2", store.loadOutputOverride("run-1", "X").orElseThrow().get(0).getValue());
        assertEquals(1, repository.count());
    }

    @Test
    void outputs_only_row_does_not_read_as_a_cleared_inputs_override() {
        store.saveOutputsOverride("run-1", "X", List.of(edited("result", "out")));

        assertTrue(store.loadOverride("run-1", "X").isEmpty(),
                "an outputs-only row must NOT masquerade as 'operator cleared the inputs'");
        assertTrue(store.loadOutputOverride("run-1", "X").isPresent());
    }

    @Test
    void inputs_only_row_has_no_output_override() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", "in")));

        assertTrue(store.loadOutputOverride("run-1", "X").isEmpty());
        assertTrue(store.loadOverride("run-1", "X").isPresent());
    }

    @Test
    void output_lanes_are_sealed_under_distinct_aad() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", "in")));
        store.saveOutputsOverride("run-1", "X", List.of(edited("result", "out")));

        // Splice the outputs blob into the inputs column: the inputs AAD (run|node) must refuse
        // the outputs envelope (sealed under run|node|out) — tamper fails, never silently opens.
        OperatorInputOverrideEntity row = repository
                .findById(new OperatorInputOverrideKey("run-1", "X")).orElseThrow();
        String outputsJson = row.getOutputsJson();
        row.setInputsJson(outputsJson);
        repository.save(row);

        org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                () -> store.loadOverride("run-1", "X"),
                "an outputs blob spliced into the inputs lane must fail authentication");
    }

    @Test
    void retention_delete_drops_every_override_of_a_run_batch() {
        store.saveOverride("run-1", "X", List.of(edited("prompt", "a")));
        store.saveOverride("run-1", "Y", List.of(edited("prompt", "b")));
        store.saveOverride("run-2", "X", List.of(edited("prompt", "c")));

        long deleted = store.deleteOverridesForRuns(List.of("run-1"));

        assertEquals(2, deleted);
        assertTrue(store.loadOverride("run-1", "X").isEmpty());
        assertTrue(store.loadOverride("run-1", "Y").isEmpty());
        assertTrue(store.loadOverride("run-2", "X").isPresent(), "other runs stay untouched");
    }
}
