package com.studioquack.sqs.sandbox.redactor;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The redactor scrubs the type-aware PII / secret patterns before any log is persisted ("redact
 * before persist"). Confirms each pattern fires and that a structured tree is scrubbed recursively.
 */
class RedactorTest {

    private final Redactor redactor = new Redactor();

    @Test
    void scrubs_structural_secrets_jwt_and_keys() {
        String jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abcDEF_ghi-JKL";
        assertEquals("[redacted-jwt]", redactor.redact(jwt));

        assertTrue(redactor.redact("key AKIAIOSFODNN7EXAMPLE here").contains("[redacted-aws-key]"));
        assertTrue(redactor.redact("Authorization: Bearer abcdef1234567890xyz")
                .contains("Bearer [redacted-token]"));

        String pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEuff...\n-----END RSA PRIVATE KEY-----";
        assertEquals("[redacted-private-key]", redactor.redact(pem));
    }

    @Test
    void scrubs_pii_email_ip_and_turkish_identifiers() {
        assertEquals("[redacted-email]", redactor.redact("user@example.com"));
        assertTrue(redactor.redact("host 10.0.0.5 down").contains("[redacted-ip]"));
        // TCKN (11-digit) + credit-card are KVKK identifiers.
        assertTrue(redactor.redact("tckn 12345678901 x").contains("[redacted-tckn]"));
        assertTrue(redactor.redact("card 4111 1111 1111 1111 x").contains("[redacted-card]"));
    }

    @Test
    void scrubs_helper_and_llm_env_tokens() {
        assertTrue(redactor.redact("SQ_HELPER_BEARER=h_abcdef123456").contains("SQ_HELPER_BEARER=[redacted-token]"));
        assertTrue(redactor.redact("SQVM_LLM_JOB_TOKEN=abcdef123456xyz")
                .contains("SQVM_LLM_JOB_TOKEN=[redacted-token]"));
    }

    @Test
    void redacts_tree_recursively_without_mutating_input() {
        Map<String, Object> tree = Map.of(
                "note", "reach me at ceo@corp.com",
                "nested", List.of("token Bearer abcdef1234567890", "clean"));
        Object scrubbed = redactor.redactTree(tree);
        String flat = String.valueOf(scrubbed);
        assertTrue(flat.contains("[redacted-email]"));
        assertTrue(flat.contains("Bearer [redacted-token]"));
        assertFalse(flat.contains("ceo@corp.com"), "original PII gone from the scrubbed tree");
        // input untouched
        assertEquals("reach me at ceo@corp.com", tree.get("note"));
    }

    @Test
    void resolved_literal_is_redacted_regardless_of_length() {
        assertEquals("pin=[redacted-secret]", redactor.redact("pin=12", List.of("12")));
        assertEquals("[redacted-secret]ok", redactor.redact("xok", List.of("x")));
    }

    @Test
    void empty_and_null_short_circuit() {
        assertEquals("", redactor.redact(""));
        assertEquals(null, redactor.redact(null));
        assertEquals("plain text 123", redactor.redact("plain text 123"));
    }
}
