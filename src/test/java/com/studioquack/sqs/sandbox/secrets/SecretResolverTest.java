package com.studioquack.sqs.sandbox.secrets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The secret resolver's six {@code $secretRef} placeholder forms all resolve into in-memory dotenv
 * text (never a host file), and the unresolvable legacy form surfaces a structured failure (never a
 * leaked placeholder).
 */
class SecretResolverTest {

    /** 24-hex secret ids (the {@code sec_<24hex>} shape the ref pattern requires). */
    private static final String ID1 = "sec_0123456789abcdef01234567";
    private static final String ID2 = "sec_abcdefabcdefabcdefabcdef";
    private static final String ID3 = "sec_cccccccccccccccccccccccc";
    private static final String ID4 = "sec_dddddddddddddddddddddddd";
    private static final String ID5 = "sec_eeeeeeeeeeeeeeeeeeeeeeee";

    /** Fake keystore resolving any well-formed ref to {@code PLAIN_<id>}. */
    private final SecretKeyStore keyStore = ref ->
            Optional.of(new SecretKeyStore.ResolvedSecret(null, "PLAIN_" + ref.id()));

    @Test
    void six_placeholder_forms_resolve_into_in_memory_dotenv(@TempDir Path tmp) throws IOException {
        SecretResolver resolver = new SecretResolver(keyStore);

        Map<String, Object> tree = new LinkedHashMap<>();
        // Form 1 — object wrapper.
        tree.put("form1_obj", Map.of("$secretRef", ID1 + ":v1"));
        // Form 2 — object wrapper WITH metadata siblings (type/shortCode).
        Map<String, Object> form2 = new LinkedHashMap<>();
        form2.put("$secretRef", ID2 + ":v2");
        form2.put("type", "password");
        form2.put("shortCode", "DB_PW");
        tree.put("form2_meta", form2);
        // Form 3 — compact string.
        tree.put("form3_compact", "$secretRef:" + ID3 + ":v1");
        // Form 4 — secret-shaped type + bare-ref value.
        Map<String, Object> form4 = new LinkedHashMap<>();
        form4.put("type", "api_key");
        form4.put("value", ID4 + ":v3");
        form4.put("shortCode", "API");
        tree.put("form4_bare", form4);
        // Form 5 — JSON-stringified wrapper.
        tree.put("form5_jsonstr", "{\"$secretRef\":\"" + ID5 + ":v1\"}");
        // Form 6 — legacy dotenv form → unresolvable (structured failure, no leak).
        tree.put("form6_legacy", "${SQVM_SECRET__LEGACY}");

        String tmpdirBefore = System.getProperty("java.io.tmpdir");
        System.setProperty("java.io.tmpdir", tmp.toString());
        SecretResolution res;
        try {
            res = resolver.resolveToEnv(tree);
        } finally {
            System.setProperty("java.io.tmpdir", tmpdirBefore);
        }

        assertEquals(5, res.resolvedCount(), "five well-formed forms resolved to plaintext");
        assertTrue(res.hasEnv(), "dotenv text was rendered");
        List<ResolveFailure> failures = res.failures();
        assertEquals(1, failures.size(), "only the legacy form failed");
        assertTrue(failures.get(0).ref().contains("SQVM_SECRET__LEGACY"), "failure carries the safe ref, not a value");

        String env = res.envContent();
        assertTrue(env.contains("PLAIN_" + ID1), "form1 plaintext in dotenv");
        assertTrue(env.contains("PLAIN_" + ID2), "form2 plaintext in dotenv");
        assertTrue(env.contains("PLAIN_" + ID3), "form3 plaintext in dotenv");
        assertTrue(env.contains("PLAIN_" + ID4), "form4 plaintext in dotenv");
        assertTrue(env.contains("PLAIN_" + ID5), "form5 plaintext in dotenv");
        assertFalse(env.contains("SQVM_SECRET__LEGACY"), "legacy placeholder never lands in dotenv");

        // The object-wrapper forms are collapsed to plaintext in the walked tree (in place).
        assertEquals("PLAIN_" + ID1, tree.get("form1_obj"), "form1 collapsed to plaintext in place");

        // Nothing was written under the temp dir: the resolver takes no path and creates no file.
        try (var walk = Files.walk(tmp)) {
            assertEquals(0, walk.filter(Files::isRegularFile).count(), "resolver wrote a file to the host");
        }
    }

    @Test
    void no_secrets_renders_no_dotenv() {
        SecretResolver resolver = new SecretResolver(keyStore);
        // The resolver walks + mutates the tree in place (Go parity), so a real (mutable) JSON tree.
        Map<String, Object> tree = new LinkedHashMap<>();
        tree.put("plain", "just-a-value");
        tree.put("n", 42);
        SecretResolution res = resolver.resolveToEnv(tree);
        assertEquals(0, res.resolvedCount());
        assertFalse(res.hasEnv(), "no dotenv when nothing resolves");
        assertNull(res.envContent());
    }

    @Test
    void keystore_miss_is_a_structured_failure_not_a_leaked_placeholder() {
        SecretKeyStore empty = ref -> Optional.empty();
        SecretResolver resolver = new SecretResolver(empty);
        Map<String, Object> tree = new LinkedHashMap<>();
        tree.put("k", Map.of("$secretRef", ID1 + ":v1"));
        SecretResolution res = resolver.resolveToEnv(tree);
        assertEquals(0, res.resolvedCount());
        assertEquals(1, res.failures().size(), "a keystore miss is a structured failure");
        assertFalse(res.hasEnv());
    }
}
