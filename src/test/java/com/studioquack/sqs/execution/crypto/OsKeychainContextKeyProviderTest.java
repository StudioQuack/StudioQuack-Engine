package com.studioquack.sqs.execution.crypto;

import com.studioquack.sqs.secrets.os.InMemorySecretManager;
import com.studioquack.sqs.secrets.os.OsSecretException;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import com.studioquack.sqs.secrets.os.UnavailableSecretManager;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for {@link OsKeychainContextKeyProvider} — the create-on-first-run key custody and
 * fail-safe degradation when custody is unavailable, exercised with the test-only in-memory fake.
 */
class OsKeychainContextKeyProviderTest {

    private final OsSecretManagerProperties props = new OsSecretManagerProperties();

    private OsKeychainContextKeyProvider provider(OsSecretManager mgr) {
        return new OsKeychainContextKeyProvider(mgr, props);
    }

    // -----------------------------------------------------------------------------------------
    // Create-on-first-run + persistence.
    // -----------------------------------------------------------------------------------------

    @Test
    void creates_a_valid_aes_key_on_first_run_and_persists_it() {
        InMemorySecretManager mgr = new InMemorySecretManager();
        OsKeychainContextKeyProvider p = provider(mgr);

        String key = p.contextEncryptionKeyBase64();
        assertNotNull(key, "a key is created on first run when a manager is available");
        byte[] raw = Base64.getDecoder().decode(key);
        assertEquals(32, raw.length, "created key is AES-256 (32 bytes)");
        assertTrue(p.source().startsWith("os-keychain:"), "source reflects the OS backend");

        // Persisted under the derived service/account so a later boot re-reads the same key.
        String service = props.getServicePrefix() + ".execution-context";
        assertEquals(Optional.of(key), mgr.get(service, props.getAccount()),
                "the created key is stored back in the OS secret manager");

        // A fresh provider over the SAME manager reads the SAME key (no rotation on restart).
        OsKeychainContextKeyProvider p2 = provider(mgr);
        assertEquals(key, p2.contextEncryptionKeyBase64(), "the key is stable across provider instances");
    }

    @Test
    void resolution_is_cached_so_the_store_is_touched_once() {
        // A counting manager proves get() is not called again after the first resolution.
        int[] gets = {0};
        OsSecretManager counting = new InMemorySecretManager() {
            @Override
            public Optional<String> get(String service, String account) {
                gets[0]++;
                return super.get(service, account);
            }
        };
        OsKeychainContextKeyProvider p = provider(counting);
        String first = p.contextEncryptionKeyBase64();
        String second = p.contextEncryptionKeyBase64();
        assertEquals(first, second);
        assertEquals(1, gets[0], "the OS store is read at most once per provider (cached)");
    }

    // -----------------------------------------------------------------------------------------
    // Fail-safe: unavailable custody and custody errors both degrade to disabled (never throw).
    // -----------------------------------------------------------------------------------------

    @Test
    void disabled_when_no_manager_available() {
        OsKeychainContextKeyProvider p = provider(new UnavailableSecretManager());
        assertNull(p.contextEncryptionKeyBase64(), "no custody → encryption disabled");
        assertEquals("disabled(no-os-secret-manager)", p.source());
    }

    @Test
    void custody_error_degrades_to_disabled_rather_than_crashing_boot() {
        // A manager that claims availability but fails to persist — provider must catch and disable.
        OsSecretManager flaky = new OsSecretManager() {
            @Override public boolean available() { return true; }
            @Override public String name() { return "flaky"; }
            @Override public Optional<String> get(String s, String a) { return Optional.empty(); }
            @Override public void put(String s, String a, String secret) {
                throw new OsSecretException("simulated custody failure");
            }
            @Override public boolean delete(String s, String a) { return false; }
        };
        OsKeychainContextKeyProvider p = provider(flaky);
        assertNull(p.contextEncryptionKeyBase64(), "a custody write failure disables encryption, no crash");
    }
}
