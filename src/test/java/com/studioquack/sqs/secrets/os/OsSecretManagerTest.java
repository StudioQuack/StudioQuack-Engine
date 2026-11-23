package com.studioquack.sqs.secrets.os;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hardening test — the {@link OsSecretManager} abstraction and its runtime selection. */
class OsSecretManagerTest {

    /** Unique per-run so parallel/repeat runs never collide and cleanup is exact. */
    private final String service = "com.studioquack.sqs.test." + UUID.randomUUID();
    private final String account = "unit";

    @AfterEach
    void cleanupKeychain() {
        // Best-effort: remove any real keychain item this test may have created, on every OS.
        if (isMacKeychainAvailable()) {
            new MacKeychainSecretManager().delete(service, account);
        }
    }

    private boolean isMacKeychainAvailable() {
        return System.getProperty("os.name", "").toLowerCase().contains("mac")
                && CliRunner.lookPath("security") != null;
    }

    // -----------------------------------------------------------------------------------------
    // macOS real Keychain — gated, self-cleaning.
    // -----------------------------------------------------------------------------------------

    @Test
    void macos_keychain_round_trips_store_find_delete() {
        MacKeychainSecretManager mgr = new MacKeychainSecretManager();
        Assumptions.assumeTrue(isMacKeychainAvailable(),
                "not macOS or `security` absent — skipping the real-Keychain round-trip");
        assertTrue(mgr.available(), "keychain manager reports available on macOS");
        assertEquals("macos-keychain", mgr.name());

        // Absent before we write.
        assertTrue(mgr.get(service, account).isEmpty(), "no entry before store");

        // Store → find (value survives a round-trip, including chars a text CLI could mangle).
        String secret = "keychain-secret::v1 with spaces\nand-a-newline=and=equals";
        mgr.put(service, account, secret);
        assertEquals(Optional.of(secret), mgr.get(service, account), "find returns the stored value");

        // Update in place (add-generic-password -U path).
        String updated = "rotated-value-2";
        mgr.put(service, account, updated);
        assertEquals(Optional.of(updated), mgr.get(service, account), "update replaces the value");

        // Delete → absent again; second delete is idempotent-ish (false, nothing to remove).
        assertTrue(mgr.delete(service, account), "delete removes the entry");
        assertTrue(mgr.get(service, account).isEmpty(), "entry gone after delete");
        assertFalse(mgr.delete(service, account), "second delete finds nothing");
    }

    // -----------------------------------------------------------------------------------------
    // Cross-platform contract via the in-memory fake (runs everywhere).
    // -----------------------------------------------------------------------------------------

    @Test
    void in_memory_backend_round_trips_on_every_os() {
        OsSecretManager mgr = new InMemorySecretManager();
        assertTrue(mgr.available());
        assertEquals("in-memory(test)", mgr.name());

        assertTrue(mgr.get("svc", "a").isEmpty());
        mgr.put("svc", "a", "P@ss");
        assertEquals(Optional.of("P@ss"), mgr.get("svc", "a"));
        assertEquals(Optional.empty(), mgr.get("svc", "other"), "distinct account is a distinct slot");
        assertTrue(mgr.delete("svc", "a"));
        assertFalse(mgr.delete("svc", "a"));
    }

    @Test
    void unavailable_manager_is_inert_and_refuses_writes() {
        OsSecretManager mgr = new UnavailableSecretManager();
        assertFalse(mgr.available());
        assertEquals("unavailable", mgr.name());
        assertTrue(mgr.get("s", "a").isEmpty());
        assertFalse(mgr.delete("s", "a"));
        assertThrows(OsSecretException.class, () -> mgr.put("s", "a", "v"),
                "an unavailable store must refuse to persist rather than pretend");
    }

    // -----------------------------------------------------------------------------------------
    // Factory selection rules.
    // -----------------------------------------------------------------------------------------

    @Test
    void factory_disabled_by_config_yields_unavailable() {
        OsSecretManagerProperties props = new OsSecretManagerProperties();
        props.setEnabled(false);
        OsSecretManager mgr = OsSecretManagerFactory.create(props);
        assertFalse(mgr.available());
        assertEquals("unavailable", mgr.name());
    }

    @Test
    void factory_off_pin_is_unavailable() {
        OsSecretManagerProperties props = new OsSecretManagerProperties();
        props.setProvider("off");
        OsSecretManager mgr = OsSecretManagerFactory.create(props);
        assertFalse(mgr.available());
    }

    @Test
    void factory_auto_selects_a_native_store_on_macos() {
        Assumptions.assumeTrue(isMacKeychainAvailable(), "not macOS or `security` absent");
        OsSecretManager mgr = OsSecretManagerFactory.create(new OsSecretManagerProperties());
        assertTrue(mgr.available(), "auto selects the Keychain on macOS");
        assertEquals("macos-keychain", mgr.name());
    }

    @Test
    void factory_machine_file_pin_reflects_the_host_machine_id() {
        OsSecretManagerProperties props = new OsSecretManagerProperties();
        props.setProvider("machine-file");
        OsSecretManager mgr = OsSecretManagerFactory.create(props);
        assertEquals("machine-file", mgr.name(), "the pin selects the machine-bound store");
        boolean linuxWithMachineId = System.getProperty("os.name", "").toLowerCase().contains("linux")
                && java.nio.file.Files.isRegularFile(java.nio.file.Path.of("/etc/machine-id"));
        assertEquals(linuxWithMachineId, mgr.available(),
                "available exactly when a host machine identity is readable");
    }

    @Test
    void factory_auto_on_headless_linux_prefers_the_persistent_store_over_keyctl() {
        boolean headlessLinux = System.getProperty("os.name", "").toLowerCase().contains("linux")
                && CliRunner.lookPath("secret-tool") == null
                && java.nio.file.Files.isRegularFile(java.nio.file.Path.of("/etc/machine-id"));
        Assumptions.assumeTrue(headlessLinux,
                "only on a Linux host with a machine-id and no libsecret");
        OsSecretManager mgr = OsSecretManagerFactory.create(new OsSecretManagerProperties());
        assertTrue(mgr.available());
        assertEquals("machine-file", mgr.name(),
                "auto must never land on the reboot-volatile kernel keyring when a machine-id exists");
    }
}
