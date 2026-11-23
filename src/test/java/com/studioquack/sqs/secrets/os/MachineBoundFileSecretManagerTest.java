package com.studioquack.sqs.secrets.os;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract + persistence tests for {@link MachineBoundFileSecretManager} — the reboot-persistent
 * headless-Linux custody store. Uses the package-private test seam (fixed key, temp store file)
 * so the suite never touches the real host machine-id.
 */
class MachineBoundFileSecretManagerTest {

    private static final SecureRandom RANDOM = new SecureRandom();

    @TempDir
    Path dir;

    private SecretKeySpec key;
    private Path storeFile;

    @BeforeEach
    void setUp() {
        byte[] raw = new byte[32];
        RANDOM.nextBytes(raw);
        key = new SecretKeySpec(raw, "AES");
        storeFile = dir.resolve("secrets/machine-bound-store.properties");
    }

    private MachineBoundFileSecretManager mgr() {
        return new MachineBoundFileSecretManager(key, storeFile);
    }

    @Test
    void round_trips_put_get_delete() {
        MachineBoundFileSecretManager m = mgr();
        assertTrue(m.available());
        assertEquals("machine-file", m.name());

        assertTrue(m.get("svc", "acct").isEmpty(), "no entry before put");
        String secret = "s3cr3t::with spaces\nand=newlines\tand-tabs";
        m.put("svc", "acct", secret);
        assertEquals(Optional.of(secret), m.get("svc", "acct"));

        m.put("svc", "acct", "rotated");
        assertEquals(Optional.of("rotated"), m.get("svc", "acct"), "put replaces in place");

        assertTrue(m.delete("svc", "acct"));
        assertTrue(m.get("svc", "acct").isEmpty());
        assertFalse(m.delete("svc", "acct"), "second delete is a no-op");
    }

    @Test
    void values_survive_a_manager_restart_the_whole_point() {
        // Simulates a daemon reboot: a brand-new manager instance over the same store file must
        // still open every entry — the property keyctl could not offer.
        mgr().put("svc", "a", "value-one");
        mgr().put("svc", "b", "value-two");

        MachineBoundFileSecretManager afterReboot = mgr();
        assertEquals(Optional.of("value-one"), afterReboot.get("svc", "a"));
        assertEquals(Optional.of("value-two"), afterReboot.get("svc", "b"));
    }

    @Test
    void entries_are_isolated_per_service_and_account() {
        MachineBoundFileSecretManager m = mgr();
        m.put("svc", "a", "A");
        m.put("svc", "b", "B");
        m.put("other", "a", "C");
        assertEquals(Optional.of("A"), m.get("svc", "a"));
        assertEquals(Optional.of("B"), m.get("svc", "b"));
        assertEquals(Optional.of("C"), m.get("other", "a"));
    }

    @Test
    void a_different_machine_key_opens_nothing_fail_closed() {
        mgr().put("svc", "acct", "host-secret");

        byte[] otherRaw = new byte[32];
        RANDOM.nextBytes(otherRaw);
        MachineBoundFileSecretManager clonedHost = new MachineBoundFileSecretManager(
                new SecretKeySpec(otherRaw, "AES"), storeFile);
        assertTrue(clonedHost.get("svc", "acct").isEmpty(),
                "a cloned/reimaged host (different machine key) must fail closed");
    }

    @Test
    void a_ciphertext_relocated_to_another_entry_name_refuses_to_open() throws Exception {
        MachineBoundFileSecretManager m = mgr();
        m.put("svc", "victim", "top-secret");

        // Attacker copies the sealed value onto an entry name they can trick a caller into reading.
        Properties p = new Properties();
        p.load(new ByteArrayInputStream(Files.readAllBytes(storeFile)));
        String sealed = p.getProperty("svc|victim");
        p.setProperty("svc|attacker-controlled", sealed);
        p.store(Files.newOutputStream(storeFile), null);

        assertTrue(m.get("svc", "attacker-controlled").isEmpty(),
                "per-entry AAD must bind the ciphertext to its entry name");
        assertEquals(Optional.of("top-secret"), m.get("svc", "victim"), "the original still opens");
    }

    @Test
    void corrupt_store_content_fails_closed_not_loud() throws Exception {
        Files.createDirectories(storeFile.getParent());
        Files.writeString(storeFile, "svc|acct=!!!not-base64!!!", StandardCharsets.UTF_8);
        assertTrue(mgr().get("svc", "acct").isEmpty(), "undecodable value ⇒ empty, never throws");

        Files.writeString(storeFile, "\u0000\u0001binary-garbage", StandardCharsets.UTF_8);
        assertTrue(mgr().get("svc", "acct").isEmpty(), "unparseable file ⇒ empty store");
    }

    @Test
    void store_file_is_owner_only_on_posix() throws Exception {
        mgr().put("svc", "acct", "secret");
        try {
            var perms = Files.getPosixFilePermissions(storeFile);
            assertEquals(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"), perms,
                    "store file must be 0600");
            var dirPerms = Files.getPosixFilePermissions(storeFile.getParent());
            assertEquals(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------"), dirPerms,
                    "secrets dir must be 0700");
        } catch (UnsupportedOperationException e) {
            // Non-POSIX test host — the ACL path is covered by the data-dir permissions instead.
        }
    }

    @Test
    void unavailable_without_a_key() {
        MachineBoundFileSecretManager m = new MachineBoundFileSecretManager(null, storeFile);
        assertFalse(m.available());
    }

    @Test
    void distinct_service_prefixes_derive_distinct_keys() {
        // Guards the HKDF info separation: two namespaced deployments on one host must not share
        // a key, or one deployment could open the other's store file.
        byte[] a = MachineBoundKeySource.hkdfSha256(
                "machine-id".getBytes(StandardCharsets.UTF_8),
                "salt".getBytes(StandardCharsets.UTF_8),
                "com.studioquack.sqs".getBytes(StandardCharsets.UTF_8), 32);
        byte[] b = MachineBoundKeySource.hkdfSha256(
                "machine-id".getBytes(StandardCharsets.UTF_8),
                "salt".getBytes(StandardCharsets.UTF_8),
                "com.studioquack.sqs.other".getBytes(StandardCharsets.UTF_8), 32);
        assertNotEquals(Base64.getEncoder().encodeToString(a), Base64.getEncoder().encodeToString(b));
    }
}
