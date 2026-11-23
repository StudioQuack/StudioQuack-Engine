package com.studioquack.sqs.channel;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.DaemonBoundSealedStore;
import com.studioquack.sqs.secrets.DaemonFileCustody;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.InMemorySecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import com.studioquack.sqs.secrets.os.UnavailableSecretManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Daemon-bound custody for the {@link MtlsIdentity} private key
 * ({@code <dataDir>/mtls/identity.key}): sealed round-trip with a stable keypair, wrong-engine
 * AAD refusal, legacy plaintext passthrough + migrate-on-load, and the custody-unavailable
 * plaintext fallback. The public key / leaf / CA stay plaintext and are untouched by this.
 */
class MtlsIdentitySealedTest {

    private static final String ENV_A = "env-mtls-a";
    private static final String ENV_B = "env-mtls-b";

    @TempDir
    Path dataDir;

    @Test
    void sealed_roundTrip_keypairStableAndFileSealed() throws Exception {
        DaemonFileCustody custody = custody(new InMemorySecretManager(), ENV_A);

        String first = MtlsIdentity.loadOrCreate(dataDir, custody.mtlsIdentityKey(dataDir))
                .csrPublicKeyPem();

        byte[] wire = Files.readAllBytes(keyPath());
        assertTrue(startsWithMagic(wire), "identity.key must be daemon-bound sealed");
        assertFalse(new String(wire, StandardCharsets.ISO_8859_1).contains("BEGIN PRIVATE KEY"),
                "the PKCS8 PEM must not be recoverable from the sealed file");
        // The public half stays plaintext (it is not secret).
        assertTrue(Files.readString(dataDir.resolve("mtls").resolve("identity.pub"))
                .contains("BEGIN PUBLIC KEY"));

        String second = MtlsIdentity.loadOrCreate(dataDir, custody.mtlsIdentityKey(dataDir))
                .csrPublicKeyPem();
        assertEquals(first, second, "the sealed keypair reloads byte-identical across restarts");
        assertArrayEquals(wire, Files.readAllBytes(keyPath()),
                "reloading an already-sealed key must not re-seal/rewrite the file");
    }

    @Test
    void sealed_underEnvA_refusesOpenUnderEnvB() throws Exception {
        InMemorySecretManager osSecrets = new InMemorySecretManager();
        SqsChannelProperties channelA = channel(ENV_A);
        SecretStoreNamespace namespaceA = new SecretStoreNamespace(new OsSecretManagerProperties(), channelA);
        DaemonBoundSealedStore storeA = new DaemonBoundSealedStore(osSecrets, namespaceA, ENV_A,
                DaemonFileCustody.PURPOSE_MTLS_IDENTITY_KEY, keyPath());

        MtlsIdentity.loadOrCreate(dataDir, storeA).csrPublicKeyPem();
        assertTrue(startsWithMagic(Files.readAllBytes(keyPath())), "precondition: sealed on disk");

        // Same host key namespace, different envId in the AAD → open fails. Materialisation is
        // deferred, so the refusal surfaces on first use of the keypair.
        MtlsIdentity foreign = MtlsIdentity.loadOrCreate(dataDir,
                new DaemonBoundSealedStore(osSecrets, namespaceA, ENV_B,
                        DaemonFileCustody.PURPOSE_MTLS_IDENTITY_KEY, keyPath()));
        assertThrows(IllegalStateException.class, foreign::csrPublicKeyPem,
                "a key sealed under env A must refuse to open under env B");
    }

    @Test
    void legacyPlaintext_loadsAndMigratesToSealed() throws Exception {
        // Pre-upgrade daemon: plaintext PKCS8 PEM written by the custody-less path.
        String first = MtlsIdentity.loadOrCreate(dataDir).csrPublicKeyPem();
        assertTrue(Files.readString(keyPath()).contains("BEGIN PRIVATE KEY"),
                "precondition: plaintext PEM on disk");

        DaemonFileCustody custody = custody(new InMemorySecretManager(), ENV_A);
        String reloaded = MtlsIdentity.loadOrCreate(dataDir, custody.mtlsIdentityKey(dataDir))
                .csrPublicKeyPem();

        assertEquals(first, reloaded, "legacy plaintext key must still load (in-place upgrade)");
        assertTrue(startsWithMagic(Files.readAllBytes(keyPath())),
                "the load should have migrated the plaintext key to a sealed blob");
        assertEquals(first, MtlsIdentity.loadOrCreate(dataDir, custody.mtlsIdentityKey(dataDir))
                .csrPublicKeyPem(), "the sealed key reloads identically after migration");
    }

    @Test
    void custodyUnavailable_persistsPlaintextFallback_andStillWorks() throws Exception {
        DaemonFileCustody noCustody = custody(new UnavailableSecretManager(), ENV_A);

        String first = MtlsIdentity.loadOrCreate(dataDir, noCustody.mtlsIdentityKey(dataDir))
                .csrPublicKeyPem();

        assertTrue(Files.readString(keyPath()).contains("BEGIN PRIVATE KEY"),
                "no OS custody → the previous plaintext 0600 behaviour, never a boot blocker");
        // A legacy (custody-less) reader — e.g. an older binary — loads the same file.
        String legacyReload = MtlsIdentity.loadOrCreate(dataDir).csrPublicKeyPem();
        assertEquals(first, legacyReload);
    }

    // --- helpers ------------------------------------------------------------------------------

    private Path keyPath() {
        return dataDir.resolve("mtls").resolve("identity.key");
    }

    private static DaemonFileCustody custody(OsSecretManager os, String envId) {
        SqsChannelProperties channel = channel(envId);
        return new DaemonFileCustody(os, new SecretStoreNamespace(new OsSecretManagerProperties(), channel),
                channel);
    }

    private static SqsChannelProperties channel(String envId) {
        SqsChannelProperties channel = new SqsChannelProperties();
        channel.setEnvId(envId);
        return channel;
    }

    private static boolean startsWithMagic(byte[] wire) {
        byte[] magic = DaemonBoundSealedStore.MAGIC;
        if (wire.length < magic.length) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if (wire[i] != magic[i]) {
                return false;
            }
        }
        return true;
    }
}
