package com.studioquack.sqs.channel;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.DaemonBoundSealedStore;
import com.studioquack.sqs.secrets.DaemonFileCustody;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.InMemorySecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import com.studioquack.sqs.secrets.os.UnavailableSecretManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Daemon-bound custody for {@link KemIkmStore} ({@code <dataDir>/kem/ikm.b64}): sealed round-trip,
 * wrong-engine AAD refusal, legacy plaintext passthrough + migrate-on-load, and the
 * custody-unavailable plaintext fallback that keeps the daemon bootable.
 */
class KemIkmStoreSealedTest {

    private static final String ENV_A = "env-kem-a";
    private static final String ENV_B = "env-kem-b";

    @TempDir
    Path dataDir;

    @Test
    void sealed_roundTrip_writesMagicAndReadsBack() throws Exception {
        DaemonFileCustody custody = custody(new InMemorySecretManager(), ENV_A);
        KemIkmStore store = KemIkmStore.under(dataDir, custody.kemIkm(dataDir));
        byte[] ikm = ikm();

        store.save(ikm);

        byte[] wire = Files.readAllBytes(store.path());
        assertTrue(startsWithMagic(wire), "sealed file must carry the SQDB1 magic");
        assertFalse(new String(wire, java.nio.charset.StandardCharsets.ISO_8859_1)
                        .contains(java.util.Base64.getUrlEncoder().withoutPadding()
                                .encodeToString(ikm)),
                "the base64url IKM must not be recoverable from the sealed file");

        Optional<byte[]> loaded = store.load();
        assertTrue(loaded.isPresent());
        assertArrayEquals(ikm, loaded.get());

        // Idempotent: opening an already-sealed blob must not re-seal/rewrite it.
        store.load();
        assertArrayEquals(wire, Files.readAllBytes(store.path()),
                "a second load of a sealed blob must not rewrite the file");
    }

    @Test
    void sealed_underEnvA_refusesOpenUnderEnvB() throws Exception {
        InMemorySecretManager osSecrets = new InMemorySecretManager();
        SqsChannelProperties channelA = channel(ENV_A);
        SecretStoreNamespace namespaceA = new SecretStoreNamespace(new OsSecretManagerProperties(), channelA);

        KemIkmStore storeA = KemIkmStore.under(dataDir,
                new DaemonBoundSealedStore(osSecrets, namespaceA, ENV_A,
                        DaemonFileCustody.PURPOSE_KEM_IKM, ikmFile()));
        storeA.save(ikm());

        // Same host key namespace, different envId in the AAD → GCM auth failure → empty.
        KemIkmStore storeB = KemIkmStore.under(dataDir,
                new DaemonBoundSealedStore(osSecrets, namespaceA, ENV_B,
                        DaemonFileCustody.PURPOSE_KEM_IKM, ikmFile()));
        assertTrue(storeB.load().isEmpty(), "a blob sealed under env A must refuse to open under env B");
    }

    @Test
    void legacyPlaintext_loadsAndMigratesToSealed() throws Exception {
        // Pre-upgrade daemon: plaintext base64url written by the legacy (custody-less) store.
        KemIkmStore legacy = KemIkmStore.under(dataDir);
        byte[] ikm = ikm();
        legacy.save(ikm);
        assertFalse(startsWithMagic(Files.readAllBytes(legacy.path())), "precondition: plaintext file");

        DaemonFileCustody custody = custody(new InMemorySecretManager(), ENV_A);
        KemIkmStore sealedAware = KemIkmStore.under(dataDir, custody.kemIkm(dataDir));

        Optional<byte[]> loaded = sealedAware.load();
        assertTrue(loaded.isPresent(), "legacy plaintext must still load (in-place upgrade)");
        assertArrayEquals(ikm, loaded.get());
        assertTrue(startsWithMagic(Files.readAllBytes(legacy.path())),
                "the load should have migrated the plaintext file to a sealed blob");

        // Second load: now genuinely sealed, still opens.
        Optional<byte[]> reloaded = sealedAware.load();
        assertTrue(reloaded.isPresent());
        assertArrayEquals(ikm, reloaded.get());
    }

    @Test
    void custodyUnavailable_fallsBackToPlaintext_andStillWorks() throws Exception {
        DaemonFileCustody noCustody = custody(new UnavailableSecretManager(), ENV_A);
        KemIkmStore degraded = KemIkmStore.under(dataDir, noCustody.kemIkm(dataDir));
        byte[] ikm = ikm();

        degraded.save(ikm);

        assertFalse(startsWithMagic(Files.readAllBytes(degraded.path())),
                "no OS custody → the previous plaintext 0600 behaviour, never a boot blocker");
        Optional<byte[]> loaded = degraded.load();
        assertTrue(loaded.isPresent());
        assertArrayEquals(ikm, loaded.get());
        // …and the legacy reader (no custody wired, e.g. an old binary) reads the same file.
        assertArrayEquals(ikm, KemIkmStore.under(dataDir).load().orElseThrow());
    }

    // --- helpers ------------------------------------------------------------------------------

    private static DaemonFileCustody custody(com.studioquack.sqs.secrets.os.OsSecretManager os,
                                             String envId) {
        SqsChannelProperties channel = channel(envId);
        return new DaemonFileCustody(os, new SecretStoreNamespace(new OsSecretManagerProperties(), channel),
                channel);
    }

    private static SqsChannelProperties channel(String envId) {
        SqsChannelProperties channel = new SqsChannelProperties();
        channel.setEnvId(envId);
        return channel;
    }

    private Path ikmFile() {
        return dataDir.resolve("kem").resolve("ikm.b64");
    }

    private static byte[] ikm() {
        byte[] ikm = new byte[32];
        for (int i = 0; i < ikm.length; i++) {
            ikm[i] = (byte) (0x40 + i);
        }
        return ikm;
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
