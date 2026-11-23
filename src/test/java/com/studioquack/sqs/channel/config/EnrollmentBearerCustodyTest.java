package com.studioquack.sqs.channel.config;

import com.studioquack.sqs.secrets.DaemonBoundSealedStore;
import com.studioquack.sqs.secrets.DaemonFileCustody;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.InMemorySecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import com.studioquack.sqs.secrets.os.UnavailableSecretManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link EnrollmentBearerCustody}: the installer-written bearer line is sealed into
 * {@code <dataDir>/enrollment/bearer.bin} and scrubbed from {@code enrollment.properties} (every
 * other line preserved); a later boot with no bearer property loads it back from the sealed blob;
 * custody-unavailable leaves the plaintext file untouched; a blob sealed under another env never
 * opens here.
 */
class EnrollmentBearerCustodyTest {

    private static final String ENV_A = "env-bearer-a";
    private static final String ENV_B = "env-bearer-b";
    private static final String BEARER = "sqb-test-bearer-0123456789abcdef";
    private static final String NEW_BEARER = "sqb-test-bearer-reinstall-fedcba9876543210";

    @TempDir
    Path dataDir;

    @TempDir
    Path configDir;

    @Test
    void migrate_sealsAndScrubs_keepingOtherLines() throws Exception {
        Path enrollment = enrollmentProps(
                "# installer-written enrolment config",
                "sqs.channel.env-id=" + ENV_A,
                "sqs.channel.bearer=" + BEARER,
                "sqs.backend.base-url=https://be.example.test",
                "sqs.channel.tls-ca-file=/etc/sq/edge.pem");
        SqsChannelProperties props = props(ENV_A, BEARER);
        DaemonFileCustody custody = custody(new InMemorySecretManager(), props);

        new EnrollmentBearerCustody(props, custody, new MockEnvironment(), List.of(enrollment))
                .migrateOrLoad();

        // The bearer line is gone; every other line survived verbatim.
        String rewritten = Files.readString(enrollment);
        assertFalse(rewritten.contains(BEARER), "the bearer must not remain in the file");
        for (String line : List.of("# installer-written enrolment config",
                "sqs.channel.env-id=" + ENV_A,
                "sqs.backend.base-url=https://be.example.test",
                "sqs.channel.tls-ca-file=/etc/sq/edge.pem")) {
            assertTrue(rewritten.contains(line), "non-secret line must stay plaintext: " + line);
        }
        // The sealed blob holds it, bound to this env, and is not readable as text.
        DaemonBoundSealedStore store = custody.enrollmentBearer(dataDir);
        byte[] wire = Files.readAllBytes(store.path());
        assertTrue(startsWithMagic(wire));
        assertFalse(new String(wire, StandardCharsets.ISO_8859_1).contains(BEARER));
        assertEquals(BEARER, new String(store.load().orElseThrow(), StandardCharsets.UTF_8));

        // Idempotent: a second run with the (in-memory) property still set changes nothing.
        byte[] before = Files.readAllBytes(store.path());
        new EnrollmentBearerCustody(props, custody, new MockEnvironment(), List.of(enrollment))
                .migrateOrLoad();
        assertEquals(-1, java.util.Arrays.mismatch(before, Files.readAllBytes(store.path())),
                "same bearer must not be re-sealed");
    }

    @Test
    void load_whenPropertyAbsent_installsFromSealedBlob() throws Exception {
        SqsChannelProperties writerProps = props(ENV_A, BEARER);
        InMemorySecretManager osSecrets = new InMemorySecretManager();
        custody(osSecrets, writerProps).enrollmentBearer(dataDir)
                .save(BEARER.getBytes(StandardCharsets.UTF_8));

        // Next boot: the installer file no longer carries the bearer (scrubbed) → property blank.
        SqsChannelProperties readerProps = props(ENV_A, "");
        DaemonFileCustody readerCustody = custody(osSecrets, readerProps);
        new EnrollmentBearerCustody(readerProps, readerCustody, new MockEnvironment(), List.of())
                .migrateOrLoad();

        assertEquals(BEARER, readerProps.getInstallerBearer(),
                "the bearer must be restored from daemon-bound sealed custody");
        assertEquals(BEARER, readerProps.getBearer());
    }

    @Test
    void custodyUnavailable_leavesPlaintextUntouched_andStillBoots() throws Exception {
        Path enrollment = enrollmentProps(
                "sqs.channel.env-id=" + ENV_A,
                "sqs.channel.bearer=" + BEARER);
        String original = Files.readString(enrollment);
        SqsChannelProperties props = props(ENV_A, BEARER);
        DaemonFileCustody noCustody = custody(new UnavailableSecretManager(), props);

        new EnrollmentBearerCustody(props, noCustody, new MockEnvironment(), List.of(enrollment))
                .migrateOrLoad();

        assertEquals(original, Files.readString(enrollment),
                "no OS custody → enrollment.properties keeps its plaintext bearer (fail safe)");
        assertFalse(Files.exists(dataDir.resolve("enrollment").resolve("bearer.bin")),
                "no sealed blob may be written without custody");
        assertEquals(BEARER, props.getBearer(), "the daemon still enrols on the plaintext bearer");
    }

    @Test
    void sealedUnderEnvA_loadsNothingUnderEnvB() throws Exception {
        InMemorySecretManager osSecrets = new InMemorySecretManager();
        custody(osSecrets, props(ENV_A, "")).enrollmentBearer(dataDir)
                .save(BEARER.getBytes(StandardCharsets.UTF_8));

        // Same host, but a DIFFERENT engine (env B): the env-scoped namespace + AAD refuse it.
        SqsChannelProperties foreignProps = props(ENV_B, "");
        DaemonFileCustody foreignCustody = custody(osSecrets, foreignProps);
        new EnrollmentBearerCustody(foreignProps, foreignCustody, new MockEnvironment(), List.of())
                .migrateOrLoad();

        assertTrue(foreignProps.getInstallerBearer().isBlank(),
                "a bearer sealed under env A must never open under env B");
    }

    /**
     * Reinstall on the same computer: the installer keeps the enrolment directory and writes a NEW
     * bearer. The old kemIkm was deactivated by StudioQuack, so it must go — the engine enrols
     * again on the new bearer instead of signing with a dead key.
     */
    @Test
    void newInstallerBearer_discardsThePreviousKemIkm() throws Exception {
        InMemorySecretManager osSecrets = new InMemorySecretManager();
        SqsChannelProperties first = props(ENV_A, BEARER);
        DaemonFileCustody custody = custody(osSecrets, first);
        new EnrollmentBearerCustody(first, custody, new MockEnvironment(),
                List.of(enrollmentProps("sqs.channel.env-id=" + ENV_A, "sqs.channel.bearer=" + BEARER)))
                .migrateOrLoad();
        com.studioquack.sqs.channel.KemIkmStore ikm =
                com.studioquack.sqs.channel.KemIkmStore.under(dataDir, custody.kemIkm());
        ikm.save(new byte[]{1, 2, 3, 4});

        SqsChannelProperties reinstalled = props(ENV_A, NEW_BEARER);
        new EnrollmentBearerCustody(reinstalled, custody(osSecrets, reinstalled), new MockEnvironment(),
                List.of(enrollmentProps("sqs.channel.env-id=" + ENV_A, "sqs.channel.bearer=" + NEW_BEARER)))
                .migrateOrLoad();

        assertTrue(ikm.load().isEmpty(), "the deactivated kemIkm must be discarded");
        assertEquals(NEW_BEARER, new String(custody.enrollmentBearer(dataDir).load().orElseThrow(),
                StandardCharsets.UTF_8), "the new bearer is the one sealed for the next enrolment");
    }

    /** An ordinary restart or same-bundle upgrade must keep the engine's identity. */
    @Test
    void sameInstallerBearer_keepsTheKemIkm() throws Exception {
        SqsChannelProperties props = props(ENV_A, BEARER);
        DaemonFileCustody custody = custody(new InMemorySecretManager(), props);
        Path enrollment = enrollmentProps("sqs.channel.env-id=" + ENV_A, "sqs.channel.bearer=" + BEARER);
        new EnrollmentBearerCustody(props, custody, new MockEnvironment(), List.of(enrollment)).migrateOrLoad();
        com.studioquack.sqs.channel.KemIkmStore ikm =
                com.studioquack.sqs.channel.KemIkmStore.under(dataDir, custody.kemIkm());
        ikm.save(new byte[]{1, 2, 3, 4});

        enrollmentProps("sqs.channel.env-id=" + ENV_A, "sqs.channel.bearer=" + BEARER);
        new EnrollmentBearerCustody(props(ENV_A, BEARER), custody, new MockEnvironment(), List.of(enrollment))
                .migrateOrLoad();

        assertTrue(ikm.load().isPresent(), "the same bearer is not a reinstall");
    }

    /** Without OS custody the bearer stays plaintext; a fingerprint file still detects a new one. */
    @Test
    void custodyUnavailable_newInstallerBearer_discardsThePreviousKemIkm() throws Exception {
        SqsChannelProperties first = props(ENV_A, BEARER);
        new EnrollmentBearerCustody(first, custody(new UnavailableSecretManager(), first), new MockEnvironment(),
                List.of(enrollmentProps("sqs.channel.env-id=" + ENV_A, "sqs.channel.bearer=" + BEARER)))
                .migrateOrLoad();
        com.studioquack.sqs.channel.KemIkmStore ikm = com.studioquack.sqs.channel.KemIkmStore.under(dataDir);
        ikm.save(new byte[]{1, 2, 3, 4});

        SqsChannelProperties sameAgain = props(ENV_A, BEARER);
        new EnrollmentBearerCustody(sameAgain, custody(new UnavailableSecretManager(), sameAgain),
                new MockEnvironment(), List.of()).migrateOrLoad();
        assertTrue(ikm.load().isPresent(), "a restart on the same plaintext bearer keeps the identity");

        SqsChannelProperties reinstalled = props(ENV_A, NEW_BEARER);
        new EnrollmentBearerCustody(reinstalled, custody(new UnavailableSecretManager(), reinstalled),
                new MockEnvironment(), List.of()).migrateOrLoad();
        assertTrue(ikm.load().isEmpty(), "a new plaintext bearer discards the deactivated kemIkm");
    }

    // --- helpers ------------------------------------------------------------------------------

    private Path enrollmentProps(String... lines) throws Exception {
        Path file = configDir.resolve("enrollment.properties");
        Files.write(file, (String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private SqsChannelProperties props(String envId, String bearer) {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId(envId);
        props.setBearer(bearer);
        props.setDataDir(dataDir.toString());
        return props;
    }

    private static DaemonFileCustody custody(OsSecretManager os, SqsChannelProperties props) {
        return new DaemonFileCustody(os, new SecretStoreNamespace(new OsSecretManagerProperties(), props),
                props);
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
