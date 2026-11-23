package com.studioquack.sqs.sandbox.secrets;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.InMemorySecretManager;
import com.studioquack.sqs.secrets.os.OsSecretException;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hardening test — the OS-secret-manager-backed {@link OsSecretKeyStore}. A {@code sec_…:vN} ref
 * resolves to the plaintext stored in the OS manager (exercised with the in-memory fake), and the
 * downstream custody flow is proven unchanged: the plaintext reaches only the per-job tmpfs
 * {@code.env} (unlinked on exit) and NEVER a log line.
 */
class OsSecretKeyStoreTest {

    private static final String SECRET_ID = "sec_0123456789abcdef01234567";
    private static final String MISSING_ID = "sec_ffffffffffffffffffffffff";
    private static final String KNOWN_SECRET = "resolved-DB-secret-Kp7Qx2Lm";

    private final OsSecretManagerProperties props = new OsSecretManagerProperties();

    /** Unenrolled namespace (blank envId) — the legacy host-shared service, as before. */
    private SecretStoreNamespace namespace() {
        return namespace("");
    }

    private SecretStoreNamespace namespace(String envId) {
        SqsChannelProperties channel = new SqsChannelProperties();
        channel.setEnvId(envId);
        return new SecretStoreNamespace(props, channel);
    }

    /** Seeds a manager with the known secret under the exact coordinates the keystore reads. */
    private InMemorySecretManager seededManager() {
        InMemorySecretManager mgr = new InMemorySecretManager();
        mgr.put(props.getServicePrefix() + ".secret", SECRET_ID + ":v1", KNOWN_SECRET);
        return mgr;
    }

    // -----------------------------------------------------------------------------------------
    // resolve()
    // -----------------------------------------------------------------------------------------

    @Test
    void resolves_a_ref_to_the_stored_plaintext() {
        OsSecretKeyStore store = new OsSecretKeyStore(seededManager(), namespace());
        Optional<SecretKeyStore.ResolvedSecret> got = store.resolve(new SecretRef(SECRET_ID, 1));
        assertTrue(got.isPresent(), "the stored ref resolves");
        assertEquals(KNOWN_SECRET, got.get().value(), "resolves to the stored plaintext");
    }

    @Test
    void a_missing_ref_is_empty_not_a_placeholder() {
        OsSecretKeyStore store = new OsSecretKeyStore(seededManager(), namespace());
        assertTrue(store.resolve(new SecretRef(MISSING_ID, 2)).isEmpty(), "a miss is empty");
        // Version is part of the address: v2 of a stored v1 is a different (absent) entry.
        assertTrue(store.resolve(new SecretRef(SECRET_ID, 2)).isEmpty(), "a different version is a miss");
    }

    @Test
    void an_enrolled_engine_resolves_only_its_own_scoped_namespace() {
        InMemorySecretManager mgr = new InMemorySecretManager();
        // The OLD engine's leftover, still sitting in the host-shared legacy namespace.
        mgr.put(props.getServicePrefix() + ".secret", SECRET_ID + ":v1", KNOWN_SECRET);
        // THIS engine's own entry, committed under its env-scoped namespace.
        mgr.put(props.getServicePrefix() + ".secret.env-new", SECRET_ID + ":v2", "fresh-secret");

        OsSecretKeyStore store = new OsSecretKeyStore(mgr, namespace("env-new"));

        assertTrue(store.resolve(new SecretRef(SECRET_ID, 1)).isEmpty(),
                "a reinstalled/new engine must NEVER resolve the previous engine's leftover entry");
        assertEquals("fresh-secret", store.resolve(new SecretRef(SECRET_ID, 2)).orElseThrow().value(),
                "the engine's own scoped entry still resolves");
    }

    @Test
    void a_backend_error_is_a_fail_safe_miss() {
        OsSecretManager flaky = new OsSecretManager() {
            @Override public boolean available() { return true; }
            @Override public String name() { return "flaky"; }
            @Override public Optional<String> get(String s, String a) {
                throw new OsSecretException("simulated backend error");
            }
            @Override public void put(String s, String a, String secret) { }
            @Override public boolean delete(String s, String a) { return false; }
        };
        OsSecretKeyStore store = new OsSecretKeyStore(flaky, namespace());
        assertTrue(store.resolve(new SecretRef(SECRET_ID, 1)).isEmpty(),
                "a backend error yields empty (never a leaked/placeholder value)");
    }

    // -----------------------------------------------------------------------------------------
    // End-to-end via SecretResolver: in-memory dotenv, never on host disk, never logged.
    // -----------------------------------------------------------------------------------------

    @Test
    void resolver_over_os_keystore_renders_dotenv_in_memory_and_never_logs_plaintext(@TempDir Path tmp)
            throws IOException {
        ListAppender<ILoggingEvent> logs = attachRootAppender();
        try {
            SecretResolver resolver = new SecretResolver(new OsSecretKeyStore(seededManager(), namespace()));
            Map<String, Object> tree = new LinkedHashMap<>();
            tree.put("db", Map.of("$secretRef", SECRET_ID + ":v1"));
            tree.put("miss", Map.of("$secretRef", MISSING_ID + ":v2"));

            String tmpdirBefore = System.getProperty("java.io.tmpdir");
            System.setProperty("java.io.tmpdir", tmp.toString());
            SecretResolution res;
            try {
                res = resolver.resolveToEnv(tree);
            } finally {
                System.setProperty("java.io.tmpdir", tmpdirBefore);
            }

            assertEquals(1, res.resolvedCount(), "the stored ref resolved; the miss did not");
            assertTrue(res.hasEnv(), "dotenv text was rendered");
            assertTrue(res.envContent().contains(KNOWN_SECRET),
                    "the plaintext is delivered only through the in-memory dotenv text");

            // Nothing under the temp dir holds the plaintext — the resolver writes no host file.
            assertNoResidualPlaintext(tmp);

            // The miss is logged as a structured failure, but the resolved plaintext is NEVER logged.
            assertFalse(logs.list.isEmpty(), "the resolver logged the structured miss");
            for (ILoggingEvent e : logs.list) {
                assertFalse(e.getFormattedMessage().contains(KNOWN_SECRET),
                        () -> "secret plaintext leaked into a log message: " + e.getFormattedMessage());
                if (e.getArgumentArray() != null) {
                    for (Object arg : e.getArgumentArray()) {
                        assertFalse(String.valueOf(arg).contains(KNOWN_SECRET),
                                "secret plaintext leaked into a log argument");
                    }
                }
            }
        } finally {
            detachRootAppender(logs);
        }
    }

    // ---- helpers ----------------------------------------------------------------------------

    private void assertNoResidualPlaintext(Path scratch) throws IOException {
        try (Stream<Path> walk = Files.walk(scratch)) {
            List<Path> offenders = walk.filter(Files::isRegularFile).filter(p -> {
                try {
                    return Files.readString(p).contains(KNOWN_SECRET);
                } catch (IOException e) {
                    return false;
                }
            }).toList();
            assertTrue(offenders.isEmpty(), () -> "residual plaintext left on disk: " + offenders);
        }
    }

    private ListAppender<ILoggingEvent> attachRootAppender() {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        root.addAppender(appender);
        return appender;
    }

    private void detachRootAppender(ListAppender<ILoggingEvent> appender) {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.detachAppender(appender);
        appender.stop();
    }
}
