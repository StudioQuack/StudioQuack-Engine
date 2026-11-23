package com.studioquack.sqs.gate;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.secrets.SecretKeyStore;
import com.studioquack.sqs.sandbox.secrets.SecretResolution;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
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
 * SECURITY — "secrets never on disk / never logged". Drives the {@link SecretResolver} with a fake
 * {@link SecretKeyStore} that returns a known plaintext secret and proves the three custody
 * guarantees the gate requires
 */
class SecretsOffDiskTest {

    /** A 24-hex secret id (the {@code sec_<24hex>} shape the ref pattern requires). */
    private static final String SECRET_ID = "sec_0123456789abcdef01234567";
    private static final String MISSING_ID = "sec_ffffffffffffffffffffffff";
    /** The known plaintext the fake keystore returns — asserted to never touch logs / residual disk. */
    private static final String KNOWN_SECRET = "pl4intext-DB-secret-Zx9Q7bkTVX28";

    /** Fake keystore: resolves only {@link #SECRET_ID} to the known plaintext; misses everything else. */
    private final SecretKeyStore keyStore = ref -> SECRET_ID.equals(ref.id())
            ? Optional.of(new SecretKeyStore.ResolvedSecret("DB_PW", KNOWN_SECRET))
            : Optional.empty();

    // -----------------------------------------------------------------------------------------
    // #1 — plaintext lives only in memory; the daemon host's disk never sees it.
    // -----------------------------------------------------------------------------------------

    @Test
    void secret_resolves_in_memory_and_never_touches_host_disk(@TempDir Path tmp) throws IOException {
        SecretResolver resolver = new SecretResolver(keyStore);
        Map<String, Object> tree = new LinkedHashMap<>();
        tree.put("db", Map.of("$secretRef", SECRET_ID + ":v1"));

        // Point the JVM temp dir at an empty, observable directory for the duration of the resolve.
        String tmpdirBefore = System.getProperty("java.io.tmpdir");
        System.setProperty("java.io.tmpdir", tmp.toString());
        SecretResolution res;
        try {
            res = resolver.resolveToEnv(tree);
        } finally {
            System.setProperty("java.io.tmpdir", tmpdirBefore);
        }

        // The plaintext is delivered as dotenv text (piped over stdin into the container tmpfs).
        assertTrue(res.hasEnv(), "dotenv text was rendered");
        assertTrue(res.envContent().contains(KNOWN_SECRET),
                "the plaintext is delivered through the in-memory dotenv text");
        assertEquals(1, res.resolvedCount());

        // No file anywhere under the temp dir holds the plaintext — none was written at all.
        assertNoResidualPlaintext(tmp);
    }

    // -----------------------------------------------------------------------------------------
    // #2 — the secret plaintext is never written to any log during resolution.
    // -----------------------------------------------------------------------------------------

    @Test
    void secret_plaintext_is_never_written_to_logs_during_resolution() {
        ListAppender<ILoggingEvent> logs = attachRootAppender();
        try {
            SecretResolver resolver = new SecretResolver(keyStore);
            // Mix a resolvable secret (success — nothing logged), a keystore MISS and a legacy form
            // (both surface STRUCTURED failures the resolver logs at WARN) so we exercise the log path.
            Map<String, Object> tree = new LinkedHashMap<>();
            tree.put("ok", Map.of("$secretRef", SECRET_ID + ":v1"));
            tree.put("miss", Map.of("$secretRef", MISSING_ID + ":v2"));
            tree.put("legacy", "${SQVM_SECRET__LEGACY}");

            resolver.resolveToEnv(tree);

            // The failures ARE logged (so we know the log path fired) …
            assertFalse(logs.list.isEmpty(), "the resolver logged its structured failures");
            // … yet NOT ONE captured event carries the plaintext, in the message or the raw args.
            for (ILoggingEvent event : logs.list) {
                assertFalse(event.getFormattedMessage().contains(KNOWN_SECRET),
                        () -> "secret plaintext leaked into a log message: " + event.getFormattedMessage());
                if (event.getArgumentArray() != null) {
                    for (Object arg : event.getArgumentArray()) {
                        assertFalse(String.valueOf(arg).contains(KNOWN_SECRET),
                                "secret plaintext leaked into a log argument");
                    }
                }
            }
            // The failure logs carry only the SAFE ref tokens (id + version), never a value.
            String joined = logs.list.stream().map(ILoggingEvent::getFormattedMessage)
                    .reduce("", (a, b) -> a + "\n" + b);
            assertTrue(joined.contains(MISSING_ID) || joined.contains("SQVM_SECRET__LEGACY"),
                    "the structured failure logs cite the safe ref, not the value");
        } finally {
            detachRootAppender(logs);
        }
    }

    // -----------------------------------------------------------------------------------------
    // #3 — the redactor scrubs secret-shaped tokens before any log is persisted (defence-in-depth).
    // -----------------------------------------------------------------------------------------

    @Test
    void redactor_scrubs_secret_shaped_tokens_before_persist() {
        Redactor redactor = new Redactor();

        String jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZWNyZXQifQ.aGVsbG8-signature_abc";
        assertTrue(redactor.redact("token=" + jwt).contains("[redacted-jwt]"), "JWT scrubbed");
        assertFalse(redactor.redact("token=" + jwt).contains(jwt), "raw JWT gone");

        String awsKey = "AKIAIOSFODNN7EXAMPLE";
        assertTrue(redactor.redact("aws " + awsKey).contains("[redacted-aws-key]"), "AWS key scrubbed");
        assertFalse(redactor.redact("aws " + awsKey).contains(awsKey), "raw AWS key gone");

        String bearer = "Authorization: Bearer abcdef1234567890secrettoken";
        assertTrue(redactor.redact(bearer).contains("Bearer [redacted-token]"), "bearer token scrubbed");
        assertFalse(redactor.redact(bearer).contains("abcdef1234567890secrettoken"), "raw bearer gone");
    }

    // ---- helpers ----------------------------------------------------------------------------

    /** Walks the directory and fails if any regular file contains the plaintext. */
    private void assertNoResidualPlaintext(Path dir) throws IOException {
        try (Stream<Path> walk = Files.walk(dir)) {
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
