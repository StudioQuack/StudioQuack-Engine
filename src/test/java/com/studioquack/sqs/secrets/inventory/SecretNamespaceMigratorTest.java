package com.studioquack.sqs.secrets.inventory;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The boot migration that drains the legacy host-shared namespace into the engine-scoped one
 * (see {@link SecretNamespaceMigrator}).
 */
class SecretNamespaceMigratorTest {

    private static final String LEGACY = "com.studioquack.sqs.test.secret";
    private static final String SCOPED = "com.studioquack.sqs.test.secret.env-1";

    private FakeSecrets secrets;
    private ClientOwnedSecretRepository repo;

    @BeforeEach
    void setUp() {
        secrets = new FakeSecrets();
        repo = mock(ClientOwnedSecretRepository.class);
    }

    private SecretNamespaceMigrator migrator(String envId, List<ClientOwnedSecretEntity> rows) {
        OsSecretManagerProperties props = new OsSecretManagerProperties();
        props.setServicePrefix("com.studioquack.sqs.test");
        SqsChannelProperties channel = new SqsChannelProperties();
        channel.setEnvId(envId);
        when(repo.findAll()).thenReturn(rows);
        return new SecretNamespaceMigrator(repo, secrets, new SecretStoreNamespace(props, channel));
    }

    private static ClientOwnedSecretEntity row(String secretId, int maxVersion) {
        return new ClientOwnedSecretEntity(secretId, maxVersion, OffsetDateTime.now());
    }

    @Test
    void moves_every_owned_version_into_the_scoped_namespace_and_deletes_the_legacy_copy() {
        String id = "sec_aaaaaaaaaaaaaaaaaaaaaaaa";
        secrets.put(LEGACY, id + ":v1", "v1-plaintext");
        secrets.put(LEGACY, id + ":v2", "v2-plaintext");

        migrator("env-1", List.of(row(id, 2))).migrateLegacyNamespace();

        assertEquals("v1-plaintext", secrets.get(SCOPED, id + ":v1").orElse(null));
        assertEquals("v2-plaintext", secrets.get(SCOPED, id + ":v2").orElse(null));
        assertTrue(secrets.get(LEGACY, id + ":v1").isEmpty(), "the legacy copy is deleted after the move");
        assertTrue(secrets.get(LEGACY, id + ":v2").isEmpty());
    }

    @Test
    void unenrolled_daemon_is_a_noop() {
        String id = "sec_bbbbbbbbbbbbbbbbbbbbbbbb";
        secrets.put(LEGACY, id + ":v1", "dev-plaintext");

        migrator("", List.of(row(id, 1))).migrateLegacyNamespace();

        assertEquals("dev-plaintext", secrets.get(LEGACY, id + ":v1").orElse(null),
                "no scoped namespace exists for an unenrolled daemon — nothing moves");
    }

    @Test
    void a_failed_move_keeps_the_legacy_copy_for_the_next_boot() {
        String id = "sec_cccccccccccccccccccccccc";
        secrets.put(LEGACY, id + ":v1", "fragile");
        secrets.failPutOn(SCOPED);

        migrator("env-1", List.of(row(id, 1))).migrateLegacyNamespace();

        assertEquals("fragile", secrets.get(LEGACY, id + ":v1").orElse(null),
                "the legacy copy is only deleted after the scoped write succeeded");
    }

    /** In-memory OsSecretManager with an optional put-failure switch. */
    static class FakeSecrets implements OsSecretManager {
        private final Map<String, String> values = new ConcurrentHashMap<>();
        private String failPutService;

        void failPutOn(String service) {
            this.failPutService = service;
        }

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public Optional<String> get(String service, String account) {
            return Optional.ofNullable(values.get(service + "\0" + account));
        }

        @Override
        public void put(String service, String account, String secret) {
            if (service.equals(failPutService)) {
                throw new RuntimeException("simulated store error");
            }
            values.put(service + "\0" + account, secret);
        }

        @Override
        public boolean delete(String service, String account) {
            return values.remove(service + "\0" + account) != null;
        }
    }
}
