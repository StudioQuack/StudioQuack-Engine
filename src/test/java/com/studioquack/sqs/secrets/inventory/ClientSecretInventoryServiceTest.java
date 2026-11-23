package com.studioquack.sqs.secrets.inventory;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The client secret-ownership inventory + purge, including the headline SECURITY regression:
 * install → commit K1 → uninstall(purge) → re-enroll → resolving the old secret ref must MISS.
 */
class ClientSecretInventoryServiceTest {

    private OsSecretManagerProperties props;
    private SqsChannelProperties channel;
    private FakeSecrets secrets;
    private Map<String, ClientOwnedSecretEntity> db;
    private ClientSecretInventoryService inventory;
    private String service;

    @BeforeEach
    void setUp() {
        props = new OsSecretManagerProperties();
        props.setServicePrefix("com.studioquack.sqs.test");
        // Blank envId → the legacy host-shared namespace, matching the pre-scoping coordinates
        // the existing cases assert against.
        channel = new SqsChannelProperties();
        secrets = new FakeSecrets();

        // Mockito-backed repository over an in-memory map (only the methods the service uses).
        ClientOwnedSecretRepository repo = mock(ClientOwnedSecretRepository.class);
        db = new LinkedHashMap<>();
        when(repo.findById(anyString())).thenAnswer(i -> Optional.ofNullable(db.get(i.getArgument(0))));
        when(repo.save(any(ClientOwnedSecretEntity.class))).thenAnswer(i -> {
            ClientOwnedSecretEntity e = i.getArgument(0);
            db.put(e.getSecretId(), e);
            return e;
        });
        when(repo.findAll()).thenAnswer(i -> new ArrayList<>(db.values()));
        when(repo.existsById(anyString())).thenAnswer(i -> db.containsKey(i.getArgument(0)));
        doAnswer(i -> {
            db.clear();
            return null;
        }).when(repo).deleteAll();
        doAnswer(i -> {
            db.remove(i.getArgument(0));
            return null;
        }).when(repo).deleteById(anyString());
        repoRef = repo;

        inventory = newInventory(channel);
        service = "com.studioquack.sqs.test.secret";
    }

    private ClientSecretInventoryService newInventory(SqsChannelProperties ch) {
        return new ClientSecretInventoryService(
                repo(), secrets, new SecretStoreNamespace(props, ch));
    }

    /** The same in-memory repository instance across inventory rebuilds (mirrors one real DB). */
    private ClientOwnedSecretRepository repo() {
        return repoRef;
    }

    private ClientOwnedSecretRepository repoRef;

    @Test
    void recordCommit_upsertsAndOnlyRaisesMaxVersion() {
        String id = "sec_aaaaaaaaaaaaaaaaaaaaaaaa";
        inventory.recordCommit(id, 1);
        assertEquals(1, db.get(id).getMaxVersion());
        inventory.recordCommit(id, 3);
        assertEquals(3, db.get(id).getMaxVersion());
        // An older rotate must never lower the recorded max.
        inventory.recordCommit(id, 2);
        assertEquals(3, db.get(id).getMaxVersion());
    }

    @Test
    void remove_dropsTheOwnershipRowAndIsIdempotent() {
        String id = "sec_aaaaaaaaaaaaaaaaaaaaaaaa";
        inventory.recordCommit(id, 2);
        assertTrue(db.containsKey(id));
        inventory.remove(id);
        assertFalse(db.containsKey(id));
        
        // Second call must not throw (idempotency for retries)
        inventory.remove(id);
        assertFalse(db.containsKey(id));
    }

    @Test
    void deleteAllVersions_tracked_touchesOnlyRecordedVersions() {
        String id = "sec_444444444444444444444444";
        secrets.put(service, id + ":v1", "v1");
        secrets.put(service, id + ":v2", "v2");
        inventory.recordCommit(id, 2);

        assertEquals(2, inventory.deleteAllVersions(id));
        assertTrue(secrets.get(service, id + ":v1").isEmpty());
        assertTrue(secrets.get(service, id + ":v2").isEmpty());
        assertFalse(db.containsKey(id));
        assertEquals(2, secrets.deleteCalls, "one OS-store call per recorded version, never a blind range");
    }

    @Test
    void deleteAllVersions_untracked_stopsAtFirstMissingVersion() {
        String id = "sec_555555555555555555555555";
        secrets.put(service, id + ":v1", "v1");
        secrets.put(service, id + ":v2", "v2");

        assertEquals(2, inventory.deleteAllVersions(id));
        assertTrue(secrets.get(service, id + ":v2").isEmpty());
        assertEquals(3, secrets.deleteCalls);

        // A retried delete costs a single OS-store call.
        secrets.deleteCalls = 0;
        assertEquals(0, inventory.deleteAllVersions(id));
        assertEquals(1, secrets.deleteCalls);
    }

    @Test
    void purgeAllOwned_deletesEveryOwnedVersionButNeverOthers() {
        String owned = "sec_11111111111111111111111a";
        secrets.put(service, owned + ":v1", "k1-v1");
        secrets.put(service, owned + ":v2", "k1-v2");
        inventory.recordCommit(owned, 2);

        // An UNOWNED entry in the SAME namespace (another app / never recorded here).
        String foreign = "sec_2222222222222222222222bb";
        secrets.put(service, foreign + ":v1", "other-app");

        int purged = inventory.purgeAllOwned();

        assertEquals(1, purged);
        assertTrue(secrets.get(service, owned + ":v1").isEmpty());
        assertTrue(secrets.get(service, owned + ":v2").isEmpty());
        // Never touch entries we did not commit.
        assertEquals("other-app", secrets.get(service, foreign + ":v1").orElse(null));
        assertTrue(db.isEmpty());
    }

    @Test
    void securityRegression_purgedSecretIsUnresolvableAfterReenroll() {
        // install → commit K1 (v1)
        String k1 = "sec_dddddddddddddddddddddddd";
        secrets.put(service, k1 + ":v1", "TOP-SECRET");
        inventory.recordCommit(k1, 1);
        assertEquals("TOP-SECRET", secrets.get(service, k1 + ":v1").orElse(null));

        // uninstall(purge)
        inventory.purgeAllOwned();

        // re-enroll (fresh inventory, SAME OS store) → resolving the OLD ref MUST miss.
        assertTrue(secrets.get(service, k1 + ":v1").isEmpty(),
                "a re-enrolled engine must never resolve the purged secret");
    }

    @Test
    void purgeAllOwned_sweepsBothScopedAndLegacyNamespaces() {
        // An ENROLLED engine (env-1): entries written post-upgrade live under the scoped
        // service; a pre-upgrade leftover may still sit under the legacy shared service.
        SqsChannelProperties enrolled = new SqsChannelProperties();
        enrolled.setEnvId("env-1");
        ClientSecretInventoryService scopedInventory = newInventory(enrolled);
        String scoped = "com.studioquack.sqs.test.secret.env-1";

        String id = "sec_333333333333333333333333cc";
        secrets.put(scoped, id + ":v1", "scoped-value");
        secrets.put(service, id + ":v1", "legacy-leftover");
        scopedInventory.recordCommit(id, 1);

        int purged = scopedInventory.purgeAllOwned();

        assertEquals(1, purged);
        assertTrue(secrets.get(scoped, id + ":v1").isEmpty(), "the scoped entry is purged");
        assertTrue(secrets.get(service, id + ":v1").isEmpty(),
                "the legacy-namespace leftover is purged too — uninstall must not leave it behind");
        assertTrue(db.isEmpty());
    }

    /** In-memory OsSecretManager keyed by (service, account); mirrors SecretCommandHandlersTest. */
    static class FakeSecrets implements OsSecretManager {
        private final Map<String, String> values = new ConcurrentHashMap<>();
        int deleteCalls;

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
            values.put(service + "\0" + account, secret);
        }

        @Override
        public boolean delete(String service, String account) {
            deleteCalls++;
            return values.remove(service + "\0" + account) != null;
        }
    }
}
