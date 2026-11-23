package com.studioquack.sqs.entitlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.execution.crypto.ContextKeyProvider;
import jakarta.annotation.PostConstruct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prerequisite — sealed entitlement snapshot custody: restart survival, fail-closed corrupt load,
 * no plaintext on disk, and boot {@code @PostConstruct} restore.
 */
class EntitlementSnapshotStoreTest {

    /** Fixed AES-256 key (32 raw bytes) — same shape ContextKeyProvider yields in production. */
    private static final String FIXED_KEY_B64 = Base64.getEncoder().encodeToString(
            "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));

    /** Distinctive fake hash substring used only to prove the sealed file is not plaintext. */
    private static final String PLAINTEXT_MARKER = "$2a$10$KNOWNHASHMARKER";

    @TempDir
    Path tempDir;

    private ObjectMapper mapper;
    private ContextKeyProvider keyProvider;
    private SqsChannelProperties channelProperties;
    private EntitlementSnapshotStore store;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        keyProvider = () -> FIXED_KEY_B64;
        channelProperties = new SqsChannelProperties();
        channelProperties.setDataDir(tempDir.toString());
        store = new EntitlementSnapshotStore(mapper, keyProvider, channelProperties);
    }

    @Test
    void snapshotSurvivesSimulatedRestart() {
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        String plaintextKey = "sqk_test_ingress_key_for_h5";
        String realHash = encoder.encode(plaintextKey);

        EnvironmentEntitlements snapshot = new EnvironmentEntitlements(
                7, false, true, false, false, List.of(realHash));
        store.save(snapshot);
        assertTrue(Files.isRegularFile(store.path()), "sealed file written under dataDir/entitlements/");

        // Simulated restart: fresh store + holder on the same dataDir / key.
        EntitlementSnapshotStore storeAfterRestart =
                new EntitlementSnapshotStore(mapper, keyProvider, channelProperties);
        EntitlementsHolder holderAfterRestart =
                new EntitlementsHolder(1, 30, storeAfterRestart);

        assertTrue(holderAfterRestart.restoreFromStore(), "boot restore must find the sealed snapshot");
        assertTrue(holderAfterRestart.webhookRelay());
        assertEquals(1, holderAfterRestart.apiKeyHashes().size());
        assertEquals(realHash, holderAfterRestart.apiKeyHashes().get(0));

        // Prove ingress auth still works against the restored hash (no server round-trip).
        assertTrue(encoder.matches(plaintextKey, holderAfterRestart.apiKeyHashes().get(0)),
                "BCryptPasswordEncoder.matches must succeed against the restored hash");
    }

    @Test
    void newPushReplacesDisk() {
        EnvironmentEntitlements a = new EnvironmentEntitlements(
                1, false, true, false, false, List.of("hash-A"));
        EnvironmentEntitlements b = new EnvironmentEntitlements(
                14, true, true, true, true, List.of("hash-B1", "hash-B2"));

        store.save(a);
        store.save(b);

        Optional<EnvironmentEntitlements> loaded = store.load();
        assertTrue(loaded.isPresent());
        EnvironmentEntitlements snap = loaded.get();
        assertEquals(b.logRetentionDays(), snap.logRetentionDays());
        assertEquals(b.monitoringEnabled(), snap.monitoringEnabled());
        assertEquals(b.webhookRelay(), snap.webhookRelay());
        assertEquals(b.doclingEnabled(), snap.doclingEnabled());
        assertEquals(b.webhookMessageEventsEnabled(), snap.webhookMessageEventsEnabled());
        assertEquals(b.apiKeyHashes(), snap.apiKeyHashes());
    }

    @Test
    void corruptFile_failsClosedToEmpty_withWarn() throws Exception {
        Files.createDirectories(store.path().getParent());
        Files.write(store.path(), "this-is-not-aes-gcm-ciphertext".getBytes(StandardCharsets.UTF_8));

        Optional<EnvironmentEntitlements> loaded = store.load();
        assertTrue(loaded.isEmpty(), "corrupt sealed file must fail closed to empty (never restore garbage)");
    }

    @Test
    void sealedFile_isNotPlaintext() throws Exception {
        EnvironmentEntitlements snapshot = new EnvironmentEntitlements(
                7, false, true, false, false, List.of(PLAINTEXT_MARKER + "paddingxx"));
        store.save(snapshot);
        assertTrue(Files.isRegularFile(store.path()));

        byte[] onDisk = Files.readAllBytes(store.path());
        String asUtf8 = new String(onDisk, StandardCharsets.UTF_8);
        String asIso = new String(onDisk, StandardCharsets.ISO_8859_1);
        assertFalse(asUtf8.contains(PLAINTEXT_MARKER),
                "sealed file must not contain the hash marker as UTF-8 plaintext");
        assertFalse(asIso.contains(PLAINTEXT_MARKER),
                "sealed file must not contain the hash marker as ISO-8859-1 plaintext");
    }

    @Test
    void bootRestore_runsViaPostConstruct() throws Exception {
        Method restore = EntitlementsHolder.class.getDeclaredMethod("restorePersistedSnapshot");
        assertNotNull(restore.getAnnotation(PostConstruct.class),
                "restorePersistedSnapshot must be @PostConstruct so Spring runs it at boot");

        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        String plaintextKey = "sqk_boot_restore_ordering";
        String hash = encoder.encode(plaintextKey);
        store.save(new EnvironmentEntitlements(
                7, false, true, false, false, List.of(hash)));

        // Ordering contract: restoreFromStore before reading apiKeyHashes yields the hashes.
        EntitlementsHolder holder = new EntitlementsHolder(1, 30, store);
        assertTrue(holder.apiKeyHashes().isEmpty(), "pre-restore seed has no hashes");
        assertTrue(holder.restoreFromStore());
        assertEquals(List.of(hash), holder.apiKeyHashes());
        assertTrue(encoder.matches(plaintextKey, holder.apiKeyHashes().get(0)));
    }

    @Test
    void apply_persistsViaInjectedStore() {
        EntitlementsHolder holder = new EntitlementsHolder(1, 30, store);
        EnvironmentEntitlements pushed = new EnvironmentEntitlements(
                7, false, true, false, false, List.of("persist-roundtrip-hash"));

        holder.apply(pushed);

        Optional<EnvironmentEntitlements> fromDisk = store.load();
        assertTrue(fromDisk.isPresent());
        assertEquals(pushed.apiKeyHashes(), fromDisk.get().apiKeyHashes());
        assertTrue(fromDisk.get().webhookRelay());

        // Fresh holder restores what apply sealed.
        EntitlementsHolder restarted = new EntitlementsHolder(1, 30, store);
        assertTrue(restarted.restoreFromStore());
        assertEquals(pushed.apiKeyHashes(), restarted.apiKeyHashes());
    }

    @Test
    void custodyUnavailable_saveIsNoOp_loadEmpty() {
        ContextKeyProvider noKey = () -> null;
        EntitlementSnapshotStore noCustody =
                new EntitlementSnapshotStore(mapper, noKey, channelProperties);

        assertFalse(noCustody.custodyAvailable());
        noCustody.save(new EnvironmentEntitlements(
                7, false, true, false, false, List.of("should-not-land")));
        assertFalse(Files.exists(noCustody.path()), "no key → save must not write a file");
        assertTrue(noCustody.load().isEmpty());
    }

    // ---- freshness stamp (envelope format) ----------------------------------------------------

    @Test
    void stampedSave_writesEnvelope_andLoadSealedReadsTheStamp() {
        EnvironmentEntitlements snapshot = new EnvironmentEntitlements(
                7, false, true, true, false, List.of());
        java.time.Instant stamp = java.time.Instant.parse("2026-08-26T01:49:10Z");

        store.save(snapshot, stamp);

        Optional<EntitlementSnapshotStore.SealedSnapshot> loaded = store.loadSealed();
        assertTrue(loaded.isPresent());
        assertEquals(stamp, loaded.get().appliedAt(), "the envelope carries the push stamp");
        assertTrue(loaded.get().snapshot().doclingEnabled());
        assertTrue(store.load().isPresent(), "the legacy load() view keeps working on an envelope");
        assertTrue(store.load().get().doclingEnabled());
    }

    @Test
    void legacyBareFile_loadsWithNullStamp() {
        // Pre-guard daemons wrote the bare snapshot (no envelope). It must still load —
        // stamped null, so the freshness guard stays open until the first stamped push.
        store.save(new EnvironmentEntitlements(
                7, false, true, false, false, List.of()));

        Optional<EntitlementSnapshotStore.SealedSnapshot> loaded = store.loadSealed();

        assertTrue(loaded.isPresent());
        assertTrue(loaded.get().appliedAt() == null, "a pre-guard file carries no stamp");
        assertEquals(7, loaded.get().snapshot().logRetentionDays());
    }

    @Test
    void restoredStamp_stillRejectsStalePush_afterRestart() {
        // The incident shape across a restart: the sealed stamp must keep rejecting a
        // replayed pre-restart snapshot — the in-memory guard alone resets at boot.
        EntitlementsHolder first = new EntitlementsHolder(1, 30, store);
        assertTrue(first.applyIfFresh(new EnvironmentEntitlements(
                        30, false, false, true, false, List.of()),
                java.time.Instant.parse("2026-08-26T01:49:10Z")));

        EntitlementsHolder afterRestart = new EntitlementsHolder(1, 30,
                new EntitlementSnapshotStore(mapper, keyProvider, channelProperties));
        assertTrue(afterRestart.restoreFromStore());

        assertFalse(afterRestart.applyIfFresh(new EnvironmentEntitlements(
                        1, false, false, false, false, List.of()),
                java.time.Instant.parse("2026-08-26T01:48:00Z")),
                "a replayed pre-restart snapshot must still be rejected");
        assertTrue(afterRestart.doclingEnabled(), "the restored fresh state survives the replay");
    }

    @Test
    void oldBeOrOldDiskJson_withFixOpsEnabled_deserializesAndIgnoresUnknownKey() throws Exception {
        // Production-incident guard: a an older snapshot or sealed snapshot.aesgcm still emits
        // fixOpsEnabled. ignoreUnknown swallows it; remaining current fields must land.
        String json = "{\"logRetentionDays\":7,\"fixOpsEnabled\":true,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":true,\"doclingEnabled\":false,"
                + "\"webhookMessageEventsEnabled\":false,\"logPropagation\":false,"
                + "\"apiKeyHashes\":[]}";
        EnvironmentEntitlements snap = mapper.readValue(json, EnvironmentEntitlements.class);
        assertEquals(7, snap.logRetentionDays());
        assertTrue(snap.webhookRelay());
        assertFalse(snap.monitoringEnabled());
    }

    @Test
    void newBeJson_withoutFixOpsEnabled_deserializes() throws Exception {
        String json = "{\"logRetentionDays\":7,\"monitoringEnabled\":false,"
                + "\"webhookRelay\":true,\"doclingEnabled\":false,"
                + "\"webhookMessageEventsEnabled\":false,\"logPropagation\":false,"
                + "\"apiKeyHashes\":[]}";
        EnvironmentEntitlements snap = mapper.readValue(json, EnvironmentEntitlements.class);
        assertEquals(7, snap.logRetentionDays());
        assertTrue(snap.webhookRelay());
        assertFalse(snap.monitoringEnabled());
    }
}
