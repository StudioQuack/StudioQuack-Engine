package com.studioquack.sqs.webhook;

import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.secrets.SecretKeyStore;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DaemonWebhookKeyStoreTest {

    private Map<String, DaemonWebhookKeyEntity> db;
    private DaemonWebhookKeyRepository repo;
    private DaemonWebhookKeyStore store;

    @BeforeEach
    void setUp() {
        db = new HashMap<>();
        repo = mock(DaemonWebhookKeyRepository.class);
        when(repo.existsById(anyString())).thenAnswer(inv -> db.containsKey(inv.getArgument(0)));
        when(repo.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(db.get(inv.getArgument(0))));
        when(repo.findByKeyId(anyString())).thenAnswer(inv -> {
            String keyId = inv.getArgument(0);
            return db.values().stream().filter(e -> keyId.equals(e.getKeyId())).findFirst();
        });
        when(repo.save(any(DaemonWebhookKeyEntity.class))).thenAnswer(inv -> {
            DaemonWebhookKeyEntity e = inv.getArgument(0);
            db.put(e.getRefId(), e);
            return e;
        });
        store = new DaemonWebhookKeyStore(repo);
    }

    @Test
    void createVerifyRotateRevoke_lifecycle() {
        String refId = "whk_0123456789abcdef01234567";
        var created = store.create(refId, "Inbound Hook", false);

        assertEquals(refId, created.refId());
        assertTrue(WebhookKeyFormat.isValidKeyId(created.keyId()));
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();
        assertEquals(created.keyId(), parsed.keyId());
        assertEquals(43, parsed.secret().length());

        // Plaintext secret is NEVER persisted; only 64-char SHA-256 hex digest.
        DaemonWebhookKeyEntity saved = db.get(refId);
        assertNotNull(saved);
        assertEquals(64, saved.getSecretSha256().length());
        assertNotEquals(parsed.secret(), saved.getSecretSha256());

        // Valid secret passes URL_KEY and HEADER_KEY; KEY_ID_ONLY rejected when allowKeyIdRoute=false.
        assertTrue(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY, parsed.secret()).valid());
        assertTrue(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_HEADER_KEY, parsed.secret()).valid());
        assertFalse(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY, "wrong_secret_value_000000000000000000000000").valid());
        assertFalse(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY, null).valid());

        // Rotate atomically updates (keyId, secret) while preserving refId.
        var rotated = store.rotate(refId, created.keyId());
        assertEquals(refId, rotated.refId());
        assertNotEquals(created.rawKey(), rotated.rawKey());
        var rotatedParsed = WebhookKeyFormat.parse(rotated.rawKey()).orElseThrow();

        assertFalse(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY, parsed.secret()).valid());
        assertTrue(store.verify(refId, rotated.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY, rotatedParsed.secret()).valid());

        // Revoke wipes digest and is idempotent on repeat/absent calls.
        assertTrue(store.revoke(refId, rotated.keyId()));
        assertEquals(DaemonWebhookKeyEntity.STATUS_REVOKED, db.get(refId).getStatus());
        assertNull(db.get(refId).getSecretSha256());
        assertFalse(store.verify(refId, rotated.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY, rotatedParsed.secret()).valid());
        assertTrue(store.revoke(refId, rotated.keyId()));
        assertTrue(store.revoke("whk_nonexistent000000000000", null));
        assertThrows(IllegalStateException.class, () -> store.rotate(refId, rotated.keyId()));
    }

    @Test
    void allowKeyIdRouteTrue_permitsKeyIdOnlyAuthMode() {
        String refId = "whk_fedcba9876543210fedcba98";
        var created = store.create(refId, "Header Delegated Hook", true);

        var ok = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY, null);
        assertTrue(ok.valid());
        assertTrue(ok.allowKeyIdRoute());
    }

    @Test
    void redactorScrubsWebhookKeys_and_secretResolverPreservesWebhookKeyRef() {
        Redactor redactor = new Redactor();
        String raw = "callback=https://api.example.com/runtime/message-events/sqwh_Ab12Cd34_secretTokenValue0123456789ABCDEFabcdef123";
        String scrubbed = redactor.redact(raw);
        assertFalse(scrubbed.contains("sqwh_"), "redactor must scrub sqwh_ tokens: " + scrubbed);
        assertTrue(scrubbed.contains("[redacted-webhook-key]"));

        SecretKeyStore keyStore = mock(SecretKeyStore.class);
        SecretResolver resolver = new SecretResolver(keyStore);
        Map<String, Object> nodeTree = new LinkedHashMap<>();
        nodeTree.put("webhook_key", new LinkedHashMap<>(WebhookKeyRefHelper.placeholder("whk_0123456789abcdef01234567")));
        resolver.resolveInline(nodeTree);
        assertEquals("whk_0123456789abcdef01234567",
                WebhookKeyRefHelper.extractWebhookKeyRef(nodeTree.get("webhook_key")).orElse(null));
    }
}
