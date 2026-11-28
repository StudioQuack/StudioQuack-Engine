package com.studioquack.sqs.webhook;

import com.studioquack.common.dto.webhook.EngineApiKeyScope;
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
    void createVerifyRotateRevoke_nodeScopedLifecycle() {
        String refId = "whk_0123456789abcdef01234567";
        var created = store.create(refId, "Inbound Hook", EngineApiKeyScope.NODE_SCOPED, false);

        assertEquals(refId, created.refId());
        assertTrue(WebhookKeyFormat.isValidKeyId(created.keyId()));
        assertTrue(created.rawKey().startsWith(WebhookKeyFormat.WIRE_PREFIX));
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();
        assertEquals(created.keyId(), parsed.keyId());
        assertEquals(43, parsed.secret().length());

        // Plaintext secret is NEVER persisted; only 64-char SHA-256 hex digest.
        DaemonWebhookKeyEntity saved = db.get(refId);
        assertNotNull(saved);
        assertEquals(EngineApiKeyScope.NODE_SCOPED, saved.getKeyScope());
        assertFalse(saved.isAllowKeyIdRoute());
        assertEquals(64, saved.getSecretSha256().length());
        assertNotEquals(parsed.secret(), saved.getSecretSha256());

        // Valid secret passes URL_KEY and HEADER_KEY under NODE_SCOPED; KEY_ID_ONLY rejected when allowKeyIdRoute=false.
        var urlOk = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                parsed.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertTrue(urlOk.valid());
        assertEquals(EngineApiKeyScope.NODE_SCOPED, urlOk.keyScope());
        assertFalse(urlOk.allowKeyIdRoute());

        assertTrue(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_HEADER_KEY,
                parsed.secret(), EngineApiKeyScope.NODE_SCOPED).valid());
        assertTrue(store.verifySecret(refId, created.keyId(), parsed.secret(),
                EngineApiKeyScope.NODE_SCOPED).valid());

        var badSecret = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                "wrong_secret_value_000000000000000000000000", EngineApiKeyScope.NODE_SCOPED);
        assertFalse(badSecret.valid());
        assertEquals("invalid_api_secret", badSecret.failureReason());

        var keyIdDisallowed = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY,
                null, EngineApiKeyScope.NODE_SCOPED);
        assertFalse(keyIdDisallowed.valid());
        assertEquals("key_id_route_not_allowed", keyIdDisallowed.failureReason());
        assertFalse(store.verifyKeyIdOnly(refId, created.keyId()).valid());

        // Rotate atomically updates (keyId, secret) while preserving refId and scope.
        var rotated = store.rotate(refId, created.keyId());
        assertEquals(refId, rotated.refId());
        assertNotEquals(created.rawKey(), rotated.rawKey());
        assertTrue(rotated.rawKey().startsWith(WebhookKeyFormat.WIRE_PREFIX));
        var rotatedParsed = WebhookKeyFormat.parse(rotated.rawKey()).orElseThrow();

        assertFalse(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                parsed.secret(), EngineApiKeyScope.NODE_SCOPED).valid());
        var rotatedOk = store.verify(refId, rotated.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                rotatedParsed.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertTrue(rotatedOk.valid());
        assertEquals(EngineApiKeyScope.NODE_SCOPED, rotatedOk.keyScope());

        // Revoke wipes digest and is idempotent on repeat/absent calls.
        assertTrue(store.revoke(refId, rotated.keyId()));
        assertEquals(DaemonWebhookKeyEntity.STATUS_REVOKED, db.get(refId).getStatus());
        assertNull(db.get(refId).getSecretSha256());
        var revokedCheck = store.verify(refId, rotated.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                rotatedParsed.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertFalse(revokedCheck.valid());
        assertEquals("revoked_api_key", revokedCheck.failureReason());
        assertTrue(store.revoke(refId, rotated.keyId()));
        assertTrue(store.revoke("whk_nonexistent000000000000", null));
        assertThrows(IllegalStateException.class, () -> store.rotate(refId, rotated.keyId()));
    }

    @Test
    void engineWideKey_verifiesUnderEngineWide_rejectsNodeScoped_andForcesAllowKeyIdRouteFalse() {
        String refId = "whk_aaaa1111bbbb2222cccc3333";
        // Requesting allowKeyIdRoute=true for ENGINE_WIDE must be forced false.
        var created = store.create(refId, "Engine Trigger Key", EngineApiKeyScope.ENGINE_WIDE, true);
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();

        DaemonWebhookKeyEntity saved = db.get(refId);
        assertNotNull(saved);
        assertEquals(EngineApiKeyScope.ENGINE_WIDE, saved.getKeyScope());
        assertFalse(saved.isAllowKeyIdRoute(), "ENGINE_WIDE keys must force allowKeyIdRoute=false");

        var okSecret = store.verifySecret(refId, created.keyId(), parsed.secret(), EngineApiKeyScope.ENGINE_WIDE);
        assertTrue(okSecret.valid());
        assertEquals(EngineApiKeyScope.ENGINE_WIDE, okSecret.keyScope());
        assertFalse(okSecret.allowKeyIdRoute());
        assertTrue(store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_HEADER_KEY,
                parsed.secret(), EngineApiKeyScope.ENGINE_WIDE).valid());

        // Rejected under NODE_SCOPED with key_scope_mismatch.
        var wrongScopeDirect = store.verifySecret(refId, created.keyId(), parsed.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertFalse(wrongScopeDirect.valid());
        assertEquals("key_scope_mismatch", wrongScopeDirect.failureReason());

        var wrongScopeDispatch = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                parsed.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertFalse(wrongScopeDispatch.valid());
        assertEquals("key_scope_mismatch", wrongScopeDispatch.failureReason());

        // KEY_ID_ONLY is never permitted for ENGINE_WIDE keys.
        var keyIdDirect = store.verifyKeyIdOnly(refId, created.keyId());
        assertFalse(keyIdDirect.valid());
        assertEquals("key_scope_mismatch", keyIdDirect.failureReason());

        var keyIdUnderEngineWide = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY,
                null, EngineApiKeyScope.ENGINE_WIDE);
        assertFalse(keyIdUnderEngineWide.valid());
        assertEquals("key_scope_mismatch", keyIdUnderEngineWide.failureReason());

        var keyIdUnderNodeScoped = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY,
                null, EngineApiKeyScope.NODE_SCOPED);
        assertFalse(keyIdUnderNodeScoped.valid());
        assertEquals("key_scope_mismatch", keyIdUnderNodeScoped.failureReason());

        // Rotate preserves ENGINE_WIDE scope and allowKeyIdRoute=false.
        var rotated = store.rotate(refId, created.keyId());
        var rotatedParsed = WebhookKeyFormat.parse(rotated.rawKey()).orElseThrow();
        assertTrue(store.verifySecret(refId, rotated.keyId(), rotatedParsed.secret(),
                EngineApiKeyScope.ENGINE_WIDE).valid());
        var rotatedWrongScope = store.verifySecret(refId, rotated.keyId(), rotatedParsed.secret(),
                EngineApiKeyScope.NODE_SCOPED);
        assertFalse(rotatedWrongScope.valid());
        assertEquals("key_scope_mismatch", rotatedWrongScope.failureReason());
        assertFalse(db.get(refId).isAllowKeyIdRoute());
    }

    @Test
    void nodeScopedKey_verifiesUnderNodeScoped_rejectsEngineWide_andSupportsKeyIdOnly() {
        String refId = "whk_fedcba9876543210fedcba98";
        var created = store.create(refId, "Header Delegated Hook", EngineApiKeyScope.NODE_SCOPED, true);
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();

        DaemonWebhookKeyEntity saved = db.get(refId);
        assertNotNull(saved);
        assertEquals(EngineApiKeyScope.NODE_SCOPED, saved.getKeyScope());
        assertTrue(saved.isAllowKeyIdRoute());

        var secretOk = store.verifySecret(refId, created.keyId(), parsed.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertTrue(secretOk.valid());
        assertEquals(EngineApiKeyScope.NODE_SCOPED, secretOk.keyScope());
        assertTrue(secretOk.allowKeyIdRoute());

        // Rejected under ENGINE_WIDE with key_scope_mismatch.
        var wrongScopeDirect = store.verifySecret(refId, created.keyId(), parsed.secret(), EngineApiKeyScope.ENGINE_WIDE);
        assertFalse(wrongScopeDirect.valid());
        assertEquals("key_scope_mismatch", wrongScopeDirect.failureReason());

        var wrongScopeDispatch = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_HEADER_KEY,
                parsed.secret(), EngineApiKeyScope.ENGINE_WIDE);
        assertFalse(wrongScopeDispatch.valid());
        assertEquals("key_scope_mismatch", wrongScopeDispatch.failureReason());

        // Supports KEY_ID_ONLY when allowKeyIdRoute=true under NODE_SCOPED.
        var keyIdDirect = store.verifyKeyIdOnly(refId, created.keyId());
        assertTrue(keyIdDirect.valid());
        assertEquals(EngineApiKeyScope.NODE_SCOPED, keyIdDirect.keyScope());
        assertTrue(keyIdDirect.allowKeyIdRoute());

        var keyIdDispatch = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY,
                null, EngineApiKeyScope.NODE_SCOPED);
        assertTrue(keyIdDispatch.valid());
        assertEquals(EngineApiKeyScope.NODE_SCOPED, keyIdDispatch.keyScope());
        assertTrue(keyIdDispatch.allowKeyIdRoute());

        var keyIdWrongScope = store.verify(refId, created.keyId(), DaemonWebhookKeyStore.AUTH_MODE_KEY_ID_ONLY,
                null, EngineApiKeyScope.ENGINE_WIDE);
        assertFalse(keyIdWrongScope.valid());
        assertEquals("key_scope_mismatch", keyIdWrongScope.failureReason());
    }

    @Test
    void redactorScrubsEngineApiAndLegacyWebhookKeys_and_secretResolverPreservesWebhookKeyRef() {
        Redactor redactor = new Redactor();

        String rawApiKey = "callback=https://api.example.com/runtime/message-events/sqk_Ab12Cd34_secretTokenValue0123456789ABCDEFabcdef123";
        String scrubbedApiKey = redactor.redact(rawApiKey);
        assertFalse(scrubbedApiKey.contains("sqk_"), "redactor must scrub sqk_ tokens: " + scrubbedApiKey);
        assertTrue(scrubbedApiKey.contains("[redacted-api-key]"));

        String rawLegacy = "callback=https://api.example.com/runtime/message-events/sqwh_Ab12Cd34_secretTokenValue0123456789ABCDEFabcdef123";
        String scrubbedLegacy = redactor.redact(rawLegacy);
        assertFalse(scrubbedLegacy.contains("sqwh_"), "redactor must scrub legacy sqwh_ tokens: " + scrubbedLegacy);
        assertTrue(scrubbedLegacy.contains("[redacted-webhook-key]"));

        SecretKeyStore keyStore = mock(SecretKeyStore.class);
        SecretResolver resolver = new SecretResolver(keyStore);
        Map<String, Object> nodeTree = new LinkedHashMap<>();
        nodeTree.put("webhook_key", new LinkedHashMap<>(WebhookKeyRefHelper.placeholder("whk_0123456789abcdef01234567")));
        resolver.resolveInline(nodeTree);
        assertEquals("whk_0123456789abcdef01234567",
                WebhookKeyRefHelper.extractWebhookKeyRef(nodeTree.get("webhook_key")).orElse(null));
    }
}
