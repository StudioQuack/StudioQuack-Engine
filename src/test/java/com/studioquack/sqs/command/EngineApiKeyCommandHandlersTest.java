package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.webhook.EngineApiKeyScope;
import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.webhook.DaemonWebhookKeyEntity;
import com.studioquack.sqs.webhook.DaemonWebhookKeyRepository;
import com.studioquack.sqs.webhook.DaemonWebhookKeyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EngineApiKeyCommandHandlersTest {

    private static final String ENV_ID = "env_test_api_key";
    private static final String INSTALLER_BEARER = "test-installer-bearer-0123456789abcdef";

    private ObjectMapper mapper;
    private SqsChannelProperties channel;
    private OsSecretManager osSecrets;
    private SecretStoreNamespace namespace;
    private Map<String, DaemonWebhookKeyEntity> db;
    private DaemonWebhookKeyStore store;
    private byte[] ikm;

    private EngineApiKeyCommandHandlers.CreateEngineApiKeyCommandHandler createHandler;
    private EngineApiKeyCommandHandlers.RotateEngineApiKeyCommandHandler rotateHandler;
    private EngineApiKeyCommandHandlers.RevokeEngineApiKeyCommandHandler revokeHandler;

    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        mapper = new ObjectMapper();
        channel = new SqsChannelProperties();
        channel.setEnvId(ENV_ID);
        channel.setBearer(INSTALLER_BEARER);
        channel.setDataDir(tempDir.toString());

        osSecrets = mock(OsSecretManager.class);
        when(osSecrets.available()).thenReturn(false);
        namespace = new SecretStoreNamespace(new com.studioquack.sqs.secrets.os.OsSecretManagerProperties(), channel);
        ikm = EnvelopeCrypto.ikmFromBearer(INSTALLER_BEARER);

        db = new HashMap<>();
        DaemonWebhookKeyRepository repo = mock(DaemonWebhookKeyRepository.class);
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

        createHandler = new EngineApiKeyCommandHandlers.CreateEngineApiKeyCommandHandler(
                mapper, channel, osSecrets, namespace, store);
        rotateHandler = new EngineApiKeyCommandHandlers.RotateEngineApiKeyCommandHandler(
                mapper, channel, osSecrets, namespace, store);
        revokeHandler = new EngineApiKeyCommandHandlers.RevokeEngineApiKeyCommandHandler(
                mapper, channel, osSecrets, namespace, store);
    }

    @Test
    void nodeScoped_createRotateRevoke_roundTripThroughFr200SecureEnvelopes() throws Exception {
        assertEquals(EngineApiKeyCommandHandlers.COMMAND_CREATE, createHandler.type());
        assertEquals(EngineApiKeyCommandHandlers.COMMAND_ROTATE, rotateHandler.type());
        assertEquals(EngineApiKeyCommandHandlers.COMMAND_REVOKE, revokeHandler.type());

        String refId = "whk_111122223333444455556666";

        // 1. Create NODE_SCOPED key with allowKeyIdRoute=true
        DaemonCommand createCmd = sealedCommand("cmd-node-1", EngineApiKeyCommandHandlers.COMMAND_CREATE,
                EngineApiKeyCommandHandlers.AAD_CREATE,
                Map.of("refId", refId, "name", "Main Hook",
                        "keyScope", EngineApiKeyScope.NODE_SCOPED.name(), "allowKeyIdRoute", true));
        DaemonCommandOutcome createOut = createHandler.handle(createCmd);
        assertTrue(createOut.succeeded());

        SecureEnvelope createEnv = mapper.readValue(createOut.output(), SecureEnvelope.class);
        JsonNode createdJson = mapper.readTree(EnvelopeCrypto.open(
                ikm, createEnv, Direction.AGENT_TO_BE, ENV_ID, EngineApiKeyCommandHandlers.AAD_CREATE));
        assertEquals(refId, createdJson.get("refId").asText());
        String keyId1 = createdJson.get("keyId").asText();
        String rawKey1 = createdJson.get("rawKey").asText();
        assertTrue(rawKey1.startsWith(WebhookKeyFormat.WIRE_PREFIX));
        var parsed1 = WebhookKeyFormat.parse(rawKey1).orElseThrow();
        assertEquals(keyId1, parsed1.keyId());

        assertTrue(store.verifySecret(refId, keyId1, parsed1.secret(), EngineApiKeyScope.NODE_SCOPED).valid());
        assertTrue(store.verifyKeyIdOnly(refId, keyId1).valid());
        var wrongScope1 = store.verifySecret(refId, keyId1, parsed1.secret(), EngineApiKeyScope.ENGINE_WIDE);
        assertFalse(wrongScope1.valid());
        assertEquals("key_scope_mismatch", wrongScope1.failureReason());

        // 2. Rotate NODE_SCOPED key — preserves scope and allowKeyIdRoute
        DaemonCommand rotateCmd = sealedCommand("cmd-node-2", EngineApiKeyCommandHandlers.COMMAND_ROTATE,
                EngineApiKeyCommandHandlers.AAD_ROTATE,
                Map.of("refId", refId, "previousKeyId", keyId1));
        DaemonCommandOutcome rotateOut = rotateHandler.handle(rotateCmd);
        assertTrue(rotateOut.succeeded());

        SecureEnvelope rotateEnv = mapper.readValue(rotateOut.output(), SecureEnvelope.class);
        JsonNode rotatedJson = mapper.readTree(EnvelopeCrypto.open(
                ikm, rotateEnv, Direction.AGENT_TO_BE, ENV_ID, EngineApiKeyCommandHandlers.AAD_ROTATE));
        assertEquals(refId, rotatedJson.get("refId").asText());
        String keyId2 = rotatedJson.get("keyId").asText();
        String rawKey2 = rotatedJson.get("rawKey").asText();
        assertNotEquals(rawKey1, rawKey2);
        assertTrue(rawKey2.startsWith(WebhookKeyFormat.WIRE_PREFIX));
        var parsed2 = WebhookKeyFormat.parse(rawKey2).orElseThrow();
        assertEquals(keyId2, parsed2.keyId());

        assertFalse(store.verifySecret(refId, keyId1, parsed1.secret(), EngineApiKeyScope.NODE_SCOPED).valid());
        assertTrue(store.verifySecret(refId, keyId2, parsed2.secret(), EngineApiKeyScope.NODE_SCOPED).valid());
        assertTrue(store.verifyKeyIdOnly(refId, keyId2).valid());

        // 3. Revoke NODE_SCOPED key
        DaemonCommand revokeCmd = sealedCommand("cmd-node-3", EngineApiKeyCommandHandlers.COMMAND_REVOKE,
                EngineApiKeyCommandHandlers.AAD_REVOKE,
                Map.of("refId", refId, "keyId", keyId2));
        DaemonCommandOutcome revokeOut = revokeHandler.handle(revokeCmd);
        assertTrue(revokeOut.succeeded());
        JsonNode revokedJson = mapper.readTree(revokeOut.output());
        assertTrue(revokedJson.get("revoked").asBoolean());

        var afterRevoke = store.verifySecret(refId, keyId2, parsed2.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertFalse(afterRevoke.valid());
        assertEquals("revoked_api_key", afterRevoke.failureReason());
    }

    @Test
    void engineWide_createRotateRevoke_roundTripThroughFr200SecureEnvelopes() throws Exception {
        String refId = "whk_aaaa2222bbbb3333cccc4444";

        // 1. Create ENGINE_WIDE key (allowKeyIdRoute=true must be coerced to false)
        DaemonCommand createCmd = sealedCommand("cmd-eng-1", EngineApiKeyCommandHandlers.COMMAND_CREATE,
                EngineApiKeyCommandHandlers.AAD_CREATE,
                Map.of("refId", refId, "name", "Engine Message Trigger",
                        "keyScope", EngineApiKeyScope.ENGINE_WIDE.name(), "allowKeyIdRoute", true));
        DaemonCommandOutcome createOut = createHandler.handle(createCmd);
        assertTrue(createOut.succeeded());

        SecureEnvelope createEnv = mapper.readValue(createOut.output(), SecureEnvelope.class);
        JsonNode createdJson = mapper.readTree(EnvelopeCrypto.open(
                ikm, createEnv, Direction.AGENT_TO_BE, ENV_ID, EngineApiKeyCommandHandlers.AAD_CREATE));
        assertEquals(refId, createdJson.get("refId").asText());
        String keyId1 = createdJson.get("keyId").asText();
        String rawKey1 = createdJson.get("rawKey").asText();
        assertTrue(rawKey1.startsWith(WebhookKeyFormat.WIRE_PREFIX));
        var parsed1 = WebhookKeyFormat.parse(rawKey1).orElseThrow();
        assertEquals(keyId1, parsed1.keyId());

        assertEquals(EngineApiKeyScope.ENGINE_WIDE, db.get(refId).getKeyScope());
        assertFalse(db.get(refId).isAllowKeyIdRoute());
        assertTrue(store.verifySecret(refId, keyId1, parsed1.secret(), EngineApiKeyScope.ENGINE_WIDE).valid());
        var wrongScope = store.verifySecret(refId, keyId1, parsed1.secret(), EngineApiKeyScope.NODE_SCOPED);
        assertFalse(wrongScope.valid());
        assertEquals("key_scope_mismatch", wrongScope.failureReason());
        assertFalse(store.verifyKeyIdOnly(refId, keyId1).valid());

        // 2. Rotate ENGINE_WIDE key — preserves ENGINE_WIDE scope
        DaemonCommand rotateCmd = sealedCommand("cmd-eng-2", EngineApiKeyCommandHandlers.COMMAND_ROTATE,
                EngineApiKeyCommandHandlers.AAD_ROTATE,
                Map.of("refId", refId, "previousKeyId", keyId1));
        DaemonCommandOutcome rotateOut = rotateHandler.handle(rotateCmd);
        assertTrue(rotateOut.succeeded());

        SecureEnvelope rotateEnv = mapper.readValue(rotateOut.output(), SecureEnvelope.class);
        JsonNode rotatedJson = mapper.readTree(EnvelopeCrypto.open(
                ikm, rotateEnv, Direction.AGENT_TO_BE, ENV_ID, EngineApiKeyCommandHandlers.AAD_ROTATE));
        assertEquals(refId, rotatedJson.get("refId").asText());
        String keyId2 = rotatedJson.get("keyId").asText();
        String rawKey2 = rotatedJson.get("rawKey").asText();
        assertNotEquals(rawKey1, rawKey2);
        var parsed2 = WebhookKeyFormat.parse(rawKey2).orElseThrow();

        assertFalse(store.verifySecret(refId, keyId1, parsed1.secret(), EngineApiKeyScope.ENGINE_WIDE).valid());
        assertTrue(store.verifySecret(refId, keyId2, parsed2.secret(), EngineApiKeyScope.ENGINE_WIDE).valid());
        assertFalse(store.verifySecret(refId, keyId2, parsed2.secret(), EngineApiKeyScope.NODE_SCOPED).valid());

        // 3. Revoke ENGINE_WIDE key
        DaemonCommand revokeCmd = sealedCommand("cmd-eng-3", EngineApiKeyCommandHandlers.COMMAND_REVOKE,
                EngineApiKeyCommandHandlers.AAD_REVOKE,
                Map.of("refId", refId, "keyId", keyId2));
        DaemonCommandOutcome revokeOut = revokeHandler.handle(revokeCmd);
        assertTrue(revokeOut.succeeded());
        assertTrue(mapper.readTree(revokeOut.output()).get("revoked").asBoolean());

        var afterRevoke = store.verifySecret(refId, keyId2, parsed2.secret(), EngineApiKeyScope.ENGINE_WIDE);
        assertFalse(afterRevoke.valid());
        assertEquals("revoked_api_key", afterRevoke.failureReason());
    }

    @Test
    void create_rejectsInvalidKeyScope_andDefaultsBlankScopeToNodeScoped() throws Exception {
        DaemonCommand badScopeCmd = sealedCommand("cmd-bad-scope", EngineApiKeyCommandHandlers.COMMAND_CREATE,
                EngineApiKeyCommandHandlers.AAD_CREATE,
                Map.of("refId", "whk_999988887777666655554444", "name", "Bad Scope",
                        "keyScope", "INVALID_SCOPE", "allowKeyIdRoute", false));
        DaemonCommandOutcome badOut = createHandler.handle(badScopeCmd);
        assertFalse(badOut.succeeded());
        assertTrue(badOut.error().contains("invalid keyScope"), "unexpected error: " + badOut.error());

        String defaultRefId = "whk_777766665555444433332222";
        DaemonCommand defaultScopeCmd = sealedCommand("cmd-default-scope", EngineApiKeyCommandHandlers.COMMAND_CREATE,
                EngineApiKeyCommandHandlers.AAD_CREATE,
                Map.of("refId", defaultRefId, "name", "Default Scope", "allowKeyIdRoute", false));
        DaemonCommandOutcome defaultOut = createHandler.handle(defaultScopeCmd);
        assertTrue(defaultOut.succeeded());
        assertEquals(EngineApiKeyScope.NODE_SCOPED, db.get(defaultRefId).getKeyScope());
    }

    private DaemonCommand sealedCommand(String id, String type, String aadTag, Object inner) throws Exception {
        SecureEnvelope env = EnvelopeCrypto.sealJson(ikm, Direction.BE_TO_AGENT, ENV_ID, aadTag, inner, mapper);
        return new DaemonCommand(id, type, "2026-11-28T00:00:00Z", mapper.writeValueAsString(env));
    }
}
