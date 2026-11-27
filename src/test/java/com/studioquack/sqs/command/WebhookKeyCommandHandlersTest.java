package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WebhookKeyCommandHandlersTest {

    private static final String ENV_ID = "env_test_whk";
    private static final String INSTALLER_BEARER = "test-installer-bearer-0123456789abcdef";

    private ObjectMapper mapper;
    private SqsChannelProperties channel;
    private OsSecretManager osSecrets;
    private SecretStoreNamespace namespace;
    private DaemonWebhookKeyStore store;
    private byte[] ikm;

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

        Map<String, DaemonWebhookKeyEntity> db = new HashMap<>();
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
    }

    @Test
    void createRotateRevoke_roundTripThroughFr200SecureEnvelopes() throws Exception {
        var createHandler = new WebhookKeyCommandHandlers.CreateWebhookKeyCommandHandler(
                mapper, channel, osSecrets, namespace, store);
        var rotateHandler = new WebhookKeyCommandHandlers.RotateWebhookKeyCommandHandler(
                mapper, channel, osSecrets, namespace, store);
        var revokeHandler = new WebhookKeyCommandHandlers.RevokeWebhookKeyCommandHandler(
                mapper, channel, osSecrets, namespace, store);

        String refId = "whk_111122223333444455556666";

        // 1. Create
        DaemonCommand createCmd = sealedCommand("cmd-1", WebhookKeyCommandHandlers.COMMAND_CREATE,
                WebhookKeyCommandHandlers.AAD_CREATE,
                Map.of("refId", refId, "name", "Main Hook", "allowKeyIdRoute", false));
        DaemonCommandOutcome createOut = createHandler.handle(createCmd);
        assertTrue(createOut.succeeded());

        SecureEnvelope createEnv = mapper.readValue(createOut.output(), SecureEnvelope.class);
        JsonNode createdJson = mapper.readTree(EnvelopeCrypto.open(
                ikm, createEnv, Direction.AGENT_TO_BE, ENV_ID, WebhookKeyCommandHandlers.AAD_CREATE));
        assertEquals(refId, createdJson.get("refId").asText());
        String keyId1 = createdJson.get("keyId").asText();
        String rawKey1 = createdJson.get("rawKey").asText();
        assertTrue(WebhookKeyFormat.parse(rawKey1).isPresent());
        assertEquals(keyId1, WebhookKeyFormat.parse(rawKey1).get().keyId());

        // 2. Rotate
        DaemonCommand rotateCmd = sealedCommand("cmd-2", WebhookKeyCommandHandlers.COMMAND_ROTATE,
                WebhookKeyCommandHandlers.AAD_ROTATE,
                Map.of("refId", refId, "previousKeyId", keyId1));
        DaemonCommandOutcome rotateOut = rotateHandler.handle(rotateCmd);
        assertTrue(rotateOut.succeeded());

        SecureEnvelope rotateEnv = mapper.readValue(rotateOut.output(), SecureEnvelope.class);
        JsonNode rotatedJson = mapper.readTree(EnvelopeCrypto.open(
                ikm, rotateEnv, Direction.AGENT_TO_BE, ENV_ID, WebhookKeyCommandHandlers.AAD_ROTATE));
        assertEquals(refId, rotatedJson.get("refId").asText());
        String keyId2 = rotatedJson.get("keyId").asText();
        String rawKey2 = rotatedJson.get("rawKey").asText();
        assertNotEquals(rawKey1, rawKey2);
        assertTrue(WebhookKeyFormat.parse(rawKey2).isPresent());

        // 3. Revoke
        DaemonCommand revokeCmd = sealedCommand("cmd-3", WebhookKeyCommandHandlers.COMMAND_REVOKE,
                WebhookKeyCommandHandlers.AAD_REVOKE,
                Map.of("refId", refId, "keyId", keyId2));
        DaemonCommandOutcome revokeOut = revokeHandler.handle(revokeCmd);
        assertTrue(revokeOut.succeeded());
        JsonNode revokedJson = mapper.readTree(revokeOut.output());
        assertTrue(revokedJson.get("revoked").asBoolean());
    }

    private DaemonCommand sealedCommand(String id, String type, String aadTag, Object inner) throws Exception {
        SecureEnvelope env = EnvelopeCrypto.sealJson(ikm, Direction.BE_TO_AGENT, ENV_ID, aadTag, inner, mapper);
        return new DaemonCommand(id, type, "2026-11-28T00:00:00Z", mapper.writeValueAsString(env));
    }
}
