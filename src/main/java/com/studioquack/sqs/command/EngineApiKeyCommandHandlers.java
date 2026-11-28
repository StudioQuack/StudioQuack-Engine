package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.webhook.EngineApiKeyScope;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.EnvelopeClient;
import com.studioquack.sqs.channel.KemIkmStore;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.DaemonFileCustody;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.webhook.DaemonWebhookKeyStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Path;

/**
 * Control-plane-pushed unified engine API key lifecycle commands
 * ({@code create-engine-api-key}, {@code rotate-engine-api-key}, {@code revoke-engine-api-key})
 * covering both {@link EngineApiKeyScope#ENGINE_WIDE} and {@link EngineApiKeyScope#NODE_SCOPED}
 * keys.
 *
 * <p>Requests arrive sealed in FR-200 {@link SecureEnvelope}s ({@code Direction.BE_TO_AGENT});
 * create and rotate responses return the one-time {@code rawKey} ({@code sqk_<keyId>_<secret>})
 * sealed in FR-200 {@link SecureEnvelope}s ({@code Direction.AGENT_TO_BE}) so key material is
 * never exposed in plaintext across command transport or result buffers.
 */
@Slf4j
public final class EngineApiKeyCommandHandlers {

    public static final String COMMAND_CREATE = "create-engine-api-key";
    public static final String COMMAND_ROTATE = "rotate-engine-api-key";
    public static final String COMMAND_REVOKE = "revoke-engine-api-key";

    public static final String AAD_CREATE = "engine-api-key-create";
    public static final String AAD_ROTATE = "engine-api-key-rotate";
    public static final String AAD_REVOKE = "engine-api-key-revoke";

    private EngineApiKeyCommandHandlers() {
    }

    @Component
    @RequiredArgsConstructor
    public static class CreateEngineApiKeyCommandHandler implements DaemonCommandHandler {
        private final ObjectMapper mapper;
        private final SqsChannelProperties channel;
        private final OsSecretManager osSecrets;
        private final SecretStoreNamespace namespace;
        private final DaemonWebhookKeyStore store;

        @Override
        public String type() {
            return COMMAND_CREATE;
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            EnvelopeClient envelopeClient = buildEnvelopeClient(channel, osSecrets, namespace, mapper);
            CreateRequest req = open(command, AAD_CREATE, CreateRequest.class, mapper, envelopeClient);
            if (req.refId() == null || !WebhookKeyRefHelper.isValidRefId(req.refId())) {
                return DaemonCommandOutcome.failed("create-engine-api-key: valid refId is required");
            }
            EngineApiKeyScope scope;
            try {
                scope = EngineApiKeyScope.fromOrDefault(req.keyScope(), EngineApiKeyScope.NODE_SCOPED);
            } catch (IllegalArgumentException badScope) {
                return DaemonCommandOutcome.failed("create-engine-api-key: invalid keyScope");
            }
            DaemonWebhookKeyStore.CreatedKey created =
                    store.create(req.refId(), req.name(), scope);
            KeyMaterialResponse resp = new KeyMaterialResponse(
                    created.refId(), created.keyId(), created.rawKey());
            SecureEnvelope sealed = envelopeClient.sealJson(AAD_CREATE, resp);
            return DaemonCommandOutcome.ok(mapper.writeValueAsString(sealed));
        }
    }

    @Component
    @RequiredArgsConstructor
    public static class RotateEngineApiKeyCommandHandler implements DaemonCommandHandler {
        private final ObjectMapper mapper;
        private final SqsChannelProperties channel;
        private final OsSecretManager osSecrets;
        private final SecretStoreNamespace namespace;
        private final DaemonWebhookKeyStore store;

        @Override
        public String type() {
            return COMMAND_ROTATE;
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            EnvelopeClient envelopeClient = buildEnvelopeClient(channel, osSecrets, namespace, mapper);
            RotateRequest req = open(command, AAD_ROTATE, RotateRequest.class, mapper, envelopeClient);
            if (req.refId() == null || !WebhookKeyRefHelper.isValidRefId(req.refId())) {
                return DaemonCommandOutcome.failed("rotate-engine-api-key: valid refId is required");
            }
            DaemonWebhookKeyStore.CreatedKey rotated = store.rotate(req.refId(), req.previousKeyId());
            KeyMaterialResponse resp = new KeyMaterialResponse(
                    rotated.refId(), rotated.keyId(), rotated.rawKey());
            SecureEnvelope sealed = envelopeClient.sealJson(AAD_ROTATE, resp);
            return DaemonCommandOutcome.ok(mapper.writeValueAsString(sealed));
        }
    }

    @Component
    @RequiredArgsConstructor
    public static class RevokeEngineApiKeyCommandHandler implements DaemonCommandHandler {
        private final ObjectMapper mapper;
        private final SqsChannelProperties channel;
        private final OsSecretManager osSecrets;
        private final SecretStoreNamespace namespace;
        private final DaemonWebhookKeyStore store;

        @Override
        public String type() {
            return COMMAND_REVOKE;
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            EnvelopeClient envelopeClient = buildEnvelopeClient(channel, osSecrets, namespace, mapper);
            RevokeRequest req = open(command, AAD_REVOKE, RevokeRequest.class, mapper, envelopeClient);
            if ((req.refId() == null || req.refId().isBlank())
                    && (req.keyId() == null || req.keyId().isBlank())) {
                return DaemonCommandOutcome.failed("revoke-engine-api-key: refId or keyId is required");
            }
            boolean revoked = store.revoke(req.refId(), req.keyId());
            return DaemonCommandOutcome.ok(mapper.writeValueAsString(new RevokeResponse(revoked)));
        }
    }

    static EnvelopeClient buildEnvelopeClient(SqsChannelProperties channel,
                                              OsSecretManager osSecrets,
                                              SecretStoreNamespace namespace,
                                              ObjectMapper mapper) {
        Path dataDir = Path.of(channel.getDataDir());
        byte[] kemIkm = KemIkmStore.loadOrNull(dataDir, DaemonFileCustody
                .kemIkmStore(osSecrets, namespace, channel.getEnvId(), dataDir));
        return EnvelopeClient.preferKemIkm(
                channel.getEnvId(), kemIkm, channel.getInstallerBearer(), mapper);
    }

    static <T> T open(DaemonCommand command, String aadTag, Class<T> type,
                      ObjectMapper mapper, EnvelopeClient envelopeClient) throws Exception {
        if (command == null || command.payload() == null || command.payload().isBlank()) {
            throw new IllegalArgumentException(aadTag + ": missing sealed payload");
        }
        SecureEnvelope envelope = mapper.readValue(command.payload(), SecureEnvelope.class);
        return envelopeClient.openJson(envelope, aadTag, type);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CreateRequest(String refId, String name, String keyScope) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record RotateRequest(String refId, String previousKeyId) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record RevokeRequest(String refId, String keyId) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    record KeyMaterialResponse(String refId, String keyId, String rawKey) {
    }

    record RevokeResponse(boolean revoked) {
    }
}
