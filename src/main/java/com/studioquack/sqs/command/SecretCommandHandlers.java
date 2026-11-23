package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.EnvelopeClient;
import com.studioquack.sqs.channel.KemIkmStore;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.sandbox.secrets.SecretRef;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.inventory.ClientSecretInventoryService;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * BE-pushed secret mutations (`commit-secret`, `rotate-secret`, `delete-secret`). The BE stores only an
 * {@link SecureEnvelope} in the heartbeat command queue.
 */
@Slf4j
final class SecretCommandHandlers {

    static final String AAD_COMMIT = "secret-commit";
    static final String AAD_ROTATE = "secret-rotate";
    static final String AAD_DELETE = "secret-delete";

    private SecretCommandHandlers() {
    }

    @Component
    @RequiredArgsConstructor
    static class CommitSecretCommandHandler implements DaemonCommandHandler {
        private final ObjectMapper mapper;
        private final SqsChannelProperties channel;
        private final OsSecretManager osSecrets;
        private final SecretStoreNamespace namespace;
        private final ClientSecretInventoryService inventory;

        @Override
        public String type() {
            return "commit-secret";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            CommitRequest req = open(command, AAD_COMMIT, CommitRequest.class, mapper, channel,
                    osSecrets, namespace);
            if (req.plaintext() == null) {
                return DaemonCommandOutcome.failed("commit-secret: plaintext is required");
            }
            String secretId = "sec_" + randomHex();
            DaemonCommandOutcome outcome = persist(secretId, 1, req.plaintext(), mapper, osSecrets, namespace);
            // Record ownership only after the OS-store write succeeded, so purge can later delete it.
            if (outcome.succeeded()) {
                inventory.recordCommit(secretId, 1);
            }
            return outcome;
        }
    }

    @Component
    @RequiredArgsConstructor
    static class RotateSecretCommandHandler implements DaemonCommandHandler {
        private final ObjectMapper mapper;
        private final SqsChannelProperties channel;
        private final OsSecretManager osSecrets;
        private final SecretStoreNamespace namespace;
        private final ClientSecretInventoryService inventory;

        private final ConcurrentMap<String, Integer> versions = new ConcurrentHashMap<>();

        @Override
        public String type() {
            return "rotate-secret";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            RotateRequest req = open(command, AAD_ROTATE, RotateRequest.class, mapper, channel,
                    osSecrets, namespace);
            if (req.secretId() == null || req.secretId().isBlank()) {
                return DaemonCommandOutcome.failed("rotate-secret: secretId is required");
            }
            if (req.plaintext() == null) {
                return DaemonCommandOutcome.failed("rotate-secret: plaintext is required");
            }
            int next = Math.max(2, versions.merge(req.secretId(), 1, Integer::sum));
            DaemonCommandOutcome outcome = persist(req.secretId(), next, req.plaintext(), mapper, osSecrets, namespace);
            // Raise the recorded max version so purge deletes every version we wrote.
            if (outcome.succeeded()) {
                inventory.recordCommit(req.secretId(), next);
            }
            return outcome;
        }
    }

    @Component
    @RequiredArgsConstructor
    static class DeleteSecretCommandHandler implements DaemonCommandHandler {
        private final ObjectMapper mapper;
        private final SqsChannelProperties channel;
        private final OsSecretManager osSecrets;
        private final SecretStoreNamespace namespace;
        private final ClientSecretInventoryService inventory;

        @Override
        public String type() {
            return "delete-secret";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            DeleteRequest req = open(command, AAD_DELETE, DeleteRequest.class, mapper, channel,
                    osSecrets, namespace);
            if (req.secretId() == null || req.secretId().isBlank()) {
                return DaemonCommandOutcome.failed("delete-secret: secretId is required");
            }
            int removed = inventory.deleteAllVersions(req.secretId());
            log.info("secret-command: deleted secretId={} versions={} from OS store backend={}",
                    req.secretId(), removed, osSecrets.name());
            return DaemonCommandOutcome.ok("{}");
        }
    }

    private static <T> T open(DaemonCommand command, String aadTag, Class<T> type,
                              ObjectMapper mapper, SqsChannelProperties channel,
                              OsSecretManager osSecrets, SecretStoreNamespace namespace) throws Exception {
        if (command == null || command.payload() == null || command.payload().isBlank()) {
            throw new IllegalArgumentException(aadTag + ": missing sealed payload");
        }
        SecureEnvelope envelope = mapper.readValue(command.payload(), SecureEnvelope.class);
        // Open with kemIkm when persisted; installer bearer only as legacy fallback.
        // Never use getBearer() here: after hybrid it is sqikm1.… (channel auth), not envelope IKM.
        // Custody: the persisted kemIkm is daemon-bound sealed when the OS secret manager allows
        // (legacy plaintext files still load vithe store's passthrough).
        Path dataDir = Path.of(channel.getDataDir());
        byte[] kemIkm = KemIkmStore.loadOrNull(dataDir, com.studioquack.sqs.secrets.DaemonFileCustody
                .kemIkmStore(osSecrets, namespace, channel.getEnvId(), dataDir));
        EnvelopeClient envelopeClient = EnvelopeClient.preferKemIkm(
                channel.getEnvId(), kemIkm, channel.getInstallerBearer(), mapper);
        return envelopeClient.openJson(envelope, aadTag, type);
    }

    private static DaemonCommandOutcome persist(String secretId, int version, String plaintext,
                                                ObjectMapper mapper, OsSecretManager osSecrets,
                                                SecretStoreNamespace namespace) throws Exception {
        Optional<SecretRef> parsed = SecretRef.parse(secretId + ":v" + version);
        if (parsed.isEmpty()) {
            return DaemonCommandOutcome.failed("secret: invalid generated reference");
        }
        byte[] bytes = plaintext.getBytes(StandardCharsets.UTF_8);
        osSecrets.put(namespace.secretService(), parsed.get().wire(), plaintext);
        CommitResponse response = new CommitResponse(secretId, version, sha256B64Url(bytes), bytes.length);
        log.info("secret-command: committed secretId={} version={} length={} backend={}",
                secretId, version, bytes.length, osSecrets.name());
        return DaemonCommandOutcome.ok(mapper.writeValueAsString(response));
    }

    private static String randomHex() {
        byte[] buf = new byte[12];
        new SecureRandom().nextBytes(buf);
        return HexFormat.of().formatHex(buf);
    }

    private static String sha256B64Url(byte[] bytes) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        return Base64.getUrlEncoder().withoutPadding().encodeToString(md.digest(bytes));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record CommitRequest(
            String key,
            String mode,
            String description,
            String plaintext) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record RotateRequest(String secretId, String plaintext) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record DeleteRequest(String secretId) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record CommitResponse(
            @JsonProperty("secret_id") String secretId,
            int version,
            @JsonProperty("plaintext_hash_b64") String plaintextHashB64,
            @JsonProperty("plaintext_length") int plaintextLength) {
    }
}
