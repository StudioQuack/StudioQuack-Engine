package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.EnvelopeClient;
import com.studioquack.sqs.channel.KemIkmStore;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciBuildPathValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Handles BE-pushed {@code stage-oci-build-file}: unseals the envelope (AAD {@code oci-build-stage-file}),
 * verifies size + SHA-256, and writes the file into the build context under
 * {@code ${SQS_DATA_DIR}/oci-build/<buildId>/}. Wire contract: payload: {envId, buildId, path, sizeBytes,
 * sha256, content: SecureEnvelope JSON} result: {path, sizeBytes, sha256} Never logs file bytes — metadata
 * (buildId, path, sizeBytes, sha256) only.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StageOciBuildFileCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stage-oci-build-file";

    /**
     * SecureEnvelope AAD jobKey — must match BE {@code AAD_TAG_OCI_BUILD_STAGE}.
     */
    public static final String AAD_OCI_BUILD_STAGE = "oci-build-stage-file";

    /**
     * Per-file ceiling (defence in depth; BE and {@link OciBuildContextStore} also enforce).
     */
    static final long MAX_STAGE_FILE_BYTES = 5L * 1024 * 1024;

    private final ObjectMapper objectMapper;
    private final OciBuildContextStore contextStore;
    private final SqsChannelProperties channel;

    /**
     * Daemon-bound custody for the persisted kemIkm. Field-injected so existing constructor call sites (unit
     * tests) stay untouched; a missing bean keeps the legacy plaintext-0600 behaviour.
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private org.springframework.beans.factory.ObjectProvider<com.studioquack.sqs.secrets.DaemonFileCustody>
            fileCustody;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        JsonNode payload;
        try {
            payload = objectMapper.readTree(command.payload() == null ? "{}" : command.payload());
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: malformed payload: " + message(parse));
        }

        String buildId = text(payload, "buildId");
        if (buildId == null || buildId.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: payload missing 'buildId'");
        }
        return stageOne(buildId, payload, payload);
    }

    /**
     * Stages one file entry {@code {path, sizeBytes, sha256, content}} under {@code buildId}. Package-visible: the
     * batch handler ({@code stage-oci-build-files}) loops this per element so both wire shapes share validation,
     * unseal, hash-check and store semantics.
     */
    DaemonCommandOutcome stageOne(String buildId, JsonNode fileNode, JsonNode envelopeContext) {
        String pathRaw = text(fileNode, "path");
        String sha256Claim = text(fileNode, "sha256");
        JsonNode contentNode = fileNode == null ? null : fileNode.get("content");

        if (pathRaw == null || pathRaw.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: payload missing 'path'");
        }
        if (sha256Claim == null || sha256Claim.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: payload missing 'sha256'");
        }
        if (contentNode == null || contentNode.isNull() || !contentNode.isObject()) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: payload missing 'content' object");
        }
        if (fileNode.path("sizeBytes").isMissingNode() || fileNode.path("sizeBytes").isNull()) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: payload missing 'sizeBytes'");
        }
        long sizeBytes = fileNode.path("sizeBytes").asLong(-1L);
        if (sizeBytes < 0) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: invalid sizeBytes");
        }
        if (sizeBytes > MAX_STAGE_FILE_BYTES) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: sizeBytes exceeds " + MAX_STAGE_FILE_BYTES);
        }

        byte[] rawBytes;
        try {
            rawBytes = unsealContent(envelopeContext, contentNode);
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: could not open content envelope: " + message(e));
        }

        if (rawBytes.length != sizeBytes) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: decrypted size does not match sizeBytes");
        }
        String actualSha;
        try {
            actualSha = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(rawBytes));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: sha256 compute failed: " + message(e));
        }
        if (!actualSha.equalsIgnoreCase(sha256Claim.trim())) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: sha256 mismatch");
        }
        String sha256 = actualSha.toLowerCase(Locale.ROOT);

        final String normalisedPath;
        try {
            // Store re-validates path + caps; normalise here for the success result.
            normalisedPath = OciBuildPathValidator.normalizeAndValidate(pathRaw);
            // Early feedback. The build path re-checks the file podman will use.
            if ("Containerfile".equals(normalisedPath) || "Dockerfile".equals(normalisedPath)) {
                com.studioquack.sqs.sandbox.oci.ContainerfilePolicy.validate(
                        new String(rawBytes, java.nio.charset.StandardCharsets.UTF_8));
            }
            contextStore.writeFile(buildId, pathRaw, rawBytes);
        } catch (IllegalArgumentException bad) {
            return DaemonCommandOutcome.failed("stage-oci-build-file: " + bad.getMessage());
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: write failed: " + message(e));
        }

        // Metadata only — never log content / raw bytes.
        log.info("[T-31] stage-oci-build-file ok buildId={} path={} sizeBytes={} sha256={}",
                buildId, normalisedPath, sizeBytes, sha256);
        try {
            var out = objectMapper.createObjectNode();
            out.put("path", normalisedPath);
            out.put("sizeBytes", sizeBytes);
            out.put("sha256", sha256);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-file: could not encode result: " + message(e));
        }
    }

    /**
     * Unseal {@code content} as a {@link SecureEnvelope}. Prefer payload {@code envId} when present and equal to
     * the channel env; otherwise use the channel envId.
     */
    byte[] unsealContent(JsonNode payload, JsonNode contentNode) throws Exception {
        SecureEnvelope envelope = objectMapper.treeToValue(contentNode, SecureEnvelope.class);
        String channelEnv = channel.getEnvId();
        String payloadEnv = text(payload, "envId");
        String envId = (payloadEnv != null && !payloadEnv.isBlank()
                && payloadEnv.equals(channelEnv))
                ? payloadEnv
                : channelEnv;
        // kemIkm when persisted; installer bearer only as legacy fallback.
        byte[] kemIkm = KemIkmStore.loadOrNull(Path.of(channel.getDataDir()),
                com.studioquack.sqs.secrets.DaemonFileCustody.resolveKemIkm(fileCustody, channel));
        EnvelopeClient envelopeClient = EnvelopeClient.preferKemIkm(
                envId, kemIkm, channel.getInstallerBearer(), objectMapper);
        return envelopeClient.open(envelope, AAD_OCI_BUILD_STAGE);
    }

    static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
