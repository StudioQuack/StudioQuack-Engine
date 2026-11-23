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
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

/**
 * Handles the BE-pushed {@code stateful-instance-put-file} command: unseals the envelope (AAD
 * {@code bespoke-put-file}), verifies size + SHA-256, and writes the file into the running stateful container
 * under {@code /data/sq-bespoke/<templateId>/<name>} via {@code podman exec -i … base64 -d}. Wire contract:
 * payload: {envId, imageRef, instanceId, templateId, name, sizeBytes, sha256, content: SecureEnvelope JSON}
 * result: {name, sizeBytes, sha256} Never logs file bytes — metadata (templateId, name, sizeBytes, sha256)
 * only.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulInstancePutFileCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stateful-instance-put-file";

    /**
     * SecureEnvelope AAD jobKey — must match BE {@code AAD_TAG_BESPOKE_PUT}.
     */
    public static final String AAD_BESPOKE_PUT = "bespoke-put-file";

    /**
     * Per-file ceiling (defence in depth; BE also enforces).
     */
    static final long MAX_BESPOKE_FILE_BYTES = 5L * 1024 * 1024;

    private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(60);

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;
    private final OciCli cli;
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
                    "stateful-instance-put-file: malformed payload: " + message(parse));
        }

        String instanceId = text(payload, "instanceId");
        String templateIdRaw = text(payload, "templateId");
        String nameRaw = text(payload, "name");
        String sha256Claim = text(payload, "sha256");
        JsonNode contentNode = payload == null ? null : payload.get("content");

        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: payload missing 'instanceId'");
        }
        if (templateIdRaw == null || templateIdRaw.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: payload missing 'templateId'");
        }
        if (nameRaw == null || nameRaw.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: payload missing 'name'");
        }
        if (contentNode == null || contentNode.isNull() || !contentNode.isObject()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: payload missing 'content' object");
        }
        if (sha256Claim == null || sha256Claim.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: payload missing 'sha256'");
        }
        if (payload.path("sizeBytes").isMissingNode() || payload.path("sizeBytes").isNull()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: payload missing 'sizeBytes'");
        }
        long sizeBytes = payload.path("sizeBytes").asLong(-1L);
        if (sizeBytes < 0) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: invalid sizeBytes");
        }
        if (sizeBytes > MAX_BESPOKE_FILE_BYTES) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: sizeBytes exceeds " + MAX_BESPOKE_FILE_BYTES);
        }

        final String templateId;
        final String name;
        try {
            templateId = BespokeFilePaths.requireTemplateId(templateIdRaw);
            name = BespokeFilePaths.requireBasename(nameRaw);
        } catch (IllegalArgumentException bad) {
            return DaemonCommandOutcome.failed("stateful-instance-put-file: " + bad.getMessage());
        }

        return manager.byInstanceId(instanceId)
                .map(inst -> putFile(payload, inst, templateId, name, sizeBytes, sha256Claim.trim(), contentNode))
                .orElseGet(() -> DaemonCommandOutcome.failed(
                        "stateful-instance-put-file: unknown instanceId " + instanceId));
    }

    private DaemonCommandOutcome putFile(JsonNode payload, StatefulInstance inst,
                                         String templateId, String name,
                                         long sizeBytes, String sha256Claim,
                                         JsonNode contentNode) {
        if (!inst.isRunning()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: instance is not running (status="
                            + inst.status() + ")");
        }
        if (BespokeFilePaths.isPlatformManaged(inst)) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: platform-managed instance refuses bespoke files");
        }
        if (!cli.available()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: container runtime unavailable");
        }

        byte[] rawBytes;
        try {
            rawBytes = unsealContent(payload, contentNode);
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: could not open content envelope: " + message(e));
        }

        if (rawBytes.length != sizeBytes) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: decrypted size does not match sizeBytes");
        }
        String actualSha;
        try {
            actualSha = HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(rawBytes));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: sha256 compute failed: " + message(e));
        }
        if (!actualSha.equalsIgnoreCase(sha256Claim)) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: sha256 mismatch");
        }
        // Canonical lowercase hex in the success result.
        String sha256 = actualSha.toLowerCase(java.util.Locale.ROOT);

        String dir = BespokeFilePaths.containerDir(templateId);
        String path = BespokeFilePaths.containerPath(templateId, name);
        String script = "mkdir -p " + BespokeFilePaths.shSingleQuote(dir)
                + " && base64 -d > " + BespokeFilePaths.shSingleQuote(path);
        List<String> argv = List.of(
                cli.cliPath(), "exec", "-i", inst.containerName(),
                "sh", "-c", script);
        byte[] stdin = Base64.getEncoder().encode(rawBytes);

        OciCli.Result r = cli.run(argv, stdin, WRITE_TIMEOUT);
        if (r.timedOut() || r.exitCode() != 0) {
            String err = r.firstErrLine();
            if (err == null || err.isBlank()) {
                err = r.timedOut() ? "timed out" : ("exit=" + r.exitCode());
            }
            log.warn("[T-29] stateful-instance-put-file failed templateId={} name={} sizeBytes={} sha256={}: {}",
                    templateId, name, sizeBytes, sha256, err);
            return DaemonCommandOutcome.failed("stateful-instance-put-file failed: " + err);
        }

        // Metadata only — never log content / raw bytes.
        log.info("[T-29] bespoke put templateId={} name={} sizeBytes={} sha256={}",
                templateId, name, sizeBytes, sha256);
        try {
            var out = objectMapper.createObjectNode();
            out.put("name", name);
            out.put("sizeBytes", sizeBytes);
            out.put("sha256", sha256);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-put-file: could not encode result: " + message(e));
        }
    }

    /**
     * Unseal {@code content} as a {@link SecureEnvelope}. Prefer payload {@code envId} when present and equal to
     * the channel env; otherwise use the channel envId.
     */
    private byte[] unsealContent(JsonNode payload, JsonNode contentNode) throws Exception {
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
        return envelopeClient.open(envelope, AAD_BESPOKE_PUT);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
