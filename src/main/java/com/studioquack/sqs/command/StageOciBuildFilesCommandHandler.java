package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Batch variant — handles BE-pushed {@code stage-oci-build-files}: many sealed files in ONE command so a
 * workspace build costs a handful of overlay round-trips instead of one per file. Per-file semantics (AAD
 * unseal, size + SHA-256 verify, path rules, store caps) are inherited from
 * {@link StageOciBuildFileCommandHandler#stageOne} unchanged.
 */
@Slf4j
@Component
public class StageOciBuildFilesCommandHandler extends StageOciBuildFileCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE_BATCH = "stage-oci-build-files";

    /**
     * Defence-in-depth bound on files per command; the BE packs well below this.
     */
    static final int MAX_BATCH_FILES = 2000;

    private final ObjectMapper batchMapper;

    public StageOciBuildFilesCommandHandler(ObjectMapper objectMapper, OciBuildContextStore contextStore,
                                            SqsChannelProperties channel) {
        super(objectMapper, contextStore, channel);
        this.batchMapper = objectMapper;
    }

    @Override
    public String type() {
        return TYPE_BATCH;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        JsonNode payload;
        try {
            payload = batchMapper.readTree(command.payload() == null ? "{}" : command.payload());
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-files: malformed payload: " + message(parse));
        }

        String buildId = text(payload, "buildId");
        if (buildId == null || buildId.isBlank()) {
            return DaemonCommandOutcome.failed("stage-oci-build-files: payload missing 'buildId'");
        }
        JsonNode files = payload.get("files");
        if (files == null || !files.isArray() || files.isEmpty()) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-files: payload missing non-empty 'files' array");
        }
        if (files.size() > MAX_BATCH_FILES) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-files: files exceeds " + MAX_BATCH_FILES);
        }

        ArrayNode staged = batchMapper.createArrayNode();
        int i = 0;
        for (JsonNode fileNode : files) {
            DaemonCommandOutcome one = stageOne(buildId, fileNode, payload);
            if (!one.succeeded()) {
                String path = text(fileNode, "path");
                return DaemonCommandOutcome.failed("stage-oci-build-files: file[" + i + "]"
                        + (path == null ? "" : " " + path) + " failed: " + one.error());
            }
            try {
                staged.add(batchMapper.readTree(one.output()));
            } catch (Exception e) {
                return DaemonCommandOutcome.failed(
                        "stage-oci-build-files: could not read staged result: " + message(e));
            }
            i++;
        }

        log.info("[T-31] stage-oci-build-files ok buildId={} count={}", buildId, staged.size());
        ObjectNode out = batchMapper.createObjectNode();
        out.put("count", staged.size());
        out.set("files", staged);
        try {
            return DaemonCommandOutcome.ok(batchMapper.writeValueAsString(out));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed(
                    "stage-oci-build-files: could not encode result: " + message(e));
        }
    }
}
