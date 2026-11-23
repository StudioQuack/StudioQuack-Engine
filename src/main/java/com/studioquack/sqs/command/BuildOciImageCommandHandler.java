package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService.BuildRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Handles BE-pushed {@code build-oci-image}: validate + accept immediately, then
 * {@link OciImageBuildService#submit} runs {@code podman build} asynchronously. Wire contract: payload:
 * {envId, buildId, ref, displayName?, wallClockSeconds?} result: {accepted:true, buildId, ref} — NEVER waits
 * for the build Missing staged context fails the command outcome without submitting (: command channel must
 * not be held open for the build).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BuildOciImageCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "build-oci-image";

    private final ObjectMapper objectMapper;
    private final OciBuildContextStore contextStore;
    private final OciImageBuildService buildService;

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
            return DaemonCommandOutcome.failed("build-oci-image: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String buildId = text(payload, "buildId");
        String ref = text(payload, "ref");
        String displayName = text(payload, "displayName");
        if (buildId == null || buildId.isBlank()) {
            return DaemonCommandOutcome.failed("build-oci-image: payload missing 'buildId'");
        }
        if (ref == null || ref.isBlank()) {
            return DaemonCommandOutcome.failed("build-oci-image: payload missing 'ref'");
        }
        // : refuse before submit when the staged context is gone — do not queue a doomed build.
        if (!contextStore.contextExists(buildId)) {
            return DaemonCommandOutcome.failed(
                    "build-oci-image: build context missing for buildId=" + buildId);
        }
        int wall = payload.hasNonNull("wallClockSeconds")
                ? payload.get("wallClockSeconds").asInt(OciImageBuildService.DEFAULT_WALL_CLOCK_SECONDS)
                : OciImageBuildService.DEFAULT_WALL_CLOCK_SECONDS;
        if (wall <= 0) {
            wall = OciImageBuildService.DEFAULT_WALL_CLOCK_SECONDS;
        }
        try {
            buildService.submit(new BuildRequest(envId, buildId, ref, displayName, wall));
            ObjectNode out = objectMapper.createObjectNode();
            out.put("accepted", true);
            out.put("buildId", buildId);
            out.put("ref", ref);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception ex) {
            log.warn("build-oci-image.failed buildId={} ref={}: {}", buildId, ref, ex.toString());
            return DaemonCommandOutcome.failed("build-oci-image failed: " + message(ex));
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
