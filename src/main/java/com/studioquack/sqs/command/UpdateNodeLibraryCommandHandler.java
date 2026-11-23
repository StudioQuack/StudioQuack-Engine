package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Handles the BE-pushed {@code update-node-library} command (Engine Settings → Stateful Instances → Update on
 * the SQ Node Library platform row): re-runs the built-in node library's download → verify → build → recycle
 * pipeline ON DEMAND so a node-library release rolls out without reinstalling / restarting the engine. Wire
 * contract: payload: {envId} result: {accepted:true} | {accepted:false, reason} — disabled / unenrolled /
 * already in progress The handler never waits for the pipeline: {@link BuiltinNodeLibraryManager#updateNow()}
 * ACKs synchronously and the download + maven-in-image build (minutes) runs on its own thread, exactly like
 * {@code build-oci-image} (: the command channel must not be held open for a build).
 */
@Slf4j
@Component
public class UpdateNodeLibraryCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "update-node-library";

    private final ObjectMapper objectMapper;
    private final BuiltinNodeLibraryManager nodeLibraryManager;
    /**
     * The daemon's enrolled env — a payload naming a DIFFERENT env is a misrouted command.
     */
    private final String enrolledEnvId;

    public UpdateNodeLibraryCommandHandler(ObjectMapper objectMapper,
                                           BuiltinNodeLibraryManager nodeLibraryManager,
                                           @Value("${sqs.channel.env-id:}") String enrolledEnvId) {
        this.objectMapper = objectMapper;
        this.nodeLibraryManager = nodeLibraryManager;
        this.enrolledEnvId = enrolledEnvId == null ? "" : enrolledEnvId.trim();
    }

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
            return DaemonCommandOutcome.failed("update-node-library: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        if (envId == null || envId.isBlank()) {
            return DaemonCommandOutcome.failed("update-node-library: payload missing 'envId'");
        }
        if (!enrolledEnvId.isBlank() && !enrolledEnvId.equals(envId.trim())) {
            // Reachability ≠ authorization: the command channel is authenticated, but a payload
            // naming another env would act on THIS host's single system executor regardless
            // refuse loudly instead of updating the wrong daemon's library.
            return DaemonCommandOutcome.failed("update-node-library: payload envId does not match "
                    + "this daemon's enrolled environment");
        }
        try {
            BuiltinNodeLibraryManager.UpdateAck ack = nodeLibraryManager.updateNow();
            ObjectNode out = objectMapper.createObjectNode();
            out.put("accepted", ack.accepted());
            if (ack.reason() != null) {
                out.put("reason", ack.reason());
            }
            if (!ack.accepted()) {
                log.info("update-node-library: refused env={} reason={}", envId, ack.reason());
            }
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception ex) {
            log.warn("update-node-library.failed env={}: {}", envId, ex.toString());
            return DaemonCommandOutcome.failed("update-node-library failed: " + message(ex));
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
