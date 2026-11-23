package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.ContainerLogMerge;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles the BE-pushed {@code stateful-instance-logs} command: reads the tracked instance's container logs (
 * {@code podman logs --tail N}) and returns them so the operator can watch a stateful instance's output from
 * the engine page. (default 200, max 1000), since?
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulInstanceLogsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stateful-instance-logs";

    private static final int DEFAULT_TAIL = 200;
    private static final int MAX_TAIL = 1000;
    private static final Duration LOGS_TIMEOUT = Duration.ofSeconds(30);

    private final ObjectMapper objectMapper;
    private final StatefulInstanceManager manager;
    private final OciCli cli;

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
            return DaemonCommandOutcome.failed("stateful-instance-logs: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String instanceId = text(payload, "instanceId");
        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-instance-logs: payload missing 'instanceId'");
        }
        int tail = clampTail(payload.path("tail").isInt() ? payload.path("tail").asInt() : DEFAULT_TAIL);
        String since = text(payload, "since");

        return manager.byInstanceId(instanceId)
                .map(inst -> readLogs(envId, inst, tail, since))
                .orElseGet(() -> DaemonCommandOutcome.failed(
                        "stateful-instance-logs: unknown instanceId " + instanceId));
    }

    private DaemonCommandOutcome readLogs(String envId, StatefulInstance inst, int tail, String since) {
        String containerName = inst.containerName();
        String containerId = inst.containerId();
        if (containerName == null || containerName.isBlank() || !cli.available()) {
            return DaemonCommandOutcome.failed("stateful-instance-logs: container runtime unavailable");
        }
        List<String> argv = new ArrayList<>(List.of(cli.cliPath(), "logs", "--timestamps",
                "--tail", String.valueOf(tail)));
        if (since != null && !since.isBlank()) {
            argv.add("--since");
            argv.add(since.trim());
        }
        argv.add(containerName);
        OciCli.Result r = cli.run(argv, LOGS_TIMEOUT);
        String logs;
        if (r.ok()) {
            logs = ContainerLogMerge.merge(r.stdout(), r.stderr());
        } else if (inst.lastBootOutput() != null) {
            // A failed start removes its container, so the runtime has nothing left to replay;
            // the output captured just before removal is the only record of why it died.
            logs = "[container " + (containerId == null ? containerName : containerId)
                    + " was removed after a failed start — captured boot output]\n" + inst.lastBootOutput();
        } else {
            log.warn("[SQ-6b] stateful-instance-logs env={} container={} failed: {}", envId, containerName, r.firstErrLine());
            return DaemonCommandOutcome.failed("stateful-instance-logs failed: " + r.firstErrLine());
        }
        try {
            var out = objectMapper.createObjectNode();
            out.put("logs", logs);
            out.put("containerId", containerId == null ? "" : containerId);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("stateful-instance-logs: could not encode result: " + message(e));
        }
    }

    private static int clampTail(int tail) {
        if (tail <= 0) {
            return DEFAULT_TAIL;
        }
        return Math.min(tail, MAX_TAIL);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
