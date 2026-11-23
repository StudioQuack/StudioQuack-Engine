package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Handles the BE-pushed {@code stateful-instance-delete-file} command: removes a single basename under
 * {@code /data/sq-bespoke/<templateId>/} inside a running user-owned stateful container via
 * {@code podman exec … rm -f <path>} (path as a separate argv element — no shell). Wire contract: payload:
 * {envId, imageRef, instanceId, templateId, name} result: {} (backend only checks error field blank)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulInstanceDeleteFileCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stateful-instance-delete-file";

    private static final Duration DELETE_TIMEOUT = Duration.ofSeconds(30);

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
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: malformed payload: " + message(parse));
        }

        String instanceId = text(payload, "instanceId");
        String templateIdRaw = text(payload, "templateId");
        String nameRaw = text(payload, "name");

        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: payload missing 'instanceId'");
        }
        if (templateIdRaw == null || templateIdRaw.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: payload missing 'templateId'");
        }
        if (nameRaw == null || nameRaw.isBlank()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: payload missing 'name'");
        }

        final String templateId;
        final String name;
        try {
            templateId = BespokeFilePaths.requireTemplateId(templateIdRaw);
            name = BespokeFilePaths.requireBasename(nameRaw);
        } catch (IllegalArgumentException bad) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: " + bad.getMessage());
        }

        return manager.byInstanceId(instanceId)
                .map(inst -> deleteFile(inst, templateId, name))
                .orElseGet(() -> DaemonCommandOutcome.failed(
                        "stateful-instance-delete-file: unknown instanceId " + instanceId));
    }

    private DaemonCommandOutcome deleteFile(StatefulInstance inst, String templateId, String name) {
        if (!inst.isRunning()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: instance is not running (status="
                            + inst.status() + ")");
        }
        if (BespokeFilePaths.isPlatformManaged(inst)) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: platform-managed instance refuses bespoke files");
        }
        if (!cli.available()) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: container runtime unavailable");
        }

        // Exact resolved path — refuse anything outside the template folder (defence in depth).
        String exactPath = BespokeFilePaths.containerPath(templateId, name);
        String expectedPrefix = BespokeFilePaths.containerDir(templateId) + "/";
        if (!exactPath.startsWith(expectedPrefix) || exactPath.contains("..")) {
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file: path refuses outside /data/sq-bespoke/"
                            + templateId + "/");
        }

        // Argv form — path is its own element; no shell injection surface.
        List<String> argv = List.of(
                cli.cliPath(), "exec", inst.containerName(),
                "rm", "-f", exactPath);

        OciCli.Result r = cli.run(argv, DELETE_TIMEOUT);
        if (r.timedOut() || r.exitCode() != 0) {
            String err = r.firstErrLine();
            if (err == null || err.isBlank()) {
                err = r.timedOut() ? "timed out" : ("exit=" + r.exitCode());
            }
            log.warn("[T-29] stateful-instance-delete-file failed templateId={} name={}: {}",
                    templateId, name, err);
            return DaemonCommandOutcome.failed(
                    "stateful-instance-delete-file failed: " + err);
        }

        log.info("[T-29] bespoke delete templateId={} name={}", templateId, name);
        return DaemonCommandOutcome.ok("{}");
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
