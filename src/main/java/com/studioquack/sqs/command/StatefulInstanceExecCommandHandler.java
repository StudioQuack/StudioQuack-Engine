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
import java.util.ArrayList;
import java.util.List;

/**
 * Handles the BE-pushed {@code stateful-instance-exec} command: runs a short operator command inside the
 * tracked stateful container ({@code podman exec}) and returns stdout / stderr / exit code, so the engine page
 * can offer a constrained "command line" for a running stateful instance. The command is parsed shell-style
 * (whitespace + quotes) and executed as {@code podman exec <container> <argv...>} with NO shell wrapper — the
 * operator can still opt into a shell explicitly with {@code sh -c '…'} when the image has one.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StatefulInstanceExecCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "stateful-instance-exec";

    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int MAX_TIMEOUT_SECONDS = 120;

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
            return DaemonCommandOutcome.failed("stateful-instance-exec: malformed payload: " + message(parse));
        }
        String envId = text(payload, "envId");
        String instanceId = text(payload, "instanceId");
        String cmd = text(payload, "cmd");
        if (instanceId == null || instanceId.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: payload missing 'instanceId'");
        }
        if (cmd == null || cmd.isBlank()) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: payload missing 'cmd'");
        }
        int timeoutSeconds = clampTimeout(payload.path("timeoutSeconds").isInt()
                ? payload.path("timeoutSeconds").asInt() : DEFAULT_TIMEOUT_SECONDS);

        return manager.byInstanceId(instanceId)
                .map(inst -> exec(envId, inst, cmd.trim(), timeoutSeconds))
                .orElseGet(() -> DaemonCommandOutcome.failed(
                        "stateful-instance-exec: unknown instanceId " + instanceId));
    }

    private DaemonCommandOutcome exec(String envId, StatefulInstance inst, String cmd, int timeoutSeconds) {
        if (!inst.isRunning()) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: instance is not running (status=" + inst.status() + ")");
        }
        if (!cli.available()) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: container runtime unavailable");
        }
        List<String> argv;
        try {
            argv = splitShellLike(cmd);
        } catch (IllegalArgumentException bad) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: " + bad.getMessage());
        }
        if (argv.isEmpty()) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: empty command");
        }
        List<String> full = new ArrayList<>(List.of(cli.cliPath(), "exec", inst.containerName()));
        full.addAll(argv);
        OciCli.Result r = cli.run(full, Duration.ofSeconds(timeoutSeconds));
        try {
            var out = objectMapper.createObjectNode();
            out.put("exitCode", r.exitCode());
            out.put("stdout", r.stdout() == null ? "" : r.stdout());
            out.put("stderr", r.stderr() == null ? "" : r.stderr());
            out.put("timedOut", r.timedOut());
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(out));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("stateful-instance-exec: could not encode result: " + message(e));
        }
    }

    /**
     * Minimal shell-like split: whitespace separates args, single/double quotes group, backslash escapes the next
     * char. Refuses an unterminated quote rather than guess.
     */
    static List<String> splitShellLike(String cmd) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        boolean hasToken = false;
        for (int i = 0; i < cmd.length(); i++) {
            char c = cmd.charAt(i);
            if (c == '\\' && !inSingle && i + 1 < cmd.length()) {
                cur.append(cmd.charAt(++i));
                hasToken = true;
            } else if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
                hasToken = true;
            } else if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                hasToken = true;
            } else if (Character.isWhitespace(c) && !inSingle && !inDouble) {
                if (hasToken) {
                    out.add(cur.toString());
                    cur.setLength(0);
                    hasToken = false;
                }
            } else {
                cur.append(c);
                hasToken = true;
            }
        }
        if (inSingle || inDouble) {
            throw new IllegalArgumentException("unterminated quote in command");
        }
        if (hasToken) {
            out.add(cur.toString());
        }
        return out;
    }

    private static int clampTimeout(int seconds) {
        if (seconds <= 0) {
            return DEFAULT_TIMEOUT_SECONDS;
        }
        return Math.min(seconds, MAX_TIMEOUT_SECONDS);
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node == null ? null : node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
