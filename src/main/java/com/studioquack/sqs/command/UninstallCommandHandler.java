package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.LifecycleResult;
import com.studioquack.sqs.maintenance.UninstallService;
import com.studioquack.sqs.maintenance.UninstallService.UninstallSummary;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * ITEM 4 — {@code uninstall}: BE-pushed engine teardown. Purges the OS-store secrets this daemon owns (the
 * security fix), optionally removes the dependency cache, best-effort reaps leftover sandbox containers, ACKs
 * with a summary, then schedules a graceful exit.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UninstallCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "uninstall";

    private final ObjectMapper objectMapper;
    private final UninstallService uninstallService;
    private final DaemonLifecycleService lifecycleService;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            boolean removeDependencies = parseFlag(command, "removeDependencies");
            boolean removePodman = parseFlag(command, "removePodman");
            UninstallSummary summary = uninstallService.purge(removeDependencies);
            // ITEMS 2 + 3 — pass both opt-ins so the detached side-car drops the dependency cache and
            // uninstalls Podman when the operator ticked them, alongside the unconditional teardown
            // (host unit, sandbox containers, and the WireGuard adapter this engine created).
            LifecycleResult exit = lifecycleService.requestUninstall(removeDependencies, removePodman);

            ObjectNode ack = objectMapper.createObjectNode();
            ack.put("purgedSecrets", summary.purgedSecrets());
            ack.put("dependenciesRemoved", summary.dependenciesRemoved());
            ack.put("reapedContainers", summary.reapedContainers());
            ack.put("exitScheduled", exit.accepted());
            ack.put("hostMechanism", exit.mechanism());
            ack.put("message", exit.message());
            // Surface purge + host-unload notes so BE logs show whether launchd/systemd was cleared.
            summary.notes().forEach(note -> ack.withArray("notes").add(note));
            if (exit.message() != null && exit.message().contains("hostNotes=")) {
                ack.withArray("notes").add(exit.message());
            }
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(ack));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("uninstall failed: "
                    + (e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }

    /**
     * Reads a boolean opt-in from the (optional) JSON payload; defaults to false. False is the safe default for
     * every flag here: each one deletes something from the operator's machine that the engine did not necessarily
     * put there exclusively, so a malformed or missing payload must never be read as consent.
     */
    private boolean parseFlag(DaemonCommand command, String field) {
        String payload = command == null ? null : command.payload();
        if (payload == null || payload.isBlank()) {
            return false;
        }
        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode flag = root.get(field);
            return flag != null && flag.asBoolean(false);
        } catch (Exception e) {
            log.warn("uninstall: could not parse payload, defaulting {}=false: {}", field, e.getMessage());
            return false;
        }
    }
}
