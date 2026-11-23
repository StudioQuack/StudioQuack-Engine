package com.studioquack.sqs.command;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.deps.DependencyEntry;
import com.studioquack.sqs.deps.DependencyInstaller;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Goal B — {@code set-deps}: applies the backend-pushed dependency manifest to the live
 * {@link DependencyManifestHolder}. Auto-discovered by {@code DaemonCommandRegistry}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SetDepsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; matches the control plane.
     */
    public static final String TYPE = "set-deps";

    private final ObjectMapper objectMapper;
    private final DependencyManifestHolder holder;
    private final DependencyInstaller installer;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String payload = command == null ? null : command.payload();
        if (payload == null || payload.isBlank()) {
            return DaemonCommandOutcome.failed("set-deps: missing manifest payload");
        }
        List<DependencyEntry> manifest;
        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode depsNode = root.get("deps");
            if (depsNode == null || depsNode.isNull()) {
                // Tolerate a bare array too.
                depsNode = root.isArray() ? root : null;
            }
            if (depsNode == null) {
                return DaemonCommandOutcome.failed("set-deps: payload missing 'deps' array");
            }
            manifest = objectMapper.convertValue(depsNode, new TypeReference<List<DependencyEntry>>() {});
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("set-deps: malformed manifest payload: " + message(parse));
        }
        holder.apply(manifest);
        // Goal 1 — kick the install pipeline for the freshly-applied manifest. Non-blocking: the
        // installer submits work to its bounded background pool and returns, so we never stall the
        // heartbeat command thread. reconcile() is idempotent (installed/in-flight tuples are skipped).
        installer.reconcile(manifest);
        int count = manifest == null ? 0 : manifest.size();
        return DaemonCommandOutcome.ok("{\"applied\":true,\"count\":" + count + "}");
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
