package com.studioquack.sqs.command;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.halt.ExecutionHaltGuard;
import com.studioquack.sqs.engine.quota.NodeExecutionMeter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * {@code set-node-quota}: the backend pushes the node-allowance snapshot (the heartbeat's {@code nodeQuota}
 * body) over the reverse channel when the subscription renews it. Applying it here instead of waiting for the
 * next beat is what lets an engine paused in limits-exceeded mode resume immediately: the meter takes the
 * fresh allowance and the guard re-evaluates its mode on the spot, restarting job acquisition so paused runs
 * continue from the node they stopped at.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SetNodeQuotaCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "set-node-quota";

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;
    private final NodeExecutionMeter meter;
    private final ExecutionHaltGuard haltGuard;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        Map<String, Object> snapshot;
        try {
            snapshot = command == null || command.payload() == null ? null
                    : objectMapper.readValue(command.payload(), MAP);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("set-node-quota: malformed payload: " + parse.getMessage());
        }
        if (snapshot == null || snapshot.isEmpty()) {
            return DaemonCommandOutcome.failed("set-node-quota: payload requires the nodeQuota snapshot");
        }
        try {
            meter.applySnapshot(snapshot);
            haltGuard.onControlPlaneContact();
            ExecutionHaltGuard.Mode mode = haltGuard.mode();
            log.info("set-node-quota: granted={} consumed={} periodEnd={} -> mode {}",
                    snapshot.get("granted"), snapshot.get("consumed"), snapshot.get("periodEnd"), mode);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(Map.of("mode", mode.name())));
        } catch (Exception ex) {
            log.warn("set-node-quota failed: {}", ex.toString());
            return DaemonCommandOutcome.failed("set-node-quota failed: " + ex.getMessage());
        }
    }
}
