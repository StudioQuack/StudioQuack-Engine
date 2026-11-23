package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.LifecycleResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * {@code restart}: schedules a graceful self-restart vithe supervisor (systemd/launchd) or, for a Windows
 * scheduled task and installs nothing supervises, the detached relaunch helper. When nothing could start the
 * daemon again, the op returns a clear failure telling the operator to restart from the host, instead of
 * stranding the daemon — see {@link DaemonLifecycleService#requestRestart()}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RestartCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "restart";

    private final ObjectMapper objectMapper;
    private final DaemonLifecycleService lifecycleService;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            LifecycleResult result = lifecycleService.requestRestart();
            return result.accepted()
                    ? DaemonCommandOutcome.ok(objectMapper.writeValueAsString(result))
                    : DaemonCommandOutcome.failed(result.message());
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("restart failed: "
                    + (e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }
}
