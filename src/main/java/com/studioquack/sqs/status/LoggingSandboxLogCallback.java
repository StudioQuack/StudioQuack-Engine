package com.studioquack.sqs.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default {@link SandboxLogCallback}: a logging no-op used whenever the secure channel is off (
 * {@code sqs.channel.enabled=false}, the default). The per-node result is already durably held in
 * the client's job queue + encrypted context store; this just skips the control plane mirror when
 * there is no channel to mirror it over.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "false", matchIfMissing = true)
public class LoggingSandboxLogCallback implements SandboxLogCallback {

    @Override
    public void report(SandboxLogRecord record) {
        if (record == null) {
            return;
        }
        log.debug("sandbox-log[local] workflowExecutionId={} node={} status={} exit={}",
                record.workflowExecutionId(), record.nodeId(), record.status(), record.exitCode());
    }

    @Override
    public void reportStarted(SandboxStartRecord record) {
        if (record == null) {
            return;
        }
        log.debug("sandbox-log[local] STARTED workflowExecutionId={} node={} language={}",
                record.workflowExecutionId(), record.nodeId(), record.language());
    }
}
