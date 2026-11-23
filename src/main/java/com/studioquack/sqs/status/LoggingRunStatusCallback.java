package com.studioquack.sqs.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default {@link RunStatusCallback}: a logging no-op used whenever the secure channel is off (
 * {@code sqs.channel.enabled=false}, the default). It records the status transition locally so the
 * daemon boots and every test runs without dialing a BE — the run index simply isn't mirrored when
 * there is no channel to mirror it over.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "false", matchIfMissing = true)
public class LoggingRunStatusCallback implements RunStatusCallback {

    @Override
    public void report(String workflowExecutionId, Status status, String detail) {
        // No BE to notify (channel off): record locally only. Never throws.
        log.info("run-status[local] workflowExecutionId={} status={} detail={}",
                workflowExecutionId, status, detail);
    }
}
