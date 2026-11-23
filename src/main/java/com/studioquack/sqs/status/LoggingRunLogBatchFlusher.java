package com.studioquack.sqs.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default {@link RunLogBatchFlusher}: a logging no-op used whenever the secure channel is off (
 * {@code sqs.channel.enabled=false}, the default) — there is no backend to flush to. The daemon
 * still holds every row locally in {@code sandbox_log}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "false", matchIfMissing = true)
public class LoggingRunLogBatchFlusher implements RunLogBatchFlusher {

    @Override
    public boolean flush(String workflowExecutionId) {
        // No BE to flush to (channel off): rows stay in the local sandbox_log store only. Report
        // "durable" so the reported-purge gate stays unaffected by a channel-less boot (the purge
        // is keyed on the terminal-status ack, which likewise never happens without a channel).
        log.debug("run-log-batch[local]: no channel — BATCH rows for run={} stay local only",
                workflowExecutionId);
        return true;
    }
}
