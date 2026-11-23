package com.studioquack.sqs.usertask;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Channel-off stand-in: records that an approval parked, without dialing any backend. The task
 * still waits; with no backend there is nobody to notify and no decision can arrive.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "false", matchIfMissing = true)
public class LoggingUserTaskApprovalSink implements UserTaskApprovalSink {

    @Override
    public Outcome open(Map<String, Object> report) {
        log.info("user-task.approval[local] parked task={} node={} workflow={}",
                report.get("taskId"), report.get("nodeId"), report.get("workflowId"));
        return Outcome.ACCEPTED;
    }
}
