package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.runtime.HeldJobRequeue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.runtime.Job;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Repair — {@code retry-incident}: moves a dead-letter (incident) job back to executable so the async executor
 * re-runs it ({@code SqsEngine.retryDeadLetterJob} → Activiti {@code moveDeadLetterJobToExecutableJob}).
 * Addresses the run by process-instance identity; an explicit {@code jobId} is an optional selector (otherwise
 * every dead-letter job on the instance is retried; a {@code jobId} that matches no dead-letter job on the
 * instance is refused and nothing is moved — the BE never carries an id it cannot resolve on the addressed
 * instance).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryIncidentCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (must be added to the control plane).
     */
    public static final String TYPE = "retry-incident";

    private final ObjectMapper objectMapper;
    private final SqsEngine sqsEngine;
    private final HeldJobRequeue heldJobRequeue;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        RepairCommandPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), RepairCommandPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("retry-incident: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.hasProcessInstance()) {
            return DaemonCommandOutcome.failed("retry-incident: payload requires processInstanceId");
        }

        // The BE may address the run by its workflowExecutionId (business key); map it to the live PI.
        String pid = sqsEngine.resolveProcessInstanceId(payload.processInstanceId());
        try {
            // An ended run (completed/failed/cancelled before the failure-hold, or a sub-PI child)
            // has no live token to re-dispatch — only a replay can restart it.
            if (sqsEngine.findProcessInstance(pid) == null) {
                return DaemonCommandOutcome.failed(
                        "retry-incident: run " + pid + " has ended — it can no longer be retried; "
                                + "use Replay");
            }
            int retried = payload.hasJobId()
                    ? retryOne(pid, payload.jobId())
                    : retryAllForInstance(pid);
            if (retried == 0) {
                return DaemonCommandOutcome.failed(
                        "retry-incident: no dead-letter job found for processInstanceId="
                                + pid
                                + (payload.hasJobId() ? " jobId=" + payload.jobId() : ""));
            }
            log.info("repair.retry-incident.done pi={} jobId={} retried={}",
                    pid, payload.jobId(), retried);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new RetryResult(pid, retried)));
        } catch (Exception ex) {
            log.warn("repair.retry-incident.failed pi={}: {}", pid, ex.toString());
            return DaemonCommandOutcome.failed("retry-incident failed: " + message(ex));
        }
    }

    /**
     * Retries a specific dead-letter job only if it belongs to the addressed instance. A {@code jobId} matching
     * nothing on the instance (missing or foreign id) is refused: 0 is returned, no job is moved, and the caller
     * answers with the "no dead-letter job found" failure.
     */
    private int retryOne(String processInstanceId, String jobId) {
        Job job = sqsEngine.deadLetterJobsForProcessInstance(processInstanceId).jobId(jobId).singleResult();
        if (job == null) {
            log.debug("repair.retry-incident pi={} jobId={} matched no dead-letter job on the "
                    + "instance — refusing, nothing is moved", processInstanceId, jobId);
            return 0;
        }
        pinRetryFlagForJob(processInstanceId, job);
        heldJobRequeue.requeue(job);
        return 1;
    }

    /**
     * Retries every dead-letter job on the instance; returns how many were moved to executable.
     */
    private int retryAllForInstance(String processInstanceId) {
        List<Job> jobs = sqsEngine.deadLetterJobsForProcessInstance(processInstanceId).list();
        int retried = 0;
        for (Job job : jobs) {
            pinRetryFlagForJob(processInstanceId, job);
            heldJobRequeue.requeue(job);
            retried++;
        }
        return retried;
    }

    /**
     * Marks the held node with a one-shot {@code __sq_retry_<nodeId>} pin before requeueing so the resulting
     * sandbox-log row records {@code retry=true} — distinguishing a Quack Logs Retry from a normal
     * multi-invocation (loop / function-port sub-PI).
     */
    private void pinRetryFlagForJob(String processInstanceId, Job job) {
        if (job == null) {
            return;
        }
        String activityId = sqsEngine.findExecutionActivityId(job.getExecutionId());
        String nodeId = heldNodeIdOf(activityId);
        if (nodeId != null) {
            sqsEngine.setProcessVariables(processInstanceId,
                    Map.of(EngineVariableNames.retryNodeVariableName(nodeId), Boolean.TRUE));
        }
    }

    private static String heldNodeIdOf(String activityId) {
        if (activityId == null || activityId.isBlank()) {
            return null;
        }
        if (activityId.startsWith("hold_")) {
            String rest = activityId.substring("hold_".length());
            return rest.isBlank() ? null : rest;
        }
        if (activityId.startsWith("node_")) {
            String rest = activityId.substring("node_".length());
            return rest.isBlank() ? null : rest;
        }
        return null;
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Success output shipped to the BE.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record RetryResult(String processInstanceId, int jobsRetried) {
    }
}
