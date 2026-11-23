package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.runtime.HeldJobRequeue;
import com.studioquack.sqs.runtime.EngineTriggerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Repair — {@code skip-step}: skips a sandbox step that is parked at its {@code ReceiveTask} by raising
 * {@code BpmnError("SQ_SKIP")} on the waiting execution (the Activiti-9-safe mechanism that replaces the
 * backend's {@code skipStep() throws UnsupportedOperationException}). This resolves the live execution parked
 * at {@code wait_<targetFlowNodeId>} and triggers it through {@link EngineTriggerService#fail} with the
 * reserved {@link EngineVariableNames#SKIP_ERROR_CODE}; the generated {@code sqErrorGateDelegate} then raises
 * the {@code BpmnError} and the interrupting error boundary routes the token off the step (to the step's
 * configured {@code SQ_SKIP} handler if one is wired, else the catch-all shared error end).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SkipStepCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (must be added to the control plane).
     */
    public static final String TYPE = "skip-step";

    /**
     * The generator's wait-state activity prefix ({@code wait_<nodeId>}).
     */
    private static final String WAIT_ACTIVITY_PREFIX = "wait_";
    /**
     * The generator's failure-hold activity prefix ({@code hold_<nodeId>}).
     */
    private static final String HOLD_ACTIVITY_PREFIX = "hold_";
    /**
     * The generator's sandbox-node activity prefix ({@code node_<nodeId>}).
     */
    private static final String NODE_ACTIVITY_PREFIX = "node_";

    private final ObjectMapper objectMapper;
    private final SqsEngine sqsEngine;
    private final EngineTriggerService engineTriggerService;
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
            return DaemonCommandOutcome.failed("skip-step: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.hasProcessInstance() || !payload.hasTargetFlowNode()) {
            return DaemonCommandOutcome.failed(
                    "skip-step: payload requires processInstanceId + targetFlowNodeId");
        }

        // The BE may address the run by its workflowExecutionId (business key); map it to the live PI.
        String pid = sqsEngine.resolveProcessInstanceId(payload.processInstanceId());
        try {
            String waitActivityId = WAIT_ACTIVITY_PREFIX + payload.targetFlowNodeId();
            String executionId = sqsEngine.findUnfinishedWaitExecution(pid, waitActivityId);
            if (executionId == null) {
                // Nothing parked at the wait: a FAILED root run holds its token at hold_<nodeId>.
                return skipHeldNode(pid, payload.targetFlowNodeId(), waitActivityId);
            }
            // Raise the reserved SQ_SKIP BpmnError on the parked step (vithe error gate + boundary).
            engineTriggerService.fail(executionId, EngineVariableNames.SKIP_ERROR_CODE,
                    "skip-step repair on " + payload.targetFlowNodeId());
            log.info("repair.skip-step.done pi={} node={} exec={}",
                    pid, payload.targetFlowNodeId(), executionId);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new SkipResult(pid, payload.targetFlowNodeId(),
                            EngineVariableNames.SKIP_ERROR_CODE, null)));
        } catch (Exception ex) {
            log.warn("repair.skip-step.failed pi={} node={}: {}",
                    pid, payload.targetFlowNodeId(), ex.toString());
            return DaemonCommandOutcome.failed("skip-step failed: " + message(ex));
        }
    }

    /**
     * Skip &amp; Continue for a run HELD at {@code hold_<targetFlowNodeId>}: pins the node id under
     * {@link EngineVariableNames#SKIP_NODE} on the process-instance scope, then requeues the hold's dead-letter
     * job so the token loops back to {@code node_<targetFlowNodeId>} and re-dispatches — the dispatch delegate
     * consumes the pin and short-circuits without a sandbox. The pin is set BEFORE the requeue so the async
     * executor can never observe the hold pass without it.
     */
    private DaemonCommandOutcome skipHeldNode(String pid, String targetFlowNodeId, String waitActivityId)
            throws Exception {
        // An ended run (completed/cancelled before or after the hold) has no live token to move
        // only a replay can restart it.
        ProcessInstance instance = sqsEngine.findProcessInstance(pid);
        if (instance == null) {
            return DaemonCommandOutcome.failed("skip-step: run " + pid
                    + " has ended — it can no longer be skipped; use Replay");
        }
        String holdActivityId = HOLD_ACTIVITY_PREFIX + targetFlowNodeId;
        Job holdJob = findHoldJob(pid, holdActivityId);
        if (holdJob == null) {
            return DaemonCommandOutcome.failed("skip-step: no waiting execution parked at "
                    + waitActivityId + " and no held job at " + holdActivityId
                    + " for processInstanceId=" + pid);
        }
        // The hold's retry target must be a real sandbox/Docling ServiceTask. A call activity's
        // failure holds at the call activity itself (there is no node_<id> task) — skipping a
        // sub-workflow CALL would mean fabricating its outputs, which is not supported.
        if (!isDispatchableNode(instance.getProcessDefinitionId(), targetFlowNodeId)) {
            return DaemonCommandOutcome.failed("skip-step: Skip & Continue is not supported for "
                    + "sub-workflow calls; use Retry or Replay");
        }
        sqsEngine.setProcessVariables(pid, Map.of(EngineVariableNames.SKIP_NODE, targetFlowNodeId));
        heldJobRequeue.requeue(holdJob);
        log.info("repair.skip-step.held pi={} node={} job={} — skip pinned, hold requeued",
                pid, targetFlowNodeId, holdJob.getId());
        return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                new SkipResult(pid, targetFlowNodeId, null, Boolean.TRUE)));
    }

    /**
     * The dead-letter job parked at {@code holdActivityId} on this instance, or {@code null}. The match is on the
     * job execution's CURRENT activity: a failure hold dead-letters with the token still at the
     * {@code hold_<nodeId>} task.
     */
    private Job findHoldJob(String pid, String holdActivityId) {
        List<Job> jobs = sqsEngine.deadLetterJobsForProcessInstance(pid).list();
        for (Job job : jobs) {
            if (holdActivityId.equals(sqsEngine.findExecutionActivityId(job.getExecutionId()))) {
                return job;
            }
        }
        return null;
    }

    /**
     * Whether the deployed model has a {@code node_<targetFlowNodeId>} ServiceTask — the sandbox / Docling
     * dispatch task the hold's retry flow loops back to. Anything else (a call activity, a missing element) means
     * the hold does not belong to a skippable dispatch node.
     */
    private boolean isDispatchableNode(String processDefinitionId, String targetFlowNodeId) {
        BpmnModel model = sqsEngine.readDeployedModel(processDefinitionId);
        if (model == null || model.getMainProcess() == null) {
            return false;
        }
        FlowElement element = model.getMainProcess().getFlowElement(NODE_ACTIVITY_PREFIX + targetFlowNodeId);
        return element instanceof ServiceTask;
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Success output shipped to the BE. {@code raisedErrorCode} is set on the parked-wait path (the
     * {@code SQ_SKIP} BpmnError raised directly); {@code resumedFromHold} marks the held-run path (skip pinned,
     * hold requeued — the dispatch delegate applies the skip on re-dispatch).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record SkipResult(String processInstanceId, String targetFlowNodeId, String raisedErrorCode,
                      Boolean resumedFromHold) {
    }
}
