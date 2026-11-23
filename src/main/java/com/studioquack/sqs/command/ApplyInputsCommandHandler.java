package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.store.OperatorInputOverrideStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Repair — {@code apply-inputs}: stores the operator-edited inbound list of the FAILED node X as a sealed
 * override keyed by {@code (workflowExecutionId, nodeId)} ({@link OperatorInputOverrideStore}). Because the
 * key is the run's business key — not a per-attempt {@code execution_context} row — the edit takes effect on
 * EVERY re-run path of the same run: Retry (the held {@code hold_X} job requeues and {@code node_X}
 * re-dispatches), Replay (the new instance runs under the SAME business key), and Skip &amp; Continue (the
 * SKIPPED sandbox-log row mirrors the edited inputs).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ApplyInputsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (must be added to the control plane).
     */
    public static final String TYPE = "apply-inputs";

    /**
     * The generator's failure-hold activity prefix ({@code hold_<nodeId>}).
     */
    private static final String HOLD_ACTIVITY_PREFIX = "hold_";

    private final ObjectMapper objectMapper;
    private final SqsEngine sqsEngine;
    private final OperatorInputOverrideStore overrideStore;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        ApplyInputsPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), ApplyInputsPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("apply-inputs: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.hasProcessInstance()) {
            return DaemonCommandOutcome.failed("apply-inputs: payload requires processInstanceId");
        }
        if (payload.inputs() == null && payload.outputs() == null) {
            return DaemonCommandOutcome.failed("apply-inputs: payload requires inputs and/or outputs array");
        }

        // The BE may address the run by its workflowExecutionId (business key); map it to the live PI.
        String pid = sqsEngine.resolveProcessInstanceId(payload.processInstanceId());
        try {
            // ACTIVE check first: parameters can only take effect on a run held at a failed step
            // (an ended run needs Replay, not a parameter patch).
            ProcessInstance instance = sqsEngine.findProcessInstance(pid);
            if (instance == null) {
                return DaemonCommandOutcome.failed(
                        "apply-inputs: run " + pid + " has ended — parameters can only be changed "
                                + "on a run held at a failed step");
            }
            String nodeId = heldNodeOf(pid);
            if (nodeId == null) {
                nodeId = singleTargetNodeId(payload.inputs());
            }
            if (nodeId == null) {
                nodeId = singleTargetNodeId(payload.outputs());
            }
            if (nodeId == null) {
                return DaemonCommandOutcome.failed(
                        "apply-inputs: no failed step is held on run " + pid);
            }
            String workflowExecutionId = instance.getBusinessKey();
            if (StringUtils.isBlank(workflowExecutionId)) {
                return DaemonCommandOutcome.failed(
                        "apply-inputs: no business key (workflowExecutionId) on processInstanceId=" + pid);
            }
            // Latest-wins at the ROW level: a call carrying one lane keeps the other's stored value.
            if (payload.inputs() != null) {
                overrideStore.saveOverride(workflowExecutionId, nodeId, payload.inputs());
            }
            if (payload.outputs() != null) {
                overrideStore.saveOutputsOverride(workflowExecutionId, nodeId, payload.outputs());
            }
            log.info("repair.apply-inputs.done pi={} wfeId={} node={} inputCount={} outputCount={}",
                    pid, workflowExecutionId, nodeId, countOrDash(payload.inputs()), countOrDash(payload.outputs()));
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                    new ApplyInputsResult(pid, nodeId, sizeOrNull(payload.inputs()),
                            sizeOrNull(payload.outputs()))));
        } catch (Exception ex) {
            log.warn("repair.apply-inputs.failed pi={}: {}", pid, ex.toString());
            return DaemonCommandOutcome.failed("apply-inputs failed: " + message(ex));
        }
    }

    /**
     * The node whose failure hold is parked on this instance: the dead-letter job whose execution's CURRENT
     * activity is {@code hold_<X>} yields {@code X} (mirrors {@code SkipStepCommandHandler#findHoldJob} — a
     * concurrent retry between the listing and the activity probe resolves to a vanished execution, a benign
     * no-match). {@code null} when the run holds no failed step.
     */
    private String heldNodeOf(String pid) {
        List<Job> jobs = sqsEngine.deadLetterJobsForProcessInstance(pid).list();
        for (Job job : jobs) {
            String activityId = sqsEngine.findExecutionActivityId(job.getExecutionId());
            if (activityId != null && activityId.startsWith(HOLD_ACTIVITY_PREFIX)) {
                return activityId.substring(HOLD_ACTIVITY_PREFIX.length());
            }
        }
        return null;
    }

    /**
     * Fallback node resolution: the {@code targetNodeId} shared by EVERY addressed input row, or {@code null} when
     * rows name different nodes (ambiguous — the caller must hold a failed step instead). Unaddressed rows carry
     * no vote.
     */
    private static String singleTargetNodeId(List<InOutValueDto> inputs) {
        String target = null;
        if (inputs == null) {
            return null;
        }
        for (InOutValueDto in : inputs) {
            if (in == null || StringUtils.isBlank(in.getTargetNodeId())) {
                continue;
            }
            if (target == null) {
                target = in.getTargetNodeId();
            } else if (!target.equals(in.getTargetNodeId())) {
                return null;
            }
        }
        return target;
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Size for the result payload; {@code null} when the lane was absent from the call.
     */
    private static Integer sizeOrNull(List<InOutValueDto> values) {
        return values == null ? null : values.size();
    }

    /**
     * Log-shape counterpart of {@link #sizeOrNull} — never logs values (PHI).
     */
    private static Object countOrDash(List<InOutValueDto> values) {
        return values == null ? "-" : values.size();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ApplyInputsPayload(String processInstanceId, List<InOutValueDto> inputs,
                              List<InOutValueDto> outputs) {
        boolean hasProcessInstance() {
            return StringUtils.isNotBlank(processInstanceId);
        }
    }

    /**
     * Counts are per-lane and omitted when the lane was absent (an absent lane keeps its stored value).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record ApplyInputsResult(String processInstanceId, String nodeId, Integer inputCount,
                             Integer outputCount) {
    }
}
