package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.status.RunLogMode;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Wire payload of a BE-pushed {@code start-run} command. Starts a process instance for an already-deployed
 * workflow, correlated by {@code businessKey == workflowExecutionId} so the status callback + monitoring can
 * find the run.
 *
 * @param workflowExecutionId stable run id — the engine businessKey (required)
 * @param processKey BPMN process key to start the latest version of (used when no pinned id)
 * @param processDefinitionId exact deployed definition id to pin (Designer Run); optional
 * @param seedVars REF-ONLY start variables (optional)
 * @param seedContext optional node context to seal into the store, yielding the seed ref
 * @param logMode per-run log policy (optional; defaults to {@link RunLogMode#STREAM})
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StartRunCommandPayload(
        String workflowExecutionId,
        String processKey,
        String processDefinitionId,
        Map<String, Object> seedVars,
        ExecutionContext seedContext,
        RunLogMode logMode) {

    /**
     * Back-compat convenience: a payload with no explicit log mode (defaults to STREAM).
     */
    public StartRunCommandPayload(String workflowExecutionId, String processKey, String processDefinitionId,
                                  Map<String, Object> seedVars, ExecutionContext seedContext) {
        this(workflowExecutionId, processKey, processDefinitionId, seedVars, seedContext, null);
    }

    /**
     * True when the payload pins an exact deployed definition (Designer Run) rather than "latest".
     */
    public boolean hasPinnedDefinition() {
        return StringUtils.isNotBlank(processDefinitionId);
    }

    /**
     * True when the payload names a process key to start the latest version of.
     */
    public boolean hasProcessKey() {
        return StringUtils.isNotBlank(processKey);
    }

    /**
     * True when the payload can address a startable process (either a pinned id or a process key).
     */
    public boolean isStartable() {
        return StringUtils.isNotBlank(workflowExecutionId) && (hasPinnedDefinition() || hasProcessKey());
    }

    /**
     * The run's log policy, defaulting to {@link RunLogMode#STREAM} when the BE omitted the field.
     */
    public RunLogMode logModeOrDefault() {
        return logMode == null ? RunLogMode.STREAM : logMode;
    }
}
