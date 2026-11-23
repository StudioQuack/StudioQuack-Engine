package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.StringUtils;

/**
 * The shared, lenient payload for every DuckTales repair command the BE pushes to the daemon. All four repair
 * verbs address a run by its process-instance identity ({@link #processInstanceId}) — never an incidentKey (
 * invariant): the incidentKey is an Activiti dead-letter job id, tenant-scoped and not stable across a replay,
 * so repairs key on the process instance.
 *
 * @param processInstanceId the run identity every repair addresses (required)
 * @param jobId the dead-letter job to retry (optional; {@code retry-incident} only)
 * @param targetFlowNodeId the flow-node whose waiting step to skip ({@code skip-step} only)
 * @param reason a short, non-PHI audit reason ({@code cancel-instance})
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RepairCommandPayload(String processInstanceId,
                                   String jobId,
                                   String targetFlowNodeId,
                                   String reason) {

    /**
     * Every repair needs a process-instance id.
     */
    public boolean hasProcessInstance() {
        return StringUtils.isNotBlank(processInstanceId);
    }

    public boolean hasJobId() {
        return StringUtils.isNotBlank(jobId);
    }

    public boolean hasTargetFlowNode() {
        return StringUtils.isNotBlank(targetFlowNodeId);
    }

    /**
     * The cancel reason, or a stable default when none was supplied.
     */
    public String reasonOrDefault() {
        return StringUtils.isNotBlank(reason) ? reason : "repair: cancel-instance";
    }
}
