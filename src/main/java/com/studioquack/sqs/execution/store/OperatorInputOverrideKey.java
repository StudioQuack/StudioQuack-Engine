package com.studioquack.sqs.execution.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Natural key of one operator input override: the run ({@code workflowExecutionId}, stable
 * across retries AND replays of the same logical run) plus the BPMN flow node id the override
 * addresses. One row per (run, node) — a later {@code apply-inputs} on the same pair overwrites
 * the earlier one (latest override wins).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OperatorInputOverrideKey implements Serializable {

    private String workflowExecutionId;
    private String nodeId;
}
