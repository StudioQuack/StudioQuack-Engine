package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Fallback strategy for any {@code bpmnType} not matched by a more specific strategy — the
 * generic sandbox-script node. Must be LAST ({@link Ordered#LOWEST_PRECEDENCE}) so specific
 * strategies win. Also handles the {@code __sq_forced_task__} sentinel the generator uses to
 * force Duckbot boundary nodes / sub-process roots through the sandbox path.
 *
 * <p>Branches on the template {@code jobType}:
 * <ul>
 *   <li>remote/long work (script/function) → async {@code ServiceTask} + {@code ReceiveTask}
 *       wait-state + error gate (non-blocking);</li>
 *   <li>guaranteed-fast in-JVM work → plain synchronous delegate.</li>
 * </ul>
 */
@Order(Ordered.LOWEST_PRECEDENCE)
@Component("activitiServiceTaskStrategy")
@RequiredArgsConstructor
public class ActivitiServiceTaskStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiBpmnElementHelper helper;

    /** Exposes the shared element helper for parameter-driven compound expansions. */
    public ActivitiBpmnElementHelper elementHelper() {
        return helper;
    }

    @Override
    public boolean supports(String bpmnType) {
        return true; // fallback for every unmatched bpmnType
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        TemplateDto template = node.getData();
        String jobType = template == null ? null : template.getJobType();
        if (helper.isAsyncSandbox(jobType)) {
            return helper.emitAsyncSandboxTask(model, process, node, nodeLabel, jobType);
        }
        return helper.emitSyncDelegateTask(model, process, node, nodeLabel, jobType);
    }
}
