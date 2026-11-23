package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.UserTask;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code user.task} → an approval wait state. The node parks the token as a plain Activiti
 * {@link UserTask} — persisted, holding no thread, container or poll while it waits — and is
 * released only by a decision delivered from the control plane ({@code correlate-message},
 * {@code eventKind=TASK}, see {@code MessageCorrelationService#decideUserTask}).
 *
 * <p>Activiti's own assignment model ({@code assignee}/{@code candidateGroups}/…) is deliberately
 * not used: who is asked is decided by the control plane inside the workflow's organization, never
 * by the engine. The node's {@code assignees}/{@code teams}/{@code subject}/{@code message}
 * parameters ride the deployed graph and are read at park time by the {@code create} task listener
 * ({@code userTaskApprovalListener}), which reports the pending approval to the control plane after the
 * engine transaction commits.
 *
 * <p>Authored boundaries host on the user task itself — it IS the wait state, so TIMER / MESSAGE /
 * SIGNAL boundaries work natively, and an ERROR boundary catches a {@code BpmnError} from a task
 * listener.
 */
@Order(1)
@Component("activitiUserTaskStrategy")
@RequiredArgsConstructor
public class ActivitiUserTaskStrategy implements ActivitiFlowNodeStrategy {

    /** Spring bean the {@code create} task listener resolves to. */
    static final String APPROVAL_LISTENER_DELEGATE = "${userTaskApprovalListener}";
    /** Activiti task-listener event fired once the task row exists (Activiti 9 has no constant for it). */
    static final String TASK_EVENT_CREATE = "create";

    private final ActivitiBoundaryEventHelper boundaryEventHelper;

    @Override
    public boolean supports(String bpmnType) {
        return "user.task".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        UserTask userTask = new UserTask();
        userTask.setId("node_" + node.getId());
        userTask.setName(nodeLabel);
        userTask.setTaskListeners(approvalListeners(userTask.getTaskListeners()));
        process.addFlowElement(userTask);
        boundaryEventHelper.attachConfiguredBoundaries(model, process, node,
                userTask.getId(), userTask.getId());
        return GeneratedNode.single(userTask.getId());
    }

    /** Appends the {@code create} listener that reports the parked approval to the control plane. */
    private static List<ActivitiListener> approvalListeners(List<ActivitiListener> existing) {
        List<ActivitiListener> listeners = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        ActivitiListener listener = new ActivitiListener();
        listener.setEvent(TASK_EVENT_CREATE);
        listener.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation(APPROVAL_LISTENER_DELEGATE);
        listeners.add(listener);
        return listeners;
    }
}
