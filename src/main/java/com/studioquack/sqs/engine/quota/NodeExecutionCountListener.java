package com.studioquack.sqs.engine.quota;

import com.studioquack.sqs.engine.halt.ExecutionHaltGuard;
import com.studioquack.sqs.engine.listener.BpmnNodeLogListener;
import lombok.RequiredArgsConstructor;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.springframework.stereotype.Component;

/**
 * Counts each {@code ACTIVITY_STARTED} of a designer-visible activity, including
 * a node the same run enters again. Synthetic generator activities are skipped
 * with the same prefix list {@link BpmnNodeLogListener} uses. The engine thread
 * only increments an {@link java.util.concurrent.atomic.AtomicLong}; the activation
 * that spends the allowance hands enforcement to {@link ExecutionHaltGuard}'s own thread.
 */
@Component
@RequiredArgsConstructor
public class NodeExecutionCountListener implements ActivitiEventListener {

    private final NodeExecutionMeter meter;
    private final ExecutionHaltGuard haltGuard;

    @Override
    public void onEvent(ActivitiEvent event) {
        try {
            if (event == null || event.getType() != ActivitiEventType.ACTIVITY_STARTED) {
                return;
            }
            if (!(event instanceof ActivitiActivityEvent activity)) {
                return;
            }
            String activityId = activity.getActivityId();
            if (BpmnNodeLogListener.shouldSkipActivityId(activityId)
                    || BpmnNodeLogListener.isGeneratorInsertedParallel(activityId)) {
                return;
            }
            meter.recordActivation();
            haltGuard.onNodeActivation();
        } catch (RuntimeException ignored) {
            // A counter must never fail the engine thread.
        }
    }

    @Override
    public boolean isFailOnException() {
        return false;
    }
}
