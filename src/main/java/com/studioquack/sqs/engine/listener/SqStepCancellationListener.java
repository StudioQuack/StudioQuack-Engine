package com.studioquack.sqs.engine.listener;

import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.springframework.stereotype.Component;

/**
 * Reaps in-flight sandbox work when Activiti cancels a parked {@code wait_<nodeId>} (interrupting
 * boundary) or the whole process instance. Log emission stays on {@link BpmnNodeLogListener} —
 * this listener only talks to {@link DispatchCancellationRegistry}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SqStepCancellationListener implements ActivitiEventListener {

    private static final String WAIT_ACTIVITY_PREFIX = "wait_";

    private final DispatchCancellationRegistry cancellationRegistry;

    @Override
    public boolean isFailOnException() {
        return false;
    }

    @Override
    public void onEvent(ActivitiEvent event) {
        try {
            if (event == null || event.getType() == null) {
                return;
            }
            if (event.getType() == ActivitiEventType.PROCESS_CANCELLED) {
                String processInstanceId = event.getProcessInstanceId();
                if (processInstanceId != null && !processInstanceId.isBlank()) {
                    cancellationRegistry.cancelRun(processInstanceId);
                }
                return;
            }
            if (event.getType() != ActivitiEventType.ACTIVITY_CANCELLED) {
                return;
            }
            String activityId = activityIdOf(event);
            if (activityId == null || !activityId.startsWith(WAIT_ACTIVITY_PREFIX)) {
                return;
            }
            String nodeId = activityId.substring(WAIT_ACTIVITY_PREFIX.length());
            cancellationRegistry.cancelStep(event.getProcessInstanceId(), nodeId);
        } catch (Exception ex) {
            // Never let a cancel reap break the engine transaction.
            log.warn("SQS step-cancel listener failed for event {}: {}",
                    event == null ? null : event.getType(), ex.toString());
        }
    }

    /** Prefer {@link ActivitiActivityEvent#getActivityId()} when the event carries an activity. */
    private static String activityIdOf(ActivitiEvent event) {
        if (event instanceof ActivitiActivityEvent activity) {
            return activity.getActivityId();
        }
        return null;
    }
}
