package com.studioquack.sqs.engine.update;

import com.studioquack.sqs.engine.listener.BpmnNodeLogListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.impl.context.Context;
import org.springframework.stereotype.Component;

/**
 * While an update is in progress, a token that just finished a real node stays
 * there instead of starting the next one: the update kills only runs with a node
 * in flight, so this run continues on the next boot. A message or timer catch
 * that is still waiting never completes, so it is not parked and survives the
 * restart on its own.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class UpdateParkListener implements ActivitiEventListener {

    private final WorkIntakeGate intake;

    @Override
    public void onEvent(ActivitiEvent event) {
        if (!intake.isUpdateInProgress() || event == null || event.getType() != ActivitiEventType.ACTIVITY_COMPLETED) {
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
        try {
            if (UpdatePark.park(Context.getCommandContext(), activity.getExecutionId())) {
                log.info("update-park: token {} stays after activity {} until the new daemon boots",
                        activity.getExecutionId(), activityId);
            }
        } catch (RuntimeException ex) {
            log.warn("update-park: could not park execution {}: {}", activity.getExecutionId(), ex.toString());
        }
    }

    @Override
    public boolean isFailOnException() {
        return false;
    }
}
