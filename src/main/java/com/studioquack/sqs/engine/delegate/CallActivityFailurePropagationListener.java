package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.springframework.stereotype.Component;

/**
 * The read half of the call-activity failure handoff. Attached by
 * {@code ActivitiCallActivityStrategy} to the {@code CallActivity}'s {@code end} event, AFTER
 * {@code callActivityIoListener} (so the completion envelope is still written first).
 *
 * <p><b>Why.</b> The generated shared {@code Event_SandboxErrorEnd} every caught
 * {@code BpmnError} routes into is a deliberate PLAIN end (an error end with no catching boundary
 * above it would abort the instance) — so a child whose run failed ends "normally" in Activiti
 * terms and the parent's token would walk on as if the call had succeeded. The child-end half
 * ({@code callActivityChildFailureListener}) pins the child's terminal error code onto the parent
 * instance scope under {@code __sq_callfail_<superExecutionId>}; this listener — running on the
 * very execution the pin is keyed by — reads it through the scope chain and stamps the parent's
 * own {@code __sq_error_code}/{@code __sq_error_message} sentinels.
 *
 * <p><b>It never throws.</b> A {@code BpmnError} raised from an END execution listener escapes the
 * boundary-catch scope and rolls the resume command back, parking both tokens as zombies. The
 * actual raise happens one element later on the generated {@code errgate_<nodeId>} error gate
 * ({@code sqErrorGateDelegate} — a JavaDelegate whose {@code BpmnError} the gate's catch-all
 * boundary catches, the proven sandbox pattern), which routes the parent to its shared error end.
 * Fully guarded: a hiccup degrades to the pre-fix behaviour (parent continues) instead of rolling
 * back the child completion.
 */
@Slf4j
@Component("callActivityFailurePropagationListener")
public class CallActivityFailurePropagationListener implements ExecutionListener {

    @Override
    public void notify(DelegateExecution execution) {
        if (!ExecutionListener.EVENTNAME_END.equals(execution.getEventName())) {
            return;
        }
        try {
            String pinName = EngineVariableNames.callFailureVariableName(execution.getId());
            Object pinned = execution.getVariable(pinName);
            if (pinned == null || String.valueOf(pinned).isBlank()) {
                return; // the child ended successfully — nothing to propagate
            }
            String errorCode = String.valueOf(pinned);
            execution.removeVariable(pinName);
            execution.setVariable(EngineVariableNames.ERROR_CODE, errorCode);
            execution.setVariable(EngineVariableNames.ERROR_MESSAGE,
                    "call.activity child failed: " + errorCode);
            log.info("callActivity.failure-propagation: stamped {} on parent pi={} "
                    + "(the error gate raises it next)", errorCode, execution.getProcessInstanceId());
        } catch (Exception ex) {
            log.warn("callActivity.failure-propagation: pin read failed — parent continues: {}",
                    ex.toString());
        }
    }
}
