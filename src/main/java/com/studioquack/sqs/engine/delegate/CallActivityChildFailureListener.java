package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * The write half of the call-activity failure handoff. The generator attaches this listener to
 * every end event (next to {@code subProcessCompletionListener}); it no-ops unless the ending
 * instance is a {@code call.activity} child (its PI root carries a super execution) AND the run
 * reached a failure end — the shared {@code Event_SandboxErrorEnd} or a lingering
 * {@code __sq_error_code} sentinel (same check {@code SubProcessCompletionListener} uses).
 *
 * <p><b>Why a pin.</b> The read half ({@code callActivityFailurePropagationListener}) runs on the
 * call activity's end event — after the child PI has ended, when only historic reads could reach
 * the child's sentinel, and historic rows written in the same command are not reliably visible to
 * an in-command query. The sentinel IS readable here, on the ending execution, so the failure is
 * pinned onto the PARENT instance scope under {@code __sq_callfail_<superExecutionId>} — a
 * runtime variable the reader resolves through the ordinary scope chain, keyed by the super
 * execution id so concurrent call activities of one parent never cross-read.
 *
 * <p>Fully guarded: a hiccup degrades to the pre-fix behaviour (the parent walks on past a failed
 * call) instead of rolling back the child's end.
 */
@Slf4j
@Component("callActivityChildFailureListener")
public class CallActivityChildFailureListener implements ExecutionListener {

    private final ObjectProvider<SqsEngine> sqsEngineProvider;

    public CallActivityChildFailureListener(ObjectProvider<SqsEngine> sqsEngineProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        if (!ExecutionListener.EVENTNAME_END.equals(execution.getEventName())) {
            return;
        }
        try {
            // The end event runs on a flow execution; the superExecutionId (→ the parent's parked
            // call-activity execution) lives on the PI root — walk up first.
            DelegateExecution piExecution = execution;
            while (piExecution != null && !piExecution.isProcessInstanceType()) {
                piExecution = piExecution.getParent();
            }
            if (piExecution == null || StringUtils.isBlank(piExecution.getSuperExecutionId())) {
                return; // not a call-activity child
            }
            if (!isFailureEnd(execution)) {
                return;
            }
            SqsEngine engine = sqsEngineProvider.getIfAvailable();
            if (engine == null) {
                return;
            }
            String superExecutionId = piExecution.getSuperExecutionId();
            String parentPiId = engine.historicProcessInstances()
                    .processInstanceId(execution.getProcessInstanceId())
                    .singleResult()
                    .getSuperProcessInstanceId();
            if (StringUtils.isBlank(parentPiId)) {
                return;
            }
            Object code = execution.getVariable(EngineVariableNames.ERROR_CODE);
            String errorCode = EngineVariableNames.isFailureSentinel(code)
                    ? String.valueOf(code)
                    : SharedErrorEndSentinelListener.FALLBACK_ERROR_CODE;
            engine.setProcessVariables(parentPiId,
                    Map.of(EngineVariableNames.callFailureVariableName(superExecutionId), errorCode));
            log.info("callActivity.child-fail-pin: child={} pinned {} for superExecution={}",
                    execution.getProcessInstanceBusinessKey(), errorCode, superExecutionId);
        } catch (Exception ex) {
            log.warn("callActivity.child-fail-pin failed: {}", ex.toString());
        }
    }

    /** A failure end: the shared sandbox-error end, or a lingering error sentinel on the run. */
    private boolean isFailureEnd(DelegateExecution execution) {
        if (ActivitiBpmnElementHelper.SHARED_ERROR_END_ID.equals(execution.getCurrentActivityId())) {
            return true;
        }
        return EngineVariableNames.isFailureSentinel(
                execution.getVariable(EngineVariableNames.ERROR_CODE));
    }
}
