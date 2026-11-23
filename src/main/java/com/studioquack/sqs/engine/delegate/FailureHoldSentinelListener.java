package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Arms the {@code __sq_error_code} failure sentinel on the failure-hold leg
 * ({@code flow_<nodeId>_err_to_hold}, TAKE event) so EVERY arrival at {@code hold_<nodeId>}
 * carries one — the exact guarantee {@link SharedErrorEndSentinelListener} gives the shared
 * error end, duplicated for the hold path because the {@code FailureHoldDelegate} distinguishes
 * <b>first arrival</b> (sentinel present → report FAILED + throw → dead-letter incident) from
 * <b>repair-retry</b> (sentinel cleared by the repair handler → pass through to the retry target)
 * purely by that sentinel.
 *
 * <p>Without it, a {@code BpmnError} that reaches the hold task WITHOUT passing the error gate
 * (a {@code call.activity} child's authored error end propagating to the parent's catch-all
 * boundary) arrives with no sentinel: the delegate would pass straight through and re-enter the
 * retry target in an unbounded re-dispatch loop instead of holding.
 *
 * <p>Placed on the sequence flow's TAKE event — never on the hold task's start — because a TAKE
 * listener fires exactly once when the boundary route is traversed, while a re-queued hold job
 * re-fires activity-start listeners and would re-stamp the sentinel the repair just cleared.
 * A typed code from the error gate / failure-propagation listener always wins (fallback only when
 * absent). Fully guarded: a hiccup must never roll back the engine transaction already on the
 * failure path.
 */
@Slf4j
@Component("failureHoldSentinelListener")
public class FailureHoldSentinelListener implements ExecutionListener {

    /** Generic code stamped when the held token carries no typed one — still a failure sentinel. */
    static final String FALLBACK_ERROR_CODE = "SQ_FAILURE_HELD";

    @Override
    public void notify(DelegateExecution execution) {
        if (!ExecutionListener.EVENTNAME_TAKE.equals(execution.getEventName())) {
            return;
        }
        try {
            Object code = execution.getVariable(EngineVariableNames.ERROR_CODE);
            if (!EngineVariableNames.isFailureSentinel(code)) {
                execution.setVariable(EngineVariableNames.ERROR_CODE, FALLBACK_ERROR_CODE);
                log.debug("failureHold.sentinel: stamped {} (no typed code on the held token)",
                        FALLBACK_ERROR_CODE);
            }
        } catch (Exception ex) {
            log.warn("failureHold.sentinel failed: {}", ex.toString());
        }
    }
}
