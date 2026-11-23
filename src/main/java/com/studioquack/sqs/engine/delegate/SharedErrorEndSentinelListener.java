package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Guarantees that EVERY arrival at the shared {@code Event_SandboxErrorEnd} carries the
 * {@code __sq_error_code} failure sentinel. The run-status listeners map a run's end to FAILED by
 * reading the sentinel (the shared error end is a plain end, so the event type alone says
 * "completed") — but a {@code BpmnError} that reaches the shared error end WITHOUT passing the
 * error gate (e.g. a {@code call.activity} child's authored error end propagating to the parent's
 * catch-all boundary) arrives with no sentinel stamped on this instance and would be misreported
 * as COMPLETED. This listener stamps a generic code only when none is present, so a typed code
 * from the error gate / failure-propagation listener always wins. Fully guarded: a hiccup must
 * never roll back the engine transaction that is already on the failure path.
 */
@Slf4j
@Component("sharedErrorEndSentinelListener")
public class SharedErrorEndSentinelListener implements ExecutionListener {

    /** Generic code stamped when the token arrives without one — still a failure sentinel. */
    static final String FALLBACK_ERROR_CODE = "SQ_ERROR_END";

    @Override
    public void notify(DelegateExecution execution) {
        if (!ExecutionListener.EVENTNAME_START.equals(execution.getEventName())) {
            return;
        }
        try {
            Object code = execution.getVariable(EngineVariableNames.ERROR_CODE);
            if (!EngineVariableNames.isFailureSentinel(code)) {
                execution.setVariable(EngineVariableNames.ERROR_CODE, FALLBACK_ERROR_CODE);
                log.debug("sharedErrorEnd.sentinel: stamped {} (no typed code on the token)",
                        FALLBACK_ERROR_CODE);
            }
        } catch (Exception ex) {
            log.warn("sharedErrorEnd.sentinel failed: {}", ex.toString());
        }
    }
}
