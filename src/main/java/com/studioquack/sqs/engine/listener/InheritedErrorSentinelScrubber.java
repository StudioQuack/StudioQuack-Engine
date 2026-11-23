package com.studioquack.sqs.engine.listener;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.springframework.stereotype.Component;

/**
 * Scrubs inherited error sentinels on process start.
 *
 * <p>A {@code call.activity} child starts with {@code inheritVariables} copying EVERY parent
 * variable — including a stale {@code __sq_error_code} family left over from a failure the parent
 * caught and handled earlier in its run. Left in place, the child's first sandbox error gate
 * ({@code SqErrorGateDelegate}) reads the inherited sentinel and raises a phantom {@code BpmnError}
 * the child never earned.
 *
 * <p>The {@link EngineVariableNames#SKIP_NODE} pin is scrubbed alongside: a skip pinned for the
 * parent's failed step would otherwise ride the same inheritance and short-circuit whichever child
 * node shares the pinned id.
 *
 * <p>A fresh instance has no legitimate error state: the only way the sentinels are present at
 * {@code PROCESS_STARTED} is inheritance (or a caller injecting them as start variables, which is
 * just as illegitimate). They are therefore removed here, once, before any node of the new instance
 * runs. The PARENT's own copies are untouched — its end-of-run failure classification
 * ({@code SqsExecutionStatusListener}) reads them off the parent instance.
 */
@Slf4j
@Component
public class InheritedErrorSentinelScrubber implements ActivitiEventListener {

    @Override
    public void onEvent(ActivitiEvent event) {
        if (event.getType() != ActivitiEventType.PROCESS_STARTED) {
            return;
        }
        try {
            DelegateExecution execution = executionOf(event);
            if (execution == null) {
                return;
            }
            for (String name : EngineVariableNames.FRESH_INSTANCE_SCRUB) {
                if (execution.getVariable(name) != null) {
                    execution.removeVariable(name);
                    log.info("inherited-sentinel.scrubbed pi={} name={} — stale parent error state "
                            + "must not fire this instance's error gates", execution.getProcessInstanceId(), name);
                }
            }
            java.util.Set<String> varNames = execution.getVariableNames();
            if (varNames != null) {
                for (String name : varNames) {
                    if (name != null && name.startsWith(EngineVariableNames.RETRY_PREFIX)) {
                        execution.removeVariable(name);
                        log.info("inherited-sentinel.scrubbed pi={} name={} — stale parent retry pin "
                                + "must not mark child nodes as retry", execution.getProcessInstanceId(), name);
                    }
                }
            }
        } catch (Exception ex) {
            // Never let a scrub hiccup break the engine transaction that started the instance.
            log.warn("inherited-sentinel.scrub failed for event {}: {}", event.getType(), ex.toString());
        }
    }

    @Override
    public boolean isFailOnException() {
        return false;
    }

    private DelegateExecution executionOf(ActivitiEvent event) {
        if (event instanceof ActivitiEntityEvent entityEvent
                && entityEvent.getEntity() instanceof DelegateExecution execution) {
            return execution;
        }
        return null;
    }
}
