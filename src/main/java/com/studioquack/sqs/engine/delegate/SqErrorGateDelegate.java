package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.BpmnError;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The post-wait error gate, replacing the stub. Bean name {@code sqErrorGateDelegate}
 * matches the {@code ${sqErrorGateDelegate}} expression on the error-gate ServiceTask
 * ({@code errgate_<nodeId>}) the generator emits after every sandbox {@code ReceiveTask}.
 *
 * <p>When {@link EngineTriggerService#fail} resumed the ReceiveTask it wrote the
 * {@link EngineVariableNames#ERROR_CODE} sentinel (+ optional message). This gate reads it and, when
 * present, throws {@code new BpmnError(code)} so the interrupting catch-all error boundary
 * ({@code err_<nodeId>}) routes the token to the shared {@code Event_SandboxErrorEnd}. On success the
 * sentinel is absent and the gate is a zero-cost pass-through onto the node's outbound edges.
 *
 * <p>REF-ONLY: it reads only the small non-PHI routing/error tokens; no payload is touched.
 */
@Slf4j
@Component("sqErrorGateDelegate")
public class SqErrorGateDelegate implements JavaDelegate {

    private final AtomicLong invocationCount = new AtomicLong();

    @Override
    public void execute(DelegateExecution execution) {
        invocationCount.incrementAndGet();
        Object errorCode = execution.getVariable(EngineVariableNames.ERROR_CODE);
        if (errorCode == null || String.valueOf(errorCode).isBlank()) {
            return; // success path — pass through to the node's outbound edges
        }
        String code = String.valueOf(errorCode);
        Object message = execution.getVariable(EngineVariableNames.ERROR_MESSAGE);
        log.info("sqErrorGateDelegate: raising BpmnError code={} node={} message={}",
                code, execution.getCurrentActivityId(), message);
        // Route the token onto the interrupting error boundary → Event_SandboxErrorEnd.
        throw new BpmnError(code, message == null ? code : String.valueOf(message));
    }

    /** @return how many times this delegate has been invoked (diagnostics/tests). */
    public long getInvocationCount() {
        return invocationCount.get();
    }
}
