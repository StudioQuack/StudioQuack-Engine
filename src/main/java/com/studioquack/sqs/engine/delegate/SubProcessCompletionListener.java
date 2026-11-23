package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Resolves the caller's parked function-port completion future the moment a sub-process
 * ({@code Process_SubProc_<root>}) reaches an end event (, replacing the stub). Bean name
 * {@code subProcessCompletionListener} matches the {@code ${subProcessCompletionListener}} execution
 * listener the generator attaches to every sub-process end event.
 *
 * <p><b>Success vs failure.</b> The generator attaches this listener to EVERY end event, including
 * the shared {@link ActivitiBpmnElementHelper#SHARED_ERROR_END_ID}. Reaching that end (or carrying
 * the {@code __sq_error_code} sentinel) is a failure → {@link SubProcessCompletionRegistry#fail};
 * any other end is a success → {@link SubProcessCompletionRegistry#complete} with the child's
 * REF-ONLY result variables (whitelist-filtered, so no PHI enters {@code sub_pi_pending}).
 *
 * <p>Correlation is {@code businessKey == childWorkflowExecutionId}. The listener is fully guarded:
 * a resolution hiccup must never roll back the engine transaction that ended the sub-process.
 *
 * <p>The registry is resolved lazily via an {@link ObjectProvider} to avoid a hard init-time cycle
 * (the registry depends on {@code EngineTriggerService} → {@code SqsEngine}); it is always present at
 * runtime by the time a sub-process can end.
 */
@Slf4j
@Component("subProcessCompletionListener")
public class SubProcessCompletionListener implements ExecutionListener {

    private final ObjectProvider<SubProcessCompletionRegistry> registryProvider;
    private final AtomicLong invocationCount = new AtomicLong();

    public SubProcessCompletionListener(ObjectProvider<SubProcessCompletionRegistry> registryProvider) {
        this.registryProvider = registryProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        invocationCount.incrementAndGet();
        String childBusinessKey = execution.getProcessInstanceBusinessKey();
        try {
            SubProcessCompletionRegistry registry = registryProvider.getIfAvailable();
            if (registry == null || childBusinessKey == null || childBusinessKey.isBlank()) {
                return;
            }
            if (isFailureEnd(execution)) {
                Object code = execution.getVariable(EngineVariableNames.ERROR_CODE);
                Object msg = execution.getVariable(EngineVariableNames.ERROR_MESSAGE);
                String reason = msg != null ? String.valueOf(msg)
                        : (code != null ? String.valueOf(code) : "sub-process reached error end");
                registry.fail(childBusinessKey, reason);
                log.info("subProcessCompletionListener: FAIL child={} activity={}",
                        childBusinessKey, execution.getCurrentActivityId());
            } else {
                Map<String, Object> refOnly = RefOnlyVariableGuard.filterToWhitelist(execution.getVariables());
                registry.complete(childBusinessKey, refOnly);
                log.info("subProcessCompletionListener: COMPLETE child={} activity={} refKeys={}",
                        childBusinessKey, execution.getCurrentActivityId(), refOnly.keySet());
            }
        } catch (Exception ex) {
            // A completion hiccup must never break the engine transaction that ended the sub-process.
            log.warn("subProcessCompletionListener failed for child={}: {}", childBusinessKey, ex.getMessage());
        }
    }

    /** A sub-process reaches failure via the shared sandbox-error end or a lingering error sentinel. */
    private boolean isFailureEnd(DelegateExecution execution) {
        if (ActivitiBpmnElementHelper.SHARED_ERROR_END_ID.equals(execution.getCurrentActivityId())) {
            return true;
        }
        return EngineVariableNames.isFailureSentinel(execution.getVariable(EngineVariableNames.ERROR_CODE));
    }

    /** @return how many times this listener has been invoked (diagnostics/tests). */
    public long getInvocationCount() {
        return invocationCount.get();
    }
}
