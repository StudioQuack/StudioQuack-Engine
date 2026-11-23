package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * The {@code failroute_<nodeId>} gateway's hold-or-end decision, evaluated as
 * {@code ${sqFailureHoldPolicy.shouldHold(execution)}} on the boundary's conditional flow.
 *
 * <p><b>HOLD is root-only.</b> A failed ROOT run stays alive at {@code hold_<nodeId>} as a
 * dead-letter incident so an operator can repair it (retry / apply-inputs / replay all need a
 * live process instance). These runs must keep ENDING at the shared error end instead:
 * <ul>
 *   <li>a {@code call.activity} child (the process instance has a super execution) ends FAILED;
 *       the parent's own error gate re-raises the propagated code on the parent scope;</li>
 *   <li>a function-port helper sub-PI child ({@code studioquack.execute()}, known to
 *       {@link SubProcessCompletionRegistry}) ends FAILED; the parked caller's completion future
 *       resolves the failure;</li>
 *   <li>an operator skip — the reserved {@link EngineVariableNames#SKIP_ERROR_CODE} raised through
 *       the error gate — ends at the shared error end even on a root run: a skip is a deliberate
 *       abandon-node decision, not a repairable failure, so holding it would strand the run at a
 *       dead-letter the operator just asked to bypass.</li>
 * </ul>
 *
 * <p><b>Never throws.</b> A decision error (engine tearing down, registry unavailable) logs and
 * returns {@code false} — the run ends FAILED at the shared error end exactly as it did before
 * the hold path existed, so a policy fault can never strand a run in an unrepairable limbo.
 */
@Slf4j
@Component("sqFailureHoldPolicy")
@RequiredArgsConstructor
public class FailureHoldPolicy {

    /**
     * Lazy: the registry's dependency chain reaches back to the engine ({@code SqsEngine}), so an
     * eager inject would cycle the context bootstrap (same pattern as the status callbacks).
     */
    private final ObjectProvider<SubProcessCompletionRegistry> subProcessRegistryProvider;

    /**
     * @return {@code true} only when the failing execution belongs to a ROOT run (no super
     *         execution, not a helper sub-PI child, not an operator skip); {@code false} for any
     *         child run, a skip, or on error
     */
    public boolean shouldHold(DelegateExecution execution) {
        try {
            // An operator skip keeps its previous behaviour — end at the shared error end, never
            // re-hold the run: the operator already decided to abandon this node.
            if (EngineVariableNames.SKIP_ERROR_CODE.equals(
                    execution.getVariable(EngineVariableNames.ERROR_CODE))) {
                return false;
            }
            // The superExecutionId lives on the process-instance root execution; the boundary
            // fires on a child scope of it, so walk up before concluding this is a root run
            // (same traversal as CallActivityChildContextListener).
            DelegateExecution piExecution = execution;
            while (piExecution != null && !piExecution.isProcessInstanceType()) {
                piExecution = piExecution.getParent();
            }
            if (piExecution != null && StringUtils.isNotBlank(piExecution.getSuperExecutionId())) {
                return false; // call-activity child — the parent owns the failure
            }
            SubProcessCompletionRegistry registry = subProcessRegistryProvider.getIfAvailable();
            if (registry != null
                    && registry.isSubPiChild(execution.getProcessInstanceBusinessKey())) {
                return false; // helper sub-PI child — the calling function owns the failure
            }
            return true;
        } catch (Exception ex) {
            log.warn("failure-hold policy evaluation failed (activity={}): {} — falling back to "
                            + "end-at-error-end", execution.getCurrentActivityId(), ex.toString());
            return false;
        }
    }
}
