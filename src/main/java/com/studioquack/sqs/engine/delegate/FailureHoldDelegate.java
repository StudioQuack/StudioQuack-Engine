package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.status.RunStatusCallback;
import com.studioquack.sqs.status.TerminalLogHandoff;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * The {@code hold_<nodeId>} task of the catch-all failure route — where a failed ROOT run parks
 * until an operator repairs it. Bean name matches the {@code ${sqFailureHoldDelegate}} expression
 * the generator emits on the async hold ServiceTask.
 *
 * <p><b>Why it throws.</b> Repair (retry-incident / apply-inputs / replay) needs a LIVE process
 * instance, so a root failure must not end at the plain shared error end. Instead this delegate
 * reports the run FAILED and throws {@link FailureHeldException} from an {@code activiti:async}
 * job: at {@code failed-job-retries: 0} Activiti moves the job straight to the dead-letter table
 * — a genuine Activiti incident, listed by {@code IncidentService} — and the token stays parked
 * at {@code hold_<nodeId>}. The throw IS the repair surface, not a bug.
 *
 * <p><b>Why a retry passes.</b> The retry-incident repair clears the {@code __sq_error_code}
 * sentinel family before re-queueing the dead-letter job, so on the re-run
 * {@link EngineVariableNames#isFailureSentinel} is false and this delegate returns normally — the
 * token flows on along {@code flow_<nodeId>_hold_to_retry} to the retry target and the node
 * re-dispatches. Every FIRST arrival carries a sentinel: a typed one from the error gate, or the
 * fallback stamped by {@link FailureHoldSentinelListener} on the hold leg's TAKE event (a
 * propagated call-activity error end carries none) — that is what makes the sentinel a sound
 * first-arrival vs repair-retry distinction.
 *
 * <p>REF-ONLY: reads only the small non-PHI error sentinels; the incident message carries code +
 * truncated message, never payload values.
 */
@Slf4j
@Component("sqFailureHoldDelegate")
@RequiredArgsConstructor
public class FailureHoldDelegate implements JavaDelegate {

    /** Generator id convention: {@code hold_<nodeId>} — stripped to recover the failed node id. */
    private static final String HOLD_ACTIVITY_PREFIX = "hold_";

    /** Cap on the carried error message — the incident row stays a one-line, non-PHI summary. */
    private static final int MAX_MESSAGE_LENGTH = 500;

    /**
     * Lazy: {@code ChannelRunStatusCallback}'s chain reaches back to the engine configurers, so an
     * eager inject would cycle the context bootstrap (same pattern as the bridge itself).
     */
    private final ObjectProvider<RunStatusCallback> runStatusCallbackProvider;

    /**
     * Lazy for the same reason. The held run never reaches a lifecycle end, so the bridge's
     * flush-before-terminal never fires for it: without this a BATCH run's rows stay on the daemon.
     */
    private final ObjectProvider<TerminalLogHandoff> terminalLogHandoffProvider;

    @Override
    public void execute(DelegateExecution execution) {
        Object errorCode = execution.getVariable(EngineVariableNames.ERROR_CODE);
        if (!EngineVariableNames.isFailureSentinel(errorCode)) {
            // Repair-retry pass: the sentinel family was cleared before the dead-letter job was
            // re-queued, so this is no longer a failure — pass through to the retry target.
            return;
        }
        String code = String.valueOf(errorCode);
        String nodeId = StringUtils.removeStart(
                StringUtils.defaultString(execution.getCurrentActivityId()), HOLD_ACTIVITY_PREFIX);
        // Report BEFORE the throw: the exception rolls the engine transaction back, and the
        // callback is non-transactional best-effort by contract — the run index must flip FAILED
        // even though the instance stays alive (that is what makes the incident repairable).
        reportFailedBestEffort(execution, code);
        String message = truncate(StringUtils.defaultString(
                stringVariable(execution, EngineVariableNames.ERROR_MESSAGE)));
        log.info("failure-hold: node {} failed [{}] — run {} held for repair (incident)",
                nodeId, code, execution.getProcessInstanceBusinessKey());
        throw new FailureHeldException(
                "node " + nodeId + " failed [" + code + "]: " + message + " — held for repair");
    }

    /**
     * Reports the held run FAILED in the same argument shape {@code RunStatusEventBridge} uses
     * for a terminal event (5-arg form; workflowId parsed from the definition id). Fully guarded:
     * a callback hiccup must never mask the hold itself.
     */
    private void reportFailedBestEffort(DelegateExecution execution, String code) {
        try {
            RunStatusCallback callback = runStatusCallbackProvider.getIfAvailable();
            if (callback == null) {
                return;
            }
            String businessKey = execution.getProcessInstanceBusinessKey();
            // Rows first, then the status — the same order the lifecycle end uses.
            TerminalLogHandoff handoff = terminalLogHandoffProvider.getIfAvailable();
            boolean logsDurable = handoff == null || handoff.handOffLogs(businessKey);
            callback.report(
                    businessKey,
                    RunStatusCallback.Status.FAILED,
                    "held for repair at " + execution.getCurrentActivityId() + " [" + code + "]",
                    workflowIdFromProcessDefinition(execution.getProcessDefinitionId()),
                    logsDurable);
        } catch (Exception ex) {
            log.warn("failure-hold: FAILED report failed (activity={}): {}",
                    execution.getCurrentActivityId(), ex.toString());
        }
    }

    /** {@code Process_<wf>:<ver>:<id>} → {@code <wf>} (draft namespace stripped); else null. */
    private static String workflowIdFromProcessDefinition(String processDefinitionId) {
        if (StringUtils.isBlank(processDefinitionId)) {
            return null;
        }
        String key = processDefinitionId;
        int colon = key.indexOf(':');
        if (colon > 0) {
            key = key.substring(0, colon);
        }
        if (!key.startsWith("Process_")) {
            return null;
        }
        String rest = key.substring("Process_".length());
        int draft = rest.indexOf(WorkflowGraph.DRAFT_MARKER);
        if (draft >= 0) {
            rest = rest.substring(0, draft);
        }
        return rest.isBlank() ? null : rest;
    }

    private static String stringVariable(DelegateExecution execution, String name) {
        Object value = execution.getVariable(name);
        return value == null ? null : String.valueOf(value);
    }

    private static String truncate(String message) {
        return message.length() <= MAX_MESSAGE_LENGTH
                ? message
                : message.substring(0, MAX_MESSAGE_LENGTH) + "…";
    }
}
