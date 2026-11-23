package com.studioquack.sqs.engine.strategy.helper;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy.GeneratedNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.Activity;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BoundaryEvent;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ErrorEventDefinition;
import org.activiti.bpmn.model.ExclusiveGateway;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ReceiveTask;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.ServiceTask;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Emits the sandbox-dispatch BPMN pattern and maps a template {@code jobType} to a delegate bean.
 * A docling node uses {@code ${doclingDelegate}}. Unknown job types use the sandbox dispatch delegate.
 *
 * <p><b>Sandbox node expansion:</b>
 * <pre>
 * node_X = ServiceTask activiti:async=true delegateExpression=${scriptDispatchDelegate}
 * wait_X = ReceiveTask (engine parks; EngineTriggerService.trigger resumes)
 * errgate_X = ServiceTask delegateExpression=${sqErrorGateDelegate} [entry for error boundary]
 * err_X = interrupting BoundaryEvent(errorEventDefinition, cancelActivity=true) on errgate_X
 * > failroute_X [system catch-all]
 * failroute_X = ExclusiveGateway ${sqFailureHoldPolicy.shouldHold(execution)} -> hold_X [ROOT]
 * > Event_SandboxErrorEnd (default flow) [child run]
 * hold_X = ServiceTask activiti:async=true delegateExpression=${sqFailureHoldDelegate}
 * > node_X (repair-retry loop-back; taken once the sentinel is cleared)
 * errcode_X_i = interrupting BoundaryEvent(errorEventDefinition) on errgate_X
 * > node_&lt;targetNodeId&gt; [typed errorRef, or no errorRef when blank]
 * btimer_X_i / bmsg_X_i / bsig_X_i = BoundaryEvent(timer|message|signal) on wait_X
 * > node_&lt;targetNodeId&gt; [configured boundaries]
 * </pre>
 * The error gate turns the {@code __sq_error_code} sentinel into a {@code BpmnError}; Activiti's
 * native error propagation routes a matched {@code BpmnError(code)} to the {@code errcode_X_i}
 * boundary declared for that code (→ its configured handler target). An unmatched code falls
 * through to the system catch-all {@code err_X} → {@code failroute_X}, unless the node authored a
 * blank-code ERROR config — that user catch-all (no {@code errorRef}) replaces {@code err_X} and
 * routes to the wired handler. Two catch-alls on one activity are forbidden. On success the gate
 * is a zero-cost pass-through. The fail-route gateway decides by run ownership: a ROOT run is held
 * alive at {@code hold_X} as a dead-letter incident (the repair surface); a call-activity child or
 * helper sub-PI child takes the default flow and ends FAILED at the shared error end — the parent
 * owns the failure.
 *
 * <p><b>Configured boundaries.</b> {@link #emitAsyncSandboxTask} hands the host node's
 * {@link TemplateDto#getBoundaryEventConfigs} to {@link ActivitiBoundaryEventHelper}, which emits
 * the ERROR boundaries on the error gate and the TIMER/MESSAGE/SIGNAL boundaries on the wait state
 * (see that class for why the hosts differ). The system catch-all {@code err_X} is emitted only
 * when no user catch-all (blank ERROR code) is planned; typed-code user boundaries still coexist
 * with it.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ActivitiBpmnElementHelper {

    public static final String SHARED_ERROR_END_ID = "Event_SandboxErrorEnd";
    public static final String SCRIPT_DISPATCH_DELEGATE = "${scriptDispatchDelegate}";
    public static final String ERROR_GATE_DELEGATE = "${sqErrorGateDelegate}";

    /**
     * Delegate expression on the {@code hold_<nodeId>} ServiceTask. The bean reports the run FAILED
     * and throws, so the async job dead-letters into a genuine Activiti incident — the repair
     * surface. On a retry (sentinel cleared) it passes the token through to the retry target.
     */
    public static final String FAILURE_HOLD_DELEGATE = "${sqFailureHoldDelegate}";

    /**
     * The {@code failroute_<nodeId>} gateway condition deciding whether a caught failure HOLDS the
     * run (ROOT — an operator repairs it) or ENDS it at the shared error end (call-activity child
     * / helper sub-PI child — the parent owns the failure). The bean name sits on the fail-closed
     * EL allowlist ({@code SqsActivitiEngineConfig#ALLOWED_EXPRESSION_BEANS}).
     */
    public static final String FAILURE_HOLD_CONDITION = "${sqFailureHoldPolicy.shouldHold(execution)}";

    /**
     * TAKE listener on the hold leg: arms a fallback {@code __sq_error_code} when the caught
     * failure carries no typed sentinel (a propagated call-activity error end), so the hold
     * delegate's first-arrival vs repair-retry distinction works on every path.
     */
    public static final String FAILURE_HOLD_SENTINEL_LISTENER = "${failureHoldSentinelListener}";
    /**
     * Delegate expression for a Docling document-conversion node. Always emitted: it resolves to
     * the unconditional {@code @Component("doclingDelegate")} {@code DoclingDelegate}, which
     * converts on the environment's own side-car or a same-owner sibling's (cross-env overlay).
     */
    public static final String DOCLING_DELEGATE = "${doclingDelegate}";

    /**
     * Canonical jobType for the generic function/script sandbox port. Routed to
     * {@link #SCRIPT_DISPATCH_DELEGATE}.
     */
    public static final String FUNCTION_SCRIPT_JOB_TYPE = "function-script-executor";

    /** jobType for a Docling document-conversion node — always deployable. */
    public static final String DOCLING_JOB_TYPE = "docling-converter";

    /** jobTypes that dispatch remote/long work → async ServiceTask + ReceiveTask wait-state. */
    private static final Set<String> ASYNC_SANDBOX_JOB_TYPE_PREFIXES = Set.of(
            "custom-script-executor", "function-script-executor", DOCLING_JOB_TYPE);

    /** jobTypes that are guaranteed-fast in-JVM work → plain synchronous delegate. */
    private static final Set<String> SYNC_JOB_TYPES = Set.of(
            "getConversationHistory", "addConversation");

    private final ActivitiBoundaryEventHelper boundaryEventHelper;

    /**
     * Maps a template {@code jobType} to the delegate bean expression a generated ServiceTask
     * must invoke. Unknown → the generic sandbox dispatch delegate (the fallback contract).
     *
     * <p>{@code docling-converter} ALWAYS resolves to {@link #DOCLING_DELEGATE} — the node is
     * always deployable and dispatches to the single {@code DoclingDelegate} (own side-car, or a
     * same-owner sibling's over the overlay). No fail-closed throw, no daemon-side enable gate.
     */
    public String resolveDelegateExpression(String jobType) {
        if (StringUtils.isBlank(jobType)) {
            return SCRIPT_DISPATCH_DELEGATE;
        }
        if (jobType.startsWith("function-script-executor") || "custom-script-executor".equals(jobType)) {
            return SCRIPT_DISPATCH_DELEGATE;
        }
        if (DOCLING_JOB_TYPE.equals(jobType)) {
            // Always deployable: the single ${doclingDelegate} bean converts on the environment's
            // own side-car or a same-owner sibling's (cross-env overlay) — never via the control plane.
            return DOCLING_DELEGATE;
        }
        // getConversationHistory / addConversation and every other unknown jobType route through
        // the generic sandbox dispatch for E1 (no conversation delegate bean exists on base).
        return SCRIPT_DISPATCH_DELEGATE;
    }

    /** @return true when the node must dispatch asynchronously (wait-state), false for in-JVM sync work. */
    public boolean isAsyncSandbox(String jobType) {
        if (StringUtils.isBlank(jobType)) {
            return true; // unknown → generic sandbox path (async, safest for long/remote work)
        }
        if (SYNC_JOB_TYPES.contains(jobType)) {
            return false;
        }
        if (jobType.startsWith("function-script-executor")) {
            return true;
        }
        return ASYNC_SANDBOX_JOB_TYPE_PREFIXES.contains(jobType);
    }

    /**
 * Emits a synchronous delegate ServiceTask (in-JVM work). Entry == exit. Authored
 * boundaries host on the task itself: an ERROR boundary catches a {@code BpmnError}
 * the delegate throws; TIMER/MESSAGE/SIGNAL boundaries are valid BPMN on any activity
 * (on a fast sync task they simply rarely fire — the authored intent is honored
 * instead of silently dropped).
 *
 * @param model the owning {@link BpmnModel} — per-code error boundaries register their
 * {@code <error errorCode=…>} declarations on it 
 */
    public GeneratedNode emitSyncDelegateTask(BpmnModel model, Process process, NodeDto node,
                                              String nodeLabel, String jobType) {
        String id = "node_" + node.getId();
        ServiceTask task = new ServiceTask();
        task.setId(id);
        task.setName(nodeLabel);
        task.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        task.setImplementation(resolveDelegateExpression(jobType));
        process.addFlowElement(task);
        boundaryEventHelper.attachConfiguredBoundaries(model, process, node, id, id);
        return GeneratedNode.single(id);
    }

    /**
 * Emits the async sandbox-dispatch pattern. Entry = the async service task; exit = the error
 * gate (outbound edges leave from there).
 *
 * @param model the owning {@link BpmnModel} — per-code error boundaries register their
 * {@code <error errorCode=…>} declarations on it 
 */
    public GeneratedNode emitAsyncSandboxTask(BpmnModel model, Process process, NodeDto node,
                                              String nodeLabel, String jobType) {
        String nid = node.getId();
        String serviceId = "node_" + nid;
        String waitId = "wait_" + nid;
        String gateId = "errgate_" + nid;

        ServiceTask dispatch = new ServiceTask();
        dispatch.setId(serviceId);
        dispatch.setName(nodeLabel);
        dispatch.setAsynchronous(true); // activiti:async — engine thread returns after dispatch
        dispatch.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        dispatch.setImplementation(resolveDelegateExpression(jobType));
        process.addFlowElement(dispatch);

        ReceiveTask wait = new ReceiveTask();
        wait.setId(waitId);
        wait.setName(nodeLabel + " (wait)");
        process.addFlowElement(wait);

        ServiceTask errorGate = new ServiceTask();
        errorGate.setId(gateId);
        errorGate.setName(nodeLabel + " (error gate)");
        errorGate.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        errorGate.setImplementation(ERROR_GATE_DELEGATE);
        process.addFlowElement(errorGate);

        // internal wiring: dispatch -> wait -> errorGate (no conditions)
        process.addFlowElement(newFlow("flow_" + nid + "_to_wait", serviceId, waitId));
        process.addFlowElement(newFlow("flow_" + nid + "_wait_to_gate", waitId, gateId));

        // System catch-all only when the user did not author their own (blank errorCode).
        // Two catch-alls on one activity are forbidden; typed-code user boundaries still coexist.
        // The repair-retry loop-back re-enters at the dispatch task so the node re-dispatches.
        if (!boundaryEventHelper.hasUserCatchAllError(node)) {
            attachErrorBoundary(process, gateId, nid, serviceId);
        }
        // ERROR boundaries host on the error gate; timer/message/signal on the wait state.
        boundaryEventHelper.attachConfiguredBoundaries(model, process, node, gateId, waitId);

        return new GeneratedNode(serviceId, gateId);
    }

    /**
     * Attaches an interrupting, catch-all error boundary to {@code attachedToId} and routes a
     * caught failure by run ownership. No {@code errorRef} → catches any {@code BpmnError} thrown
     * by the error gate.
     * <pre>
     *   err_&lt;nodeId&gt;      → failroute_&lt;nodeId&gt; (ExclusiveGateway)
     *                              ├─[{@link #FAILURE_HOLD_CONDITION}]→ hold_&lt;nodeId&gt;
     *                              └─(default flow)→ {@link #SHARED_ERROR_END_ID}
     *   hold_&lt;nodeId&gt;     → {@code retryTargetId}   (repair-retry loop-back)
     * </pre>
     * A ROOT run HOLDS: the async {@code hold_<nodeId>} task throws from its job, which — at
     * {@code failed-job-retries: 0} — dead-letters into a genuine Activiti incident and parks the
     * token there until an operator repairs it (retry-incident re-queues the job; the repair
     * clears the failure sentinel first, so the re-run delegate passes the token on to
     * {@code retryTargetId} and the node re-dispatches). A call-activity child or helper sub-PI
     * child takes the default flow and ENDS at the shared (plain) error end as before — the
     * parent owns the failure.
     *
     * @param retryTargetId the element a repaired token re-enters: the node's dispatch task for a
     *                      sandbox node, or the call activity itself for a post-call error gate
     */
    public void attachErrorBoundary(Process process, String attachedToId, String nodeId, String retryTargetId) {
        BoundaryEvent boundary = new BoundaryEvent();
        boundary.setId("err_" + nodeId);
        boundary.setAttachedToRefId(attachedToId);
        // Activiti's BpmnXMLConverter writes the mandatory bpmn:boundaryEvent/@attachedToRef
        // attribute from getAttachedToRef() (the resolved Activity OBJECT), NOT from
        // attachedToRefId (the String). Without the object reference the attribute is omitted and
        // schema validation fails on deploy. The host element was already added to the process
        // before this call, so resolve + set it here.
        FlowElement attachedTo = process.getFlowElement(attachedToId);
        if (attachedTo instanceof Activity activity) {
            boundary.setAttachedToRef(activity);
        } else {
            throw new IllegalStateException("Cannot attach error boundary '" + boundary.getId()
                    + "': host element '" + attachedToId + "' is "
                    + (attachedTo == null ? "missing" : "not an Activity (" + attachedTo.getClass().getSimpleName() + ")"));
        }
        boundary.setCancelActivity(true);
        boundary.addEventDefinition(new ErrorEventDefinition()); // no errorRef = catch-all
        process.addFlowElement(boundary);

        // Hold-or-end decision. The conditional leg is evaluated first; the default flow keeps the
        // pre-hold child behaviour (plain end at the shared error end → run reports FAILED).
        ExclusiveGateway failroute = new ExclusiveGateway();
        failroute.setId("failroute_" + nodeId);
        failroute.setName("failure route (hold or end)");
        process.addFlowElement(failroute);
        process.addFlowElement(newFlow("flow_" + nodeId + "_err_to_failroute", boundary.getId(), failroute.getId()));

        ServiceTask hold = new ServiceTask();
        hold.setId("hold_" + nodeId);
        hold.setName(nodeId + " (failed — awaiting repair)");
        // activiti:async — the delegate's FailureHeldException moves THIS job to the dead-letter
        // table, parking the token here as the incident an operator repairs.
        hold.setAsynchronous(true);
        hold.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        hold.setImplementation(FAILURE_HOLD_DELEGATE);
        process.addFlowElement(hold);

        SequenceFlow toHold = newFlow("flow_" + nodeId + "_err_to_hold", failroute.getId(), hold.getId());
        toHold.setConditionExpression(FAILURE_HOLD_CONDITION);
        // TAKE (never activity-start): stamps a fallback sentinel on exactly one traversal — a
        // propagated error end carries no typed code, and a re-queued hold job must not re-stamp
        // the sentinel the repair cleared (a start listener would re-fire on the retry).
        ActivitiListener armSentinel = new ActivitiListener();
        armSentinel.setEvent(org.activiti.engine.delegate.ExecutionListener.EVENTNAME_TAKE);
        armSentinel.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        armSentinel.setImplementation(FAILURE_HOLD_SENTINEL_LISTENER);
        toHold.setExecutionListeners(new java.util.ArrayList<>(java.util.List.of(armSentinel)));
        process.addFlowElement(toHold);

        getOrCreateSharedErrorEnd(process);
        SequenceFlow toEnd = newFlow("flow_" + nodeId + "_err_to_end", failroute.getId(), SHARED_ERROR_END_ID);
        process.addFlowElement(toEnd);
        failroute.setDefaultFlow(toEnd.getId());

        // Only reachable on the repair-retry pass (the sentinel is cleared before re-queueing).
        process.addFlowElement(newFlow("flow_" + nodeId + "_hold_to_retry", hold.getId(), retryTargetId));
    }

    /** Idempotently creates the single shared sandbox-error end event on the process. */
    public EndEvent getOrCreateSharedErrorEnd(Process process) {
        return boundaryEventHelper.getOrCreateSharedErrorEnd(process);
    }

    /** I/O mapping hook — kept as a no-op; I/O is handled inside the delegates (REF-ONLY). */
    public void applyIoMapping(Process process, ServiceTask task, NodeDto node) {
        TemplateDto template = node.getData();
        if (template == null) {
            log.debug("No template for node {} — I/O handled inside the delegate (REF-ONLY)", node.getId());
        }
        // Intentionally empty: Activiti has no element I/O-mapping extension; the delegate reads/writes
        // execution variables and the encrypted context store directly.
    }

    private SequenceFlow newFlow(String id, String sourceRef, String targetRef) {
        SequenceFlow flow = new SequenceFlow(sourceRef, targetRef);
        flow.setId(id);
        return flow;
    }
}
