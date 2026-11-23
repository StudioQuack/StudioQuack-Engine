package com.studioquack.sqs.engine.strategy.impl;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import lombok.RequiredArgsConstructor;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.CallActivity;
import org.activiti.bpmn.model.Process;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * {@code call.activity} → Activiti {@link CallActivity} (as-is). Param {@code processId} →
 * {@code calledElement}; {@code propagateAllParentVariables} → {@code inheritVariables}
 * (default inherit-all).
 *
 * <p>The authored {@code processId} is normalised into the daemon's deployed process-key space
 * ({@link #resolveCalledElement}): a bare id addresses another workflow's main process
 * ({@code Process_<id>}), and a Designer Run's namespace suffix is applied so a draft can only
 * call the same-namespace variant of the target — never the published one (the same isolation
 * rule {@code studioquack.execute()} already enforces for function sub-processes).
 * {@code WorkflowDeploymentService} fails the deploy loudly when a literal target is not
 * deployed on this daemon.
 *
 * <p><b>Short-code targeting.</b> The alternative {@code workflowShortCode} parameter addresses
 * the target by its stable short code (which survives project-version cloning, unlike a workflow
 * id). The deploy command resolves it to a literal {@code processId} up front
 * ({@code CallActivityShortCodeResolver}, with the enclosing deploy's project/version as the
 * disambiguation context), so by generation time the node normally already carries a literal.
 * Two generation-time backstops remain: both parameters blank fails the deploy here, and an
 * unresolved short code (a graph that never passed through the deploy command — e.g. the
 * self-heal redeploy from a seed-carried graph) resolves against the artifact store without
 * project context, succeeding only when the code is globally unique on this daemon.
 *
 * <p>The template's port contract (audit C5) is delivered by the attached
 * {@code ${callActivityIoListener}} execution listener: its {@code start} leg consumes the mapped
 * {@code main_input} (a {@code businessKey} is pinned to the execution-local
 * {@code __sq_callbk_<nodeId>} variable the businessKey expression below resolves — or a minted
 * {@code sub_<nanoId>} when nothing is mapped, so the child is always keyed and visible to the
 * run index; {@code variables}/{@code document} are sealed onward to the child start's
 * downstream), and its {@code end} leg fans the {@code main_output} completion envelope
 * ({@code childProcessInstanceId} + {@code result}) out to the node's downstream.
 *
 * <p><b>Child failure.</b> The child's run failure cannot surface through the engine: the
 * generated shared error end is a deliberate PLAIN end, so a failed child ends "normally" and
 * the parent would walk on as if the call had succeeded. Two channels carry the failure to the
 * parent. An authored error end in the child propagates its {@code BpmnError} natively to this
 * element — caught by an authored ERROR boundary, else by the generated catch-all
 * {@code err_<nodeId>} on the call activity. A plain shared-error-end failure is pinned by the
 * child-end listener ({@code callActivityChildFailureListener}) onto the parent scope as
 * {@code __sq_callfail_<superExecutionId>}; the attached
 * {@code ${callActivityFailurePropagationListener}} (end event, after the IO listener) reads the
 * pin and stamps the parent's {@code __sq_error_code} sentinel — it never throws, because a
 * {@code BpmnError} from an END execution listener escapes the boundary-catch scope and rolls the
 * resume command back (zombie tokens). The raise happens on the generated
 * {@code errgate_<nodeId>} error gate ({@code sqErrorGateDelegate}, the proven sandbox pattern)
 * whose own catch-all boundary ({@code err_<nodeId>_gate}) routes the parent into the fail-route
 * gateway — held alive as a repairable incident when the parent is a ROOT run, ended at its
 * shared error end (stamped with the sentinel so the run index reports FAILED) when the parent is
 * itself a child.
 *
 * <p>No child→parent variable flag by design: the daemon is REF-only, so the only legal
 * engine variables are control tokens ({@code studioquackRef}, {@code __sq_*} sentinels,
 * route/correlation pins) — blindly merging them back on child completion would clobber the
 * parent's own {@code studioquackRef} handle. Child results reach the parent through the
 * sealed execution-context store, never through engine variables. The FE template's
 * {@code propagateAllChildVariables} parameter was removed with audit item 7.
 *
 * <p>Authored boundaries host on the call activity itself: the execution parks there for the
 * whole child run, so TIMER / MESSAGE / SIGNAL subscriptions have a real window in which to
 * fire, and an ERROR boundary catches a {@code BpmnError} the child propagates from an error
 * end event. The designer already offers the Boundary Events submenu on call activities —
 * emitting them here keeps the daemon in agreement (an authored boundary must never silently
 * drop at generation).
 */
@Order(1)
@Component("activitiCallActivityStrategy")
@RequiredArgsConstructor
public class ActivitiCallActivityStrategy implements ActivitiFlowNodeStrategy {

    private final ActivitiBoundaryEventHelper boundaryEventHelper;
    private final com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper bpmnElementHelper;
    private final com.studioquack.sqs.command.CallActivityShortCodeResolver shortCodeResolver;

    /** Prefix of every process key the daemon deploys ({@code Process_<wf>}, {@code Process_SubProc_<root>}). */
    private static final String PROCESS_KEY_PREFIX = "Process_";

    /** Workflow ids, {@code Process_…} keys and draft-marker suffixes; no EL syntax. */
    private static final java.util.regex.Pattern LITERAL_PROCESS_KEY =
            java.util.regex.Pattern.compile("^[A-Za-z0-9_\\-]+$");

    @Override
    public boolean supports(String bpmnType) {
        return "call.activity".equalsIgnoreCase(bpmnType);
    }

    @Override
    public GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel) {
        String processId = node.getData().findParameterValueByShortCode("processId");
        String propagateParent = node.getData().findParameterValueByShortCode("propagateAllParentVariables");

        if (StringUtils.isBlank(processId)) {
            // Backstops for a graph that never passed the deploy command's resolver: an authored
            // workflowShortCode resolves against the artifact store with no project context (only
            // a globally unique code resolves), and both-blank fails the deploy here.
            String shortCode = node.getData().findParameterValueByShortCode("workflowShortCode");
            if (StringUtils.isNotBlank(shortCode)) {
                processId = shortCodeResolver.resolve(
                        com.studioquack.sqs.command.CallActivityShortCodeResolver.normalizeShortCode(shortCode),
                        null, null);
            } else {
                throw new IllegalArgumentException("call.activity '" + nodeLabel
                        + "': set either Process Id or Workflow Short Code — both are empty");
            }
        }

        CallActivity callActivity = new CallActivity();
        callActivity.setId("node_" + node.getId());
        callActivity.setName(nodeLabel);
        if (StringUtils.isNotBlank(processId)) {
            callActivity.setCalledElement(resolveCalledElement(processId.trim(), process.getId()));
        }
        callActivity.setInheritVariables(!"false".equalsIgnoreCase(propagateParent));
        // C5: the listener's start leg pins the mapped child businessKey — or a minted
        // sub_<nanoId> when nothing is mapped, so the child is always keyed and therefore
        // visible to the run index (the pin is set unconditionally because the fail-closed
        // expression manager throws PropertyNotFoundException on an absent identifier, and
        // CallActivityBehavior toString()s the evaluated value without a null check).
        callActivity.setBusinessKey(
                "${" + EngineVariableNames.callBusinessKeyVariableName(node.getId()) + "}");
        attachIoListener(callActivity, ExecutionListener.EVENTNAME_START);
        attachIoListener(callActivity, ExecutionListener.EVENTNAME_END);
        // AFTER the IO listener: re-detect a child's terminal failure (the shared error end is a
        // plain end, so a failed child ends "normally" in engine terms) from the
        // __sq_callfail_<superExecutionId> pin the child-end listener wrote, and stamp the
        // parent's own __sq_error_code sentinel. It must NOT throw: a BpmnError raised from an
        // END execution listener escapes the boundary-catch scope and rolls the command back
        // (zombie tokens). The raise happens on the generated error GATE below — a JavaDelegate,
        // whose BpmnError the boundary catches (the proven sandbox pattern).
        attachDelegateListener(callActivity, ExecutionListener.EVENTNAME_END,
                "${callActivityFailurePropagationListener}");
        process.addFlowElement(callActivity);
        boundaryEventHelper.attachConfiguredBoundaries(model, process, node,
                callActivity.getId(), callActivity.getId());
        // Catch-all on the CALL ACTIVITY for a natively propagated child error end (an authored
        // catch-all replaces it — two catch-alls on one activity are forbidden). A repaired token
        // re-enters at the call activity itself, so the child is called again.
        if (!boundaryEventHelper.hasUserCatchAllError(node)) {
            bpmnElementHelper.attachErrorBoundary(process, callActivity.getId(), node.getId(),
                    callActivity.getId());
        }

        // Post-call error gate (sandbox pattern): pass-through on child success; raises the
        // stamped sentinel as a BpmnError on child failure. Its own catch-all boundary routes to
        // the shared error end — always generated, since authored boundaries host on the call
        // activity and cannot see the gate.
        String gateId = "errgate_" + node.getId();
        org.activiti.bpmn.model.ServiceTask errorGate = new org.activiti.bpmn.model.ServiceTask();
        errorGate.setId(gateId);
        errorGate.setName(nodeLabel + " (error gate)");
        errorGate.setImplementationType(
                org.activiti.bpmn.model.ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        errorGate.setImplementation("${sqErrorGateDelegate}");
        process.addFlowElement(errorGate);
        org.activiti.bpmn.model.SequenceFlow toGate = new org.activiti.bpmn.model.SequenceFlow();
        toGate.setId("flow_" + node.getId() + "_to_gate");
        toGate.setSourceRef(callActivity.getId());
        toGate.setTargetRef(gateId);
        process.addFlowElement(toGate);
        // Same retry target as the activity's own catch-all: a repaired post-call failure re-runs
        // the call activity (the child is called again with its reseeded context).
        bpmnElementHelper.attachErrorBoundary(process, gateId, node.getId() + "_gate",
                callActivity.getId());
        // Entry = the call activity; exit = the gate (outbound edges leave from there).
        return new GeneratedNode(callActivity.getId(), gateId);
    }

    private static void attachIoListener(CallActivity callActivity, String event) {
        attachDelegateListener(callActivity, event, "${callActivityIoListener}");
    }

    private static void attachDelegateListener(CallActivity callActivity, String event, String delegate) {
        org.activiti.bpmn.model.ActivitiListener listener = new org.activiti.bpmn.model.ActivitiListener();
        listener.setEvent(event);
        listener.setImplementationType(
                org.activiti.bpmn.model.ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation(delegate);
        java.util.List<org.activiti.bpmn.model.ActivitiListener> listeners =
                callActivity.getExecutionListeners() == null
                        ? new java.util.ArrayList<>()
                        : new java.util.ArrayList<>(callActivity.getExecutionListeners());
        listeners.add(listener);
        callActivity.setExecutionListeners(listeners);
    }

    /**
     * Normalises the authored {@code processId} into the daemon's process-key space: a bare id
     * addresses another workflow's main process ({@code Process_<id>}); a full {@code Process_…}
     * key (e.g. {@code Process_SubProc_<rootId>}) passes through. The enclosing graph's draft
     * namespace suffix is then appended so a Designer Run calls the same-namespace variant of the
     * target, never the published process. An authored value that already carries a draft marker
     * is left untouched.
     *
     * <p>Only a literal process key is accepted. Activiti evaluates a {@code ${…}} (or
     * {@code #{…}}) {@code calledElement} as JUEL in the daemon JVM — the same escape the
     * gateway grammar closes — so any expression syntax fails the deploy here.
     *
     * @throws IllegalArgumentException when {@code authoredProcessId} is not a literal key
     */
    static String resolveCalledElement(String authoredProcessId, String enclosingProcessId) {
        if (!LITERAL_PROCESS_KEY.matcher(authoredProcessId).matches()) {
            throw new IllegalArgumentException(
                    "call.activity processId must be a literal process key, not an expression: "
                            + (authoredProcessId.length() <= 80
                                    ? authoredProcessId : authoredProcessId.substring(0, 80) + "…"));
        }
        String called = authoredProcessId.startsWith(PROCESS_KEY_PREFIX)
                ? authoredProcessId
                : PROCESS_KEY_PREFIX + authoredProcessId;
        String suffix = WorkflowGraph.namespaceSuffixOf(enclosingProcessId);
        if (!suffix.isEmpty() && !called.contains(WorkflowGraph.DRAFT_MARKER)) {
            called += suffix;
        }
        return called;
    }
}
