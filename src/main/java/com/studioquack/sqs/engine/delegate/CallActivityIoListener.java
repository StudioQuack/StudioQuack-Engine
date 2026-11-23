package com.studioquack.sqs.engine.delegate;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.CallActivity;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Delivers the {@code call.activity} template's port contract (audit C5). The generator attaches
 * this listener to every {@link CallActivity} element twice — {@code start} and {@code end}:
 *
 * <p><b>{@code start} (IN leg).</b> Reads the DuckTape-mapped {@code main_input} row targeting the
 * call-activity node. A mapped {@code businessKey} is pinned to the execution-local
 * {@code __sq_callbk_<nodeId>} variable the element's guarded businessKey expression resolves (no
 * mapping → a minted {@code sub_<nanoId>} is pinned, so the child always starts keyed and visible
 * to the run index); mapped {@code variables}/{@code document} are sealed onward as the
 * call-activity node's outputs addressed to the child start event's downstream — the child's first
 * node reads them through the ordinary input scan (dual-written to the child's own store lane,
 * which the child uses once {@code callActivityChildContextListener} reseeds its
 * {@code studioquackRef} with the child's own graph at the child start event).
 *
 * <p><b>{@code end} (OUT leg).</b> Fires when the child completes and the token leaves the call
 * activity: fans the {@code main_output} envelope out to the node's downstream with
 * {@code childProcessInstanceId} (latest historic child of this parent instance) and {@code result}
 * (the child's terminal outputs — the sealed rows its last nodes addressed to the child's end
 * event(s), merged). A child FAILURE is re-detected and raised on the parent by the dedicated
 * {@code callActivityFailurePropagationListener} attached after this one (the generated shared
 * error end is a plain end, so a failed child ends "normally" in engine terms); the template
 * declares no {@code status}/{@code error} fields — the boundary path is the failure channel.
 *
 * <p><b>Best-effort.</b> An I/O hiccup must never roll back the engine transaction that starts the
 * child or resumes the parent — a failure is logged and the token continues, exactly like
 * {@code LinkJumpExecutionListener} / {@code TimerInfoEmissionListener}. Dependencies resolve
 * lazily via {@link ObjectProvider}: the bean is referenced by a JUEL delegate expression and must
 * not join an init-time cycle.
 */
@Slf4j
@Component("callActivityIoListener")
public class CallActivityIoListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** Template IN port value of {@code call.activity} (the row the start leg consumes). */
    private static final String MAIN_IN_PORT = "main_input";
    /** Template OUT port value of {@code call.activity} (the envelope the end leg publishes). */
    private static final String MAIN_OUT_PORT = "main_output";
    private static final String FIELD_VARIABLES = "variables";
    private static final String FIELD_DOCUMENT = "document";
    private static final String FIELD_BUSINESS_KEY = "businessKey";
    private static final String FIELD_RESULT = "result";
    private static final String FIELD_CHILD_PI = "childProcessInstanceId";
    /** Minted child businessKey prefix — distinguishes a generated key from a user-mapped one. */
    static final String CHILD_KEY_PREFIX = "sub_";
    /** Generated post-call error-gate element id prefix ({@code errgate_<nodeId>}). */
    private static final String GATE_ID_PREFIX = "errgate_";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public CallActivityIoListener(ObjectProvider<SqsEngine> sqsEngineProvider,
                                ObjectProvider<ExecutionContextStore> executionContextStoreProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
        this.executionContextStoreProvider = executionContextStoreProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        ExecutionContextStore contextStore = executionContextStoreProvider.getIfAvailable();
        if (engine == null || contextStore == null) {
            return;
        }
        String activityId = execution.getCurrentActivityId();
        if (StringUtils.isBlank(activityId) || !activityId.startsWith(NODE_ID_PREFIX)) {
            return;
        }
        String nodeId = StringUtils.removeStart(activityId, NODE_ID_PREFIX);
        try {
            if (EVENTNAME_START.equals(execution.getEventName())) {
                onStart(execution, engine, contextStore, nodeId);
            } else if (EVENTNAME_END.equals(execution.getEventName())) {
                onEnd(execution, engine, contextStore, nodeId);
            }
        } catch (Exception ex) {
            log.warn("callActivityIo: {} leg failed for node={}: {}",
                    execution.getEventName(), nodeId, ex.toString());
        }
    }

    // ---- start: mapped main_input → child businessKey + child-start downstream -----------------

    private void onStart(DelegateExecution execution, SqsEngine engine,
                         ExecutionContextStore contextStore, String nodeId) {
        // Pin FIRST and unconditionally: the fail-closed expression manager throws
        // PropertyNotFoundException on an absent identifier, so the element's businessKey
        // expression must never evaluate an unset pin. The default is a minted sub_<nanoId>:
        // a keyless child is invisible to the run index (every status/log report keys off the
        // businessKey) and undeploy sweeps cannot address it. A mapped businessKey below
        // overwrites the minted default.
        String pinVariable = EngineVariableNames.callBusinessKeyVariableName(nodeId);
        String minted = CHILD_KEY_PREFIX + NanoIdUtils.randomNanoId();
        execution.setVariableLocal(pinVariable, minted);
        String businessKey = execution.getProcessInstanceBusinessKey();
        if (StringUtils.isBlank(businessKey)) {
            return;
        }
        Map<String, Object> mapped = readMappedMainInput(execution, contextStore, nodeId, businessKey);
        if (mapped == null) {
            return;
        }
        Object childBusinessKey = mapped.get(FIELD_BUSINESS_KEY);
        String childBk = childBusinessKey == null ? "" : String.valueOf(childBusinessKey);
        String effectiveChildKey = StringUtils.isNotBlank(childBk) ? childBk : minted;
        if (StringUtils.isNotBlank(childBk)) {
            execution.setVariableLocal(pinVariable, childBk);
        }
        Map<String, Object> childInput = new LinkedHashMap<>();
        if (mapped.get(FIELD_VARIABLES) != null) {
            childInput.put(FIELD_VARIABLES, mapped.get(FIELD_VARIABLES));
        }
        if (mapped.get(FIELD_DOCUMENT) != null) {
            childInput.put(FIELD_DOCUMENT, mapped.get(FIELD_DOCUMENT));
        }
        if (childInput.isEmpty()) {
            return;
        }
        CallActivity callActivity = callActivityElement(execution, engine);
        if (callActivity == null) {
            return;
        }
        if (!callActivity.isInheritVariables()) {
            log.debug("callActivityIo: node={} skips input delivery — inheritVariables=false leaves "
                    + "the child without the parent's sealed context", nodeId);
            return;
        }
        BpmnModel childModel = childModelOf(engine, callActivity);
        if (childModel == null) {
            return;
        }
        List<String> childEntry = startDownstreamIds(childModel.getMainProcess());
        if (childEntry.isEmpty()) {
            return;
        }
        List<InOutValueDto> rows = new ArrayList<>(childEntry.size());
        for (String targetId : childEntry) {
            rows.add(row(nodeId, MAIN_IN_PORT, targetId, MAIN_IN_PORT, childInput));
        }
        contextStore.saveOutputs(writeRef(contextStore, businessKey, nodeId), rows);
        if (!effectiveChildKey.equals(businessKey)) {
            // The child keys its store lanes by its OWN businessKey (mapped or minted) —
            // dual-write so the delivery reaches both keying lanes.
            contextStore.saveOutputs(writeRef(contextStore, effectiveChildKey, nodeId), rows);
        }
        log.info("callActivityIo: node={} delivered mapped input to child start downstream {}",
                nodeId, childEntry);
    }

    // ---- end: child completion → main_output envelope ------------------------------------------

    private void onEnd(DelegateExecution execution, SqsEngine engine,
                       ExecutionContextStore contextStore, String nodeId) {
        String businessKey = execution.getProcessInstanceBusinessKey();
        if (StringUtils.isBlank(businessKey)) {
            return;
        }
        CallActivity callActivity = callActivityElement(execution, engine);
        if (callActivity == null) {
            return;
        }
        List<String> downstreamIds = downstreamIdsOf(execution, engine, callActivity);
        if (downstreamIds.isEmpty()) {
            return;
        }
        Map<String, Object> envelope = new LinkedHashMap<>();
        String childPiId = latestChildInstanceId(engine, execution.getProcessInstanceId());
        if (childPiId != null) {
            envelope.put(FIELD_CHILD_PI, childPiId);
        }
        BpmnModel childModel = childModelOf(engine, callActivity);
        if (childModel != null) {
            Object result = readChildResult(execution, engine, contextStore, childModel.getMainProcess(),
                    businessKey, childPiId);
            if (result != null) {
                envelope.put(FIELD_RESULT, result);
            }
        }
        if (envelope.isEmpty()) {
            return;
        }
        ProcessContextRef writeRef = writeRef(contextStore, businessKey, nodeId);
        List<InOutValueDto> rows = new ArrayList<>(downstreamIds.size());
        for (String targetId : downstreamIds) {
            rows.add(row(nodeId, MAIN_OUT_PORT, targetId, MAIN_OUT_PORT, envelope));
        }
        contextStore.saveOutputs(writeRef, rows);
        log.info("callActivityIo: node={} fanned out the completion envelope (childPi={}) to {}",
                nodeId, childPiId, downstreamIds);
    }

    // ---- helpers -------------------------------------------------------------------------------

    private Map<String, Object> readMappedMainInput(DelegateExecution execution,
                                                    ExecutionContextStore contextStore,
                                                    String nodeId, String businessKey) {
        List<InOutValueDto> inputs =
                contextStore.loadInputsTargetingNode(lookupRef(execution, businessKey), nodeId);
        if (inputs == null) {
            return null;
        }
        for (InOutValueDto in : inputs) {
            if (!MAIN_IN_PORT.equalsIgnoreCase(StringUtils.trimToEmpty(in.getTargetPortValue()))) {
                continue;
            }
            if (in.getValue() instanceof Map<?, ?> map && !map.isEmpty()) {
                Map<String, Object> out = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    if (e.getKey() != null) {
                        out.put(String.valueOf(e.getKey()), e.getValue());
                    }
                }
                return out;
            }
        }
        return null;
    }

    /** The latest child instance of this parent — the one whose completion resumed the token. */
    private String latestChildInstanceId(SqsEngine engine, String parentProcessInstanceId) {
        var children = engine.historicProcessInstances()
                .superProcessInstanceId(parentProcessInstanceId)
                .orderByProcessInstanceStartTime().desc()
                .listPage(0, 1);
        return children.isEmpty() ? null : children.get(0).getId();
    }

    /**
     * The child's terminal outputs: the sealed rows its last nodes addressed to the child's end
     * event(s), merged into one flat object — a map-valued row spreads its entries, a scalar row
     * lands under its port id (the field-per-row convention the TASK completion seam writes), so
     * {@code result} reads exactly like the template's example ({@code {"fulfillmentId":"F-2"}}).
     * Read under BOTH store keys a child lane can write to — the inherited {@code studioquackRef}
     * handle (sandbox dispatch) and the child's own businessKey (the TASK completion seam keys by
     * it).
     */
    private Object readChildResult(DelegateExecution execution, SqsEngine engine,
                                   ExecutionContextStore contextStore, Process childProcess,
                                   String businessKey, String childPiId) {
        List<ProcessContextRef> lookups = new ArrayList<>(2);
        lookups.add(lookupRef(execution, businessKey));
        String childBk = childPiId == null ? null : engine.findBusinessKey(childPiId);
        if (StringUtils.isNotBlank(childBk) && !childBk.equals(businessKey)) {
            ProcessContextRef childRef = new ProcessContextRef();
            childRef.setExecutionId(childBk);
            lookups.add(childRef);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (FlowElement el : childProcess.getFlowElements()) {
            if (!(el instanceof EndEvent)) {
                continue;
            }
            String endNodeId = StringUtils.removeStart(el.getId(), NODE_ID_PREFIX);
            for (ProcessContextRef lookup : lookups) {
                List<InOutValueDto> rows = contextStore.loadInputsTargetingNode(lookup, endNodeId);
                if (rows == null) {
                    continue;
                }
                for (InOutValueDto row : rows) {
                    Object value = row.getValue();
                    if (value == null) {
                        continue;
                    }
                    if (value instanceof Map<?, ?> map) {
                        for (Map.Entry<?, ?> e : map.entrySet()) {
                            if (e.getKey() != null) {
                                result.put(String.valueOf(e.getKey()), e.getValue());
                            }
                        }
                    } else {
                        String key = StringUtils.defaultIfBlank(row.getPortId(), row.getPortValue());
                        result.put(StringUtils.defaultIfBlank(key,
                                StringUtils.defaultIfBlank(row.getNodeId(), endNodeId)), value);
                    }
                }
            }
        }
        return result.isEmpty() ? null : result;
    }

    /** The child none-start event's immediate downstream — the nodes the IN leg addresses. */
    private List<String> startDownstreamIds(Process childProcess) {
        StartEvent noneStart = null;
        for (FlowElement el : childProcess.getFlowElements()) {
            if (el instanceof StartEvent start
                    && (start.getEventDefinitions() == null || start.getEventDefinitions().isEmpty())) {
                if (noneStart != null) {
                    return List.of(); // ambiguous: more than one none-start — deliver nothing
                }
                noneStart = start;
            }
        }
        if (noneStart == null) {
            return List.of();
        }
        List<String> ids = new ArrayList<>();
        for (SequenceFlow flow : noneStart.getOutgoingFlows()) {
            ids.add(StringUtils.removeStart(flow.getTargetRef(), NODE_ID_PREFIX));
        }
        return ids;
    }

    private List<String> downstreamIdsOf(DelegateExecution execution, SqsEngine engine,
                                         CallActivity callActivity) {
        List<String> ids = new ArrayList<>();
        BpmnModel model = null;
        for (SequenceFlow flow : callActivity.getOutgoingFlows()) {
            String targetRef = flow.getTargetRef();
            if (targetRef != null && targetRef.startsWith(GATE_ID_PREFIX)) {
                // The generated post-call error gate is engine plumbing, invisible to the store
                // world — resolve through it to the node's logical downstream targets.
                if (model == null) {
                    model = engine.readDeployedModel(execution.getProcessDefinitionId());
                }
                FlowElement gate = model == null ? null : model.getFlowElement(targetRef);
                if (gate instanceof FlowNode gateNode) {
                    for (SequenceFlow gateFlow : gateNode.getOutgoingFlows()) {
                        ids.add(StringUtils.removeStart(gateFlow.getTargetRef(), NODE_ID_PREFIX));
                    }
                    continue;
                }
            }
            ids.add(StringUtils.removeStart(targetRef, NODE_ID_PREFIX));
        }
        return ids;
    }

    private CallActivity callActivityElement(DelegateExecution execution, SqsEngine engine) {
        BpmnModel model = engine.readDeployedModel(execution.getProcessDefinitionId());
        FlowElement element = model == null ? null : model.getFlowElement(execution.getCurrentActivityId());
        return element instanceof CallActivity callActivity ? callActivity : null;
    }

    private BpmnModel childModelOf(SqsEngine engine, CallActivity callActivity) {
        String calledElement = callActivity.getCalledElement();
        if (StringUtils.isBlank(calledElement)) {
            return null;
        }
        String definitionId = engine.findLatestDefinitionId(calledElement);
        return definitionId == null ? null : engine.readDeployedModel(definitionId);
    }

    /**
     * Read-side key: the pinned {@code studioquackRef} handle when present, else the businessKey —
     * the store is keyed by workflowExecutionId == businessKey (StartRunCommandHandler), so a run
     * started without a seedContext still finds its rows. Same idiom as
     * {@code LinkJumpExecutionListener}.
     */
    private ProcessContextRef lookupRef(DelegateExecution execution, String businessKey) {
        ProcessContextRef lookup = new ProcessContextRef();
        Object refVar = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (refVar instanceof ProcessContextRef ref && StringUtils.isNotBlank(ref.getExecutionId())) {
            lookup.setExecutionId(ref.getExecutionId());
        } else {
            lookup.setExecutionId(businessKey);
        }
        return lookup;
    }

    /** Write-side key: a snapshot handle for this run's sealed context (same keying as the read). */
    private ProcessContextRef writeRef(ExecutionContextStore contextStore, String businessKey,
                                       String nodeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setCurrentNodeId(nodeId);
        return contextStore.snapshot(ctx, null);
    }

    private static InOutValueDto row(String nodeId, String port, String targetNodeId,
                                     String targetPort, Object value) {
        InOutValueDto row = new InOutValueDto();
        row.setNodeId(nodeId);
        row.setPortId(port);
        row.setPortValue(port);
        row.setTargetNodeId(targetNodeId);
        row.setTargetPortId(targetPort);
        row.setTargetPortValue(targetPort);
        row.setValue(value);
        return row;
    }
}
