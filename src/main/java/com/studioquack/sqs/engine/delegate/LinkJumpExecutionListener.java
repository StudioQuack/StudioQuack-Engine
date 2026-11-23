package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.LinkEventDefinition;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runtime bridge for {@code link.intermediate.throw.event} — carries the link payload across the
 * jump and honours the paired catch's {@code link_payload} OUT contract ({@code name},
 * {@code payload}). Attached (event={@code end}) by {@code ActivitiLinkThrowStrategy}; fires
 * exactly when the token LEAVES the throw, i.e. as it rides the materialised sequence flow to the
 * paired catch ({@code BpmnGenerationService.pairLinkEvents}).
 *
 * <p>The throw element itself is a bare none-throw — Activiti 9 never executes a link jump — so
 * this listener is where the data contract lives: the throw's mapped {@code link} IN
 * {@code payload} is read from the sealed store (the same DuckTape row shape
 * {@code MessageReceiverCorrelationListener} reads) and re-published as the CATCH node's
 * {@code link_payload} output, one sealed row per node downstream of the catch. The link
 * {@code name} is the deploy-time pairing key (a configuration parameter), never a runtime input —
 * a mapped {@code name} is deliberately ignored because the pair is validated and materialised at
 * deploy.
 *
 * <p><b>Best-effort.</b> The jump must never roll back because a payload row failed to seal, so a
 * failure is logged and the token continues without the row, exactly like
 * {@code writeEmitResult} / {@code TimerInfoEmissionListener}.
 */
@Slf4j
@Component("linkJumpExecutionListener")
public class LinkJumpExecutionListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** IN port value the link throw template declares (carries the mapped {@code payload}). */
    private static final String LINK_IN_PORT = "link";
    /** OUT port value the link catch template declares. */
    private static final String LINK_OUT_PORT = "link_payload";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_PAYLOAD = "payload";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public LinkJumpExecutionListener(ObjectProvider<SqsEngine> sqsEngineProvider,
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
        String throwNodeId = StringUtils.removeStart(activityId, NODE_ID_PREFIX);
        try {
            String businessKey = execution.getProcessInstanceBusinessKey();
            if (StringUtils.isBlank(businessKey)) {
                // No store key a downstream scan could read the row under — nothing to write.
                return;
            }
            BpmnModel model = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (model == null) {
                log.warn("linkJump: deployed model unreadable for {} — link_payload lost for {}",
                        execution.getProcessDefinitionId(), throwNodeId);
                return;
            }
            FlowElement fe = model.getFlowElement(activityId);
            if (!(fe instanceof FlowNode throwElement) || throwElement.getOutgoingFlows() == null) {
                return;
            }
            // The materialised link flow targets the paired catch; a drawn outgoing edge (legacy
            // graphs) targets something else and is not the jump lane.
            for (SequenceFlow flow : throwElement.getOutgoingFlows()) {
                FlowElement target = model.getFlowElement(flow.getTargetRef());
                LinkEventDefinition linkDef = linkDefinitionOf(target);
                if (linkDef != null) {
                    deliver(execution, contextStore, model, (IntermediateCatchEvent) target,
                            linkDef, throwNodeId, businessKey);
                    return; // exactly one link flow per throw — deploy validation guarantees it
                }
            }
        } catch (Exception ex) {
            log.warn("linkJump: payload carry failed for activity={}: {}", activityId, ex.toString());
        }
    }

    /** Publishes the catch's {@code link_payload} envelope to every node downstream of the catch. */
    private void deliver(DelegateExecution execution, ExecutionContextStore contextStore,
                         BpmnModel model, IntermediateCatchEvent catchElement,
                         LinkEventDefinition linkDef, String throwNodeId, String businessKey) {
        String catchElementId = catchElement.getId();
        String catchNodeId = StringUtils.removeStart(catchElementId, NODE_ID_PREFIX);
        List<String> downstreamIds = catchElement.getOutgoingFlows() == null ? List.of()
                : catchElement.getOutgoingFlows().stream()
                        .map(SequenceFlow::getTargetRef)
                        .filter(StringUtils::isNotBlank)
                        .map(target -> StringUtils.removeStart(target, NODE_ID_PREFIX))
                        .distinct()
                        .collect(Collectors.toList());
        if (downstreamIds.isEmpty()) {
            return;
        }

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put(FIELD_NAME, linkDef.getName());
        Object payload = readMappedPayload(execution, contextStore, throwNodeId, businessKey);
        if (payload != null) {
            envelope.put(FIELD_PAYLOAD, payload);
        }

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setCurrentNodeId(catchNodeId);
        ProcessContextRef writeRef = contextStore.snapshot(ctx, null);

        List<InOutValueDto> outputs = new ArrayList<>(downstreamIds.size());
        for (String downstreamId : downstreamIds) {
            InOutValueDto dto = new InOutValueDto();
            dto.setNodeId(catchNodeId);
            dto.setPortId(LINK_OUT_PORT);
            dto.setPortValue(LINK_OUT_PORT);
            dto.setTargetNodeId(downstreamId);
            dto.setTargetPortId(LINK_OUT_PORT);
            dto.setTargetPortValue(LINK_OUT_PORT);
            dto.setValue(envelope);
            outputs.add(dto);
        }
        contextStore.saveOutputs(writeRef, outputs);
        log.info("linkJump.carried throw={} catch={} link={} payload={} downstream={}",
                throwNodeId, catchNodeId, linkDef.getName(), payload != null, downstreamIds.size());
    }

    /**
     * The throw's DuckTape-mapped {@code link} IN {@code payload}, if any. The store is keyed by
     * workflowExecutionId == businessKey (StartRunCommandHandler), so a run started without a
     * seedContext (no pinned studioquackRef) still finds its mapped inputs by businessKey.
     */
    private Object readMappedPayload(DelegateExecution execution, ExecutionContextStore contextStore,
                                     String throwNodeId, String businessKey) {
        ProcessContextRef lookup = new ProcessContextRef();
        Object refVar = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (refVar instanceof ProcessContextRef ref && StringUtils.isNotBlank(ref.getExecutionId())) {
            lookup.setExecutionId(ref.getExecutionId());
        } else {
            lookup.setExecutionId(businessKey);
        }
        List<InOutValueDto> inputs;
        try {
            inputs = contextStore.loadInputsTargetingNode(lookup, throwNodeId);
        } catch (Exception ex) {
            log.warn("linkJump: failed to read mapped inputs for {}: {}", throwNodeId, ex.getMessage());
            return null;
        }
        if (inputs == null || inputs.isEmpty()) {
            return null;
        }
        for (InOutValueDto in : inputs) {
            if (!LINK_IN_PORT.equalsIgnoreCase(StringUtils.trimToEmpty(in.getTargetPortValue()))) {
                continue;
            }
            if (in.getValue() instanceof Map<?, ?> map && map.get(FIELD_PAYLOAD) != null) {
                return map.get(FIELD_PAYLOAD);
            }
        }
        return null;
    }

    /** The {@link LinkEventDefinition} when {@code target} is a link catch, else {@code null}. */
    private static LinkEventDefinition linkDefinitionOf(FlowElement target) {
        if (!(target instanceof IntermediateCatchEvent catchEvent)) {
            return null;
        }
        return catchEvent.getEventDefinitions().stream()
                .filter(ed -> ed instanceof LinkEventDefinition)
                .map(ed -> (LinkEventDefinition) ed)
                .findFirst()
                .orElse(null);
    }
}
