package com.studioquack.sqs.engine.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.listener.BpmnNodeLogPayloads;
import com.studioquack.sqs.engine.runtime.BoundaryEventVariables;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.status.SandboxLogCallback;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Publishes a fired boundary event's data to the node it triggers. Bean name
 * {@code sqBoundaryEventDelegate} matches the {@code ${sqBoundaryEventDelegate}} start listener
 * {@link ActivitiBoundaryEventHelper} puts on every emitted boundary.
 *
 * <p>The designer draws the boundary's outgoing edge from a port carrying an output schema, and maps
 * its fields onto the handler's input port. This is what makes that mapping resolve to something: the
 * payload {@link BoundaryEventVariables} describes is sealed into the {@code execution_context} store
 * as an output addressed to the handler node, which is exactly how a sandbox step hands its outputs
 * to the next node — the handler's own dispatch picks it up through
 * {@link ExecutionContextStore#loadInputsTargetingNode}.
 *
 * <p>REF-ONLY (Mapping Invariant 15): nothing is written to the engine. The only boundary data that
 * ever lives in {@code ACT_*_VARIABLE} is the error gate's own {@code __sq_error_*} sentinels, which
 * this listener reads rather than writes.
 *
 * <p>An ERROR boundary's publication is also logged as a sandbox-log row of the boundary's own id,
 * carrying the row sealed for the handler: that is what the failed call's Monitoring record shows on
 * its Error tab. The other boundary types report no failure, so they are not logged.
 *
 * <p>Best-effort by design, like every other mirror on the dispatch path: a boundary must still route
 * to its handler when the store is unreachable, so nothing here is allowed to escape and roll back
 * the engine transaction.
 */
@Slf4j
@Component("sqBoundaryEventDelegate")
@RequiredArgsConstructor
public class SqBoundaryEventDelegate implements ExecutionListener {

    private final ExecutionContextStore contextStore;
    private final ActivitiBoundaryEventHelper boundaryHelper;
    private final ObjectMapper objectMapper;
    private final SandboxLogCallback sandboxLogCallback;
    private final Redactor redactor;

    @Override
    public void notify(DelegateExecution execution) {
        String elementId = firedBoundaryId(execution);
        try {
            publish(execution, elementId);
        } catch (Exception e) {
            log.warn("sqBoundaryEventDelegate: publishing boundary {} skipped ({})", elementId, e.toString());
        }
    }

    /**
     * The element id of the boundary that fired. The listener runs on the boundary's outgoing sequence
     * flow, so the flow being taken is the current flow element and its source is the boundary.
     */
    private String firedBoundaryId(DelegateExecution execution) {
        return execution.getCurrentFlowElement() instanceof SequenceFlow flow
                ? flow.getSourceRef()
                : execution.getCurrentActivityId();
    }

    private void publish(DelegateExecution execution, String elementId) {
        ProcessContextRef ref = readRef(execution);
        ExecutionContext ctx = ref == null ? null : contextStore.loadByRef(ref).orElse(null);
        if (ctx == null || ctx.getNodes() == null) {
            log.debug("sqBoundaryEventDelegate: boundary {} fired without a loadable context ref — "
                    + "its handler gets no mapped values.", elementId);
            return;
        }
        ActivitiBoundaryEventHelper.FiredBoundary fired = boundaryHelper.resolveFired(ctx.getNodes(), elementId);
        if (fired == null) {
            log.debug("sqBoundaryEventDelegate: boundary {} does not map back to a config in this graph.", elementId);
            return;
        }
        BoundaryEventConfig cfg = fired.config();
        String hostNodeId = fired.hostNode().getId();

        // The boundary's own edge is the only dependable source of its handler (the config's
        // targetNodeId is transient and does not survive the stored workflow document).
        EdgeDto handlerEdge = handlerEdge(ctx.getEdges(), hostNodeId, cfg.getId());
        if (handlerEdge == null) {
            log.debug("sqBoundaryEventDelegate: boundary {} has no outgoing edge — nothing to publish to.", elementId);
            return;
        }

        // An INGRESS_NODE MESSAGE boundary has its typed ingress output payload sealed by
        // MessageCorrelationService before the boundary fires — do not overwrite it with generic
        // metadata-only BoundaryEventVariables.
        if (cfg.isIngressNodeSource() && hasPreSealedIngressPayload(ctx, ref, hostNodeId, cfg.getId(), handlerEdge.getTarget())) {
            log.debug("sqBoundaryEventDelegate: preserving pre-sealed ingress payload for boundary={} host={} handler={}",
                    cfg.getId(), hostNodeId, handlerEdge.getTarget());
            return;
        }

        Map<String, Object> payload = BoundaryEventVariables.payload(cfg, hostNodeId,
                execution.getProcessInstanceBusinessKey(),
                asString(execution.getVariable(EngineVariableNames.ERROR_CODE)),
                asString(execution.getVariable(EngineVariableNames.ERROR_MESSAGE)),
                asString(execution.getVariable(EngineVariableNames.ERROR_KIND)),
                asString(execution.getVariable(EngineVariableNames.ERROR_EXIT_CODE)),
                Instant.now());

        InOutValueDto published = seal(ctx, cfg, hostNodeId, handlerEdge, payload);
        log.info("sqBoundaryEventDelegate.published boundary={} host={} handler={} payload={}",
                cfg.getId(), hostNodeId, handlerEdge.getTarget(), payload);
        reportErrorPublication(ctx, cfg, published);
    }

    /**
     * Logs an ERROR boundary's publication under the boundary's id (never the host's: a second row
     * for the host would read as one more call of that node and repaint its run status). The row's
     * output is the sealed handler row — host nodeId, boundary portValue, handler target — so the
     * Monitoring UI can attribute it to the host's failed call without a new log field.
     */
    private void reportErrorPublication(ExecutionContext ctx, BoundaryEventConfig cfg, InOutValueDto published) {
        if (cfg.getType() != BoundaryEventConfig.BoundaryEventType.ERROR) {
            return;
        }
        InOutValueDto logged = new InOutValueDto();
        logged.setNodeId(published.getNodeId());
        logged.setPortId(published.getPortId());
        logged.setPortValue(published.getPortValue());
        logged.setTargetNodeId(published.getTargetNodeId());
        logged.setTargetPortId(published.getTargetPortId());
        logged.setTargetPortValue(published.getTargetPortValue());
        // Redact the logged copy only — the handler must receive the payload exactly as sealed.
        logged.setValue(redactor.redactTree(published.getValue()));
        sandboxLogCallback.report(new SandboxLogCallback.SandboxLogRecord(
                ctx.getWorkflowExecutionId(),
                cfg.getId(),
                StringUtils.defaultIfBlank(cfg.getLabel(), cfg.getId()),
                "COMPLETED",
                null,
                null,
                BpmnNodeLogPayloads.LANGUAGE,
                null,
                null,
                null,
                null,
                null,
                List.of(logged),
                null,
                null,
                "NONE",
                null));
    }

    /**
     * Seals the payload onto a row of its own and returns that row. A row per boundary rather than
     * the host node's row: a non-interrupting boundary leaves the host's sandbox running, and writing
     * onto its row would have the step's own outputs overwrite the boundary's the moment the sandbox
     * returned.
     */
    private InOutValueDto seal(ExecutionContext ctx, BoundaryEventConfig cfg, String hostNodeId,
                               EdgeDto handlerEdge, Map<String, Object> payload) {
        ExecutionContext boundaryCtx = new ExecutionContext();
        boundaryCtx.setWorkflowExecutionId(ctx.getWorkflowExecutionId());
        boundaryCtx.setWorkflowId(ctx.getWorkflowId());
        boundaryCtx.setEnvId(ctx.getEnvId());
        boundaryCtx.setUserId(ctx.getUserId());
        // The boundary is its own BPMN flow node, so the row is keyed by the boundary event id.
        boundaryCtx.setCurrentNodeId(cfg.getId());

        InOutValueDto out = new InOutValueDto();
        // nodeId is the HOST node: the designer's edge leaves the host, so that is the sourceNodeId an
        // inbound mapping on the handler matches against.
        out.setNodeId(hostNodeId);
        out.setPortId(cfg.getId());
        out.setPortValue(cfg.getId());
        out.setTargetNodeId(handlerEdge.getTarget());
        PortDto targetPort = targetPort(ctx, handlerEdge);
        if (targetPort != null) {
            out.setTargetPortId(targetPort.getId());
            out.setTargetPortValue(targetPort.getValue());
        }
        out.setValue(payload);

        contextStore.saveOutputs(contextStore.snapshot(boundaryCtx, null), List.of(out));
        return out;
    }

    /** The edge tagged with this boundary's id that leaves the host node, or null while it has none. */
    private EdgeDto handlerEdge(List<EdgeDto> edges, String hostNodeId, String boundaryEventId) {
        if (edges == null || StringUtils.isAnyBlank(hostNodeId, boundaryEventId)) {
            return null;
        }
        for (EdgeDto edge : edges) {
            if (edge == null || edge.getData() == null || !hostNodeId.equals(edge.getSource())
                    || StringUtils.isBlank(edge.getTarget())) {
                continue;
            }
            if (boundaryEventId.equals(StringUtils.trimToNull(edge.getData().getBoundaryEventId()))) {
                return edge;
            }
        }
        return null;
    }

    /** The handler's input port named by the edge's target handle ({@code INPUT-<portValue>}). */
    private PortDto targetPort(ExecutionContext ctx, EdgeDto handlerEdge) {
        String handle = handlerEdge.getTargetHandle();
        int dash = handle == null ? -1 : handle.indexOf('-');
        if (dash < 0 || dash == handle.length() - 1) {
            return null;
        }
        String portValue = handle.substring(dash + 1);
        for (NodeDto node : ctx.getNodes()) {
            if (node != null && handlerEdge.getTarget().equals(node.getId()) && node.getData() != null) {
                return node.getData().findPortByPortName(portValue);
            }
        }
        return null;
    }

    private ProcessContextRef readRef(DelegateExecution execution) {
        Object v = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (v instanceof ProcessContextRef ref) {
            return ref;
        }
        if (v instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, ProcessContextRef.class);
        }
        return null;
    }

    private String asString(Object value) {
        return value == null ? null : StringUtils.trimToNull(String.valueOf(value));
    }

    private boolean hasPreSealedIngressPayload(ExecutionContext ctx, ProcessContextRef ref,
                                               String hostNodeId, String boundaryId, String handlerNodeId) {
        try {
            List<InOutValueDto> existing = contextStore.loadInputsTargetingNode(ref, handlerNodeId);
            if (existing == null || existing.isEmpty()) {
                return false;
            }
            for (InOutValueDto row : existing) {
                if (row != null && hostNodeId.equals(row.getNodeId()) && boundaryId.equals(row.getPortValue())) {
                    return true;
                }
            }
        } catch (Exception ignored) {
            return false;
        }
        return false;
    }
}
