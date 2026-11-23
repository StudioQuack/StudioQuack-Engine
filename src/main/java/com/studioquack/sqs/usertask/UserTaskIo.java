package com.studioquack.sqs.usertask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.delegate.ScriptDispatchDelegate;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.pipeline.PreparedInbound;
import com.studioquack.sqs.execution.pipeline.TransformPipeline;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@code user.task}'s inbound and outbound data, through exactly the pipeline every dispatched
 * node uses. The task runs no sandbox, so the steps a dispatch performs around one are applied
 * directly: inbound = the run context's carried entry payload (dispatch merge rule) + persisted
 * upstream rows + rows pinned from other nodes →.
 */
@Slf4j
@Component
public class UserTaskIo {

    /** INPUT port value of the user task. */
    public static final String TASK_INPUT_PORT = "task_input";
    /** Wrapper field inside Task Input whose children the recipient reviews. */
    public static final String VALUES_FIELD = "values";
    /** OUTPUT port value of the user task. */
    public static final String TASK_OUTPUT_PORT = "task_output";
    /** Field of Task Output carrying {@code approved} / {@code rejected}. */
    public static final String DECISION_FIELD = "decision";

    private final ExecutionContextStore contextStore;
    private final TransformPipeline transformPipeline;
    private final ObjectMapper mapper;

    public UserTaskIo(ExecutionContextStore contextStore, TransformPipeline transformPipeline, ObjectMapper mapper) {
        this.contextStore = contextStore;
        this.transformPipeline = transformPipeline;
        this.mapper = mapper;
    }

    /**
     * The run's context positioned on the task, with its inbound resolved.
     *
     * @param ctx The context; {@code ctx.getCurrentNode} carries the task's resolved {@code inputList}
     * and its configuration parameters
     * @param graphNodes The run graph (the pipeline clears it off {@code ctx})
     * @param graphEdges The run graph's edges
     * @param inputVariables
     */
    public record Inbound(ExecutionContext ctx, List<NodeDto> graphNodes, List<EdgeDto> graphEdges,
                          Map<String, Object> inputVariables) {

        public NodeDto node() {
            return ctx.getCurrentNode();
        }
    }

    /**
     * Resolves the task's inbound off the run's {@code studioquackRef} engine variable.
     *
     * @return empty when the run has no loadable context (nothing was mapped, or the ref is gone)
     */
    public Optional<Inbound> resolveInbound(Object studioquackRef, String nodeId) {
        ProcessContextRef ref = refOf(studioquackRef);
        ExecutionContext ctx = ref == null ? null : contextStore.loadByRef(ref).orElse(null);
        if (ctx == null || nodeId == null) {
            return Optional.empty();
        }
        List<NodeDto> graphNodes = ctx.getNodes() == null ? List.of() : new ArrayList<>(ctx.getNodes());
        List<EdgeDto> graphEdges = ctx.getEdges() == null ? List.of() : new ArrayList<>(ctx.getEdges());
        // The node the run context was minted for and the rows it carries for that node — a seeded
        // run (node trial, call-activity child) hands the entry node its payload this way.
        String refNodeId = ctx.getCurrentNodeId();
        List<InOutValueDto> carried = ctx.getNextNodeInputList();
        ctx.setCurrentNodeId(nodeId);
        ctx.setCurrentNode(null);

        List<InOutValueDto> upstream = contextStore.loadInputsTargetingNode(ref, nodeId);
        ForeignEdgeInputResolver.Resolution foreign = ForeignEdgeInputResolver.resolveWithCarriers(
                contextStore, ref, nodeId, graphEdges, ctx.getWorkflowVariables(), ctx.getProjectVariables());
        List<InOutValueDto> merged = merge(
                ScriptDispatchDelegate.mergeNodeInputs(nodeId, refNodeId, carried, upstream, null),
                foreign.rows());
        ctx.setPinnedInputCarriers(ForeignEdgeInputResolver.pinnedCarriers(merged, foreign));
        ctx.setNextNodeInputList(merged);

        PreparedInbound prepared = transformPipeline.prepareInbound(ctx);
        prepared = transformPipeline.filterInboundAgainstPortSchemas(ctx, prepared);
        Map<String, Object> inputVariables = prepared == null || prepared.inputVariables() == null
                ? Map.of() : prepared.inputVariables();
        return Optional.of(new Inbound(ctx, graphNodes, graphEdges, inputVariables));
    }

    /**
     * The Values wrapper's fields from a resolved port map. Several connections into Task Input
     * arrive as a list of messages; their Values merge in arrival order.
     */
    public static Map<String, Object> values(Map<String, Object> inputVariables) {
        Map<String, Object> merged = new LinkedHashMap<>();
        Object port = inputVariables == null ? null : inputVariables.get(TASK_INPUT_PORT);
        List<?> messages = port instanceof List<?> list ? list : port == null ? List.of() : List.of(port);
        for (Object message : messages) {
            if (message instanceof Map<?, ?> fields && fields.get(VALUES_FIELD) instanceof Map<?, ?> values) {
                values.forEach((key, value) -> {
                    if (key != null) {
                        merged.put(key.toString(), value);
                    }
                });
            }
        }
        return merged;
    }

    /**
     * Publishes the decision on Task Output exactly as a sandbox result is published and seals the
     * fanned-out rows on a fresh dispatch row, then puts the graph back on the context so gateway
     * route tokens can be resolved from it.
     *
     * @return the fanned-out output rows (empty when the task has no downstream wire)
     */
    public List<InOutValueDto> publishDecision(Inbound inbound, String decision) {
        if (inbound.node() == null || inbound.node().getData() == null || inbound.node().getData().getPorts() == null) {
            log.warn("user-task.outputs node={} declares no ports — decision not published",
                    inbound.ctx().getCurrentNodeId());
            inbound.ctx().setNodes(inbound.graphNodes());
            inbound.ctx().setEdges(inbound.graphEdges());
            return List.of();
        }
        Map<String, Object> port = new LinkedHashMap<>();
        port.put(DECISION_FIELD, decision);
        Map<String, Object> outputs = new HashMap<>();
        outputs.put(TASK_OUTPUT_PORT, port);
        transformPipeline.finalizeOutputs(inbound.ctx(), outputs);
        NodeDto node = inbound.node();
        List<InOutValueDto> rows = node == null || node.getOutputList() == null ? List.of() : node.getOutputList();
        if (!rows.isEmpty()) {
            ProcessContextRef writeRef = contextStore.snapshot(inbound.ctx(), null);
            contextStore.saveOutputs(writeRef, rows);
        }
        inbound.ctx().setNodes(inbound.graphNodes());
        inbound.ctx().setEdges(inbound.graphEdges());
        log.info("user-task.outputs node={} fannedOut={}", inbound.ctx().getCurrentNodeId(), rows.size());
        return rows;
    }

    /** Merged inbound first; a pinned row from another node joins unless it names the same wire. */
    private static List<InOutValueDto> merge(List<InOutValueDto> upstream, List<InOutValueDto> foreign) {
        LinkedHashMap<String, InOutValueDto> byKey = new LinkedHashMap<>();
        for (List<InOutValueDto> rows : List.of(upstream == null ? List.<InOutValueDto>of() : upstream,
                foreign == null ? List.<InOutValueDto>of() : foreign)) {
            for (InOutValueDto row : rows) {
                if (row != null) {
                    byKey.putIfAbsent(ForeignEdgeInputResolver.bindingKey(row), row);
                }
            }
        }
        return new ArrayList<>(byKey.values());
    }

    private ProcessContextRef refOf(Object variable) {
        if (variable instanceof ProcessContextRef ref) {
            return ref;
        }
        if (variable instanceof Map<?, ?> map) {
            return mapper.convertValue(map, ProcessContextRef.class);
        }
        return null;
    }
}
