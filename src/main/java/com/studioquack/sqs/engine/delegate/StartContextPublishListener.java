package com.studioquack.sqs.engine.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
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
 * Publishes a none start event's output ({@code start_context}) when the run starts, as sealed
 * output rows addressed to its downstream nodes — the same row shape any producer's port value
 * takes — so a DuckTape mapping from the start card runs on the next node's inbound like on any
 * other connection. No run trigger carries a start payload today, so the message is empty: what
 * flows is what the author typed on the start card, planted as a JOLT default before the shift.
 *
 * <p>Only connections whose DuckTape screen maps something get the message. On an unmapped
 * connection it would carry nothing and only add a blank message to the target port (a loop back
 * into the first node would then see two messages where it saw one).
 *
 * <p>Attached on {@code start} after {@code callActivityChildContextListener}, so a call-activity
 * child publishes against its own reseeded context. Fully guarded: a failure is logged and the run
 * continues without the start message.
 */
@Slf4j
@Component("startContextPublishListener")
public class StartContextPublishListener implements ExecutionListener {

    private static final String NODE_ID_PREFIX = "node_";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<DeployedArtifactStore> artifactStoreProvider;
    private final ObjectProvider<ExecutionContextStore> contextStoreProvider;
    private final ObjectMapper mapper;

    public StartContextPublishListener(ObjectProvider<SqsEngine> sqsEngineProvider,
                                       ObjectProvider<DeployedArtifactStore> artifactStoreProvider,
                                       ObjectProvider<ExecutionContextStore> contextStoreProvider,
                                       ObjectMapper mapper) {
        this.sqsEngineProvider = sqsEngineProvider;
        this.artifactStoreProvider = artifactStoreProvider;
        this.contextStoreProvider = contextStoreProvider;
        this.mapper = mapper;
    }

    @Override
    public void notify(DelegateExecution execution) {
        if (!ExecutionListener.EVENTNAME_START.equals(execution.getEventName())) {
            return;
        }
        try {
            publish(execution);
        } catch (Exception ex) {
            log.warn("start.context.publish failed activity={}: {}", execution.getCurrentActivityId(), ex.toString());
        }
    }

    private void publish(DelegateExecution execution) {
        ExecutionContextStore store = contextStoreProvider.getIfAvailable();
        ProcessContextRef ref = refOf(execution.getVariable(EngineVariableNames.STUDIOQUACK_REF));
        ExecutionContext runCtx = store == null || ref == null ? null : store.loadByRef(ref).orElse(null);
        if (runCtx == null) {
            return;
        }
        String startNodeId = StringUtils.removeStart(
                StringUtils.defaultString(execution.getCurrentActivityId()), NODE_ID_PREFIX);
        List<NodeDto> nodes = runCtx.getNodes();
        List<EdgeDto> edges = runCtx.getEdges();
        if (edges == null || edges.isEmpty()) {
            DeployedArtifactStore.DeployedGraph deployed = deployedGraph(execution);
            if (deployed == null) {
                return;
            }
            nodes = deployed.nodes();
            edges = deployed.edges();
        }
        List<InOutValueDto> rows = mappedStartRows(startNodeId, nodes == null ? List.of() : nodes, edges);
        if (rows.isEmpty()) {
            return;
        }
        ExecutionContext writeCtx = new ExecutionContext();
        writeCtx.setWorkflowExecutionId(StringUtils.defaultIfBlank(
                runCtx.getWorkflowExecutionId(), execution.getProcessInstanceBusinessKey()));
        writeCtx.setWorkflowId(runCtx.getWorkflowId());
        writeCtx.setCurrentNodeId(startNodeId);
        store.saveOutputs(store.snapshot(writeCtx, null), rows);
        log.info("start.context.published node={} downstream={} businessKey={}",
                startNodeId, rows.size(), writeCtx.getWorkflowExecutionId());
    }

    /** One empty-message row per mapped connection leaving the start node, addressed by its handles. */
    static List<InOutValueDto> mappedStartRows(String startNodeId, List<NodeDto> nodes, List<EdgeDto> edges) {
        List<InOutValueDto> rows = new ArrayList<>();
        for (EdgeDto edge : edges) {
            if (edge == null || !startNodeId.equals(edge.getSource()) || edge.getTarget() == null
                    || !isMappedJoltScreen(edge.getData())) {
                continue;
            }
            String sourcePort = shortCode(edge.getSourceHandle());
            String targetPort = shortCode(edge.getTargetHandle());
            InOutValueDto row = new InOutValueDto();
            row.setNodeId(startNodeId);
            row.setPortId(portId(nodeById(nodes, startNodeId), sourcePort));
            row.setPortValue(sourcePort);
            row.setTargetNodeId(edge.getTarget());
            row.setTargetPortId(portId(nodeById(nodes, edge.getTarget()), targetPort));
            row.setTargetPortValue(targetPort);
            row.setValue(new LinkedHashMap<String, Object>());
            rows.add(row);
        }
        return rows;
    }

    private static boolean isMappedJoltScreen(EdgeDataDto data) {
        return data != null && data.getEdgeTransforms() != null && !data.getEdgeTransforms().isEmpty()
                && "jolt".equalsIgnoreCase(TransformationCollector.resolveEffectiveTransformLanguage(data));
    }

    private static NodeDto nodeById(List<NodeDto> nodes, String id) {
        return nodes.stream().filter(n -> n != null && id.equals(n.getId())).findFirst().orElse(null);
    }

    /** The port a handle ({@code OUTPUT-<port>} / {@code INPUT-<port>}) names, matched by port name. */
    private static String portId(NodeDto node, String portName) {
        if (node == null || node.getData() == null || node.getData().getPorts() == null || portName == null) {
            return null;
        }
        return node.getData().getPorts().stream()
                .filter(p -> p != null && (portName.equals(p.getName()) || portName.equals(p.getValue())))
                .map(PortDto::getId).findFirst().orElse(null);
    }

    private static String shortCode(String handle) {
        int dash = handle == null ? -1 : handle.indexOf('-');
        return dash < 0 ? null : handle.substring(dash + 1);
    }

    private DeployedArtifactStore.DeployedGraph deployedGraph(DelegateExecution execution) {
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        DeployedArtifactStore artifactStore = artifactStoreProvider.getIfAvailable();
        BpmnModel model = engine == null ? null : engine.readDeployedModel(execution.getProcessDefinitionId());
        if (artifactStore == null || model == null || model.getMainProcess() == null) {
            return null;
        }
        String processKey = model.getMainProcess().getId();
        String workflowId = WorkflowGraph.workflowIdOf(processKey);
        if (workflowId == null) {
            return null;
        }
        return artifactStore.loadDeployedGraph(workflowId + WorkflowGraph.namespaceSuffixOf(processKey)).orElse(null);
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
