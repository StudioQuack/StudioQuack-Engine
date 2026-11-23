package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the link throw/catch data contract (deep-audit C3): a
 * {@code link.intermediate.throw.event} carries an {@code end} execution listener that reads the
 * throw's DuckTape-mapped {@code link} IN {@code payload} and fans the paired catch's
 * template-promised {@code link_payload} OUT envelope ({@code name}, {@code payload}) out to the
 * catch's downstream — one sealed row per target, keyed by businessKey (the start-run shape).
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-link-jump-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class LinkJumpPayloadRuntimeTest {

    private static final String LINK_NAME = "L1";

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore executionContextStore;

    @Test
    void link_throw_carries_the_mapped_payload_into_the_catchs_link_payload_envelope() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        seedMappedPayload(businessKey, "throwN", Map.of("orderId", "O-1"));
        ProcessInstance instance = deployAndStartLinkFlow(
                "linkJump-" + UUID.randomUUID(), businessKey);

        assertTrue(awaitParked(instance, "node_taskN", 30_000),
                "the token rides the materialised link flow and parks on the downstream task");

        Map<?, ?> envelope = readLinkPayloadEnvelope(businessKey, "taskN");
        assertNotNull(envelope, "the catch node's `link_payload` envelope row targets its downstream node");
        assertEquals(LINK_NAME, envelope.get("name"));
        assertEquals(Map.of("orderId", "O-1"), envelope.get("payload"),
                "the throw's mapped payload crossed the jump unaltered");
    }

    @Test
    void link_throw_without_a_mapped_payload_still_emits_the_link_name() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartLinkFlow(
                "linkJump-" + UUID.randomUUID(), businessKey);

        assertTrue(awaitParked(instance, "node_taskN", 30_000),
                "the token rides the materialised link flow and parks on the downstream task");

        Map<?, ?> envelope = readLinkPayloadEnvelope(businessKey, "taskN");
        assertNotNull(envelope, "the envelope is emitted even with no mapped payload");
        assertEquals(LINK_NAME, envelope.get("name"));
        assertFalse(envelope.containsKey("payload"), "no payload key when nothing was mapped");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /**
     * Pre-seeds the store with the pipeline's wire shape for a DuckTape mapping into the throw's
     * {@code link} IN port — the same pseudo-node pattern
     * {@code MessageCorrelationServiceTest.seedSubscriptionOverride} uses.
     */
    private void seedMappedPayload(String businessKey, String throwNodeId, Map<String, Object> payload) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setCurrentNodeId("upstreamN" + UUID.randomUUID());
        ProcessContextRef ref = executionContextStore.snapshot(ctx, null);
        InOutValueDto row = new InOutValueDto();
        row.setNodeId(ctx.getCurrentNodeId());
        row.setPortId("serviceOutput");
        row.setPortValue("serviceOutput");
        row.setTargetNodeId(throwNodeId);
        row.setTargetPortId("1c4b2bf7-0698-4be6-95ab-2d0169d0b9b9");
        row.setTargetPortValue("link");
        row.setValue(Map.of("payload", payload));
        executionContextStore.saveOutputs(ref, List.of(row));
    }

    /**
     * Deploys and starts {@code start → linkThrow(L1) ⇢ linkCatch(L1) → user.task → end} — the
     * throw/catch pair has no drawn edge; generation validates the pair and materialises the jump.
     */
    private ProcessInstance deployAndStartLinkFlow(String workflowId, String businessKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto throwN = node("throwN", "link.intermediate.throw.event", null);
        param(throwN, "name", LINK_NAME);
        NodeDto catchN = node("catchN", "link.intermediate.catch.event", null);
        param(catchN, "name", LINK_NAME);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Link Jump Runtime", "1",
                List.of(start, throwN, catchN, task, end),
                List.of(edge("startN", "throwN"),
                        edge("catchN", "taskN"),
                        edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "process started");
        return instance;
    }

    private Map<?, ?> readLinkPayloadEnvelope(String businessKey, String downstreamNodeId) {
        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<InOutValueDto> downstream = executionContextStore.loadInputsTargetingNode(readRef, downstreamNodeId);
        return downstream.stream()
                .filter(r -> "catchN".equals(r.getNodeId()) && "link_payload".equals(r.getPortId()))
                .findFirst()
                .map(InOutValueDto::getValue)
                .filter(Map.class::isInstance)
                .map(v -> (Map<?, ?>) v)
                .orElse(null);
    }

    private boolean awaitParked(ProcessInstance instance, String activityId, long budgetMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + budgetMs;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.executions().processInstanceId(instance.getId())
                    .activityId(activityId).count() > 0) {
                return true;
            }
            Thread.sleep(250);
        }
        return sqsEngine.executions().processInstanceId(instance.getId())
                .activityId(activityId).count() > 0;
    }

    private static void param(NodeDto node, String shortCode, String value) {
        ParameterDto param = new ParameterDto();
        param.setShortCode(shortCode);
        param.setValue(value);
        List<ParameterDto> existing = node.getData().getConfigurationParameters();
        List<ParameterDto> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        merged.add(param);
        node.getData().setConfigurationParameters(merged);
    }

    private NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        return edge;
    }
}
