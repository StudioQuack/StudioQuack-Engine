package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runtime proof for the timer-catch {@code timer_info} emission (deep-audit C2): a
 * {@code timer.intermediate.catch.event} carries an {@code end} execution listener that, when the
 * timer job fires and the token leaves the catch, fans the template-promised
 * {@code {firedAt, kind, due}} envelope out to the node's downstream as one sealed
 * {@code timer_info} row per target. The run is started with a businessKey and a seeded context
 * (the start-run shape), so the downstream scan keys resolve.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-timer-info-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class TimerInfoEmissionRuntimeTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore executionContextStore;

    @Test
    void timer_catch_fans_out_timer_info_to_its_downstream_when_the_timer_fires() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartTimerCatchFlow(
                "timerInfo-" + UUID.randomUUID(), businessKey, "duration", "PT2S");

        assertTrue(awaitParked(instance, "node_taskN", 30_000),
                "the PT2S timer fires within the budget and the token parks on the downstream task");

        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<InOutValueDto> downstream = executionContextStore.loadInputsTargetingNode(readRef, "taskN");
        InOutValueDto row = downstream.stream()
                .filter(r -> "catchN".equals(r.getNodeId()) && "timer_info".equals(r.getPortId()))
                .findFirst().orElse(null);
        assertNotNull(row, "the catch node's `timer_info` envelope row targets its downstream node");
        assertTrue(row.getValue() instanceof Map, "timer_info is a single object value");
        Map<?, ?> timerInfo = (Map<?, ?>) row.getValue();
        assertEquals("duration", timerInfo.get("kind"));
        assertEquals("PT2S", timerInfo.get("due"), "the effective deployed expression is reported");
        assertNotNull(timerInfo.get("firedAt"), "ISO-8601 fire timestamp");
    }

    // ---- fixtures --------------------------------------------------------------------------

    /** Deploys and starts {@code start → timer.intermediate.catch.event(type, value) → user.task}. */
    private ProcessInstance deployAndStartTimerCatchFlow(String workflowId, String businessKey,
                                                         String type, String value) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = node("catchN", "timer.intermediate.catch.event", null);
        param(catchN, "type", type);
        param(catchN, "value", value);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Timer Info Runtime", "1",
                List.of(start, catchN, task, end),
                List.of(edge("startN", "catchN"),
                        edge("catchN", "taskN"),
                        edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "process started");
        return instance;
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
