package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every fire of a timer start is its own run: the second, third … fire of a cycle each start an
 * instance under a fresh {@code tm_…} run id (the businessKey the run index and the node logs key
 * on), with the seeded {@code studioquackRef} a start-run or message start would carry.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-timer-start-run-identity-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class TimerStartRunIdentityTest {

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private ManagementService managementService;
    @Autowired
    private RuntimeService runtimeService;
    @Autowired
    private ExecutionContextStore contextStore;

    @Test
    void each_timer_fire_starts_a_run_with_its_own_run_id_and_seeded_context() throws InterruptedException {
        String workflowId = "timer-run-" + UUID.randomUUID();
        NodeDto timer = node("timerN", "timer.start.event", "start");
        param(timer, "type", "cycle");
        param(timer, "value", "R3/PT1M");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Timer Run Identity", "1",
                List.of(timer, node("taskN", "user.task", null), node("endN", "end.event", "end")),
                List.of(edge("timerN", "taskN"), edge("taskN", "endN")));
        DeploymentInfo info = deploymentService.deploy(graph, null, true);

        fireNextTimer(info.getProcessDefinitionId());
        fireNextTimer(info.getProcessDefinitionId());

        List<ProcessInstance> runs = runtimeService.createProcessInstanceQuery()
                .processDefinitionId(info.getProcessDefinitionId()).list();
        assertEquals(2, runs.size(), "two fires, two runs");
        Set<String> runIds = new HashSet<>();
        for (ProcessInstance run : runs) {
            assertNotNull(run.getBusinessKey(), "a timer-started run carries a run id");
            assertTrue(run.getBusinessKey().startsWith("tm_"), "minted timer run id: " + run.getBusinessKey());
            runIds.add(run.getBusinessKey());
            Object ref = runtimeService.getVariable(run.getId(), EngineVariableNames.STUDIOQUACK_REF);
            assertNotNull(ref, "the run is seeded like a start-run, so its first node has a context");
            assertTrue(contextStore.loadInputsTargetingNode((ProcessContextRef) ref, "taskN").stream()
                            .anyMatch(in -> "timer_info".equals(in.getPortValue())),
                    "the timer start hands timer_info to its downstream node under this run's id");
        }
        assertEquals(2, runIds.size(), "each fire mints its own run id");
    }

    /**
     * Makes the definition's pending timer-start job due now instead of in a minute, and waits for the
     * engine's own job executor to run it (executing it here as well would race the executor).
     */
    private void fireNextTimer(String processDefinitionId) throws InterruptedException {
        long before = runtimeService.createProcessInstanceQuery().processDefinitionId(processDefinitionId).count();
        Job timer = managementService.createTimerJobQuery().processDefinitionId(processDefinitionId).singleResult();
        assertNotNull(timer, "the timer start is scheduled");
        managementService.moveTimerToExecutableJob(timer.getId());
        for (int i = 0; i < 100; i++) {
            if (runtimeService.createProcessInstanceQuery().processDefinitionId(processDefinitionId).count() > before) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("the timer fire did not start a run within 10 s");
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

    private static NodeDto node(String id, String bpmnType, String nodeType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private static EdgeDto edge(String source, String target) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        return edge;
    }
}
