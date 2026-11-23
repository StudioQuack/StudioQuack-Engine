package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.delegate.ScriptDispatchDelegate;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * END-TO-END test — a python node executes in a real container and advances a 2-node workflow
 * started in-process by {@link SqsEngine}, with the stub delegate replaced by the real {@link
 * ScriptDispatchDelegate}. GATED on a live container runtime + {@code info} probe.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-e2e-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-e2e-jobs.wal",
        "sqs.sandbox.resume-poll-ms=8000",
        // Don't pre-warm the default per-language pool on boot during this focused dispatch test.
        "sqs.sandbox.warmpool.enabled=false",
        // This test's subject is the DISPATCH pipeline, not the isolation policy, and `runsc` is
        // Linux-only so a developer Mac can never satisfy a gVisor floor. The pin
        // isolation-enforce=true, which would make every job here fail closed in preflight and turn
        // a dispatch regression into an indistinguishable "isolation floor not met".
        "sqs.sandbox.isolation-enforce=false",
        // Focus on the dispatch pipeline, not overlay-egress enforcement (no overlay in this test).
        "sqs.sandbox.overlay-egress=off",
        // Isolate container ownership from other cached SpringBootTest contexts sharing ./data.
        "SQS_DATA_DIR=${java.io.tmpdir}/sqs-e2e-data"
})
@ActiveProfiles("base")
class SandboxDispatchEndToEndTest {

    @Autowired
    private WorkflowDeploymentService workflowDeploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore contextStore;
    @Autowired
    private ScriptDispatchDelegate scriptDispatchDelegate;
    @Autowired
    private ContainerRuntime runtime;

    @Test
    void python_node_runs_in_a_container_and_advances_the_two_node_workflow() throws Exception {
        Assumptions.assumeTrue(runtime.available(),
                "no container runtime available — skipping end-to-end container dispatch");
        Assumptions.assumeTrue(
                CliRunner.run(List.of(runtime.cli(), "info"), Duration.ofSeconds(15)).ok(),
                "`" + runtime.cli() + " info` failed — skipping end-to-end container dispatch");

        String workflowId = "wfE3E2E";
        String pythonNodeId = "pythonNode";
        String wfeId = UUID.randomUUID().toString();

        // 1) Graph: start → pythonNode(sandbox) → end.
        NodeDto start = node("startN", "start.event", null, "start", null);
        NodeDto py = node(pythonNodeId, "custom.sandbox", "custom-script-executor", "serviceTask",
                // user script (the helper preamble is prepended by GenerateScriptService).
                "studioquack.return_result([{\"port\": \"out\", \"value\": \"e2e-ok\"}])\n"
                        + "print('python-node-ran')\n");
        NodeDto end = node("endN", "end.event", null, "end", null);
        List<NodeDto> nodes = List.of(start, py, end);
        List<EdgeDto> edges = List.of(edge("startN", pythonNodeId), edge(pythonNodeId, "endN"));
        WorkflowGraph graph = new WorkflowGraph(workflowId, "WP-E3 E2E", "1", nodes, edges);

        // 2) Seal the node context so the delegate can rehydrate it by studioquackRef (REF-ONLY).
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(workflowId);
        ctx.setEnvId("env-e2e");
        ctx.setCurrentNodeId(pythonNodeId);
        ctx.setNodes(new ArrayList<>(nodes));
        ctx.setEdges(new ArrayList<>(edges));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);

        // 3) Deploy + start with REF-ONLY variables (businessKey == workflowExecutionId).
        var info = workflowDeploymentService.deploy(graph, null, true);
        long before = scriptDispatchDelegate.getInvocationCount();
        ProcessInstance instance = workflowDeploymentService.startProcessInstance(
                graph, info, wfeId, Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertEquals(wfeId, instance.getBusinessKey());

        // 4) Wait for the token to leave the ReceiveTask and the PI to complete.
        boolean completed = await(Duration.ofSeconds(90), () ->
                sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0);
        assertTrue(completed, "the process instance completed (token advanced past the sandbox ReceiveTask)");
        assertTrue(scriptDispatchDelegate.getInvocationCount() > before, "the real dispatch delegate fired");

        // 5) The PI finished in history + the python node's output was sealed in the context store.
        long finished = sqsEngine.historicProcessInstances()
                .processInstanceBusinessKey(wfeId).finished().count();
        assertEquals(1L, finished, "exactly one finished PI in history");

        // Success path: the token passed through the error gate (no BpmnError) to the normal end —
        // it did NOT route through the interrupting boundary → shared sandbox-error end.
        boolean tookErrorEnd = sqsEngine.historicActivityInstances(instance.getId()).list().stream()
                .anyMatch(a -> "Event_SandboxErrorEnd".equals(a.getActivityId()));
        assertFalse(tookErrorEnd, "sandbox succeeded → normal completion, not the error boundary");

        Map<String, List<InOutValueDto>> outputs = contextStore.collectOutputsByNode(wfeId);
        List<InOutValueDto> nodeOut = outputs.get(pythonNodeId);
        assertTrue(nodeOut != null && !nodeOut.isEmpty(), "the python node's outputs were sealed");
        assertTrue(nodeOut.stream().anyMatch(o -> "e2e-ok".equals(String.valueOf(o.getValue()))),
                "the container's per-port output carried through to the sealed context store");
    }

    private NodeDto node(String id, String bpmnType, String jobType, String nodeType, String script) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setJobType(jobType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        if (script != null) {
            template.setTemplateScript(new TemplateScriptDto(id + "-s", id + "-t", "python", script));
            // A declared output port (real nodes always have ports; the pipeline fans outputs onto it).
            PortDto out = new PortDto();
            out.setId("out");
            out.setName("out");
            out.setValue("out");
            out.setType(PortType.OUTPUT);
            template.setPorts(new ArrayList<>(List.of(out)));
        }
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

    private boolean await(Duration timeout, java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(200);
        }
        return cond.getAsBoolean();
    }
}
