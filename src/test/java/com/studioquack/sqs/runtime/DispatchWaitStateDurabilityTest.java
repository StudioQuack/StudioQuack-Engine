package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Durable {@code dispatch_wait_state} + daemon restart during dispatch wait. Simulates a restart by
 * parking a real ReceiveTask (bare-ref, so the delegate parks without dispatching), persisting a
 * PENDING wait-state row stamped with a PREVIOUS boot epoch, clearing in-memory state, then
 * re-running boot hydration and delivering the result — the token resumes with no duplicate.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-dws-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-dws-jobs.wal",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
class DispatchWaitStateDurabilityTest {

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private DispatchExecutionRegistry registry;
    @Autowired private DispatchResultCoordinator coordinator;
    @Autowired private DispatchWaitStateRepository repository;

    @Test
    void register_then_resolve_and_discard_write_through_to_h2() {
        registry.register("d-res", "exec-res", "pi-res", "bk-res", "nodeA", "wait_nodeA", "env");
        assertEquals(DispatchStatus.PENDING.name(),
                repository.findById("d-res").orElseThrow().getStatus(), "register persisted PENDING");

        registry.complete("d-res");
        assertEquals(DispatchStatus.RESOLVED.name(),
                repository.findById("d-res").orElseThrow().getStatus(), "complete persisted RESOLVED");

        registry.register("d-dis", "exec-dis", "pi-dis", "bk-dis", "nodeB", "wait_nodeB", "env");
        registry.discard("d-dis", "test orphan");
        DispatchWaitStateEntity discarded = repository.findById("d-dis").orElseThrow();
        assertEquals(DispatchStatus.DISCARDED.name(), discarded.getStatus(), "discard persisted DISCARDED");
        assertEquals("test orphan", discarded.getErrorMessage());
    }

    @Test
    void restart_during_dispatch_wait_hydrates_and_resumes_without_duplicate() throws Exception {
        // 1) Deploy start -> node(sandbox) -> end and park it at the ReceiveTask via a bare ref.
        String wfeId = UUID.randomUUID().toString();
        String nodeId = "workN";
        WorkflowGraph graph = twoNode(wfeId, nodeId);
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(wfeId).workflowExecutionId(wfeId).envId("env-dws").schemaVersion(1).build();
        var pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        assertNotNull(pi);

        // 2) Wait until the token is genuinely parked at wait_workN.
        String parkedExec = awaitParked(pi.getId(), "wait_" + nodeId);
        assertNotNull(parkedExec, "token parked at the ReceiveTask");

        // 3) Simulate a PREVIOUS boot: a PENDING durable row with an OLD epoch, no in-memory entry.
        String dispatchId = "d-restart";
        repository.save(DispatchWaitStateEntity.builder()
                .dispatchId(dispatchId).executionId(parkedExec).processInstanceId(pi.getId())
                .businessKey(wfeId).nodeId(nodeId).waitActivityId("wait_" + nodeId).environmentId("env-dws")
                .clientEpoch("PREVIOUS-BOOT-EPOCH").status(DispatchStatus.PENDING.name())
                .registeredAt(OffsetDateTime.now().minusMinutes(1)).build());
        assertTrue(registry.find(dispatchId).isEmpty(), "in-memory state was lost across the 'restart'");

        // 4) Boot hydration re-loads the PENDING row (token still parked → kept).
        int hydrated = registry.hydrateOnBoot();
        assertEquals(1, hydrated, "the still-parked wait-state is hydrated");
        assertTrue(registry.find(dispatchId).isPresent(), "hydrated back into the in-memory registry");

        // 5) Deliver the (re-)result → the token resumes and the PI completes; row is RESOLVED.
        boolean delivered = coordinator.deliverSuccess(dispatchId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), Map.of());
        assertTrue(delivered, "the re-delivered result resumed the parked token");
        assertTrue(await(() -> sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0),
                "the process instance completed after resume");
        assertEquals(DispatchStatus.RESOLVED.name(),
                repository.findById(dispatchId).orElseThrow().getStatus(), "wait-state marked RESOLVED");

        // 6) No duplicate: a second delivery is a no-op (idempotent).
        assertFalse(coordinator.deliverSuccess(dispatchId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), Map.of()), "second delivery is a no-op");
    }

    @Test
    void hydration_discards_orphan_when_token_no_longer_parked() {
        String dispatchId = "d-orphan";
        repository.save(DispatchWaitStateEntity.builder()
                .dispatchId(dispatchId).executionId("gone-exec").processInstanceId("gone-pi")
                .businessKey("gone-bk").nodeId("n").waitActivityId("wait_n").environmentId("env")
                .clientEpoch("PREVIOUS-BOOT-EPOCH").status(DispatchStatus.PENDING.name())
                .registeredAt(OffsetDateTime.now().minusMinutes(5)).build());

        int hydrated = registry.hydrateOnBoot();

        assertEquals(0, hydrated, "an orphan (no parked token) is not hydrated");
        assertEquals(DispatchStatus.DISCARDED.name(),
                repository.findById(dispatchId).orElseThrow().getStatus(), "orphan row DISCARDED (audited)");
        assertTrue(registry.find(dispatchId).isEmpty(), "orphan is not in the in-memory registry");
    }

    // ---- helpers -------------------------------------------------------------

    private WorkflowGraph twoNode(String wfId, String nodeId) {
        NodeDto start = simpleNode("startN", "start.event", null, "start");
        NodeDto work = simpleNode(nodeId, "custom.sandbox", "custom-script-executor", "serviceTask");
        // a declared output port so the node is a well-formed sandbox node
        PortDto out = new PortDto();
        out.setId("out"); out.setName("out"); out.setValue("out"); out.setType(PortType.OUTPUT);
        work.getData().setPorts(new ArrayList<>(List.of(out)));
        NodeDto end = simpleNode("endN", "end.event", null, "end");
        List<NodeDto> nodes = List.of(start, work, end);
        List<EdgeDto> edges = List.of(edge("startN", nodeId), edge(nodeId, "endN"));
        return new WorkflowGraph(wfId, "DWS durability", "1", nodes, edges);
    }

    private NodeDto simpleNode(String id, String bpmnType, String jobType, String nodeType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType); t.setJobType(jobType); t.setNodeType(nodeType); t.setLabel(id);
        NodeDto n = new NodeDto();
        n.setId(id); n.setData(t);
        return n;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto e = new EdgeDto();
        e.setId("e_" + source + "_" + target); e.setSource(source); e.setTarget(target);
        return e;
    }

    private String awaitParked(String processInstanceId, String waitActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            String exec = sqsEngine.findUnfinishedWaitExecution(processInstanceId, waitActivityId);
            if (exec != null) {
                return exec;
            }
            Thread.sleep(50);
        }
        return sqsEngine.findUnfinishedWaitExecution(processInstanceId, waitActivityId);
    }

    private boolean await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50);
        }
        return cond.getAsBoolean();
    }
}
