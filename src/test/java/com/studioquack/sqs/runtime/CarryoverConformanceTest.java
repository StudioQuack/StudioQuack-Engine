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
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ExclusiveGateway;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.delegate.JavaDelegate;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Conformance suite — engine-level carry-over cases that do not need the sandbox: a gateway with no
 * matching route + no default raises an incident (dead-letter); a dead-letter job retries back to
 * executable.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-carryover-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-carryover-jobs.wal"
})
@ActiveProfiles("base")
class CarryoverConformanceTest {

    @Autowired private SqsEngine sqsEngine;
    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private WorkflowReplayService replayService;
    @Autowired private OrphanExecutionSweeper sweeper;
    @Autowired private DispatchWaitStateRepository dispatchRepo;

    /** A delegate that throws on its FIRST invocation only — so a retry succeeds deterministically. */
    public static class FlakyOnceDelegate implements JavaDelegate {
        static final AtomicInteger CALLS = new AtomicInteger();

        @Override
        public void execute(org.activiti.engine.delegate.DelegateExecution execution) {
            if (CALLS.getAndIncrement() == 0) {
                throw new RuntimeException("flaky: fail once");
            }
        }
    }

    // Missing route token → incident (dead-letter)

    @Test
    void gateway_missing_token_raises_incident() throws Exception {
        String key = "Process_incident_" + UUID.randomUUID().toString().substring(0, 8);
        sqsEngine.deploy("incident", Map.of("incident.bpmn",
                gatewayNoDefaultXml(key, "${sqErrorGateDelegate}")));
        String wfeId = UUID.randomUUID().toString();
        ProcessInstance pi = sqsEngine.startLatest(key, wfeId, Map.of());

        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0),
                "a gateway with no matching route + no default flow raised an incident (dead-letter job)");
    }

    // ---- retry: dead-letter → executable → success ---------------------------

    @Test
    void retry_moves_dead_letter_job_back_to_executable() throws Exception {
        FlakyOnceDelegate.CALLS.set(0);
        String key = "Process_retry_" + UUID.randomUUID().toString().substring(0, 8);
        sqsEngine.deploy("retry", Map.of("retry.bpmn",
                asyncTaskThenEndXml(key, FlakyOnceDelegate.class.getName())));
        String wfeId = UUID.randomUUID().toString();
        ProcessInstance pi = sqsEngine.startLatest(key, wfeId, Map.of());

        // First run fails → dead-letter (failed-job-retries=0).
        assertTrue(await(() -> sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0),
                "the flaky first run dead-lettered");
        Job dead = sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).list().get(0);

        // Retry → move back to executable; the async executor re-runs and the flaky delegate now succeeds.
        sqsEngine.retryDeadLetterJob(dead.getId(), 1);
        assertTrue(await(() -> sqsEngine.processInstances().processInstanceId(pi.getId()).count() == 0),
                "after retry the job became executable, re-ran, and the run completed");
    }

    // ---- cancel --------------------------------------------------------------

    @Test
    void cancel_terminates_a_parked_run() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        WorkflowGraph graph = twoNode(wfeId, "workN");
        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessContextRef bareRef = ProcessContextRef.builder()
                .executionId(wfeId).workflowExecutionId(wfeId).envId("env").schemaVersion(1).build();
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, bareRef), true);
        assertNotNull(awaitParked(pi.getId(), "wait_workN"), "parked before cancel");

        sqsEngine.deleteProcessInstance(pi.getId(), "user cancel");

        assertTrue(await(() -> sqsEngine.processInstances().processInstanceId(pi.getId()).count() == 0),
                "the run was cancelled and removed from the runtime");
        assertTrue(sqsEngine.historicProcessInstances().processInstanceId(pi.getId()).count() > 0,
                "the cancelled run is recorded in history");
    }

    // Replay valid + missing-seed fail-fast

    @Test
    void replay_with_valid_seed_starts_latest_pi() {
        String wfId = "replayWf" + UUID.randomUUID().toString().substring(0, 6);
        WorkflowGraph graph = new WorkflowGraph(wfId, "replay", "1",
                List.of(simpleNode("startN", "start.event", null, "start"),
                        simpleNode("endN", "end.event", null, "end")),
                List.of(edge("startN", "endN")));
        deploymentService.deploy(graph, null, true);

        // Seal a recoverable seed context, then replay against the latest definition.
        String wfeId = UUID.randomUUID().toString();
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setCurrentNodeId("startN");
        ProcessContextRef seedRef = contextStore.snapshot(ctx, null);

        ProcessInstance replayed = replayService.replay(graph.processKey(), wfeId, seedRef);
        assertNotNull(replayed, "replay started a PI against the latest definition (non-empty seed)");
        assertEquals(wfeId, replayed.getBusinessKey(), "replay uses the workflowExecutionId business key");
    }

    @Test
    void replay_with_unrecoverable_seed_fails_fast() {
        ProcessContextRef bad = ProcessContextRef.builder().workflowExecutionId("wfe").build(); // no nodeRefId
        ReplaySeedRefGuard.SeedRefUnrecoverableException ex = assertThrows(
                ReplaySeedRefGuard.SeedRefUnrecoverableException.class,
                () -> replayService.replay("Process_whatever", "wfe", bad));
        assertEquals(ReplaySeedRefGuard.CODE_SEED_REF_MISSING, ex.code(),
                "replay never starts latest with an empty variable map — it fails fast for repair");
    }

    // Orphan sweeper reconciles a stale wait-state

    @Test
    void orphan_sweeper_discards_stale_unparked_dispatch() {
        String dispatchId = "sweep-" + UUID.randomUUID();
        dispatchRepo.save(DispatchWaitStateEntity.builder()
                .dispatchId(dispatchId).executionId("gone").processInstanceId("gone-pi")
                .businessKey("gone-bk").nodeId("n").waitActivityId("wait_n").environmentId("env")
                .clientEpoch("this-boot").status(DispatchStatus.PENDING.name())
                .registeredAt(OffsetDateTime.now().minusMinutes(20)) // older than the 15-min stale threshold
                .build());

        int reconciled = sweeper.sweep("ORPHANED_TEST");

        assertTrue(reconciled >= 1, "the stale, unparked dispatch was reconciled");
        assertEquals(DispatchStatus.DISCARDED.name(),
                dispatchRepo.findById(dispatchId).orElseThrow().getStatus(), "stale orphan row DISCARDED");
    }

    // ---- fixtures ------------------------------------------------------------

    private String gatewayNoDefaultXml(String processKey, String advanceDelegateExpr) {
        BpmnModel model = new BpmnModel();
        model.setTargetNamespace("http://studioquack.com/bpmn");
        Process p = new Process();
        p.setId(processKey);
        p.setExecutable(true);
        model.addProcess(p);

        StartEvent s = new StartEvent();
        s.setId("s");
        p.addFlowElement(s);
        ServiceTask t = new ServiceTask();
        t.setId("t");
        t.setName("advance");
        t.setAsynchronous(true); // reach the gateway inside an async job → a gateway throw dead-letters
        t.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        t.setImplementation(advanceDelegateExpr);
        p.addFlowElement(t);
        ExclusiveGateway gw = new ExclusiveGateway();
        gw.setId("gw");
        p.addFlowElement(gw);
        EndEvent yes = new EndEvent();
        yes.setId("e_yes");
        p.addFlowElement(yes);
        EndEvent no = new EndEvent();
        no.setId("e_no");
        p.addFlowElement(no);

        p.addFlowElement(flow("f_s_t", "s", "t"));
        p.addFlowElement(flow("f_t_gw", "t", "gw"));
        // TWO conditional flows (Activiti forbids a condition on a single outgoing flow) and NO default:
        // route__gw is never set → neither condition matches → the gateway raises an incident.
        SequenceFlow condYes = flow("f_gw_yes", "gw", "e_yes");
        condYes.setConditionExpression("${route__gw == 'YES'}");
        p.addFlowElement(condYes);
        SequenceFlow condNo = flow("f_gw_no", "gw", "e_no");
        condNo.setConditionExpression("${route__gw == 'NO'}");
        p.addFlowElement(condNo);
        return toXml(model);
    }

    private String asyncTaskThenEndXml(String processKey, String delegateClass) {
        BpmnModel model = new BpmnModel();
        model.setTargetNamespace("http://studioquack.com/bpmn");
        Process p = new Process();
        p.setId(processKey);
        p.setExecutable(true);
        model.addProcess(p);

        StartEvent s = new StartEvent();
        s.setId("s");
        p.addFlowElement(s);
        ServiceTask t = new ServiceTask();
        t.setId("t");
        t.setName("flaky");
        t.setAsynchronous(true);
        t.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_CLASS);
        t.setImplementation(delegateClass);
        p.addFlowElement(t);
        EndEvent e = new EndEvent();
        e.setId("e");
        p.addFlowElement(e);
        p.addFlowElement(flow("f_s_t", "s", "t"));
        p.addFlowElement(flow("f_t_e", "t", "e"));
        return toXml(model);
    }

    private SequenceFlow flow(String id, String from, String to) {
        SequenceFlow f = new SequenceFlow(from, to);
        f.setId(id);
        return f;
    }

    private String toXml(BpmnModel model) {
        return new String(new BpmnXMLConverter().convertToXML(model), StandardCharsets.UTF_8);
    }

    private WorkflowGraph twoNode(String wfId, String nodeId) {
        NodeDto start = simpleNode("startN", "start.event", null, "start");
        NodeDto work = simpleNode(nodeId, "custom.sandbox", "custom-script-executor", "serviceTask");
        PortDto out = new PortDto();
        out.setId("out"); out.setName("out"); out.setValue("out"); out.setType(PortType.OUTPUT);
        work.getData().setPorts(new ArrayList<>(List.of(out)));
        NodeDto end = simpleNode("endN", "end.event", null, "end");
        return new WorkflowGraph(wfId, "cancel", "1",
                List.of(start, work, end), List.of(edge("startN", nodeId), edge(nodeId, "endN")));
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

    private String awaitParked(String pi, String waitActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline) {
            String exec = sqsEngine.findUnfinishedWaitExecution(pi, waitActivityId);
            if (exec != null) {
                return exec;
            }
            Thread.sleep(50);
        }
        return null;
    }

    private boolean await(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(100);
        }
        return cond.getAsBoolean();
    }
}
