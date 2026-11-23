package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.runtime.support.FakeSandboxConfig;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BoundaryEvent;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.ErrorEventDefinition;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.activiti.engine.delegate.ExecutionListener;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Generator⇄runtime agreement regression. The plan ({@link
 * ActivitiBoundaryEventHelper#planBoundaries}) is the single owner of the element-id ⇄ config
 * mapping.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-boundary-agreement-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-boundary-agreement-jobs.wal",
        "sqs.sandbox.warmpool.enabled=false",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class BoundaryEventPlanAgreementTest {

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ActivitiBoundaryEventHelper boundaryEventHelper;

    @Test
    void the_deployed_bpmn_and_the_boundary_plan_agree_in_both_directions() {
        String wfeId = UUID.randomUUID().toString();
        NodeDto sandbox = sandboxNode("scriptN");
        BoundaryEventConfig timer = BoundaryEventConfig.timer("PT5M").withId("b_timer");
        BoundaryEventConfig message = BoundaryEventConfig.message("OrderCancel_" + wfeId.substring(0, 8), "=wfe")
                .withId("b_msg");
        BoundaryEventConfig signal = BoundaryEventConfig.signal("AbortAll_" + wfeId.substring(0, 8))
                .nonInterrupting().withId("b_sig");
        BoundaryEventConfig error = BoundaryEventConfig.error("VALIDATION_FAILED").withId("b_err");
        sandbox.getData().setBoundaryEventConfigs(List.of(timer, message, signal, error));

        List<NodeDto> nodes = List.of(start(), sandbox,
                end("timerH"), end("msgH"), end("sigH"), end("errH"), end("endN"));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "endN"),
                boundaryEdge("scriptN", "timerH", timer.getId()),
                boundaryEdge("scriptN", "msgH", message.getId()),
                boundaryEdge("scriptN", "sigH", signal.getId()),
                boundaryEdge("scriptN", "errH", error.getId()));
        WorkflowGraph graph = new WorkflowGraph("wfAgreement-" + wfeId, "agreement", "1", nodes, edges);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        BpmnModel deployed = sqsEngine.readDeployedModel(info.getProcessDefinitionId());
        assertNotNull(deployed, "the deployed definition's BPMN is readable back from the repository");
        org.activiti.bpmn.model.Process process = deployed.getMainProcess();
        assertNotNull(process, "the deployed model has a main process");

        // ---- Forward: every planned boundary is in the deployed BPMN, correctly formed. ----
        List<ActivitiBoundaryEventHelper.PlannedBoundary> plan = boundaryEventHelper.planBoundaries(sandbox);
        assertEquals(4, plan.size(), "all four configured boundaries are emittable and planned");
        for (ActivitiBoundaryEventHelper.PlannedBoundary planned : plan) {
            FlowElement element = process.getFlowElement(planned.elementId());
            BoundaryEvent boundary = assertInstanceOf(BoundaryEvent.class, element,
                    () -> "planned boundary " + planned.elementId() + " is missing from the deployed BPMN");
            assertDefinitionMatches(planned.config(), boundary);
            assertNotNull(boundary.getAttachedToRef(),
                    () -> planned.elementId() + " lost its attachedToRef in the deployed model");
            assertSeal(boundary, process);
        }

        // ---- Reverse: every configured boundary element in the deployed BPMN resolves to its config. ----
        int configured = 0;
        for (FlowElement element : process.getFlowElements()) {
            if (!(element instanceof BoundaryEvent boundary) || !isConfiguredBoundaryId(boundary.getId())) {
                continue;
            }
            configured++;
            ActivitiBoundaryEventHelper.FiredBoundary fired =
                    boundaryEventHelper.resolveFired(nodes, boundary.getId());
            assertNotNull(fired, () -> "deployed boundary " + boundary.getId() + " resolves to no config");
            assertEquals("scriptN", fired.hostNode().getId(),
                    () -> boundary.getId() + " resolved to the wrong host node");
            assertEquals(expectedPrefix(fired.config()), boundary.getId().replaceAll("_.*$", ""),
                    () -> boundary.getId() + " resolved to a config of the wrong type");
        }
        assertEquals(plan.size(), configured, "deployed configured boundaries and the plan are the same set");

        // ---- The declarations the definitions reference, and the uncaught-failure backstop. ----
        assertTrue(deployed.getMessages().stream()
                        .anyMatch(m -> message.getMessageName().equals(m.getName())),
                "the deployed model declares the boundary's message");
        assertTrue(deployed.getSignals().stream()
                        .anyMatch(s -> signal.getSignalName().equals(s.getName())),
                "the deployed model declares the boundary's signal");
        assertInstanceOf(BoundaryEvent.class, process.getFlowElement("err_scriptN"),
                "typed-code user boundaries still coexist with the system catch-all");
    }

    @Test
    void a_user_catchall_error_replaces_the_system_err_boundary() {
        String wfeId = UUID.randomUUID().toString();
        NodeDto sandbox = sandboxNode("scriptN");
        BoundaryEventConfig catchAll = BoundaryEventConfig.error("").withId("b_all");
        sandbox.getData().setBoundaryEventConfigs(List.of(catchAll));

        List<NodeDto> nodes = List.of(start(), sandbox, end("allH"), end("endN"));
        List<EdgeDto> edges = List.of(
                edge("startN", "scriptN"), edge("scriptN", "endN"),
                boundaryEdge("scriptN", "allH", catchAll.getId()));
        WorkflowGraph graph = new WorkflowGraph("wfCatchAll-" + wfeId, "catchall", "1", nodes, edges);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        BpmnModel deployed = sqsEngine.readDeployedModel(info.getProcessDefinitionId());
        org.activiti.bpmn.model.Process process = deployed.getMainProcess();
        assertNotNull(process, "the deployed model has a main process");

        List<ActivitiBoundaryEventHelper.PlannedBoundary> plan = boundaryEventHelper.planBoundaries(sandbox);
        assertEquals(1, plan.size(), "the blank ERROR is emittable and planned");
        assertEquals("errcode_scriptN_0", plan.get(0).elementId());

        FlowElement element = process.getFlowElement("errcode_scriptN_0");
        BoundaryEvent boundary = assertInstanceOf(BoundaryEvent.class, element,
                "the user catch-all is in the deployed BPMN");
        ErrorEventDefinition def = assertInstanceOf(ErrorEventDefinition.class,
                boundary.getEventDefinitions().get(0));
        assertTrue(def.getErrorRef() == null || def.getErrorRef().isBlank(),
                "a blank Error Code deploys with no errorRef");
        assertSeal(boundary, process);

        ActivitiBoundaryEventHelper.FiredBoundary fired =
                boundaryEventHelper.resolveFired(nodes, "errcode_scriptN_0");
        assertNotNull(fired, "the deployed user catch-all resolves back through the plan");
        assertEquals("scriptN", fired.hostNode().getId());
        assertEquals(BoundaryEventConfig.BoundaryEventType.ERROR, fired.config().getType());
        assertTrue(fired.config().getErrorCode() == null || fired.config().getErrorCode().isBlank());

        assertNull(process.getFlowElement("err_scriptN"),
                "a user catch-all must not coexist with the system catch-all");
    }

    /** The deployed definition type and cancelActivity must match the config the plan emitted from. */
    private void assertDefinitionMatches(BoundaryEventConfig cfg, BoundaryEvent boundary) {
        assertEquals(1, boundary.getEventDefinitions().size(),
                () -> boundary.getId() + " carries exactly one event definition");
        var definition = boundary.getEventDefinitions().get(0);
        switch (cfg.getType()) {
            case TIMER -> {
                assertInstanceOf(TimerEventDefinition.class, definition,
                        () -> boundary.getId() + " must carry a timerEventDefinition");
                assertEquals(cfg.isInterrupting(), boundary.isCancelActivity(),
                        () -> boundary.getId() + " cancelActivity must mirror the interrupting flag");
            }
            case MESSAGE -> {
                assertInstanceOf(MessageEventDefinition.class, definition,
                        () -> boundary.getId() + " must carry a messageEventDefinition");
                assertEquals(cfg.isInterrupting(), boundary.isCancelActivity(),
                        () -> boundary.getId() + " cancelActivity must mirror the interrupting flag");
            }
            case SIGNAL -> {
                assertInstanceOf(SignalEventDefinition.class, definition,
                        () -> boundary.getId() + " must carry a signalEventDefinition");
                assertEquals(cfg.isInterrupting(), boundary.isCancelActivity(),
                        () -> boundary.getId() + " cancelActivity must mirror the interrupting flag");
            }
            case ERROR -> {
                assertInstanceOf(ErrorEventDefinition.class, definition,
                        () -> boundary.getId() + " must carry an errorEventDefinition");
                // cancelActivity is NOT asserted for error boundaries: BPMN defines them as always
                // interrupting, so Activiti's converter omits the attribute on write and normalises
                // the flag away on parse — the deployed model cannot round-trip it. The runtime
                // interruption is unconditional in Activiti's error propagation and is proven by the
                // routing tests (BoundaryEventEndToEndTest / TypedErrorRepairConformanceTest).
            }
            default -> throw new IllegalStateException("unexpected planned type " + cfg.getType());
        }
    }

    /**
     * The correlation seal: the boundary's outgoing flow carries the {@code sqBoundaryEventDelegate}
     * take listener, so a fired boundary publishes what its port promised — for EVERY type, not just
     * the one the seal was written against.
     */
    private void assertSeal(BoundaryEvent boundary, org.activiti.bpmn.model.Process process) {
        List<SequenceFlow> outgoing = boundary.getOutgoingFlows();
        assertFalse(outgoing == null || outgoing.isEmpty(),
                () -> boundary.getId() + " has no outgoing flow in the deployed model");
        SequenceFlow route = outgoing.get(0);
        assertNotNull(process.getFlowElement(route.getTargetRef()),
                () -> boundary.getId() + " routes to an element that exists in the deployed model");
        boolean sealed = route.getExecutionListeners().stream().anyMatch(this::isBoundarySeal);
        assertTrue(sealed, () -> boundary.getId() + " lost its sqBoundaryEventDelegate take listener");
    }

    private boolean isBoundarySeal(ActivitiListener listener) {
        return ExecutionListener.EVENTNAME_TAKE.equals(listener.getEvent())
                && "${sqBoundaryEventDelegate}".equals(listener.getImplementation());
    }

    /** Ids the plan mints for configured boundaries (the catch-all {@code err_<node>} is not one). */
    private boolean isConfiguredBoundaryId(String id) {
        return id != null && (id.startsWith("btimer_") || id.startsWith("bmsg_")
                || id.startsWith("bsig_") || id.startsWith("errcode_"));
    }

    private String expectedPrefix(BoundaryEventConfig cfg) {
        return switch (cfg.getType()) {
            case TIMER -> "btimer";
            case MESSAGE -> "bmsg";
            case SIGNAL -> "bsig";
            case ERROR -> "errcode";
            default -> throw new IllegalStateException("unexpected planned type " + cfg.getType());
        };
    }

    // ---- fixtures ------------------------------------------------------------

    private NodeDto start() {
        return simpleNode("startN", "start.event", null, "start");
    }

    private NodeDto end(String id) {
        return simpleNode(id, "end.event", null, "end");
    }

    private NodeDto sandboxNode(String id) {
        NodeDto n = simpleNode(id, "custom.sandbox", "custom-script-executor", "serviceTask");
        n.getData().setTemplateScript(new TemplateScriptDto(id + "-s", id + "-t", "python",
                "studioquack.return_result([{\"port\": \"out\", \"value\": \"x\"}])\n"));
        PortDto out = new PortDto();
        out.setId("out"); out.setName("out"); out.setValue("out"); out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
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

    private EdgeDto boundaryEdge(String source, String target, String boundaryEventId) {
        EdgeDto e = edge(source, target);
        EdgeDataDto data = new EdgeDataDto();
        data.setBoundaryEventId(boundaryEventId);
        e.setData(data);
        return e;
    }
}
