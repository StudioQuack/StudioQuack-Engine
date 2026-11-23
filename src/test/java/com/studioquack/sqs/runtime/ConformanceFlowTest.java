package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.SchemaItemDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.support.FakeSandboxConfig;
import com.studioquack.sqs.runtime.support.FakeSandboxExecutionPort;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Conformance suite driven by an in-process {@link FakeSandboxExecutionPort} so the FULL dispatch →
 * resume path runs deterministically without a container. Covers
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-conf-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-conf-jobs.wal",
        "sqs.orphan-sweeper.enabled=false"
})
@ActiveProfiles("base")
@Import(FakeSandboxConfig.class)
class ConformanceFlowTest {

    @Autowired private WorkflowDeploymentService deploymentService;
    @Autowired private SqsEngine sqsEngine;
    @Autowired private ExecutionContextStore contextStore;
    @Autowired private FakeSandboxExecutionPort fakeSandbox;

    @BeforeEach
    void resetFake() {
        fakeSandbox.reset();
    }

    @Test
    void linear_two_node_success() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "scriptN";
        WorkflowGraph graph = new WorkflowGraph("wfLinear-" + wfeId, "linear", "1",
                List.of(start(), sandboxNode(scriptN), end("endN")),
                List.of(edge("startN", scriptN), edge(scriptN, "endN")));
        fakeSandbox.succeed(scriptN, Map.of("out", "done"));

        ProcessInstance pi = seedAndStart(graph, scriptN, wfeId);
        assertTrue(awaitCompleted(wfeId), "linear two-node flow completed");
        assertFalse(reachedErrorEnd(pi), "clean success does not route through the error boundary");
    }

    @Test
    void fast_return_script_has_no_resume_race() throws Exception {
        // The fake returns synchronously (instantly) — often before the token has parked. The durable
        // wait-state + bounded-retry resume must still resume cleanly (no "execution not found").
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "fastN";
        WorkflowGraph graph = new WorkflowGraph("wfFast-" + wfeId, "fast", "1",
                List.of(start(), sandboxNode(scriptN), end("endN")),
                List.of(edge("startN", scriptN), edge(scriptN, "endN")));
        fakeSandbox.succeed(scriptN, Map.of("out", "instant"));

        ProcessInstance pi = seedAndStart(graph, scriptN, wfeId);
        assertTrue(awaitCompleted(wfeId), "fast-returning script still resumes and completes (no race)");
        assertFalse(reachedErrorEnd(pi), "fast return is a clean success");
    }

    @Test
    void exclusive_gateway_route_hit() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "decideN";
        String gw = "gw";
        NodeDto gateway = gatewayNode(gw, "hitEnd", "= route__" + gw + " == 'HIT'");
        WorkflowGraph graph = new WorkflowGraph("wfRoute-" + wfeId, "route", "1",
                List.of(start(), sandboxNode(scriptN), gateway, end("hitEnd"), end("missEnd")),
                List.of(edge("startN", scriptN), edge(scriptN, gw),
                        edge(gw, "hitEnd"), edge(gw, "missEnd")));
        fakeSandbox.succeed(scriptN, Map.of("out", "HIT"));

        ProcessInstance pi = seedAndStart(graph, scriptN, wfeId);
        assertTrue(awaitCompleted(wfeId), "gateway flow completed");
        assertTrue(reachedActivity(pi, "node_hitEnd"), "route token HIT selected the conditional branch");
        assertFalse(reachedActivity(pi, "node_missEnd"), "the default branch was NOT taken on a route hit");
    }

    @Test
    void gateway_default_flow_fallback() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "decideN";
        String gw = "gw";
        // Condition edge requires 'HIT'; the node returns 'OTHER' → no condition matches → default flow.
        NodeDto gateway = gatewayNode(gw, "hitEnd", "= route__" + gw + " == 'HIT'");
        WorkflowGraph graph = new WorkflowGraph("wfDefault-" + wfeId, "default", "1",
                List.of(start(), sandboxNode(scriptN), gateway, end("hitEnd"), end("missEnd")),
                List.of(edge("startN", scriptN), edge(scriptN, gw),
                        edge(gw, "hitEnd"), edge(gw, "missEnd")));
        fakeSandbox.succeed(scriptN, Map.of("out", "OTHER"));

        ProcessInstance pi = seedAndStart(graph, scriptN, wfeId);
        assertTrue(awaitCompleted(wfeId), "gateway flow completed via default flow");
        assertTrue(reachedActivity(pi, "node_missEnd"), "no matching condition → the default flow was taken");
        assertFalse(reachedActivity(pi, "node_hitEnd"), "the conditional branch was NOT taken");
    }

    @Test
    void failed_script_routes_through_error_boundary() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "boomN";
        WorkflowGraph graph = new WorkflowGraph("wfFail-" + wfeId, "fail", "1",
                List.of(start(), sandboxNode(scriptN), end("endN")),
                List.of(edge("startN", scriptN), edge(scriptN, "endN")));
        fakeSandbox.fail(scriptN, "SANDBOX_FAILED", "boom");

        ProcessInstance pi = seedAndStart(graph, scriptN, wfeId);
        // The catch-all fail-route HOLDS a failed ROOT run alive as a dead-letter incident (the
        // repair surface) instead of ending it at the shared error end.
        assertTrue(awaitHeld(pi, "hold_" + scriptN),
                "failed script routed through the boundary and is held as an incident");
        assertFalse(reachedErrorEnd(pi), "a held ROOT run never reaches the shared error end");
    }

    @Test
    void skip_via_sq_skip_bpmn_error() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String scriptN = "skipN";
        WorkflowGraph graph = new WorkflowGraph("wfSkip-" + wfeId, "skip", "1",
                List.of(start(), sandboxNode(scriptN), end("endN")),
                List.of(edge("startN", scriptN), edge(scriptN, "endN")));
        fakeSandbox.fail(scriptN, EngineVariableNames.SKIP_ERROR_CODE, "skip requested");

        ProcessInstance pi = seedAndStart(graph, scriptN, wfeId);
        // An operator skip keeps its previous behaviour: SQ_SKIP is raised as a BpmnError caught
        // by the same catch-all boundary, but the run ENDS at the shared error end — a deliberate
        // abandon-node decision is never held as a repairable incident.
        assertTrue(awaitCompleted(wfeId), "SQ_SKIP ends the run instead of holding it");
        assertTrue(reachedErrorEnd(pi), "the skipped run ends at the shared error end");
        assertFalse(reachedActivity(pi, "hold_" + scriptN),
                "an operator skip never parks at the failure-hold task");
    }

    /**
     * Live M1 shape: producer {@code main_output.final_answer="hello"} → XOR with ONE outgoing
     * whose condition is {@code @ai_agent.main_output.final_answer == " "} (false). The end
     * node must not execute; the run must terminate at the shared error end.
     */
    @ParameterizedTest(name = "carrier={0} isDefaultFlow={1}")
    @MethodSource("m1FalseConditionCases")
    void exclusive_single_outgoing_false_condition_does_not_silently_pass(
            String carrier, boolean isDefaultFlow) throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String agentN = "agentN";
        String gw = "gw";
        String endId = "endN";
        NodeDto gateway = xorGateway(gw, carrier, M1_FALSE_SPACE_CONDITION);
        WorkflowGraph graph = new WorkflowGraph("wfM1-" + carrier + "-" + isDefaultFlow + "-" + wfeId,
                "m1-false", "1",
                List.of(start(), agentNode(agentN), gateway, end(endId)),
                List.of(edge("startN", agentN), edge(agentN, gw),
                        conditionedGwEdge(gw, endId, carrier, isDefaultFlow, M1_FALSE_SPACE_CONDITION)));
        fakeSandbox.succeed(agentN, Map.of("main_output", Map.of("final_answer", "hello")));

        ProcessInstance pi = seedAndStart(graph, agentN, wfeId);
        assertTrue(awaitCompleted(wfeId),
                "false sole-exit condition must still reach a terminal state (loud fail, not hang)");
        assertFalse(reachedActivity(pi, "node_" + endId),
                "FALSE condition must not execute the authored end node; carrier=" + carrier
                        + " isDefaultFlow=" + isDefaultFlow);
        assertTrue(reachedErrorEnd(pi),
                "FALSE sole-exit condition must take the synthetic/shared error end; carrier="
                        + carrier + " isDefaultFlow=" + isDefaultFlow);
    }

    /**
     * Live user graph: producer → exclusive with one empty (no isDefaultFlow) edge and one
     * typed condition that is FALSE. Empty edge is the intended default; run must not die
     * with JUEL Unknown property.
     */
    @Test
    void exclusive_empty_default_plus_false_condition_takes_default_end() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String agentN = "agentN";
        String gw = "gw";
        String defaultEnd = "endDefault";
        String condEnd = "endCond";
        NodeDto gateway = xorGateway(gw, "typed", M1_FALSE_SPACE_CONDITION);
        WorkflowGraph graph = new WorkflowGraph("wfEmptyDef-" + wfeId, "empty-default", "1",
                List.of(start(), agentNode(agentN), gateway, end(defaultEnd), end(condEnd)),
                List.of(edge("startN", agentN), edge(agentN, gw),
                        edge(gw, defaultEnd),
                        conditionedGwEdge(gw, condEnd, "typed", false, M1_FALSE_SPACE_CONDITION)));
        fakeSandbox.succeed(agentN, Map.of("main_output", Map.of("final_answer", "hello")));

        ProcessInstance pi = seedAndStart(graph, agentN, wfeId);
        assertTrue(awaitCompleted(wfeId),
                "false condition + empty sibling must complete via the default (no Unknown property)");
        assertTrue(reachedActivity(pi, "node_" + defaultEnd),
                "empty unconditioned edge is the default when the condition is false");
        assertFalse(reachedActivity(pi, "node_" + condEnd),
                "false conditioned branch must not run");
        assertFalse(reachedErrorEnd(pi), "intended default must not take the synthetic error end");
    }

    /** Same graph: condition TRUE → conditioned target, not the empty default. */
    @Test
    void exclusive_empty_default_plus_true_condition_takes_conditioned_end() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String agentN = "agentN";
        String gw = "gw";
        String defaultEnd = "endDefault";
        String condEnd = "endCond";
        NodeDto gateway = xorGateway(gw, "typed", M1_TRUE_HELLO_CONDITION);
        WorkflowGraph graph = new WorkflowGraph("wfEmptyTrue-" + wfeId, "empty-true", "1",
                List.of(start(), agentNode(agentN), gateway, end(defaultEnd), end(condEnd)),
                List.of(edge("startN", agentN), edge(agentN, gw),
                        edge(gw, defaultEnd),
                        conditionedGwEdge(gw, condEnd, "typed", false, M1_TRUE_HELLO_CONDITION)));
        fakeSandbox.succeed(agentN, Map.of("main_output", Map.of("final_answer", "hello")));

        ProcessInstance pi = seedAndStart(graph, agentN, wfeId);
        assertTrue(awaitCompleted(wfeId), "true condition completes via the conditioned branch");
        assertTrue(reachedActivity(pi, "node_" + condEnd), "true condition takes the conditioned end");
        assertFalse(reachedActivity(pi, "node_" + defaultEnd), "default must not run when a condition matches");
        assertFalse(reachedErrorEnd(pi), "true condition must not take the error end");
    }

    /**
     * Every exit conditioned and all false: synthetic error default + sentinel token.
     * Loud FAILED at the error end; no JUEL Unknown property; run reaches a terminal state.
     */
    @Test
    void exclusive_all_conditioned_no_match_takes_error_end() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String agentN = "agentN";
        String gw = "gw";
        String endA = "endA";
        String endB = "endB";
        NodeDto gateway = xorGateway(gw, "typed", M1_FALSE_SPACE_CONDITION);
        WorkflowGraph graph = new WorkflowGraph("wfAllCond-" + wfeId, "all-cond", "1",
                List.of(start(), agentNode(agentN), gateway, end(endA), end(endB)),
                List.of(edge("startN", agentN), edge(agentN, gw),
                        conditionedGwEdge(gw, endA, "typed", false, M1_FALSE_SPACE_CONDITION),
                        conditionedGwEdge(gw, endB, "typed", false,
                                "@ai_agent.main_output.final_answer == \"nope\"")));
        fakeSandbox.succeed(agentN, Map.of("main_output", Map.of("final_answer", "hello")));

        ProcessInstance pi = seedAndStart(graph, agentN, wfeId);
        assertTrue(awaitCompleted(wfeId),
                "all-conditioned miss must reach a terminal state (no Unknown property hang)");
        assertFalse(reachedActivity(pi, "node_" + endA));
        assertFalse(reachedActivity(pi, "node_" + endB));
        assertTrue(reachedErrorEnd(pi),
                "all-conditioned no-match must take the synthetic/shared error end");
    }

    /** True-condition control: sole conditioned exit that matches still reaches the end node. */
    @Test
    void exclusive_single_outgoing_true_condition_still_takes_end() throws Exception {
        String wfeId = UUID.randomUUID().toString();
        String agentN = "agentN";
        String gw = "gw";
        String endId = "endN";
        NodeDto gateway = xorGateway(gw, "typed", M1_TRUE_HELLO_CONDITION);
        WorkflowGraph graph = new WorkflowGraph("wfM1true-" + wfeId, "m1-true", "1",
                List.of(start(), agentNode(agentN), gateway, end(endId)),
                List.of(edge("startN", agentN), edge(agentN, gw),
                        conditionedGwEdge(gw, endId, "typed", false, M1_TRUE_HELLO_CONDITION)));
        fakeSandbox.succeed(agentN, Map.of("main_output", Map.of("final_answer", "hello")));

        ProcessInstance pi = seedAndStart(graph, agentN, wfeId);
        assertTrue(awaitCompleted(wfeId), "true sole-exit condition completes");
        assertTrue(reachedActivity(pi, "node_" + endId), "true condition still takes the authored end");
        assertFalse(reachedErrorEnd(pi), "true condition must not take the synthetic error default");
    }

    static Stream<Arguments> m1FalseConditionCases() {
        return Stream.of(
                Arguments.of("typed", false),
                Arguments.of("typed", true),
                Arguments.of("schemaItems", false),
                Arguments.of("schemaItems", true),
                Arguments.of("nodeParam", false),
                Arguments.of("nodeParam", true));
    }

    // ---- fixtures ------------------------------------------------------------

    private ProcessInstance seedAndStart(WorkflowGraph graph, String sandboxNodeId, String wfeId) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(wfeId);
        ctx.setWorkflowId(graph.workflowId());
        ctx.setEnvId("env-conf");
        ctx.setCurrentNodeId(sandboxNodeId);
        ctx.setNodes(new ArrayList<>(graph.nodes()));
        ctx.setEdges(new ArrayList<>(graph.edges()));
        ProcessContextRef ref = contextStore.snapshot(ctx, null);

        DeploymentInfo info = deploymentService.deploy(graph, null, true);
        ProcessInstance pi = deploymentService.startProcessInstance(graph, info, wfeId,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(pi, "process instance started");
        return pi;
    }

    private NodeDto start() {
        return simpleNode("startN", "start.event", null, "start");
    }

    private NodeDto end(String id) {
        return simpleNode(id, "end.event", null, "end");
    }

    /** Live M1 authored text: double-quoted one space — false when the agent answered non-blank. */
    private static final String M1_FALSE_SPACE_CONDITION =
            "@ai_agent.main_output.final_answer == \" \"";
    private static final String M1_TRUE_HELLO_CONDITION =
            "@ai_agent.main_output.final_answer == \"hello\"";

    private NodeDto sandboxNode(String id) {
        NodeDto n = simpleNode(id, "custom.sandbox", "custom-script-executor", "serviceTask");
        n.getData().setTemplateScript(new TemplateScriptDto(id + "-s", id + "-t", "python",
                "studioquack.return_result([{\"port\": \"out\", \"value\": \"x\"}])\n"));
        PortDto out = new PortDto();
        out.setId("out"); out.setName("out"); out.setValue("out"); out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
    }

    /** Producer matching the live M1 graph: shortCode {@code ai_agent}, port {@code main_output}. */
    private NodeDto agentNode(String id) {
        NodeDto n = sandboxNode(id);
        n.getData().setNodeShortCode("ai_agent");
        PortDto out = new PortDto();
        out.setId("main_output");
        out.setName("main_output");
        out.setValue("main_output");
        out.setType(PortType.OUTPUT);
        n.getData().setPorts(new ArrayList<>(List.of(out)));
        return n;
    }

    private NodeDto xorGateway(String id, String carrier, String expr) {
        NodeDto n = simpleNode(id, "exclusive.gateway", null, "gateway");
        if ("nodeParam".equals(carrier)) {
            ParameterDto p = new ParameterDto();
            p.setShortCode("condition");
            p.setValue(expr);
            n.getData().setConfigurationParameters(new ArrayList<>(List.of(p)));
        }
        return n;
    }

    /**
     * Sole XOR exit carrying the given FE condition carrier. {@code isDefaultFlow} may also be
     * set — the bug is that a false condition was taken because that flag (or generation-time
     * defaulting) won.
     */
    private EdgeDto conditionedGwEdge(String gw, String end, String carrier, boolean isDefault, String expr) {
        EdgeDto e = edge(gw, end);
        EdgeDataDto data = new EdgeDataDto();
        data.setIsDefaultFlow(isDefault);
        if ("typed".equals(carrier)) {
            data.setCondition(expr);
        } else if ("schemaItems".equals(carrier)) {
            SchemaItemDto row = new SchemaItemDto();
            row.setShortCode("condition");
            row.setValue(expr);
            data.setSchemaItems(new ArrayList<>(List.of(row)));
        }
        e.setData(data);
        return e;
    }

    private NodeDto gatewayNode(String id, String conditionalTargetNodeId, String feelCondition) {
        NodeDto n = simpleNode(id, "exclusive.gateway", null, "gateway");
        EdgeTransformDto cond = new EdgeTransformDto();
        cond.setTargetNodeId(conditionalTargetNodeId);
        cond.setSourcePath("conditionExpression");
        cond.setSourcePathValue(feelCondition);
        n.setEdgeTransformListForCurrentNode(new ArrayList<>(List.of(cond)));
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

    private boolean reachedErrorEnd(ProcessInstance pi) {
        return reachedActivity(pi, ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
    }

    /**
     * Awaits the failure-hold incident shape: a dead-letter job for the run AND the token parked
     * at {@code holdActivityId}. The parked check uses the RUNTIME execution query, never historic
     * activity instances: a first-execution dead-letter rolls the job's transaction back, so the
     * hold task leaves no historic activity record while the execution stays parked.
     */
    private boolean awaitHeld(ProcessInstance pi, String holdActivityId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.deadLetterJobsForProcessInstance(pi.getId()).count() > 0
                    && sqsEngine.executions().processInstanceId(pi.getId())
                            .activityId(holdActivityId).count() > 0) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private boolean reachedActivity(ProcessInstance pi, String activityId) {
        return sqsEngine.historicActivityInstances(pi.getId()).list().stream()
                .anyMatch(a -> activityId.equals(a.getActivityId()));
    }

    private boolean awaitCompleted(String wfeId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < deadline) {
            if (sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0
                    && sqsEngine.historicProcessInstances().processInstanceBusinessKey(wfeId).finished().count() > 0) {
                return true;
            }
            Thread.sleep(100);
        }
        return sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0;
    }
}
