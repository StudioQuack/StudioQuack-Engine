package com.studioquack.sqs.command;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.MessageCorrelationService.MessageCorrelationOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.SqEventType;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventRequest;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.activiti.engine.runtime.Execution;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The SHARED {@link MessageCorrelationService} against the embedded Activiti engine on an in-memory
 * database. This is the ONE correlation core both the reverse-channel {@code
 * CorrelateMessageCommandHandler} and the public HTTP ingress call, so proving it here proves both
 * paths
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-correlation-svc-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class MessageCorrelationServiceTest {

    private static final String MESSAGE_NAME = "orderApproved";

    /** The {@link DeploymentInfo} of the most recent fixture deploy (for artifact-row persistence). */
    private com.studioquack.common.dto.flow.DeploymentInfo lastDeployInfo;

    @Autowired
    private MessageCorrelationService correlationService;
    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore executionContextStore;
    @Autowired
    private ExecutionContextCrypto executionContextCrypto;
    @Autowired
    private com.studioquack.sqs.command.store.DeployedArtifactStore artifactStore;

    @Test
    void parked_message_catch_is_correlated_vars_delivered_and_execution_advances() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("svcCorrelate", businessKey);

        Execution parked = sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME);
        assertNotNull(parked, "instance must park on the message catch subscription before correlation");

        MessageCorrelationOutcome outcome = correlationService.correlate(
                MESSAGE_NAME, businessKey, Map.of("route__correlated", "yes"));

        assertTrue(outcome.correlated(), "the parked message event was correlated");
        assertEquals(parked.getId(), outcome.executionId(), "the resolved parked execution id is reported");
        assertEquals(MESSAGE_NAME, outcome.messageName(), "the message name is echoed back");

        // Execution advanced: the message subscription is consumed but the instance is still running.
        assertNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the message subscription is consumed once correlated");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()),
                "the instance advanced to the next wait state (user task), it did not end");

        // The delivered REF-ONLY variable is durably usable (pinned at PI scope before advancing).
        List<String> varNames = sqsEngine.historicVariables(instance.getId()).list().stream()
                .map(v -> v.getVariableName()).toList();
        assertTrue(varNames.contains("route__correlated"), "the delivered route token landed; vars=" + varNames);
        assertEquals("yes", sqsEngine.getProcessVariable(instance.getId(), "route__correlated"),
                "the delivered value is readable at the instance scope");
    }

    @Test
    void no_waiting_execution_returns_not_correlated() {
        MessageCorrelationOutcome outcome = correlationService.correlate(
                MESSAGE_NAME, "no-such-run-" + UUID.randomUUID(), null);

        assertFalse(outcome.correlated(), "no waiting execution → correlated:false");
        assertNull(outcome.executionId(), "no executionId is reported on a no-match");
        assertEquals(MESSAGE_NAME, outcome.messageName(), "the message name is still echoed on a no-match");
    }

    @Test
    void wrong_message_name_is_not_correlated_and_leaves_instance_parked() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("svcCorrelateWrongMsg", businessKey);

        MessageCorrelationOutcome outcome = correlationService.correlate("someOtherMessage", businessKey, null);

        assertFalse(outcome.correlated(), "a mismatched message name does not correlate");
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the parked instance is left untouched by a mismatched message");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()), "the instance still runs");
    }

    @Test
    void blank_message_name_throws_illegal_argument() {
        assertThrows(IllegalArgumentException.class,
                () -> correlationService.correlate("  ", "bk-1", null),
                "a blank messageName is a client error");
    }

    @Test
    void blank_correlation_key_throws_illegal_argument() {
        assertThrows(IllegalArgumentException.class,
                () -> correlationService.correlate(MESSAGE_NAME, "  ", null),
                "a blank correlationKey is a client error");
    }

    @Test
    void phi_variable_is_rejected_by_ref_only_guard() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("svcCorrelatePhi", businessKey);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> correlationService.correlate(MESSAGE_NAME, businessKey, Map.of("patientName", "SECRET-9f3a")),
                "a PHI variable must fail the REF-ONLY guard before reaching the engine");
        assertTrue(ex.getMessage().contains("REF-ONLY"), "error names the REF-ONLY violation: " + ex.getMessage());

        // Nothing was delivered: the instance is still parked and the PHI var never reached history.
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the instance is still parked; no message was delivered");
        List<String> varNames = sqsEngine.historicVariables(instance.getId()).list().stream()
                .map(v -> v.getVariableName()).toList();
        assertFalse(varNames.contains("patientName"), "the PHI var never reached the engine; vars=" + varNames);
    }

    // Event-kind dispatch

    @Test
    void correlateEvent_nullKind_defaultsToIntermediateMessage() {
        String businessKey = UUID.randomUUID().toString();
        deployAndStartMessageCatchFlow("svcEventDefault", businessKey);

        // A null/absent eventKind must behave exactly like the message path.
        MessageCorrelationOutcome outcome = correlationService.correlateEvent(
                null, MESSAGE_NAME, businessKey, null, Map.of("route__correlated", "yes"));

        assertTrue(outcome.correlated(), "null eventKind is treated as an intermediate MESSAGE");
        assertEquals(MESSAGE_NAME, outcome.messageName());
    }

    @Test
    void correlateEvent_explicitMessageKind_correlates() {
        String businessKey = UUID.randomUUID().toString();
        deployAndStartMessageCatchFlow("svcEventMsg", businessKey);

        MessageCorrelationOutcome outcome = correlationService.correlateEvent(
                "MESSAGE", MESSAGE_NAME, businessKey, null, null);

        assertTrue(outcome.correlated(), "an explicit MESSAGE kind correlates the parked catch");
    }

    @Test
    void correlateEvent_unknownKind_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> correlationService.correlateEvent("teleport", MESSAGE_NAME, "bk-1", null, null),
                "an unknown eventKind is a client error");
    }

    // User-task decision publishes gateway route tokens

    @Test
    void userTask_decision_publishes_route_token_and_takes_the_conditioned_branch() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartUserTaskGatewayFlow("svcTaskRoute", businessKey);

        MessageCorrelationOutcome outcome = correlationService.decideUserTask(decision(
                businessKey, "svcTaskRoute", "approved"));

        assertTrue(outcome.correlated(), "the parked user task was completed");
        assertFalse(sqsEngine.isProcessInstanceActive(instance.getId()),
                "the run advanced through the gateway to an end event");
        List<String> finished = finishedActivityIds(instance.getId());
        assertTrue(finished.contains("node_approvedN"),
                "the conditioned (APPROVED) branch end ran; activities=" + finished);
        assertFalse(finished.contains("node_rejectedN"),
                "the default branch was not taken; activities=" + finished);
    }

    @Test
    void userTask_rejection_with_no_true_condition_takes_the_default_branch() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartUserTaskGatewayFlow("svcTaskRouteDefault", businessKey);

        MessageCorrelationOutcome outcome = correlationService.decideUserTask(decision(
                businessKey, "svcTaskRouteDefault", "rejected"));

        assertTrue(outcome.correlated(), "the parked user task was completed");
        assertFalse(sqsEngine.isProcessInstanceActive(instance.getId()),
                "the run advanced through the gateway to an end event");
        List<String> finished = finishedActivityIds(instance.getId());
        assertTrue(finished.contains("node_rejectedN"),
                "no true condition → the default branch end ran; activities=" + finished);
        assertFalse(finished.contains("node_approvedN"),
                "the conditioned branch was not taken; activities=" + finished);
    }

    // ---- CORE: webhook message-event addressing + correlation, msg sealed REF-only ------------

    @Test
    void webhook_messageStart_launches_new_instance_and_seals_msg_as_ref_not_plain_var() {
        String workflowId = "wfWebhookStart-" + UUID.randomUUID();
        deployMessageStartFlow(workflowId, "orderIntake");

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", null, "orderIntake",
                Map.of("orderId", "ORD-42", "amount", 99)));

        assertTrue(outcome.correlated(), "a message-start always launches (or the caller sees a 404 upstream)");
        assertNotNull(outcome.processInstanceId(), "a new process instance id is reported");
        assertTrue(sqsEngine.isProcessInstanceActive(outcome.processInstanceId()), "the new instance is running");

        // REF-ONLY: the ONLY engine variable is studioquackRef — never a plain "orderId"/"amount"/"msg".
        List<String> varNames = variableNames(outcome.processInstanceId());
        assertTrue(varNames.contains(EngineVariableNames.STUDIOQUACK_REF),
                "the sealed msg is published as the studioquackRef handle; vars=" + varNames);
        assertTrue(sqsEngine.getProcessVariable(outcome.processInstanceId(), EngineVariableNames.STUDIOQUACK_REF)
                        instanceof ProcessContextRef,
                "studioquackRef is a ProcessContextRef handle, not raw data");
        assertFalse(varNames.contains("orderId"), "msg field 'orderId' never reaches the engine as a plain var");
        assertFalse(varNames.contains("amount"), "msg field 'amount' never reaches the engine as a plain var");
        assertFalse(varNames.contains("msg"), "msg itself is never published as a plain engine variable");
    }

    // ---- C7: message.start receiver config (businessKeyExpression / correlationKey) is LIVE ----

    @Test
    void webhook_messageStart_businessKeyExpression_derives_the_run_correlation_key_from_the_payload() {
        String workflowId = "wfStartExpr-" + UUID.randomUUID();
        deployMessageStartFlowWithConfig(workflowId, "orderKeyed", null, "${msg.orderId}");

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", null, "orderKeyed",
                Map.of("orderId", "ORD-9", "amount", 5)));

        assertTrue(outcome.correlated(), "the expression-keyed start fires");
        String businessKey = sqsEngine.findBusinessKey(outcome.processInstanceId());
        assertNotNull(businessKey);
        assertTrue(businessKey.startsWith("wh_"),
                "the run id is a minted unique id — a payload-derived key is never the businessKey");
        assertEquals("ORD-9", sqsEngine.getProcessVariable(outcome.processInstanceId(),
                        EngineVariableNames.START_CORRELATION_KEY),
                "the payload-derived key rides as the run's correlation key");
    }

    @Test
    void webhook_messageStart_blank_expression_result_rejects_the_start() {
        String workflowId = "wfStartExprBlank-" + UUID.randomUUID();
        deployMessageStartFlowWithConfig(workflowId, "orderKeyedBlank", null, "${msg.missing}");

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", null, "orderKeyedBlank",
                Map.of("orderId", "ORD-9")));

        assertFalse(outcome.correlated(),
                "an expression resolving blank rejects the start — no run with an unfindable key");
        assertNull(outcome.processInstanceId());
    }

    @Test
    void deploy_rejects_a_second_workflow_subscribing_the_same_message_start_name() {
        // Two different process keys on one message-start name make every inbound start
        // ambiguous — the deploy guard fails the second deploy loudly. A new version of the
        // SAME workflow redeploys fine (the latest supersedes).
        String first = "wfDupA-" + UUID.randomUUID();
        String second = "wfDupB-" + UUID.randomUUID();
        String sharedName = "shared." + UUID.randomUUID();
        deployMessageStartFlow(first, sharedName);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> deployMessageStartFlow(second, sharedName));
        assertTrue(ex.getMessage().contains(sharedName),
                "the violation names the contested message name; msg=" + ex.getMessage());

        assertDoesNotThrow(() -> deployMessageStartFlow(first, sharedName),
                "redeploying the SAME workflow (new version) is not a duplicate");
    }

    @Test
    void webhook_messageStart_static_correlationKey_filters_the_inbound_key() {
        String workflowId = "wfStartKey-" + UUID.randomUUID();
        deployMessageStartFlowWithConfig(workflowId, "filteredStart", "O-123", null);

        WebhookEventOutcome mismatch = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", "O-999", "filteredStart", null));
        assertFalse(mismatch.correlated(), "a non-matching addressed key does not start the flow");

        WebhookEventOutcome keyless = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", null, "filteredStart", null));
        assertFalse(keyless.correlated(), "a filtered start needs an addressed key (no minted fallback)");

        WebhookEventOutcome match = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", "O-123", "filteredStart", null));
        assertTrue(match.correlated(), "the matching addressed key starts the flow");
        assertEquals("O-123", sqsEngine.getProcessVariable(match.processInstanceId(),
                        EngineVariableNames.START_CORRELATION_KEY),
                "the matched key rides as the run's correlation key (the businessKey is minted)");
    }

    @Test
    void webhook_messageStart_seeds_a_graph_hydrated_ref_so_the_first_node_can_dispatch() {
        // Regression: a message-started run used to carry NO studioquackRef (msg-less start) or a
        // graph-less msg-envelope ref — the first dispatchable node read it, found no context, and
        // parked forever ("no dispatchable context"). The start path now seeds the same
        // graph-hydrated ref a start-run pins.
        String workflowId = "wfStartSeed-" + UUID.randomUUID();
        deployMessageStartFlow(workflowId, "seededStart");
        // The channel deploy command persists the definition + graph rows the start-seed hydrates
        // from; the fixture deploys via the engine service directly, so persist them explicitly.
        artifactStore.saveDefinition(workflowId, "1", lastDeployInfo);
        artifactStore.saveDeployedGraph(workflowId,
                List.of(node("startN", "message.start.event", "start"),
                        node("taskN", "user.task", null),
                        node("endN", "end.event", "end")),
                List.of(edge("startN", "taskN"), edge("taskN", "endN")));

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", null, "seededStart", null));

        assertTrue(outcome.correlated(), "the msg-less start fires");
        Object v = sqsEngine.getProcessVariable(outcome.processInstanceId(),
                EngineVariableNames.STUDIOQUACK_REF);
        assertTrue(v instanceof ProcessContextRef,
                "a msg-less start still pins the studioquackRef handle");
        ExecutionContext seeded = executionContextStore.loadByRef((ProcessContextRef) v).orElseThrow();
        assertEquals(workflowId, seeded.getWorkflowId(), "the seed addresses the deployed workflow");
        assertEquals(sqsEngine.findBusinessKey(outcome.processInstanceId()),
                seeded.getWorkflowExecutionId(), "the seed is keyed by the run's businessKey");
        assertNotNull(seeded.getNodes(), "the seeded context carries the deployed graph");
        assertFalse(seeded.getNodes().isEmpty(),
                "graph-hydrated: the first dispatchable node resolves its node definition");
    }

    @Test
    void legacy_eventKindStart_seeds_the_same_graph_hydrated_ref() {
        // The legacy ingress (eventKind=START, no eventType) dispatches to startByMessage — a
        // separate leg from correlateWebhook. It must seed the graph-hydrated ref too, or a
        // legacy-started run parks at its first dispatchable node.
        String workflowId = "wfLegacyStartSeed-" + UUID.randomUUID();
        deployMessageStartFlow(workflowId, "legacySeededStart");
        artifactStore.saveDefinition(workflowId, "1", lastDeployInfo);
        artifactStore.saveDeployedGraph(workflowId,
                List.of(node("startN", "message.start.event", "start"),
                        node("taskN", "user.task", null),
                        node("endN", "end.event", "end")),
                List.of(edge("startN", "taskN"), edge("taskN", "endN")));

        String callerKey = "legacy-" + UUID.randomUUID();
        MessageCorrelationOutcome outcome = correlationService.correlateEvent(
                "START", "legacySeededStart", callerKey, null, null);

        assertTrue(outcome.correlated(), "the legacy START leg launches the flow");
        Object v = sqsEngine.getProcessVariable(outcome.executionId(),
                EngineVariableNames.STUDIOQUACK_REF);
        assertTrue(v instanceof ProcessContextRef, "the legacy start pins the studioquackRef handle");
        ExecutionContext seeded = executionContextStore.loadByRef((ProcessContextRef) v).orElseThrow();
        assertEquals(workflowId, seeded.getWorkflowId());
        String minted = sqsEngine.findBusinessKey(outcome.executionId());
        assertNotNull(minted);
        assertTrue(minted.startsWith("wh_"),
                "the run id is minted — the caller's correlationKey is correlation data, not identity");
        assertEquals(minted, seeded.getWorkflowExecutionId());
        assertEquals(callerKey, sqsEngine.getProcessVariable(outcome.executionId(),
                        EngineVariableNames.START_CORRELATION_KEY),
                "the caller key is seeded as the run's correlation key");
        assertNotNull(seeded.getNodes(), "the seeded context carries the deployed graph");
        assertFalse(seeded.getNodes().isEmpty());
    }

    @Test
    void two_starts_reusing_one_correlation_key_yield_two_distinct_runs() {
        // Regression for the run-index collision: the caller's correlationKey is correlation DATA,
        // never the run id — two starts with the SAME key (any message name, any env) must produce
        // two fully independent runs. Previously the second run's status/log callbacks collided
        // with the first run's execution_history row and were refused (Error_EnvMismatch).
        String workflowId = "wfReuseKey-" + UUID.randomUUID();
        String messageName = "reuseKeyStart." + UUID.randomUUID();
        deployMessageStartFlow(workflowId, messageName);

        MessageCorrelationOutcome first = correlationService.correlateEvent(
                "START", messageName, "test-1", null, null);
        MessageCorrelationOutcome second = correlationService.correlateEvent(
                "START", messageName, "test-1", null, null);

        assertTrue(first.correlated() && second.correlated(), "both starts fire");
        assertNotEquals(first.executionId(), second.executionId(), "two process instances");
        String firstKey = sqsEngine.findBusinessKey(first.executionId());
        String secondKey = sqsEngine.findBusinessKey(second.executionId());
        assertNotNull(firstKey);
        assertNotNull(secondKey);
        assertNotEquals(firstKey, secondKey, "each run carries its own minted businessKey");
        assertEquals("test-1", sqsEngine.getProcessVariable(first.executionId(),
                EngineVariableNames.START_CORRELATION_KEY));
        assertEquals("test-1", sqsEngine.getProcessVariable(second.executionId(),
                EngineVariableNames.START_CORRELATION_KEY));
    }

    @Test
    void catch_of_a_message_started_run_is_addressable_by_its_start_key() {
        // The run's businessKey is minted (a sender never learns it); a downstream catch must still
        // correlate when the sender addresses the key the run was STARTED with — the third
        // fallback behind businessKey and the receiver-authored alias pin.
        String workflowId = "wfStartThenCatch-" + UUID.randomUUID();
        String startMsg = "startMe." + UUID.randomUUID();
        String catchMsg = "catchMe." + UUID.randomUUID();
        NodeDto start = node("startN", "message.start.event", "start");
        ParameterDto name = new ParameterDto();
        name.setShortCode("name");
        name.setValue(startMsg);
        start.getData().setConfigurationParameters(new ArrayList<>(List.of(name)));
        NodeDto catchNode = messageCatchNode("msgN", catchMsg);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Start-then-catch Flow", "1",
                List.of(start, catchNode, task, end),
                List.of(edge("startN", "msgN"), edge("msgN", "taskN"), edge("taskN", "endN")));
        deploymentService.deploy(graph, null, true);

        String startKey = "order-" + UUID.randomUUID();
        MessageCorrelationOutcome started = correlationService.correlateEvent(
                "START", startMsg, startKey, null, null);
        assertTrue(started.correlated(), "the message start fires and the run parks on the catch");

        assertNull(sqsEngine.findMessageEventExecution(startKey, catchMsg),
                "the start key is NOT the run's businessKey — businessKey addressing must miss");
        MessageCorrelationOutcome caught = correlationService.correlate(catchMsg, startKey, null);
        assertTrue(caught.correlated(), "the catch correlates by the run's start key");
    }

    @Test
    void webhook_messageCatch_correlates_parked_instance_and_seals_msg_as_ref() {        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("svcWebhookCatch", businessKey);

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_CATCH, null, "msgN", businessKey, MESSAGE_NAME,
                Map.of("approvedBy", "mgr-7")));

        assertTrue(outcome.correlated(), "the parked message catch is correlated");
        assertEquals(instance.getId(), outcome.processInstanceId());
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()), "the instance advanced, did not end");

        List<String> varNames = variableNames(instance.getId());
        assertTrue(varNames.contains(EngineVariableNames.STUDIOQUACK_REF),
                "the sealed msg is published as the studioquackRef handle; vars=" + varNames);
        assertFalse(varNames.contains("approvedBy"), "msg field never reaches the engine as a plain var");
        assertFalse(varNames.contains("msg"), "msg itself is never published as a plain engine variable");
    }

    @Test
    void webhook_messageCatch_noMatch_isBenignNotCorrelated() {
        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_CATCH, null, "msgN", "no-such-run-" + UUID.randomUUID(),
                MESSAGE_NAME, Map.of("k", "v")));

        assertFalse(outcome.correlated(), "no waiting execution → correlated:false, not a throw");
        assertNull(outcome.processInstanceId());
    }

    @Test
    void webhook_signalStart_broadcasts_and_reports_no_processInstanceId() {
        String workflowId = "wfWebhookSignal-" + UUID.randomUUID();
        deploySignalStartFlow(workflowId, "globalAbort");

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.SIGNAL_START, workflowId, "startN", null, "globalAbort", null));

        assertTrue(outcome.correlated(), "a signal broadcast is reported as correlated");
        assertNull(outcome.processInstanceId(), "a broadcast signal-start reports no single instance id");
    }

    @Test
    void webhook_unsupportedEventType_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> correlationService.correlateWebhook(new WebhookEventRequest(
                        null, null, "n1", "bk-1", MESSAGE_NAME, null)),
                "a null eventType is a client error the ingress maps to 400");
    }

    @Test
    void webhook_catchWithoutWorkflowInstanceId_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> correlationService.correlateWebhook(new WebhookEventRequest(
                        SqEventType.MESSAGE_CATCH, null, "n1", null, MESSAGE_NAME, null)),
                "workflowInstanceId is required for every non-start kind");
    }

    /**
     * BOUNDARY_*: when the daemon holds no deployed graph for {@code workflowId} (e.g. the artifact
     * store has no record — the address resolver would 404 in production before ever reaching this
     * service), {@link MessageCorrelationService} degrades safely to sealing against the HOST node id
     * itself rather than throwing, so an inconsistent/racing deploy can never crash correlation.
     */
    @Test
    void webhook_boundaryMessage_withNoDeployedGraph_fallsBackToHostNodeAndStillSeals() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("svcWebhookBoundaryFallback", businessKey);

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.BOUNDARY_MESSAGE, "no-such-deployed-workflow-id", "msgN", businessKey,
                MESSAGE_NAME, Map.of("cancelReason", "customer request")));

        assertTrue(outcome.correlated(), "the parked catch still correlates via (businessKey, name)");
        List<String> varNames = variableNames(instance.getId());
        assertTrue(varNames.contains(EngineVariableNames.STUDIOQUACK_REF), "msg is still sealed as a ref");
        assertFalse(varNames.contains("cancelReason"), "msg field never reaches the engine as a plain var");
    }

    // ---- Authored shared correlationKey (receiver pin + alias fallback) -------------------

    /**
     * The reported bug: a catch with an authored {@code correlationKey} must be correlatable by
     * that SHARED key even when no run carries it as a businessKey (a cross-flow sender cannot
     * know the receiver run's workflowExecutionId). The catch pins the key at park time; the
     * correlation falls back to the pin when the businessKey lookup misses.
     */
    @Test
    void parked_message_catch_is_correlated_by_authored_shared_key_when_businessKey_misses() {
        String sharedKey = "ord-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlowWithKey(
                "svcAliasCorrelate-" + UUID.randomUUID(), businessKey, sharedKey);

        // The pin landed under the whitelisted name on the parked execution.
        List<String> varNames = variableNames(instance.getId());
        assertTrue(varNames.contains(EngineVariableNames.messageCorrelationVariableName(MESSAGE_NAME)),
                "the authored correlationKey is pinned at park time; vars=" + varNames);

        // The businessKey lookup misses (no run carries the shared key as its businessKey)…
        assertNull(sqsEngine.findMessageEventExecution(sharedKey, MESSAGE_NAME),
                "precondition: the shared key is nobody's businessKey");
        // …but the authored-key fallback correlates.
        MessageCorrelationOutcome outcome = correlationService.correlate(MESSAGE_NAME, sharedKey, null);

        assertTrue(outcome.correlated(), "the authored shared key correlates via the pin fallback");
        assertNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the message subscription is consumed once correlated");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()),
                "the instance advanced to the user task, it did not end");
    }

    @Test
    void webhook_messageCatch_correlates_by_authored_shared_key_and_seals_under_real_businessKey() {
        String sharedKey = "ord-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlowWithKey(
                "svcAliasWebhook-" + UUID.randomUUID(), businessKey, sharedKey);

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_CATCH, null, "msgN", sharedKey, MESSAGE_NAME,
                Map.of("approvedBy", "mgr-7")));

        assertTrue(outcome.correlated(), "the authored shared key correlates via the pin fallback");
        assertEquals(instance.getId(), outcome.processInstanceId());
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()),
                "the instance advanced, did not end");

        // The sealed msg rows are stored under the receiver's REAL businessKey (not the alias),
        // so the receiver's own downstream input scan finds them.
        ProcessContextRef ref = new ProcessContextRef();
        ref.setExecutionId(businessKey);
        List<InOutValueDto> rows = executionContextStore.loadInputsTargetingNode(ref, "msgN");
        assertTrue(rows.stream().anyMatch(r -> "approvedBy".equals(r.getTargetPortId())),
                "sealed msg fields are readable under the receiver's real businessKey");
    }

    @Test
    void webhook_messageCatch_fans_out_the_template_envelope_to_the_downstream_node() {
        String businessKey = UUID.randomUUID().toString();
        deployAndStartMessageCatchFlow("svcCatchEnvelope-" + UUID.randomUUID(), businessKey);

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_CATCH, null, "msgN", businessKey, MESSAGE_NAME,
                Map.of("approvedBy", "mgr-7")));
        assertTrue(outcome.correlated(), "the parked message catch is correlated");

        // The template's promised OUT port `message` arrives at the catch's downstream (taskN) as
        // ONE row carrying the full envelope — the fields the DuckTape schema tree offers.
        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<InOutValueDto> downstream = executionContextStore.loadInputsTargetingNode(readRef, "taskN");
        InOutValueDto row = downstream.stream()
                .filter(r -> "msgN".equals(r.getNodeId()) && "message".equals(r.getPortId()))
                .findFirst().orElse(null);
        assertNotNull(row, "the catch node's `message` envelope row targets its downstream node");
        assertTrue(row.getValue() instanceof Map, "the envelope is a single object value");
        Map<?, ?> envelope = (Map<?, ?>) row.getValue();
        assertEquals(MESSAGE_NAME, envelope.get("messageName"));
        assertEquals(businessKey, envelope.get("correlationKey"),
                "the key the sender addressed (here the run businessKey)");
        assertEquals(Map.of("approvedBy", "mgr-7"), envelope.get("payload"), "payload verbatim");
        assertNotNull(envelope.get("payloadRef"), "sealed-store row reference");
        assertNotNull(envelope.get("envelopeMeta"));
        Map<?, ?> meta = (Map<?, ?>) envelope.get("envelopeMeta");
        String expectedAlg = executionContextCrypto.sealAlgorithm() == null
                ? "none" : executionContextCrypto.sealAlgorithm();
        assertEquals(expectedAlg, meta.get("sealAlg"),
                "sealAlg mirrors the daemon's ACTUAL crypto state, never a hardcoded claim");
        assertNull(meta.get("kid"), "no key-id concept exists; the field is never written");
        assertNotNull(envelope.get("receivedAt"));
        assertNotNull(envelope.get("messageId"));
        assertEquals("webhook", envelope.get("source"));
        assertEquals("external", envelope.get("senderTier"));

        // The legacy flat rows targeting the event node itself still stand (boundary-handler seam
        // and observability) — the envelope is additive, not a replacement.
        List<InOutValueDto> flat = executionContextStore.loadInputsTargetingNode(readRef, "msgN");
        assertTrue(flat.stream().anyMatch(r -> "approvedBy".equals(r.getTargetPortId())),
                "the flat per-field rows targeting the event node are unchanged");
    }

    @Test
    void webhook_messageStart_fans_out_the_envelope_to_the_start_nodes_downstream() {
        String workflowId = "wfStartEnvelope-" + UUID.randomUUID();
        // Message-start subscriptions are engine-wide unique by name — never reuse another test's.
        String messageName = "orderIntakeEnv-" + UUID.randomUUID();
        deployMessageStartFlow(workflowId, messageName);

        WebhookEventOutcome outcome = correlationService.correlateWebhook(new WebhookEventRequest(
                SqEventType.MESSAGE_START, workflowId, "startN", null, messageName,
                Map.of("orderId", "ORD-42")));
        assertTrue(outcome.correlated(), "a message-start launches a new instance");
        assertNotNull(outcome.processInstanceId());

        // The started instance's businessKey is the one the seal used (wh_* generated here).
        String businessKey = sqsEngine.findBusinessKey(outcome.processInstanceId());
        ProcessContextRef readRef = new ProcessContextRef();
        readRef.setExecutionId(businessKey);
        List<InOutValueDto> downstream = executionContextStore.loadInputsTargetingNode(readRef, "taskN");
        InOutValueDto row = downstream.stream()
                .filter(r -> "startN".equals(r.getNodeId()) && "message".equals(r.getPortId()))
                .findFirst().orElse(null);
        assertNotNull(row, "the start node's `message` envelope row targets its downstream node");
        Map<?, ?> envelope = (Map<?, ?>) row.getValue();
        assertEquals(messageName, envelope.get("messageName"));
        assertEquals(businessKey, envelope.get("correlationKey"));
        assertEquals(Map.of("orderId", "ORD-42"), envelope.get("payload"));
        assertEquals("webhook", envelope.get("source"));
        assertEquals("external", envelope.get("senderTier"));
    }

    @Test
    void wrong_authored_key_is_a_benign_no_match_and_leaves_instance_parked() {
        String sharedKey = "ord-" + UUID.randomUUID();
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlowWithKey(
                "svcAliasMiss-" + UUID.randomUUID(), businessKey, sharedKey);

        MessageCorrelationOutcome outcome = correlationService.correlate(
                MESSAGE_NAME, "different-" + UUID.randomUUID(), null);

        assertFalse(outcome.correlated(), "a non-matching key correlates nothing");
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the parked instance is left untouched");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()), "the instance still runs");
    }

    @Test
    void catch_without_authored_key_pins_no_correlation_variable() {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow(
                "svcNoPin-" + UUID.randomUUID(), businessKey);

        List<String> varNames = variableNames(instance.getId());
        assertFalse(varNames.stream().anyMatch(n -> n.startsWith(EngineVariableNames.MSG_CORR_PREFIX)),
                "no authored correlationKey → no pin variable; vars=" + varNames);
    }

    // ---- DuckTape-mapped runtime override (subscription port) -----------------------------

    /**
     * The DuckTape lane: a mapped row on the catch's INPUT {@code subscription} port arrives in
     * the pipeline's wire shape ({@code targetPortValue="subscription"}, value = the mapped
     * payload object) and supplies the correlation key at runtime when the panel is blank.
     */
    @Test
    void ducktape_mapped_correlation_key_pins_and_correlates() {
        String businessKey = UUID.randomUUID().toString();
        String sharedKey = "ORDER-" + UUID.randomUUID();
        seedSubscriptionOverride(businessKey, "msgN", Map.of("correlationKey", sharedKey));

        ProcessInstance instance = deployAndStartMessageCatchFlow(
                "svcDtOverride-" + UUID.randomUUID(), businessKey);

        List<String> varNames = variableNames(instance.getId());
        assertTrue(varNames.contains(EngineVariableNames.messageCorrelationVariableName(MESSAGE_NAME)),
                "the DuckTape-mapped correlationKey is pinned at park time; vars=" + varNames);
        assertNull(sqsEngine.findMessageEventExecution(sharedKey, MESSAGE_NAME),
                "precondition: the mapped key is nobody's businessKey");

        MessageCorrelationOutcome outcome = correlationService.correlate(MESSAGE_NAME, sharedKey, null);
        assertTrue(outcome.correlated(), "correlation by the DuckTape-mapped shared key delivers");
    }

    /**
     * Precedence: a mapped value on the subscription port WINS over the panel's literal
     * {@code correlationKey} — a mapping is a deliberate per-wire authoring act, the panel value
     * is the default.
     */
    @Test
    void ducktape_mapped_key_wins_over_the_panel_value() {
        String businessKey = UUID.randomUUID().toString();
        String panelKey = "PANEL-" + UUID.randomUUID();
        String mappedKey = "MAPPED-" + UUID.randomUUID();
        seedSubscriptionOverride(businessKey, "msgN", Map.of("correlationKey", mappedKey));

        ProcessInstance instance = deployAndStartMessageCatchFlowWithKey(
                "svcDtPrecedence-" + UUID.randomUUID(), businessKey, panelKey);

        MessageCorrelationOutcome byPanel = correlationService.correlate(MESSAGE_NAME, panelKey, null);
        assertFalse(byPanel.correlated(), "the panel value lost to the mapped override");
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "still parked after the panel-key miss");

        MessageCorrelationOutcome byMapped = correlationService.correlate(MESSAGE_NAME, mappedKey, null);
        assertTrue(byMapped.correlated(), "the mapped override key correlates");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()), "advanced to the user task");
    }

    /** Pre-seeds the store with the pipeline's wire shape for a DuckTape mapping into the catch. */
    private void seedSubscriptionOverride(String businessKey, String catchNodeId,
                                          Map<String, Object> payload) {
        // saveOutputs persists through the ref's currentNodeRefId, so the seed needs a real
        // snapshot entity — the same pseudo-node pattern MessageCorrelationService.sealMsg uses.
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setCurrentNodeId("upstreamN" + UUID.randomUUID());
        ProcessContextRef ref = executionContextStore.snapshot(ctx, null);
        InOutValueDto row = new InOutValueDto();
        row.setNodeId(ctx.getCurrentNodeId());
        row.setPortId("serviceOutput");
        row.setPortValue("serviceOutput");
        row.setTargetNodeId(catchNodeId);
        // The pipeline writes the target port's template UUID as targetPortId; the listener
        // matches on targetPortValue ("subscription") + the payload's correlationKey field.
        row.setTargetPortId("d2806806-b470-4264-9a77-e7665093b90c");
        row.setTargetPortValue("subscription");
        row.setValue(payload);
        executionContextStore.saveOutputs(ref, List.of(row));
    }

    // ---- fixtures -------------------------------------------------------

    private List<String> variableNames(String processInstanceId) {
        return sqsEngine.historicVariables(processInstanceId).list().stream()
                .map(v -> v.getVariableName()).toList();
    }

    /** Deploys (never starts) {@code messageStart(name) → userTask → end}; a webhook launches it. */
    private void deployMessageStartFlow(String workflowId, String messageName) {
        deployMessageStartFlowWithConfig(workflowId, messageName, null, null);
    }

    /** As {@link #deployMessageStartFlow} plus the optional C7 receiver config params. */
    private void deployMessageStartFlowWithConfig(String workflowId, String messageName,
                                                  String correlationKey, String businessKeyExpression) {
        NodeDto start = node("startN", "message.start.event", "start");
        List<ParameterDto> params = new ArrayList<>();
        ParameterDto name = new ParameterDto();
        name.setShortCode("name");
        name.setValue(messageName);
        params.add(name);
        if (correlationKey != null) {
            ParameterDto p = new ParameterDto();
            p.setShortCode("correlationKey");
            p.setValue(correlationKey);
            params.add(p);
        }
        if (businessKeyExpression != null) {
            ParameterDto p = new ParameterDto();
            p.setShortCode("businessKeyExpression");
            p.setValue(businessKeyExpression);
            params.add(p);
        }
        start.getData().setConfigurationParameters(params);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Webhook Message Start Flow", "1",
                List.of(start, task, end), List.of(edge("startN", "taskN"), edge("taskN", "endN")));
        lastDeployInfo = deploymentService.deploy(graph, null, true);
    }

    /** Deploys (never starts) {@code signalStart(name) → userTask → end}; a webhook broadcast launches it. */
    private void deploySignalStartFlow(String workflowId, String signalName) {
        NodeDto start = node("startN", "signal.start.event", "start");
        ParameterDto name = new ParameterDto();
        name.setShortCode("name");
        name.setValue(signalName);
        start.getData().setConfigurationParameters(new ArrayList<>(List.of(name)));
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Webhook Signal Start Flow", "1",
                List.of(start, task, end), List.of(edge("startN", "taskN"), edge("taskN", "endN")));
        deploymentService.deploy(graph, null, true);
    }

    /**
     * Deploys {@code start → messageCatch → userTask → end} and starts one instance with
     * {@code businessKey == workflowExecutionId}; it parks on the message catch (subscription =
     * {@link #MESSAGE_NAME}).
     */
    private ProcessInstance deployAndStartMessageCatchFlow(String workflowId, String businessKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto message = messageCatchNode("msgN", MESSAGE_NAME);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Correlate Message Flow", "1",
                List.of(start, message, task, end),
                List.of(edge("startN", "msgN"), edge("msgN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "instance started for the message-catch flow");
        return instance;
    }

    /**
     * As {@link #deployAndStartMessageCatchFlow} but the catch node also carries an authored
     * {@code correlationKey} configuration parameter — the shared key a sender can correlate by
     * without knowing this run's businessKey.
     */
    private ProcessInstance deployAndStartMessageCatchFlowWithKey(String workflowId, String businessKey,
                                                                  String correlationKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto message = messageCatchNode("msgN", MESSAGE_NAME);
        ParameterDto key = new ParameterDto();
        key.setShortCode("correlationKey");
        key.setValue(correlationKey);
        message.getData().getConfigurationParameters().add(key);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Correlate Message Flow (authored key)", "1",
                List.of(start, message, task, end),
                List.of(edge("startN", "msgN"), edge("msgN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "instance started for the message-catch flow (authored key)");
        return instance;
    }

    /**
     * Deploys {@code start → userTask → gw → approvedN (conditioned) / rejectedN (default)} and
     * starts one instance parked on the user task. The gateway's generated JUEL ({@code
     * ${route__gwN == 'approvedN'}}) only resolves when the completion publishes the route token —
     * the regression this suite guards.
     */
    /** The decision the backend would send for the task parked on {@code taskN} in this run. */
    private MessageCorrelationService.UserTaskDecision decision(String businessKey, String workflowId,
                                                                String answer) {
        String taskId = sqsEngine.findUserTask(businessKey, "taskN").getId();
        return new MessageCorrelationService.UserTaskDecision(taskId, "taskN", workflowId, businessKey, answer);
    }

    private ProcessInstance deployAndStartUserTaskGatewayFlow(String workflowId, String businessKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto task = com.studioquack.sqs.usertask.UserTaskFixtures.withUserTaskPorts(node("taskN", "user.task", null));
        task.getData().setNodeShortCode("taskN");
        NodeDto gateway = com.studioquack.sqs.usertask.UserTaskFixtures.withInputPort(
                node("gwN", "exclusive.gateway", null), "in");
        NodeDto approved = node("approvedN", "end.event", "end");
        NodeDto rejected = node("rejectedN", "end.event", "end");
        EdgeDto approveEdge = edge("gwN", "approvedN");
        EdgeDataDto approveData = new EdgeDataDto();
        approveData.setCondition("@taskN.task_output.decision == 'approved'");
        approveEdge.setData(approveData);
        EdgeDto rejectEdge = edge("gwN", "rejectedN");
        EdgeDataDto rejectData = new EdgeDataDto();
        rejectData.setIsDefaultFlow(true);
        rejectEdge.setData(rejectData);
        WorkflowGraph graph = new WorkflowGraph(workflowId, "User Task Gateway Flow", "1",
                List.of(start, task, gateway, approved, rejected),
                List.of(edge("startN", "taskN"),
                        com.studioquack.sqs.usertask.UserTaskFixtures.wire("taskN", "task_output", "gwN", "in"),
                        approveEdge, rejectEdge));
        // Seed the studioquackRef exactly like start-run does (the completion's route-token
        // resolution reads the run's graph from the sealed context behind this handle).
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowExecutionId(businessKey);
        seed.setNodes(graph.nodes());
        seed.setEdges(graph.edges());
        ProcessContextRef ref = executionContextStore.snapshot(seed, null);
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey,
                Map.of(EngineVariableNames.STUDIOQUACK_REF, ref), true);
        assertNotNull(instance, "instance started for the user-task gateway flow");
        return instance;
    }

    private List<String> finishedActivityIds(String processInstanceId) {
        return sqsEngine.historicActivityInstances(processInstanceId).finished().list().stream()
                .map(a -> a.getActivityId()).toList();
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

    /** A message intermediate catch node whose authored {@code name} parameter fixes the message name. */
    private NodeDto messageCatchNode(String id, String messageName) {
        NodeDto node = node(id, "message.intermediate.catch.event", null);
        ParameterDto name = new ParameterDto();
        name.setShortCode("name");
        name.setValue(messageName);
        node.getData().setConfigurationParameters(new ArrayList<>(List.of(name)));
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
