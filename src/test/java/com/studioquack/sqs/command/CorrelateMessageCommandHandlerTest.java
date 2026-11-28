package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.webhook.EngineApiKeyScope;
import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.SecurityTelemetryAccumulator;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.webhook.DaemonWebhookKeyStore;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code correlate-message} command handler against the embedded Activiti engine on an
 * in-memory database. Proves the reverse-channel message-event correlation seam and inline
 * daemon-side {@link EngineApiKeyScope#ENGINE_WIDE} API key verification end-to-end.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-correlate-msg-cmd-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.channel.enabled=false",
        "sqs.channel.env-id=env-correlate",
        "sqs.channel.bearer=test-correlate-bearer-0123456789abcdef",
        "sqs.channel.data-dir=${java.io.tmpdir}/sqs-correlate-msg-cmd-test-no-kem",
        "sqs.secrets.os.enabled=false"
})
@ActiveProfiles("base")
class CorrelateMessageCommandHandlerTest {

    private static final String ENV_ID = "env-correlate";
    private static final String TEST_BEARER = "test-correlate-bearer-0123456789abcdef";
    private static final String MESSAGE_NAME = "orderApproved";

    @Autowired
    private CorrelateMessageCommandHandler handler;
    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private DaemonWebhookKeyStore webhookKeyStore;
    @Autowired
    private ObjectMapper mapper;

    @Test
    void parked_message_catch_is_correlated_vars_delivered_and_execution_advances() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("wfCorrelate", businessKey);

        // The instance is parked on the message-catch subscription for (businessKey, MESSAGE_NAME).
        Execution parked = sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME);
        assertNotNull(parked, "instance must park on the message catch subscription before correlation");

        // A message with a REF-ONLY (route__) token is correlated to the parked run.
        String payload = mapper.writeValueAsString(Map.of(
                "envId", ENV_ID,
                "messageName", MESSAGE_NAME,
                "businessKey", businessKey,
                "variables", Map.of("route__correlated", "yes")));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-1", payload));

        assertTrue(outcome.succeeded(), () -> "correlate error: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.TRUE, out.get("correlated"), "the parked message event was correlated");
        assertEquals(parked.getId(), out.get("executionId"), "the resolved parked execution id is reported");
        assertEquals(MESSAGE_NAME, out.get("messageName"), "the message name is echoed back");

        // Execution advanced: the message subscription is consumed, but the instance is still running
        // (now parked on the user task) — the token moved past the catch event.
        assertNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the message subscription is consumed once correlated");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()),
                "the instance advanced to the next wait state (user task), it did not end");

        // The delivered REF-ONLY variable is durably usable by the instance (recorded in history at
        // historyLevel=FULL): the handler pins it at the process-instance scope before advancing.
        List<String> varNames = sqsEngine.historicVariables(instance.getId()).list().stream()
                .map(v -> v.getVariableName()).toList();
        assertTrue(varNames.contains("route__correlated"), "the delivered route token landed; vars=" + varNames);
        assertEquals("yes", sqsEngine.getProcessVariable(instance.getId(), "route__correlated"),
                "the delivered value is readable at the instance scope");
    }

    @Test
    void correlation_key_alias_resolves_when_business_key_absent() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        deployAndStartMessageCatchFlow("wfCorrelateAlias", businessKey);

        // No businessKey field — only correlationKey (they are the same workflowExecutionId).
        String payload = mapper.writeValueAsString(Map.of(
                "envId", ENV_ID,
                "messageName", MESSAGE_NAME,
                "correlationKey", businessKey));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-alias", payload));

        assertTrue(outcome.succeeded(), () -> "correlate error: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.TRUE, out.get("correlated"), "correlationKey alias resolves the parked run");
        assertNotNull(out.get("executionId"), "a real execution was correlated via the correlationKey alias");
    }

    @Test
    void no_waiting_execution_returns_ok_not_correlated() throws Exception {
        // A businessKey with no running instance: a benign no-match, reported as ok{correlated:false}.
        String payload = mapper.writeValueAsString(Map.of(
                "envId", ENV_ID,
                "messageName", MESSAGE_NAME,
                "businessKey", "no-such-run-" + UUID.randomUUID()));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-nomatch", payload));

        assertTrue(outcome.succeeded(), () -> "no-match must be ok, not failed: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.FALSE, out.get("correlated"), "no waiting execution → correlated:false");
        assertFalse(out.containsKey("executionId"), "no executionId is emitted on a no-match; out=" + out);
        assertEquals(MESSAGE_NAME, out.get("messageName"), "the message name is still echoed on a no-match");
    }

    @Test
    void wrong_message_name_is_not_correlated_and_leaves_instance_parked() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("wfCorrelateWrongMsg", businessKey);

        // Right run, wrong message name: nothing is waiting on this message → no-match.
        String payload = mapper.writeValueAsString(Map.of(
                "envId", ENV_ID,
                "messageName", "someOtherMessage",
                "businessKey", businessKey));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-wrongmsg", payload));

        assertTrue(outcome.succeeded(), () -> "wrong-message no-match must be ok: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.FALSE, out.get("correlated"), "a mismatched message name does not correlate");

        // The real parked instance was NOT consumed — it is still waiting on its own message.
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the parked instance is left untouched by a mismatched message");
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()), "the instance still runs");
    }

    @Test
    void malformed_payload_returns_failed_not_throw() {
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-bad", "{ not-json"));
        assertFalse(outcome.succeeded(), "malformed payload must fail");
        assertNotNull(outcome.error(), "failure carries an error message for the control plane");
        assertTrue(outcome.error().startsWith("correlate-message:"), "error is namespaced; got: " + outcome.error());
    }

    @Test
    void payload_without_message_name_returns_failed() throws Exception {
        String payload = mapper.writeValueAsString(Map.of("envId", ENV_ID, "businessKey", "bk-1"));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-nomsg", payload));
        assertFalse(outcome.succeeded(), "a payload without messageName must fail");
        assertNotNull(outcome.error());
    }

    @Test
    void payload_without_correlation_key_returns_failed() throws Exception {
        String payload = mapper.writeValueAsString(Map.of("envId", ENV_ID, "messageName", MESSAGE_NAME));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-nokey", payload));
        assertFalse(outcome.succeeded(), "a payload without businessKey/correlationKey must fail");
        assertNotNull(outcome.error());
    }

    @Test
    void phi_variable_is_rejected_by_ref_only_guard() throws Exception {
        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("wfCorrelatePhi", businessKey);

        // A non-whitelisted (PHI-bearing) variable must be rejected before it can reach ACT_*_VARIABLE.
        String payload = mapper.writeValueAsString(Map.of(
                "envId", ENV_ID,
                "messageName", MESSAGE_NAME,
                "businessKey", businessKey,
                "variables", Map.of("patientName", "SECRET-9f3a")));
        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-phi", payload));

        assertFalse(outcome.succeeded(), "a PHI variable must fail the REF-ONLY guard");
        assertNotNull(outcome.error());
        assertTrue(outcome.error().contains("REF-ONLY"), "error names the REF-ONLY violation: " + outcome.error());

        // Nothing was delivered: the instance is still parked and the PHI var never reached history.
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                "the instance is still parked; no message was delivered");
        List<String> varNames = sqsEngine.historicVariables(instance.getId()).list().stream()
                .map(v -> v.getVariableName()).toList();
        assertFalse(varNames.contains("patientName"), "the PHI var never reached the engine; vars=" + varNames);
    }

    @Test
    void inline_engine_wide_api_key_verifies_and_correlates_parked_message() throws Exception {
        String refId = WebhookKeyFormat.generateRefId();
        var created = webhookKeyStore.create(refId, "Engine Wide Key", EngineApiKeyScope.ENGINE_WIDE);
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();

        String businessKey = UUID.randomUUID().toString();
        ProcessInstance instance = deployAndStartMessageCatchFlow("wfCorrelateEngineKey", businessKey);
        Execution parked = sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME);
        assertNotNull(parked);

        SecureEnvelope verifyEnv = sealApiKeyVerification(refId, created.keyId(), parsed.secret());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("envId", ENV_ID);
        body.put("messageName", MESSAGE_NAME);
        body.put("businessKey", businessKey);
        body.put("apiKeyVerification", verifyEnv);

        DaemonCommandOutcome outcome = handler.handle(command("c-correlate-engkey-ok", mapper.writeValueAsString(body)));
        assertTrue(outcome.succeeded(), () -> "valid ENGINE_WIDE key must correlate: " + outcome.error());
        Map<?, ?> out = mapper.readValue(outcome.output(), Map.class);
        assertEquals(Boolean.TRUE, out.get("correlated"));
        assertEquals(parked.getId(), out.get("executionId"));
        assertNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME));
        assertTrue(sqsEngine.isProcessInstanceActive(instance.getId()));
    }

    @Test
    void inline_node_scoped_api_key_is_rejected_and_leaves_instance_parked() throws Exception {
        String refId = WebhookKeyFormat.generateRefId();
        var created = webhookKeyStore.create(refId, "Node Scoped Key", EngineApiKeyScope.NODE_SCOPED);
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();

        String businessKey = UUID.randomUUID().toString();
        deployAndStartMessageCatchFlow("wfCorrelateNodeKeyDenied", businessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME));

        SecureEnvelope verifyEnv = sealApiKeyVerification(refId, created.keyId(), parsed.secret());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("envId", ENV_ID);
        body.put("messageName", MESSAGE_NAME);
        body.put("businessKey", businessKey);
        body.put("apiKeyVerification", verifyEnv);

        SecurityTelemetryAccumulator telemetry = new SecurityTelemetryAccumulator();
        handler.setSecurityTelemetry(telemetry);
        try {
            DaemonCommandOutcome outcome = handler.handle(
                    command("c-correlate-nodekey-denied", mapper.writeValueAsString(body)));
            assertFalse(outcome.succeeded(), "NODE_SCOPED key must be rejected on correlate-message");
            assertEquals(CorrelateMessageCommandHandler.ERROR_INVALID_API_KEY, outcome.error());
            assertEquals(1, telemetry.drain().get(CorrelateMessageCommandHandler.TELEMETRY_WEBHOOK_KEY_AUTH));
            assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME),
                    "parked instance must remain untouched on key rejection");
        } finally {
            handler.setSecurityTelemetry(null);
        }
    }

    @Test
    void inline_engine_wide_wrong_secret_and_revoked_key_are_rejected() throws Exception {
        String refId = WebhookKeyFormat.generateRefId();
        var created = webhookKeyStore.create(refId, "Revocable Engine Key", EngineApiKeyScope.ENGINE_WIDE);
        var parsed = WebhookKeyFormat.parse(created.rawKey()).orElseThrow();

        String businessKey = UUID.randomUUID().toString();
        deployAndStartMessageCatchFlow("wfCorrelateBadAndRevokedKey", businessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME));

        // 1. Wrong secret
        Map<String, Object> wrongSecretBody = new LinkedHashMap<>();
        wrongSecretBody.put("envId", ENV_ID);
        wrongSecretBody.put("messageName", MESSAGE_NAME);
        wrongSecretBody.put("businessKey", businessKey);
        wrongSecretBody.put("apiKeyVerification",
                sealApiKeyVerification(refId, created.keyId(), "wrong_secret_000000000000000000000000000000"));
        DaemonCommandOutcome badSecretOut = handler.handle(
                command("c-correlate-bad-secret", mapper.writeValueAsString(wrongSecretBody)));
        assertFalse(badSecretOut.succeeded());
        assertEquals(CorrelateMessageCommandHandler.ERROR_INVALID_API_KEY, badSecretOut.error());
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME));

        // 2. Revoked key presenting its previously valid secret
        assertTrue(webhookKeyStore.revoke(refId, created.keyId()));
        Map<String, Object> revokedBody = new LinkedHashMap<>();
        revokedBody.put("envId", ENV_ID);
        revokedBody.put("messageName", MESSAGE_NAME);
        revokedBody.put("businessKey", businessKey);
        revokedBody.put("apiKeyVerification",
                sealApiKeyVerification(refId, created.keyId(), parsed.secret()));
        DaemonCommandOutcome revokedOut = handler.handle(
                command("c-correlate-revoked", mapper.writeValueAsString(revokedBody)));
        assertFalse(revokedOut.succeeded());
        assertEquals(CorrelateMessageCommandHandler.ERROR_INVALID_API_KEY, revokedOut.error());
        assertNotNull(sqsEngine.findMessageEventExecution(businessKey, MESSAGE_NAME));
    }

    // ---- fixtures -------------------------------------------------------

    private SecureEnvelope sealApiKeyVerification(String refId, String keyId, String secret) throws Exception {
        byte[] ikm = EnvelopeCrypto.ikmFromBearer(TEST_BEARER);
        return EnvelopeCrypto.sealJson(
                ikm,
                Direction.BE_TO_AGENT,
                ENV_ID,
                CorrelateMessageCommandHandler.AAD_API_KEY_VERIFY,
                Map.of("refId", refId, "keyId", keyId, "presentedSecret", secret),
                mapper);
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

    private DaemonCommand command(String id, String payload) {
        return new DaemonCommand(id, CorrelateMessageCommandHandler.TYPE, "2026-07-13T00:00:00Z", payload);
    }
}
