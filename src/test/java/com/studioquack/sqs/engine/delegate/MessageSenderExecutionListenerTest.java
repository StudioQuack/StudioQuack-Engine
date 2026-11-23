package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.engine.WorkflowGraph;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Intra-engine sender bridge — proves that a {@code message.intermediate.throw.event} or {@code
 * message.end.event} fired from one process definition delivers to a waiting {@code
 * message.intermediate.catch.event} / {@code message.start.event} on the SAME embedded engine,
 * sharing the same {@link com.studioquack.sqs.command.MessageCorrelationService} seam that external
 * webhook ingress uses. Covers the "C" collaboration scenario.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-sender-listener-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class MessageSenderExecutionListenerTest {

    private static final String MESSAGE_NAME = "orderApproved";

    @Autowired
    private WorkflowDeploymentService deploymentService;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ExecutionContextStore executionContextStore;

    // -------------------------------------------------------------------------------------------
    // Scenario C: sender (workflow A) throws → receiver (workflow B) parked on a catch is delivered.
    // -------------------------------------------------------------------------------------------

    @Test
    void throw_event_delivers_to_waiting_catch_in_a_different_process_definition() {
        String sharedBusinessKey = UUID.randomUUID().toString();

        // Receiver: independent workflow definition parked on message.intermediate.catch.event.
        ProcessInstance receiver = deployAndStartCatchFlow(
                "receiverThrow-" + UUID.randomUUID(), sharedBusinessKey);
        Execution parked = sqsEngine.findMessageEventExecution(sharedBusinessKey, MESSAGE_NAME);
        assertNotNull(parked, "receiver must park on the message catch subscription before the sender fires");

        // Sender: DIFFERENT workflow definition. Its throw event fires on token arrival, the
        // listener resolves messageName + (default) correlationKey = sender's own businessKey.
        deployAndStartThrowFlow(
                "senderThrow-" + UUID.randomUUID(), sharedBusinessKey, /*correlationKeyOverride*/ null);

        // Receiver's catch subscription is consumed (the intra-engine bridge delivered the message).
        assertNull(sqsEngine.findMessageEventExecution(sharedBusinessKey, MESSAGE_NAME),
                "receiver's catch was correlated by the sender's throw event");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()),
                "receiver advanced past its catch (to the user task), it did not end");
    }

    // -------------------------------------------------------------------------------------------
    // Scenario: message.end.event as sender — same bridge (EndEvent extends ThrowEvent).
    // -------------------------------------------------------------------------------------------

    @Test
    void message_end_event_delivers_to_waiting_catch_on_the_way_out() {
        String sharedBusinessKey = UUID.randomUUID().toString();

        ProcessInstance receiver = deployAndStartCatchFlow(
                "receiverEnd-" + UUID.randomUUID(), sharedBusinessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(sharedBusinessKey, MESSAGE_NAME));

        // Sender workflow: start → message.end.event  (no wait states; runs to completion synchronously).
        deployAndStartMessageEndFlow(
                "senderEnd-" + UUID.randomUUID(), sharedBusinessKey);

        assertNull(sqsEngine.findMessageEventExecution(sharedBusinessKey, MESSAGE_NAME),
                "message-end-event delivered the message to the parked catch before completing");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()));
    }

    // -------------------------------------------------------------------------------------------
    // Scenario: per-node correlationKey override targets a business key different from the
    //           sender's own instance businessKey (BPMN "correlation on a domain id" pattern).
    // -------------------------------------------------------------------------------------------

    @Test
    void correlationKey_override_targets_a_different_receiver_businessKey() {
        String receiverBusinessKey = "ord-" + UUID.randomUUID();
        String senderBusinessKey = "sender-" + UUID.randomUUID(); // deliberately different

        ProcessInstance receiver = deployAndStartCatchFlow(
                "receiverOverride-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME));

        // Sender uses a per-node correlationKey override pointing at the receiver's businessKey.
        deployAndStartThrowFlow(
                "senderOverride-" + UUID.randomUUID(), senderBusinessKey,
                /*correlationKeyOverride*/ receiverBusinessKey);

        assertNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "override correlationKey targeted the receiver, not the sender's own businessKey");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()));
    }

    // -------------------------------------------------------------------------------------------
    // Scenario (reported bug): receiver authors a SHARED correlationKey on the catch; the sender
    // authors the SAME key on the throw/end. Neither run's businessKey equals the shared key —
    // delivery must still correlate via the receiver's parked-time correlation pin.
    // -------------------------------------------------------------------------------------------

    @Test
    void throw_event_delivers_to_catch_via_shared_authored_correlation_key() {
        String sharedKey = "shared-" + UUID.randomUUID();
        String receiverBusinessKey = UUID.randomUUID().toString(); // ≠ sharedKey
        String senderBusinessKey = UUID.randomUUID().toString();   // ≠ sharedKey, ≠ receiver's

        ProcessInstance receiver = deployAndStartCatchFlowWithKey(
                "receiverSharedKey-" + UUID.randomUUID(), receiverBusinessKey, sharedKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "receiver parked under its own businessKey");

        deployAndStartThrowFlow("senderSharedKey-" + UUID.randomUUID(), senderBusinessKey, sharedKey);

        assertNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "receiver's catch was correlated via the shared authored correlationKey");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()),
                "receiver advanced past its catch, it did not end");
    }

    @Test
    void message_end_event_delivers_to_catch_via_shared_authored_correlation_key() {
        String sharedKey = "shared-" + UUID.randomUUID();
        String receiverBusinessKey = UUID.randomUUID().toString();

        ProcessInstance receiver = deployAndStartCatchFlowWithKey(
                "receiverEndSharedKey-" + UUID.randomUUID(), receiverBusinessKey, sharedKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME));

        deployAndStartMessageEndFlow("senderEndSharedKey-" + UUID.randomUUID(),
                UUID.randomUUID().toString(), sharedKey);

        assertNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "message-end-event correlated the parked catch via the shared authored key");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()));
    }

    // -------------------------------------------------------------------------------------------
    // Scenario: no waiting catch → benign no-op; sender's own instance still completes normally.
    // -------------------------------------------------------------------------------------------

    @Test
    void no_waiting_catch_is_a_benign_no_op_sender_still_completes() {
        String senderBusinessKey = UUID.randomUUID().toString();
        // No receiver deployed. Sender runs anyway; listener logs no-match and the sender's flow
        // MUST NOT be rolled back by the missing correlation target.
        ProcessInstance sender = deployAndStartThrowFlow(
                "senderNoMatch-" + UUID.randomUUID(), senderBusinessKey, null);

        assertNotNull(sender, "sender starts and reaches its own park state (user task) regardless of receiver");
        assertTrue(sqsEngine.isProcessInstanceActive(sender.getId()));
    }

    // -------------------------------------------------------------------------------------------
    // Scenario: targetKind=MESSAGE_START launches a NEW receiver instance (message.start.event).
    // -------------------------------------------------------------------------------------------

    @Test
    void targetKind_MESSAGE_START_launches_a_new_receiver_instance() {
        String startMessageName = "receiverBoot-" + UUID.randomUUID();
        // Deploy a receiver definition whose start event is message.start.event(startMessageName).
        deployMessageStartReceiverFlow("receiverStart-" + UUID.randomUUID(), startMessageName);

        long beforeCount = sqsEngine.processInstances().active().count();

        // Sender: message.intermediate.throw.event(startMessageName), targetKind=MESSAGE_START.
        deployAndStartThrowFlowFull(
                "senderStart-" + UUID.randomUUID(), UUID.randomUUID().toString(),
                startMessageName, /*correlationKey*/ null, "MESSAGE_START");

        long afterCount = sqsEngine.processInstances().active().count();
        // Sender + newly-started receiver → the active count grew by at least 2 (both park on
        // their user tasks). Under a naive "no-op sender" it would grow by only 1.
        assertTrue(afterCount >= beforeCount + 2,
                "sender's throw with targetKind=MESSAGE_START should have launched a new receiver instance"
                        + " (before=" + beforeCount + " after=" + afterCount + ")");
    }

    // -------------------------------------------------------------------------------------------
    // Scenario: the throw's AUTHORED correlationKey rides the START fan-out leg as the addressed
    // key — a message-start node with a static correlationKey filter fires when both sides
    // authored the same key, stays silent on a mismatch, and the sender's businessKey fallback
    // must never be forwarded (an unkeyed throw cannot trip a filtered start).
    // -------------------------------------------------------------------------------------------

    @Test
    void throw_with_matching_authored_key_launches_filtered_message_start() {
        String startMessageName = "filteredBoot-" + UUID.randomUUID();
        String sharedKey = "key-" + UUID.randomUUID();
        String receiverWorkflowId = "receiverFiltered-" + UUID.randomUUID();
        deployMessageStartReceiverFlowWithKey(receiverWorkflowId, startMessageName, sharedKey);

        deployAndStartThrowFlowFull("senderKeyedHit-" + UUID.randomUUID(), UUID.randomUUID().toString(),
                startMessageName, sharedKey, "MESSAGE_START");

        ProcessInstance started = sqsEngine.processInstances()
                .processDefinitionKey("Process_" + receiverWorkflowId).singleResult();
        assertNotNull(started, "filtered start fired: both sides authored the same key");
        assertTrue(started.getBusinessKey().startsWith("wh_"),
                "the started run's businessKey is a minted unique id, never the correlation key");
        assertEquals(sharedKey, sqsEngine.getProcessVariable(started.getId(),
                        com.studioquack.sqs.engine.runtime.EngineVariableNames.START_CORRELATION_KEY),
                "the matched key rides as the started run's correlation key");
    }

    @Test
    void throw_with_mismatched_authored_key_leaves_filtered_start_unfired() {
        String startMessageName = "filteredMiss-" + UUID.randomUUID();
        String receiverWorkflowId = "receiverFilteredMiss-" + UUID.randomUUID();
        deployMessageStartReceiverFlowWithKey(receiverWorkflowId, startMessageName,
                "key-A-" + UUID.randomUUID());

        ProcessInstance sender = deployAndStartThrowFlowFull(
                "senderKeyedMiss-" + UUID.randomUUID(), UUID.randomUUID().toString(),
                startMessageName, "key-B-" + UUID.randomUUID(), "MESSAGE_START");

        assertEquals(0, sqsEngine.processInstances()
                        .processDefinitionKey("Process_" + receiverWorkflowId).count(),
                "addressed key ≠ the start's filter — a benign no-match, no instance");
        assertTrue(sqsEngine.isProcessInstanceActive(sender.getId()),
                "a filtered-out start never rolls the sender back (BPMN throw semantics)");
    }

    @Test
    void throw_without_authored_key_leaves_filtered_start_unfired() {
        String startMessageName = "filteredNoKey-" + UUID.randomUUID();
        String receiverWorkflowId = "receiverFilteredNoKey-" + UUID.randomUUID();
        deployMessageStartReceiverFlowWithKey(receiverWorkflowId, startMessageName,
                "key-" + UUID.randomUUID());

        ProcessInstance sender = deployAndStartThrowFlowFull(
                "senderNoKey-" + UUID.randomUUID(), UUID.randomUUID().toString(),
                startMessageName, /*correlationKey*/ null, "MESSAGE_START");

        assertEquals(0, sqsEngine.processInstances()
                        .processDefinitionKey("Process_" + receiverWorkflowId).count(),
                "the sender's businessKey fallback must not leak as the addressed key");
        assertTrue(sqsEngine.isProcessInstanceActive(sender.getId()));
    }

    @Test
    void ducktape_override_key_launches_filtered_message_start() {
        String startMessageName = "filteredDt-" + UUID.randomUUID();
        String sharedKey = "key-dt-" + UUID.randomUUID();
        String receiverWorkflowId = "receiverFilteredDt-" + UUID.randomUUID();
        deployMessageStartReceiverFlowWithKey(receiverWorkflowId, startMessageName, sharedKey);

        String senderBusinessKey = "senderDtStart-" + UUID.randomUUID();
        // The panel authors a WRONG name and NO key; the mapped message payload wins both lanes.
        seedMessageInputOverride(senderBusinessKey, "throwN",
                Map.of("name", startMessageName, "correlationKey", sharedKey));
        deployAndStartThrowFlowFull("senderDtStart-" + UUID.randomUUID(), senderBusinessKey,
                "wrong.name." + UUID.randomUUID(), null, "MESSAGE_START");

        assertEquals(1, sqsEngine.processInstances()
                        .processDefinitionKey("Process_" + receiverWorkflowId).count(),
                "the DuckTape-mapped correlationKey addresses the filtered start");
    }

    // Throw events persist emit_result rows for downstream nodes. End events do not.

    @Test
    void throw_event_writes_emit_result_success_and_timestamp_targeting_downstream() {
        String sharedBusinessKey = UUID.randomUUID().toString();

        // Receiver parked on catch, so the throw's correlation succeeds.
        deployAndStartCatchFlow("receiverEmit-" + UUID.randomUUID(), sharedBusinessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(sharedBusinessKey, MESSAGE_NAME));

        // Sender parks on user task after the throw; we can inspect its emit_result rows.
        deployAndStartThrowFlow("senderEmit-" + UUID.randomUUID(), sharedBusinessKey, null);

        // The throw's outgoing sequence flow targets "taskN" — the user task. Every row the
        // sender wrote should target that node id (designer id, "node_" prefix stripped).
        ProcessContextRef ref = new ProcessContextRef();
        ref.setExecutionId(sharedBusinessKey);
        List<InOutValueDto> taskInputs = executionContextStore.loadInputsTargetingNode(ref, "taskN");
        assertNotNull(taskInputs);
        InOutValueDto successRow = taskInputs.stream()
                .filter(r -> "throwN".equals(r.getNodeId()) && "success".equals(r.getTargetPortId()))
                .findFirst().orElse(null);
        InOutValueDto timestampRow = taskInputs.stream()
                .filter(r -> "throwN".equals(r.getNodeId()) && "timestamp".equals(r.getTargetPortId()))
                .findFirst().orElse(null);
        assertNotNull(successRow, "throw must publish OUTPUT.emit_result.success targeting the downstream node");
        assertNotNull(timestampRow, "throw must publish OUTPUT.emit_result.timestamp targeting the downstream node");
        assertEquals(Boolean.TRUE, successRow.getValue(),
                "correlation matched a waiting catch → success=true");
        assertNotNull(timestampRow.getValue(), "timestamp value must be populated");
        // Belt-and-suspenders: an ISO-8601-ish shape (T + Z tail) is expected from OffsetDateTime.
        assertTrue(timestampRow.getValue().toString().contains("T"),
                "timestamp should be an ISO-8601 instant; got=" + timestampRow.getValue());
    }

    @Test
    void throw_event_emit_result_success_is_false_when_no_receiver_is_waiting() {
        // No receiver deployed → the throw's correlateWebhook returns not-correlated.
        deployAndStartThrowFlow("senderEmitMiss-" + UUID.randomUUID(),
                UUID.randomUUID().toString(), null);

        // No specific businessKey to look up here — a fresh sender is parked, its own downstream
        // must still carry a success=false row. Snapshot query by the sender's own executionId.
    }

    @Test
    void message_end_event_does_not_write_emit_result_rows_because_it_has_no_downstream() {
        String sharedBusinessKey = UUID.randomUUID().toString();

        deployAndStartCatchFlow("receiverEndEmit-" + UUID.randomUUID(), sharedBusinessKey);

        deployAndStartMessageEndFlow("senderEndEmit-" + UUID.randomUUID(), sharedBusinessKey);

        // The end event's own designer id ("endMsgN") targets nobody. Any node-id lookup should
        // yield no rows where the throw's success/timestamp appears (because none were written).
        ProcessContextRef ref = new ProcessContextRef();
        ref.setExecutionId(sharedBusinessKey);
        List<InOutValueDto> inputsToStart = executionContextStore.loadInputsTargetingNode(ref, "startN");
        boolean anyEmitRow = inputsToStart.stream()
                .anyMatch(r -> "endMsgN".equals(r.getNodeId())
                        && ("success".equals(r.getTargetPortId()) || "timestamp".equals(r.getTargetPortId())));
        assertFalse(anyEmitRow, "message.end.event has no OUTPUT port; no emit_result rows should be persisted");
    }

    // DeliveryMode (audit): sync runs the retry policy; fire_and_forget is exactly one attempt;
    // legacy "async" never existed at runtime and is echoed truthfully as sync. All three deliver
    // to NO receiver, so every attempt is a benign no-match.

    @Test
    void fire_and_forget_collapses_the_retry_policy_to_a_single_attempt() {
        String senderBusinessKey = UUID.randomUUID().toString();
        deployAndStartThrowFlowWithDelivery("senderFnF-" + UUID.randomUUID(), senderBusinessKey,
                "fire_and_forget", "{\"maxAttempts\":5,\"backoffMs\":1}");

        assertEmitResultDelivery(senderBusinessKey, 1, "fire_and_forget");
    }

    @Test
    void sync_mode_honours_the_retry_policy_attempt_count() {
        String senderBusinessKey = UUID.randomUUID().toString();
        deployAndStartThrowFlowWithDelivery("senderSync-" + UUID.randomUUID(), senderBusinessKey,
                "sync", "{\"maxAttempts\":3,\"backoffMs\":1,\"backoffMultiplier\":1,\"maxBackoffMs\":1}");

        assertEmitResultDelivery(senderBusinessKey, 3, "sync");
    }

    @Test
    void a_legacy_async_delivery_mode_is_normalised_and_echoed_as_sync() {
        String senderBusinessKey = UUID.randomUUID().toString();
        deployAndStartThrowFlowWithDelivery("senderAsync-" + UUID.randomUUID(), senderBusinessKey,
                "async", "{\"maxAttempts\":2,\"backoffMs\":1}");

        assertEmitResultDelivery(senderBusinessKey, 2, "sync");
    }

    // -------------------------------------------------------------------------------------------
    // DuckTape lane: a mapped row on the sender's INPUT `message` port (pipeline wire shape:
    // targetPortValue="message", value = mapped payload object) overrides BOTH the panel name
    // and the panel correlationKey.
    // -------------------------------------------------------------------------------------------

    @Test
    void ducktape_message_input_overrides_panel_name_and_correlation_key() {
        String receiverBusinessKey = "ord-" + UUID.randomUUID();
        String senderBusinessKey = "sender-" + UUID.randomUUID();

        ProcessInstance receiver = deployAndStartCatchFlow(
                "receiverDt-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME));

        // The panel authors a WRONG name and a WRONG key; the mapped message payload must win both.
        seedMessageInputOverride(senderBusinessKey, "throwN",
                Map.of("name", MESSAGE_NAME, "correlationKey", receiverBusinessKey));
        deployAndStartThrowFlowFull(
                "senderDt-" + UUID.randomUUID(), senderBusinessKey,
                "wrong.name." + UUID.randomUUID(),
                "PANEL-" + UUID.randomUUID(), /*targetKind*/ null);

        assertNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "the DuckTape-mapped name + correlationKey beat the panel values and delivered");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()),
                "receiver advanced past its catch");
    }

    // -------------------------------------------------------------------------------------------
    // Scenario (reported bug, st19 manual test): the receiver parks its catch on a PARALLEL
    // BRANCH — the pin variable rides a CHILD execution, not the process-instance root. The
    // authored-key correlation must still find it (the pin is execution-local by design).
    // -------------------------------------------------------------------------------------------

    @Test
    void throw_event_delivers_to_catch_parked_on_a_parallel_child_execution() {
        String sharedKey = "par-" + UUID.randomUUID();
        String receiverBusinessKey = UUID.randomUUID().toString();
        String senderBusinessKey = UUID.randomUUID().toString();

        ProcessInstance receiver = deployAndStartParallelCatchFlow(
                "receiverPar-" + UUID.randomUUID(), receiverBusinessKey, sharedKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "receiver parked on the catch (businessKey path sees child executions)");

        deployAndStartThrowFlow("senderPar-" + UUID.randomUUID(), senderBusinessKey, sharedKey);

        assertNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "catch on a parallel branch must correlate via the authored shared key");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()),
                "receiver advanced past its catch, it did not end");
    }

    // -------------------------------------------------------------------------------------------
    // Scenario (reported bug, st19 manual test): TWO receivers parked on the SAME authored
    // (messageName, correlationKey) — an ambiguous correlation target. The sender must survive:
    // a delivery mishap never rolls back the sender's own transaction (BPMN throw semantics).
    // -------------------------------------------------------------------------------------------

    @Test
    void ambiguous_shared_key_is_a_benign_no_match_and_the_sender_survives() {
        String sharedKey = "dup-" + UUID.randomUUID();

        ProcessInstance receiverA = deployAndStartCatchFlowWithKey(
                "receiverDupA-" + UUID.randomUUID(), UUID.randomUUID().toString(), sharedKey);
        ProcessInstance receiverB = deployAndStartCatchFlowWithKey(
                "receiverDupB-" + UUID.randomUUID(), UUID.randomUUID().toString(), sharedKey);

        ProcessInstance sender = deployAndStartThrowFlow(
                "senderDup-" + UUID.randomUUID(), UUID.randomUUID().toString(), sharedKey);

        assertTrue(sqsEngine.isProcessInstanceActive(sender.getId()),
                "an ambiguous correlation target must not roll back the sender's transaction");
        assertTrue(sqsEngine.isProcessInstanceActive(receiverA.getId()),
                "ambiguous receiver A stays parked (no arbitrary delivery)");
        assertTrue(sqsEngine.isProcessInstanceActive(receiverB.getId()),
                "ambiguous receiver B stays parked (no arbitrary delivery)");
    }

    // -------------------------------------------------------------------------------------------
    // payloadRef / envelopeMeta (audit C6): a mapped payloadRef forwards the referenced
    // pre-sealed envelope VERBATIM (the literal payload field is ignored); an unresolvable
    // ref fails the send loudly instead of publishing an empty message.
    // -------------------------------------------------------------------------------------------

    @Test
    void payloadRef_forwards_the_pre_sealed_envelope_and_ignores_the_literal_payload() {
        String receiverBusinessKey = "ord-" + UUID.randomUUID();
        String senderBusinessKey = "sender-" + UUID.randomUUID();

        ProcessInstance receiver = deployAndStartCatchFlow(
                "receiverRef-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME));

        // A previously sealed envelope, as a catch's OUT payloadRef would cite it: producer node
        // "webhook_src_*" carrying {orderId, amount} under the SENDER's key.
        ExecutionContext src = new ExecutionContext();
        src.setWorkflowExecutionId(senderBusinessKey);
        src.setCurrentNodeId("webhook_src_" + UUID.randomUUID());
        ProcessContextRef srcRef = executionContextStore.snapshot(src, null);
        List<InOutValueDto> sealed = new ArrayList<>();
        for (String field : List.of("orderId", "amount")) {
            InOutValueDto row = new InOutValueDto();
            row.setNodeId(src.getCurrentNodeId());
            row.setPortId(field);
            row.setPortValue(field);
            row.setValue("orderId".equals(field) ? "O-1" : "42");
            sealed.add(row);
        }
        executionContextStore.saveOutputs(srcRef, sealed);

        // DuckTape maps payloadRef (+ envelopeMeta + a literal payload that must be IGNORED).
        seedMessageInputOverride(senderBusinessKey, "throwN", Map.of(
                "name", MESSAGE_NAME,
                "correlationKey", receiverBusinessKey,
                "payloadRef", srcRef.getCurrentNodeRefId(),
                "envelopeMeta", Map.of("envelopeId", srcRef.getCurrentNodeRefId(), "kid", "k-1"),
                "payload", Map.of("ignored", "yes")));
        deployAndStartThrowFlowFull(
                "senderRef-" + UUID.randomUUID(), senderBusinessKey,
                "wrong.name." + UUID.randomUUID(), "PANEL-" + UUID.randomUUID(), /*targetKind*/ null);

        assertNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "the forwarded envelope delivered to the parked catch");
        assertTrue(sqsEngine.isProcessInstanceActive(receiver.getId()));

        // The receiver's seal contains the forwarded fields, not the literal payload.
        ProcessContextRef receiverRef = new ProcessContextRef();
        receiverRef.setExecutionId(receiverBusinessKey);
        List<InOutValueDto> delivered = executionContextStore.loadInputsTargetingNode(receiverRef, "catchN");
        assertTrue(delivered.stream()
                        .anyMatch(r -> "orderId".equals(r.getPortId()) && "O-1".equals(r.getValue())),
                "forwarded envelope field orderId reached the receiver");

        // Provenance is honest on the bridge: the sender is a LOCAL flow, not external ingress.
        Map<?, ?> envelope = executionContextStore.loadInputsTargetingNode(receiverRef, "taskN").stream()
                .filter(r -> "catchN".equals(r.getNodeId()) && "message".equals(r.getPortId()))
                .map(r -> (Map<?, ?>) r.getValue())
                .findFirst().orElse(null);
        assertNotNull(envelope, "the catch envelope fanned out to the downstream node");
        assertEquals("bridge", envelope.get("source"));
        assertEquals("sqs", envelope.get("senderTier"),
                "the sender's tier is this daemon's own (test tier=base), not 'external'");
        assertTrue(delivered.stream()
                        .anyMatch(r -> "amount".equals(r.getPortId()) && "42".equals(r.getValue())),
                "forwarded envelope field amount reached the receiver");
        assertFalse(delivered.stream().anyMatch(r -> "ignored".equals(r.getPortId())),
                "the literal payload is ignored when payloadRef is set");
    }

    @Test
    void unresolvable_payloadRef_fails_the_send_instead_of_delivering_an_empty_message() {
        String receiverBusinessKey = "ord-" + UUID.randomUUID();
        String senderBusinessKey = "sender-" + UUID.randomUUID();

        deployAndStartCatchFlow("receiverMiss-" + UUID.randomUUID(), receiverBusinessKey);
        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME));

        seedMessageInputOverride(senderBusinessKey, "throwN", Map.of(
                "name", MESSAGE_NAME,
                "correlationKey", receiverBusinessKey,
                "payloadRef", "env_missing_" + UUID.randomUUID()));
        deployAndStartThrowFlowFull(
                "senderMiss-" + UUID.randomUUID(), senderBusinessKey, MESSAGE_NAME, null, null);

        assertNotNull(sqsEngine.findMessageEventExecution(receiverBusinessKey, MESSAGE_NAME),
                "an unresolvable payloadRef must NOT deliver — the catch stays parked");
        ProcessContextRef ref = new ProcessContextRef();
        ref.setExecutionId(senderBusinessKey);
        List<InOutValueDto> rows = executionContextStore.loadInputsTargetingNode(ref, "taskN");
        InOutValueDto successRow = rows.stream()
                .filter(r -> "throwN".equals(r.getNodeId()) && "success".equals(r.getTargetPortId()))
                .findFirst().orElse(null);
        assertNotNull(successRow, "emit_result.success must be persisted for the failed forward");
        assertEquals(Boolean.FALSE, successRow.getValue(),
                "the failed forward reports success=false");
    }

    /** Pre-seeds the store with the pipeline's wire shape for a DuckTape mapping into the sender. */
    private void seedMessageInputOverride(String businessKey, String senderNodeId,
                                          Map<String, Object> payload) {        // saveOutputs persists through the ref's currentNodeRefId, so the seed needs a real
        // snapshot entity — the same pseudo-node pattern MessageCorrelationService.sealMsg uses.
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setCurrentNodeId("upstreamN" + UUID.randomUUID());
        ProcessContextRef ref = executionContextStore.snapshot(ctx, null);
        InOutValueDto row = new InOutValueDto();
        row.setNodeId(ctx.getCurrentNodeId());
        row.setPortId("serviceOutput");
        row.setPortValue("serviceOutput");
        row.setTargetNodeId(senderNodeId);
        // The pipeline writes the target port's template UUID as targetPortId; the listener
        // matches on targetPortValue ("message") + the payload's name/correlationKey fields.
        row.setTargetPortId("9d2b9a10-0000-0000-0000-000000000000");
        row.setTargetPortValue("message");
        row.setValue(payload);
        executionContextStore.saveOutputs(ref, List.of(row));
    }

    // ---- fixtures --------------------------------------------------------------------------

    /**
     * Deploys and starts {@code start → message.intermediate.catch.event(orderApproved)
     * → user.task → end}; the instance parks on the catch subscription.
     */
    private ProcessInstance deployAndStartCatchFlow(String workflowId, String businessKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = messageEventNode("catchN", "message.intermediate.catch.event", MESSAGE_NAME);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test Receiver", "1",
                List.of(start, catchN, task, end),
                List.of(edge("startN", "catchN"), edge("catchN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "receiver started");
        return instance;
    }

    /**
     * As {@link #deployAndStartCatchFlow} but the catch node also authors a shared
     * {@code correlationKey} — pinned at park time so a sender can correlate without knowing
     * this run's businessKey.
     */
    private ProcessInstance deployAndStartCatchFlowWithKey(String workflowId, String businessKey,
                                                           String correlationKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = messageEventNode("catchN", "message.intermediate.catch.event", MESSAGE_NAME);
        addOptionalParam(catchN, "correlationKey", correlationKey);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test Receiver (authored key)", "1",
                List.of(start, catchN, task, end),
                List.of(edge("startN", "catchN"), edge("catchN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "receiver started (authored key)");
        return instance;
    }

    /**
     * The catch sits on a parallel branch (auto-inserted fork/join): its parked execution is a
     * CHILD of the process-instance root — the topology that hid the authored correlation-key
     * pin from the alias query (execution-local variable, root-scoped query).
     */
    private ProcessInstance deployAndStartParallelCatchFlow(String workflowId, String businessKey,
                                                            String correlationKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto catchN = messageEventNode("catchN", "message.intermediate.catch.event", MESSAGE_NAME);
        addOptionalParam(catchN, "correlationKey", correlationKey);
        NodeDto taskA = node("taskA", "user.task", null);
        NodeDto taskB = node("taskB", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test Receiver (parallel)", "1",
                List.of(start, catchN, taskA, taskB, end),
                List.of(edge("startN", "catchN"), edge("catchN", "taskA"), edge("taskA", "endN"),
                        edge("startN", "taskB"), edge("taskB", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, businessKey, null, true);
        assertNotNull(instance, "receiver started (parallel branch)");
        return instance;
    }

    /** Deploys and starts a sender flow with a {@code message.intermediate.throw.event}. */
    private ProcessInstance deployAndStartThrowFlow(String workflowId, String senderBusinessKey,
                                                    String correlationKeyOverride) {
        return deployAndStartThrowFlowFull(workflowId, senderBusinessKey, MESSAGE_NAME,
                correlationKeyOverride, /*targetKind*/ null);
    }

    /** As above with full control over messageName and targetKind. */
    private ProcessInstance deployAndStartThrowFlowFull(String workflowId, String senderBusinessKey,
                                                        String messageName, String correlationKeyOverride,
                                                        String targetKind) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto throwN = messageEventNode("throwN", "message.intermediate.throw.event", messageName);
        addOptionalParam(throwN, "correlationKey", correlationKeyOverride);
        addOptionalParam(throwN, "targetKind", targetKind);
        NodeDto task = node("taskN", "user.task", null); // park after the throw so sender stays alive
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test Throw", "1",
                List.of(start, throwN, task, end),
                List.of(edge("startN", "throwN"), edge("throwN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, senderBusinessKey, null, true);
        assertNotNull(instance, "sender started");
        return instance;
    }

    /** Deploys and starts a sender flow with a {@code message.end.event} (no wait state after). */
    private void deployAndStartMessageEndFlow(String workflowId, String senderBusinessKey) {
        deployAndStartMessageEndFlow(workflowId, senderBusinessKey, null);
    }

    /** As above, optionally authoring a {@code correlationKey} on the end event. */
    private void deployAndStartMessageEndFlow(String workflowId, String senderBusinessKey,
                                              String correlationKey) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto endMsg = messageEventNode("endMsgN", "message.end.event", MESSAGE_NAME);
        addOptionalParam(endMsg, "correlationKey", correlationKey);
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test MessageEnd", "1",
                List.of(start, endMsg),
                List.of(edge("startN", "endMsgN")));
        deploymentService.deployAndStart(graph, null, senderBusinessKey, null, true);
        // instance may have completed synchronously; that is expected — the listener fires "start"
        // event on the end event before completion.
    }

    /** Deploys (never starts) {@code message.start.event(name) → userTask → end}. */
    private void deployMessageStartReceiverFlow(String workflowId, String messageName) {
        NodeDto start = messageEventNode("startN", "message.start.event", messageName);
        // message.start.event doesn't take extra sender params, but the helper is generic; the
        // start-event strategy simply ignores unknown params.
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test Message-Start Receiver", "1",
                List.of(start, task, end),
                List.of(edge("startN", "taskN"), edge("taskN", "endN")));
        deploymentService.deploy(graph, null, true);
    }

    /**
     * As {@link #deployMessageStartReceiverFlow} but the start node authors a static
     * {@code correlationKey} filter — the start fires only when the inbound delivery's addressed
     * key equals it.
     */
    private void deployMessageStartReceiverFlowWithKey(String workflowId, String messageName,
                                                       String correlationKey) {
        NodeDto start = messageEventNode("startN", "message.start.event", messageName);
        addOptionalParam(start, "correlationKey", correlationKey);
        NodeDto task = node("taskN", "user.task", null);
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test Filtered Start Receiver", "1",
                List.of(start, task, end),
                List.of(edge("startN", "taskN"), edge("taskN", "endN")));
        deploymentService.deploy(graph, null, true);
    }

    /** As {@link #deployAndStartThrowFlow} but authoring {@code deliveryMode} / {@code retryPolicy}. */
    private ProcessInstance deployAndStartThrowFlowWithDelivery(String workflowId, String senderBusinessKey,
                                                                String deliveryMode, String retryPolicy) {
        NodeDto start = node("startN", "start.event", "start");
        NodeDto throwN = messageEventNode("throwN", "message.intermediate.throw.event", MESSAGE_NAME);
        addOptionalParam(throwN, "deliveryMode", deliveryMode);
        addOptionalParam(throwN, "retryPolicy", retryPolicy);
        NodeDto task = node("taskN", "user.task", null); // park after the throw so sender stays alive
        NodeDto end = node("endN", "end.event", "end");
        WorkflowGraph graph = new WorkflowGraph(workflowId, "Sender Test DeliveryMode", "1",
                List.of(start, throwN, task, end),
                List.of(edge("startN", "throwN"), edge("throwN", "taskN"), edge("taskN", "endN")));
        ProcessInstance instance = deploymentService.deployAndStart(graph, null, senderBusinessKey, null, true);
        assertNotNull(instance, "sender started");
        return instance;
    }

    /** Asserts the {@code emit_result.attempts} / {@code emit_result.deliveryMode} rows on taskN. */
    private void assertEmitResultDelivery(String senderBusinessKey, int expectedAttempts,
                                          String expectedMode) {
        ProcessContextRef ref = new ProcessContextRef();
        ref.setExecutionId(senderBusinessKey);
        List<InOutValueDto> rows = executionContextStore.loadInputsTargetingNode(ref, "taskN");
        assertNotNull(rows);
        InOutValueDto attemptsRow = rows.stream()
                .filter(r -> "throwN".equals(r.getNodeId()) && "attempts".equals(r.getTargetPortId()))
                .findFirst().orElse(null);
        InOutValueDto modeRow = rows.stream()
                .filter(r -> "throwN".equals(r.getNodeId()) && "deliveryMode".equals(r.getTargetPortId()))
                .findFirst().orElse(null);
        assertNotNull(attemptsRow, "emit_result.attempts must be persisted for the downstream node");
        assertNotNull(modeRow, "emit_result.deliveryMode must be persisted for the downstream node");
        assertEquals(expectedAttempts, ((Number) attemptsRow.getValue()).intValue(),
                "delivery attempts actually made");
        assertEquals(expectedMode, modeRow.getValue(),
                "emit_result.deliveryMode echoes the truthful effective mode");
    }

    private static void addOptionalParam(NodeDto node, String shortCode, String value) {
        if (value == null) {
            return;
        }
        ParameterDto param = new ParameterDto();
        param.setShortCode(shortCode);
        param.setValue(value);
        List<ParameterDto> existing = node.getData().getConfigurationParameters();
        List<ParameterDto> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        merged.add(param);
        node.getData().setConfigurationParameters(merged);
    }

    private NodeDto messageEventNode(String id, String bpmnType, String messageName) {
        NodeDto node = node(id, bpmnType, null);
        ParameterDto nameParam = new ParameterDto();
        nameParam.setShortCode("name");
        nameParam.setValue(messageName);
        node.getData().setConfigurationParameters(new ArrayList<>(List.of(nameParam)));
        return node;
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




