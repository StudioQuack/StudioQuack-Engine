package com.studioquack.sqs.command;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiMessageReceiverConfigurator;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.GatewayRouteResolver;
import com.studioquack.sqs.webhook.WebhookMessageNames;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.ExtensionAttribute;
import org.activiti.bpmn.model.ExtensionElement;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The single, shared realization of client-side message-event correlation onto this env's embedded Activiti
 * engine. Both reverse-channel ingress handlers call this ONE service so the correlation logic (and its
 * REF-ONLY safety) is defined exactly once: {@link CorrelateMessageCommandHandler} (classic
 * {@code correlate-message}) and {@link IngestWebhookCommandHandler} (parameter-driven
 * {@code ingest-webhook-key-event}).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageCorrelationService {

    /**
     * Prefix for the synthetic pseudo-node id a sealed webhook {@code msg} is recorded under.
     */
    private static final String WEBHOOK_SOURCE_PREFIX = "webhook_";

    /**
     * OUTPUT port value the message catch/start templates declare for the caught envelope.
     */
    private static final String MESSAGE_OUT_PORT = "message";
    /**
     * OUTPUT port value the signal catch/start templates declare for the caught envelope.
     */
    private static final String SIGNAL_OUT_PORT = "signal_payload";

    private final SqsEngine sqsEngine;
    private final ExecutionContextStore executionContextStore;
    private final DeployedArtifactStore artifactStore;
    private final ActivitiBoundaryEventHelper boundaryEventHelper;
    private final ExecutionContextCrypto executionContextCrypto;
    private final GatewayRouteResolver routeResolver;
    private final ObjectMapper objectMapper;
    private final com.studioquack.sqs.usertask.UserTaskIo userTaskIo;
    private final com.studioquack.sqs.engine.listener.BpmnNodeLogPayloads bpmnLogPayloads;
    private final org.springframework.beans.factory.ObjectProvider<com.studioquack.sqs.status.SandboxLogCallback>
            sandboxLogCallback;

    /**
     * Null in slice tests that construct this service by hand.
     */
    private com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setExecutionHaltGuard(com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard) {
        this.executionHaltGuard = executionHaltGuard;
    }

    /**
     * Every start path (BE relay, mesh, intra-engine) refuses a new instance while the engine is halted.
     */
    private void refuseNewRunWhenHalted() {
        if (executionHaltGuard != null) {
            executionHaltGuard.requireActive();
        }
    }

    /**
     * This daemon's own environment id, stamped onto the seeded start context (sandbox jobs read it).
     */
    @Value("${sqs.channel.envId:}")
    private String ownEnvId;

    /**
     * Correlates {@code messageName} into the run identified by {@code correlationKey} (=
     * {@code workflowExecutionId}/businessKey), delivering the REF-only {@code variables}.
     *
     * @param messageName the BPMN message name the instance is waiting on (required, non-blank)
     * @param correlationKey the run's businessKey / workflowExecutionId — OR the receiver-authored shared
     * correlation key (fallback lookup when no run carries that businessKey) — (required, non-blank)
     * @param variables REF-only routing/reference tokens to deliver (may be {@code null}/empty)
     * @return {@link MessageCorrelationOutcome#correlated} with the resolved execution id when a parked instance
     * matched, otherwise {@link MessageCorrelationOutcome#notCorrelated} (a benign no-match)
     * @throws IllegalArgumentException when {@code messageName} or {@code correlationKey} is blank
     * @throws IllegalStateException when {@code variables} carries a non-whitelisted (PHI) key
     */
    public MessageCorrelationOutcome correlate(String messageName, String correlationKey,
                                               Map<String, Object> variables) {
        if (StringUtils.isBlank(messageName)) {
            throw new IllegalArgumentException("messageName is required");
        }
        if (StringUtils.isBlank(correlationKey)) {
            throw new IllegalArgumentException("businessKey or correlationKey is required");
        }

        // REF-ONLY (Mapping Invariant #15): reject a PHI-bearing payload BEFORE it can reach
        // ACT_*_VARIABLE. Throws IllegalStateException naming the offending key(s).
        RefOnlyVariableGuard.assertRefOnly(variables);

        Execution parked = sqsEngine.findMessageEventExecution(correlationKey, messageName);
        if (parked == null) {


            // Address the catch. businessKey addressing always wins; this runs only on its miss.
            parked = sqsEngine.findMessageEventExecutionByCorrelationKey(messageName, correlationKey);
            if (parked != null) {
                log.info("message-correlation.alias-hit messageName={} correlationKey={}", messageName, correlationKey);
            }
        }
        if (parked == null) {
            // Third fallback: the key may be the START correlation key a message-started run was
            // launched with (its businessKey is a minted id the sender never learns).
            parked = sqsEngine.findMessageEventExecutionByStartKey(messageName, correlationKey);
            if (parked != null) {
                log.info("message-correlation.start-key-hit messageName={} correlationKey={}", messageName, correlationKey);
            }
        }
        if (parked == null) {
            // Nothing waiting on this (businessKey, messageName): a benign no-match, not a failure.
            log.info("message-correlation.no-match messageName={} correlationKey={}", messageName, correlationKey);
            return MessageCorrelationOutcome.notCorrelated(messageName);
        }

        String executionId = parked.getId();


        // The token past the catch event.
        if (variables != null && !variables.isEmpty()) {
            sqsEngine.setProcessVariables(parked.getProcessInstanceId(), variables);
        }
        sqsEngine.messageEventReceived(messageName, executionId, variables);
        log.info("message-correlation.done messageName={} correlationKey={} executionId={}",
                messageName, correlationKey, executionId);
        return MessageCorrelationOutcome.correlated(executionId, messageName);
    }


    // Every inbound BPMN event kind is dispatched here so start, timer, intermediate, signal,
    // and message events share the same REF-only rules. A user-task decision enters only
    // through decideUserTask.


    /**
     * The BPMN event kinds an external caller can deliver to a running/started workflow.
     */
    public enum MessageEventKind {
        /**
         * Intermediate message catch (the original behaviour).
         */
        MESSAGE,
        /**
         * Signal catch (scoped by run when a correlation key is given, else broadcast).
         */
        SIGNAL,
        /**
         * Message start event — launches a NEW instance.
         */
        START,
        /**
         * Timer intermediate catch advanced on demand (engine trigger).
         */
        TIMER,
        /**
         * A user-task approval decision. Accepted ONLY from the control plane's user-task decision service over the
         * reverse channel ({@link #decideUserTask}) — the backend holds the first-answer-wins record and retires every
         * recipient's notification, so a decision that reached the engine any other way would bypass both.
         */
        TASK;

        /**
         * Lenient-but-strict parse: {@code null}/blank ⇒ {@link #MESSAGE} (keeps the pre- wire contract where no
         * {@code eventKind} meant an intermediate message); an unknown NON-blank value is a client error (mapped to
         * 400 / a failed command by the callers).
         */
        public static MessageEventKind from(String raw) {
            if (StringUtils.isBlank(raw)) {
                return MESSAGE;
            }
            try {
                return MessageEventKind.valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException unknown) {
                throw new IllegalArgumentException("unsupported eventKind: " + raw
                        + " (expected one of MESSAGE, SIGNAL, START, TIMER, TASK)");
            }
        }
    }

    /**
     * Single entry point that dispatches an inbound event to the right engine seam by {@code eventKind}.
     * {@code name} is the message name (MESSAGE/START) or the signal name (SIGNAL); {@code correlationKey} is the
     * run's businessKey; and {@code activityId} names the timer catch node (TIMER).
     */
    public MessageCorrelationOutcome correlateEvent(String eventKind, String name, String correlationKey,
                                                    String activityId, Map<String, Object> variables) {
        MessageEventKind kind = MessageEventKind.from(eventKind);
        switch (kind) {
            case MESSAGE:
                return correlate(name, correlationKey, variables);
            case SIGNAL:
                return correlateSignal(name, correlationKey, variables);
            case START:
                return startByMessage(name, correlationKey, variables);
            case TIMER:
                return triggerTimer(correlationKey, activityId, variables);
            case TASK:
                throw new IllegalArgumentException("eventKind TASK is answered through sq-backend's "
                        + "user-task decision endpoint, which needs the workflow, node, instance and decision");
            default:
                throw new IllegalArgumentException("unsupported eventKind: " + eventKind);
        }
    }

    /**
     * Signal delivery. With a {@code correlationKey} the signal is scoped to the single execution parked on that
     * signal within the run (deterministic, like a message); without one it is broadcast to every subscription in
     * this env's engine (BPMN signal semantics — no single execution id to report).
     */
    private MessageCorrelationOutcome correlateSignal(String signalName, String correlationKey,
                                                      Map<String, Object> variables) {
        if (StringUtils.isBlank(signalName)) {
            throw new IllegalArgumentException("signalName is required");
        }
        RefOnlyVariableGuard.assertRefOnly(variables);

        if (StringUtils.isNotBlank(correlationKey)) {
            Execution parked = sqsEngine.findSignalEventExecution(correlationKey, signalName);
            if (parked == null) {
                log.info("signal-correlation.no-match signalName={} correlationKey={}", signalName, correlationKey);
                return MessageCorrelationOutcome.notCorrelated(signalName);
            }
            if (variables != null && !variables.isEmpty()) {
                sqsEngine.setProcessVariables(parked.getProcessInstanceId(), variables);
            }
            sqsEngine.signalEventReceived(signalName, parked.getId(), variables);
            log.info("signal-correlation.done signalName={} correlationKey={} executionId={}",
                    signalName, correlationKey, parked.getId());
            return MessageCorrelationOutcome.correlated(parked.getId(), signalName);
        }

        // No key: broadcast. Delivered to the engine's signal bus; no execution id.
        sqsEngine.signalEventReceived(signalName, variables);
        log.info("signal-correlation.broadcast signalName={}", signalName);
        return MessageCorrelationOutcome.correlated(null, signalName);
    }

    /**
     * Message-start: launch a NEW instance keyed to the message name. Run id vs correlation key.
     */
    private MessageCorrelationOutcome startByMessage(String messageName, String correlationKey,
                                                     Map<String, Object> variables) {
        refuseNewRunWhenHalted();
        if (StringUtils.isBlank(messageName)) {
            throw new IllegalArgumentException("messageName is required");
        }
        RefOnlyVariableGuard.assertRefOnly(variables);
        // Same seed as the webhook leg: without a graph-hydrated studioquackRef the run's first
        // dispatchable node parks forever ("no dispatchable context").
        String key = "wh_" + NanoIdUtils.randomNanoId();
        Map<String, Object> vars = variables == null || variables.isEmpty()
                ? new HashMap<>() : new HashMap<>(variables);
        if (StringUtils.isNotBlank(correlationKey)) {
            vars.put(EngineVariableNames.START_CORRELATION_KEY, correlationKey.trim());
        }
        ProcessContextRef seedRef = seedStartContext(messageName, key,
                sqsEngine.findMessageStartNodeId(messageName));
        if (seedRef != null) {
            vars.put(EngineVariableNames.STUDIOQUACK_REF, seedRef);
        }
        RefOnlyVariableGuard.assertRefOnly(vars);
        String processInstanceId = sqsEngine.startProcessInstanceByMessage(messageName, key, vars);
        log.info("message-start.done messageName={} businessKey={} correlationKey={} processInstanceId={}",
                messageName, key, correlationKey, processInstanceId);
        return MessageCorrelationOutcome.correlated(processInstanceId, messageName);
    }

    /**
     * Timer: advance the token parked at {@code activityId} within the run via an engine trigger (the "timer ⇒
     * engine trigger" mapping). Resolves the parked execution directly, falling back to the
     * unfinished-historic-activity lookup for the single-token wait gotcha; a no-match is benign.
     */
    private MessageCorrelationOutcome triggerTimer(String correlationKey, String activityId,
                                                   Map<String, Object> variables) {
        if (StringUtils.isBlank(correlationKey)) {
            throw new IllegalArgumentException("businessKey or correlationKey is required");
        }
        if (StringUtils.isBlank(activityId)) {
            throw new IllegalArgumentException("activityId is required for a timer event");
        }
        RefOnlyVariableGuard.assertRefOnly(variables);

        String processInstanceId = sqsEngine.resolveProcessInstanceId(correlationKey);
        Execution parked = sqsEngine.findWaitingExecution(correlationKey, activityId);
        String executionId = parked != null ? parked.getId()
                : sqsEngine.findUnfinishedWaitExecution(processInstanceId, activityId);
        if (executionId == null && !activityId.startsWith(SqsEngine.NODE_ELEMENT_ID_PREFIX)) {
            // Bare designer node id: the strategies mint element ids as node_<id>, so accept
            // either form. The as-given lookup above ran first, so a non-node element id
            // (e.g. a btimer_… boundary) is never rewritten.
            String prefixed = SqsEngine.NODE_ELEMENT_ID_PREFIX + activityId;
            parked = sqsEngine.findWaitingExecution(correlationKey, prefixed);
            executionId = parked != null ? parked.getId()
                    : sqsEngine.findUnfinishedWaitExecution(processInstanceId, prefixed);
        }
        if (executionId == null) {
            log.info("timer-trigger.no-match correlationKey={} activityId={}", correlationKey, activityId);
            return MessageCorrelationOutcome.notCorrelated(activityId);
        }
        if (variables != null && !variables.isEmpty()) {
            sqsEngine.setProcessVariables(processInstanceId, variables);
        }
        sqsEngine.trigger(executionId, variables);
        log.info("timer-trigger.done correlationKey={} activityId={} executionId={}",
                correlationKey, activityId, executionId);
        return MessageCorrelationOutcome.correlated(executionId, activityId);
    }

    /**
     * OUTPUT port value of the user task; its {@code decision} field carries the answer.
     */
    public static final String TASK_OUTPUT_PORT = com.studioquack.sqs.usertask.UserTaskIo.TASK_OUTPUT_PORT;
    /**
     * Field of {@link #TASK_OUTPUT_PORT} holding {@code approved} / {@code rejected}.
     */
    public static final String DECISION_FIELD = com.studioquack.sqs.usertask.UserTaskIo.DECISION_FIELD;
    public static final String DECISION_APPROVED = "approved";
    public static final String DECISION_REJECTED = "rejected";

    /**
     * One approval decision as the control plane addressed it: the exact parked task (its id is unique per visit),
     * plus the node, workflow and instance it must belong to.
     *
     * @param taskId the Activiti task id the backend recorded when the approval opened
     * @param nodeId the canvas node id (bare or {@code node_<id>})
     * @param workflowId the deployed workflow id
     * @param instanceId the run's businessKey, or the process instance id of a keyless child
     * @param decision {@code approved} or {@code rejected} (case-insensitive)
     */
    public record UserTaskDecision(String taskId, String nodeId, String workflowId, String instanceId,
                                   String decision) {
    }

    /**
     * Completes a parked {@code user.task} with the recipient's decision. Exact addressing.
     *
     * @throws IllegalArgumentException a blank address field or a decision other than approved/rejected
     */
    public MessageCorrelationOutcome decideUserTask(UserTaskDecision req) {
        if (req == null || StringUtils.isAnyBlank(req.taskId(), req.nodeId(), req.workflowId(), req.instanceId())) {
            throw new IllegalArgumentException("a user-task decision needs taskId, nodeId, workflowId and instanceId");
        }
        String decision = normalizeDecision(req.decision());
        String nodeId = StringUtils.removeStart(req.nodeId().trim(), SqsEngine.NODE_ELEMENT_ID_PREFIX);

        Task task = sqsEngine.findActiveTask(req.taskId().trim());
        if (task == null || !addressMatches(task, nodeId, req.workflowId().trim(), req.instanceId().trim())) {
            log.info("task-decision.no-match taskId={} nodeId={} workflowId={}", req.taskId(), nodeId, req.workflowId());
            return MessageCorrelationOutcome.notCorrelated(nodeId);
        }
        com.studioquack.sqs.usertask.UserTaskIo.Inbound inbound = userTaskIo.resolveInbound(
                sqsEngine.getProcessVariable(task.getProcessInstanceId(), EngineVariableNames.STUDIOQUACK_REF),
                nodeId).orElse(null);
        List<InOutValueDto> outputs = List.of();
        if (inbound != null) {
            outputs = userTaskIo.publishDecision(inbound, decision);
        } else {
            log.warn("task-decision.no-context taskId={} nodeId={} — completing without outputs", task.getId(), nodeId);
        }
        publishGatewayRouteTokens(task, req.instanceId().trim(), outputs);
        reportDecisionLog(task, nodeId, inbound, decision);
        sqsEngine.completeUserTask(task.getId());
        log.info("task-decision.done taskId={} nodeId={} decision={} executionId={}",
                task.getId(), nodeId, decision, task.getExecutionId());
        return MessageCorrelationOutcome.correlated(task.getExecutionId(), nodeId);
    }

    /**
     * {@code approved} / {@code rejected}, case-insensitive; anything else is a client error.
     */
    static String normalizeDecision(String raw) {
        String value = StringUtils.trimToEmpty(raw).toLowerCase(Locale.ROOT);
        if (!DECISION_APPROVED.equals(value) && !DECISION_REJECTED.equals(value)) {
            throw new IllegalArgumentException("decision must be \"approved\" or \"rejected\"");
        }
        return value;
    }

    /**
     * The task sits on this node, of this workflow, in this instance (businessKey or instance id).
     */
    private boolean addressMatches(Task task, String nodeId, String workflowId, String instanceId) {
        String taskNode = StringUtils.removeStart(task.getTaskDefinitionKey(), SqsEngine.NODE_ELEMENT_ID_PREFIX);
        if (!nodeId.equals(taskNode)) {
            return false;
        }
        ProcessDefinition def = sqsEngine.findProcessDefinition(task.getProcessDefinitionId());
        if (def == null || !workflowId.equals(WorkflowGraph.workflowIdOf(def.getKey()))) {
            return false;
        }
        return instanceId.equals(task.getProcessInstanceId())
                || instanceId.equals(sqsEngine.findBusinessKey(task.getProcessInstanceId()));
    }

    /**
     * Publishes the execution-local {@code route__<gatewayId>} tokens a downstream exclusive gateway's generated
     * JUEL resolves, computed from the run's deployed graph and the task's just-sealed outputs ( item 5). Each
     * token is written with {@link SqsEngine#setRouteToken} on the task's own execution — the same execution the
     * token advances on through the gateway — so the routing variable is already in scope when the gateway
     * evaluates.
     */
    private void publishGatewayRouteTokens(Task task, String correlationKey, List<InOutValueDto> outputs) {
        ProcessContextRef ref = coerceRef(sqsEngine.getProcessVariable(
                task.getProcessInstanceId(), EngineVariableNames.STUDIOQUACK_REF));
        ExecutionContext ctx = ref == null ? null : executionContextStore.loadByRef(ref).orElse(null);
        if (ctx == null || ctx.getNodes() == null) {
            log.warn("task-completion.route-tokens skipped task={} — run context ref not loadable",
                    task.getId());
            return;
        }
        if (StringUtils.isBlank(ctx.getWorkflowExecutionId())) {
            ctx.setWorkflowExecutionId(StringUtils.defaultIfBlank(
                    sqsEngine.findBusinessKey(task.getProcessInstanceId()), correlationKey));
        }
        String nodeId = StringUtils.removeStart(task.getTaskDefinitionKey(), SqsEngine.NODE_ELEMENT_ID_PREFIX);
        Map<String, Object> tokens = routeResolver.resolveRouteTokens(
                ctx.getNodes(), ctx.getEdges(), nodeId, outputs, ctx);
        for (Map.Entry<String, Object> token : tokens.entrySet()) {
            sqsEngine.setRouteToken(task.getExecutionId(), token.getKey(), token.getValue());
        }
        if (!tokens.isEmpty()) {
            log.info("task-completion.route-tokens task={} node={} gateways={}",
                    task.getId(), nodeId, tokens.keySet());
        }
    }

    /**
     * Coerces the engine-held {@code studioquackRef} variable — a {@link ProcessContextRef}, or its map form after
     * a JSON round-trip (the same two shapes the dispatch delegates' private {@code readRef} helpers accept).
     */
    private ProcessContextRef coerceRef(Object variable) {
        if (variable instanceof ProcessContextRef ref) {
            return ref;
        }
        if (variable instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, ProcessContextRef.class);
        }
        return null;
    }

    /**
     * The task's execution-log row — Task Input as resolved, Task Output as fanned out — through the same callback
     * every node's row takes. Best-effort: a log push never blocks the decision.
     */
    private void reportDecisionLog(Task task, String nodeId, com.studioquack.sqs.usertask.UserTaskIo.Inbound inbound,
                                   String decision) {
        com.studioquack.sqs.status.SandboxLogCallback callback = sandboxLogCallback.getIfAvailable();
        if (callback == null) {
            return;
        }
        try {
            NodeDto node = inbound == null ? null : inbound.node();
            String runId = inbound != null && StringUtils.isNotBlank(inbound.ctx().getWorkflowExecutionId())
                    ? inbound.ctx().getWorkflowExecutionId()
                    : sqsEngine.findBusinessKey(task.getProcessInstanceId());
            String nodeName = node != null && node.getData() != null && StringUtils.isNotBlank(node.getData().getLabel())
                    ? node.getData().getLabel() : task.getName();
            Long waitedMs = task.getCreateTime() == null ? null
                    : Math.max(0L, System.currentTimeMillis() - task.getCreateTime().getTime());
            callback.report(bpmnLogPayloads.buildUserTaskTerminal(runId, nodeId, nodeName, waitedMs,
                    node == null ? null : node.getInputList(), node == null ? null : node.getOutputList(), decision));
        } catch (RuntimeException ex) {
            log.warn("task-decision.log skipped taskId={} nodeId={}: {}", task.getId(), nodeId, ex.toString());
        }
    }

    /**
     * Correlation result (drives the FIXED shared wire contract
     * {@code {"correlated":<bool>,"executionId":"...?","messageName":"..."}} both callers emit).
     * {@code executionId} is {@code null} on a no-match.
     */
    public record MessageCorrelationOutcome(boolean correlated, String executionId, String messageName) {

        /**
         * A match: the message was delivered to {@code executionId}.
         */
        public static MessageCorrelationOutcome correlated(String executionId, String messageName) {
            return new MessageCorrelationOutcome(true, executionId, messageName);
        }

        /**
         * A benign no-match: nothing was parked for this (businessKey, messageName).
         */
        public static MessageCorrelationOutcome notCorrelated(String messageName) {
            return new MessageCorrelationOutcome(false, null, messageName);
        }
    }

    // =========================================================================================
    // CORE — webhook message-event addressing + correlation, payload sealed REF-only.

    // This is the ONE entry point the fixed external contract


    // Delivery paths. The caller has already turned the designer
    // {@code nodeId} into a plain engine address (a message or signal name plus a business key)
    // this method does the correlation AND the REF-only seal of {@code msg}, never both a plain
    // engine variable.
    // =========================================================================================

    /**
     * The fixed external wire vocabulary for {@code eventType} (CORE contract).
     */
    public enum SqEventType {
        MESSAGE_START, MESSAGE_CATCH, SIGNAL_START, SIGNAL_CATCH, BOUNDARY_MESSAGE, BOUNDARY_SIGNAL;

        /**
         * @throws IllegalArgumentException naming the value when {@code raw} is blank or unknown.
         */
        public static SqEventType from(String raw) {
            if (StringUtils.isBlank(raw)) {
                throw new IllegalArgumentException("eventType is required");
            }
            try {
                return SqEventType.valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException unknown) {
                throw new IllegalArgumentException("unsupported eventType: " + raw + " (expected one of "
                        + "MESSAGE_START, MESSAGE_CATCH, SIGNAL_START, SIGNAL_CATCH, BOUNDARY_MESSAGE, "
                        + "BOUNDARY_SIGNAL)");
            }
        }

        public boolean isStart() {
            return this == MESSAGE_START || this == SIGNAL_START;
        }
    }

    /**
     * A fully-resolved webhook address (the caller already mapped the DESIGNER {@code nodeId} to a plain engine
     * name — see the class javadoc).
     *
     * @param eventType the fixed wire event kind
     * @param workflowId this daemon's deployed workflow id (used ONLY to load the graph for a {@code BOUNDARY_*}
     * handler lookup; may be blank otherwise)
     * @param nodeId the DESIGNER node id addressed — the host node for {@code BOUNDARY_*}, the event node itself
     * otherwise
     * @param workflowInstanceId the businessKey; required for every kind except {@code *_START} (minted fresh when
     * absent there). For {@code MESSAGE_CATCH} / {@code BOUNDARY_MESSAGE} it may ALSO be the receiver-authored
     * shared correlation key — tried as a fallback when no run carries it as a businessKey.
     * @param name the resolved message/signal name (never the raw designer nodeId)
     * @param msg the RAW, arbitrary (assume-PHI) webhook payload — sealed, never a plain engine variable (Mapping
     * Invariant 15)
     * @param source who handed the event to this daemon: {@link #SOURCE_WEBHOOK} for external ingress (sender tier
     * unauthenticated) or {@link #SOURCE_BRIDGE} for an intra-engine throw→catch bridge delivery (the sender is
     * another flow on THIS daemon, so its tier is this daemon's own). Stamped verbatim onto the catch envelope.
     */
    public record WebhookEventRequest(SqEventType eventType, String workflowId, String nodeId,
                                      String workflowInstanceId, String name, Map<String, Object> msg,
                                      String source) {

        /**
         * External webhook ingress — the sender's tier cannot be authenticated.
         */
        public static final String SOURCE_WEBHOOK = "webhook";
        /**
         * Intra-engine bridge: a throw on this daemon delivered straight to a catch/start here.
         */
        public static final String SOURCE_BRIDGE = "bridge";

        /**
         * External ingress form — provenance defaults to {@link #SOURCE_WEBHOOK}.
         */
        public WebhookEventRequest(SqEventType eventType, String workflowId, String nodeId,
                                   String workflowInstanceId, String name, Map<String, Object> msg) {
            this(eventType, workflowId, nodeId, workflowInstanceId, name, msg, SOURCE_WEBHOOK);
        }
    }

    /**
     * {@code processInstanceId} is null on a no-match, on a broadcast SIGNAL_START, or on failure.
     */
    public record WebhookEventOutcome(boolean correlated, String processInstanceId, SqEventType eventType) {

        public static WebhookEventOutcome correlated(String processInstanceId, SqEventType type) {
            return new WebhookEventOutcome(true, processInstanceId, type);
        }

        public static WebhookEventOutcome notCorrelated(SqEventType type) {
            return new WebhookEventOutcome(false, null, type);
        }
    }

    /**
     * Dispatches a fully-resolved webhook event to the right engine seam, sealing {@code req.msg()} REF-only
     * before anything about it reaches the engine.
     *
     * @throws IllegalArgumentException a blank {@code name}, a missing {@code workflowInstanceId} on a non-start
     * kind, or an unsupported {@code eventType} — the caller maps this to 400.
     */
    public WebhookEventOutcome correlateWebhook(WebhookEventRequest req) {
        if (req == null || req.eventType() == null) {
            throw new IllegalArgumentException("eventType is required");
        }
        if (StringUtils.isBlank(req.name())) {
            throw new IllegalArgumentException("the resolved message/signal name is required");
        }
        return switch (req.eventType()) {
            case MESSAGE_START -> webhookStart(req, false);
            case SIGNAL_START -> webhookStart(req, true);
            case MESSAGE_CATCH, BOUNDARY_MESSAGE -> webhookCatch(req, false);
            case SIGNAL_CATCH, BOUNDARY_SIGNAL -> webhookCatch(req, true);
        };
    }

    /**
     * {@code MESSAGE_START} / {@code SIGNAL_START}: launches a NEW instance (or broadcasts).
     */
    private WebhookEventOutcome webhookStart(WebhookEventRequest req, boolean signal) {
        refuseNewRunWhenHalted();
        // Message kinds seal/fan out against the RECEIVER-side start node, resolved from the
        // subscription itself — req.nodeId() is the SENDER's node id on the intra-engine bridge
        // (and blank on external ingress), never the start node's (audit C6).
        StartEvent startElement = signal ? null : sqsEngine.findMessageStartEvent(req.name());
        String targetNodeId = signal ? resolveTargetNodeId(req)
                : startElement != null
                        ? StringUtils.removeStart(startElement.getId(), SqsEngine.NODE_ELEMENT_ID_PREFIX)
                        : resolveTargetNodeId(req);
        // C7: the start node's authored receiver config is LIVE on the message path
        // businessKeyExpression derives the run's CORRELATION key from the payload, and a static
        // correlationKey (consulted when no expression is set) filters the start on the inbound
        // addressed key.
        // Signal starts
        // keep the legacy name-only behaviour.
        String businessKey = "wh_" + NanoIdUtils.randomNanoId();
        String startCorrelationKey = null;
        if (!signal && startElement != null) {
            Map<String, String> receiverCfg = readReceiverConfig(startElement);
            String expression = receiverCfg.get(ActivitiMessageReceiverConfigurator.ATTR_BUSINESS_KEY_EXPRESSION);
            String staticKey = receiverCfg.get(ActivitiMessageReceiverConfigurator.ATTR_CORRELATION_KEY);
            if (StringUtils.isNotBlank(expression)) {
                startCorrelationKey = resolveBusinessKeyExpression(expression, req.msg());
                if (StringUtils.isBlank(startCorrelationKey)) {
                    log.warn("webhook.message-start.rejected name={} — businessKeyExpression evaluated blank "
                            + "(the authored key derivation must resolve, or the run is unaddressable)", req.name());
                    return WebhookEventOutcome.notCorrelated(req.eventType());
                }
            } else if (StringUtils.isNotBlank(staticKey)) {
                if (!staticKey.equals(req.workflowInstanceId())) {
                    log.info("webhook.message-start.key-mismatch name={} — the authored correlationKey "
                            + "did not match the inbound addressed key", req.name());
                    return WebhookEventOutcome.notCorrelated(req.eventType());
                }
                startCorrelationKey = staticKey;
            } else {
                startCorrelationKey = StringUtils.trimToNull(req.workflowInstanceId());
            }
        } else {
            startCorrelationKey = StringUtils.trimToNull(req.workflowInstanceId());
        }
        SealResult seal = sealMsg(req, businessKey, targetNodeId);
        // Mutable copy: the start leg below may add the seeded studioquackRef handle.
        Map<String, Object> refOnlyVars = new HashMap<>(seal.refOnlyVars());
        if (!signal && startCorrelationKey != null) {
            // Correlation DATA, not identity: later catches of THIS run stay addressable by the
            // key the caller/receiver authored (third fallback in correlate()/webhookCatch()).
            // Message-start only — a signal broadcast has no single run to key.
            refOnlyVars.put(EngineVariableNames.START_CORRELATION_KEY, startCorrelationKey);
        }
        if (signal) {
            // Activiti's SignalEventReceivedCmd reaches BOTH running signal subscriptions and
            // signal-start event definitions in one broadcast — the same seam correlateSignal()
            // already uses for a keyless signal — so no distinct "start by signal" seam exists and
            // none is needed. The engine reports no execution/instance id for a broadcast.
            sqsEngine.signalEventReceived(req.name(), refOnlyVars);
            log.info("webhook.signal-start.done name={} businessKey={}", req.name(), businessKey);
            return WebhookEventOutcome.correlated(null, req.eventType());
        }
        // Check-then-act: a no-subscription start would throw ActivitiObjectNotFoundException, and
        // on a NESTED command (the sender listener's fanout inside the sender's own command) the
        // shared Activiti command context remembers that exception and rolls the SENDER's
        // transaction back at close — even though the listener catches it. A benign no-match must
        // never poison the caller's transaction.
        if (!sqsEngine.hasMessageStartSubscription(req.name())) {
            log.info("webhook.message-start.no-subscription name={}", req.name());
            return WebhookEventOutcome.notCorrelated(req.eventType());
        }
        // A message-started run must carry the same graph-hydrated studioquackRef a start-run

        // The msg envelope keeps
        // citing its own pseudo-node ref as payloadRef — only the engine-pinned handle is seeded.
        ProcessContextRef seedRef = seedStartContext(req.name(), businessKey, targetNodeId);
        if (seedRef != null) {
            refOnlyVars.put(EngineVariableNames.STUDIOQUACK_REF, seedRef);
            RefOnlyVariableGuard.assertRefOnly(refOnlyVars);
        }
        String processInstanceId = sqsEngine.startProcessInstanceByMessage(req.name(), businessKey, refOnlyVars);
        fanOutStartEnvelope(req, businessKey, targetNodeId, processInstanceId, seal.ref());
        log.info("webhook.message-start.done name={} businessKey={} correlationKey={} processInstanceId={}",
                req.name(), businessKey, startCorrelationKey, processInstanceId);
        return WebhookEventOutcome.correlated(processInstanceId, req.eventType());
    }


    // C7 — message-start receiver config: businessKeyExpression / correlationKey


    /**
     * {@code ${msg}} / {@code ${msg.field}} / {@code ${msg.field.nested}} — the supported business-key expression
     * shape (dot-path into the inbound payload).
     */
    private static final Pattern MSG_KEY_EXPR =
            Pattern.compile("^\\$\\{\\s*msg((?:\\.[A-Za-z0-9_]+)*)\\s*}$");

    /**
     * Reads the start element's {@code sq:receiverConfig} extension attributes into a plain map (empty when the
     * strategy emitted none — the nothing-authored skip rule).
     */
    private static Map<String, String> readReceiverConfig(StartEvent startElement) {
        Map<String, String> cfg = new HashMap<>(2);
        List<ExtensionElement> elements = startElement.getExtensionElements()
                .get(ActivitiMessageReceiverConfigurator.RECEIVER_CONFIG_ELEMENT);
        if (elements == null) {
            return cfg;
        }
        for (ExtensionElement element : elements) {
            if (!ActivitiMessageReceiverConfigurator.NAMESPACE.equals(element.getNamespace())) {
                continue;
            }
            for (ExtensionAttribute attr : element.getAttributes().values().stream()
                    .flatMap(List::stream).collect(Collectors.toList())) {
                if (StringUtils.isNotBlank(attr.getValue())) {
                    cfg.put(attr.getName(), attr.getValue().trim());
                }
            }
        }
        return cfg;
    }

    /**
     * Evaluates the authored {@code businessKeyExpression} against the inbound payload. Supports the
     * {@code ${msg.<path>}} dot-path shape; a value without the expression wrapper is used as a literal key
     * (lenient read, same convention as the sender side's correlationKey).
     */
    private static String resolveBusinessKeyExpression(String expression, Map<String, Object> msg) {
        String expr = expression.trim();
        var matcher = MSG_KEY_EXPR.matcher(expr);
        if (!matcher.matches()) {
            return expr;
        }
        String path = matcher.group(1);
        if (path.isEmpty()) {
            return null; // ${msg} alone cannot key a run
        }
        Object current = msg;
        for (String segment : path.substring(1).split("\\.")) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(segment);
            if (current == null) {
                return null;
            }
        }
        return String.valueOf(current);
    }

    /**
     * {@code MESSAGE_CATCH} / {@code BOUNDARY_MESSAGE} / {@code SIGNAL_CATCH} / {@code BOUNDARY_SIGNAL}.
     */
    private WebhookEventOutcome webhookCatch(WebhookEventRequest req, boolean signal) {        if (StringUtils.isBlank(req.workflowInstanceId())) {
            throw new IllegalArgumentException("workflowInstanceId is required for " + req.eventType());
        }
        String businessKey = req.workflowInstanceId();
        // Runtime correlation is uniform for a plain catch and its boundary twin: Activiti parks a


        // BPMN-GENERATION detail (which activity hosts the subscription), not a runtime lookup one.
        Execution parked = signal
                ? sqsEngine.findSignalEventExecution(businessKey, req.name())
                : sqsEngine.findMessageEventExecution(businessKey, req.name());
        if (parked == null && !signal) {
            // Fallback (message kinds only): the supplied workflowInstanceId may be the
            // receiver-AUTHORED shared correlation key (pinned at park time), not the run's
            // businessKey — the only way an intra-engine sender in another flow can address this
            // businessKey addressing always wins; this runs only on its miss.
            parked = sqsEngine.findMessageEventExecutionByCorrelationKey(req.name(), businessKey);
            if (parked != null) {
                log.info("webhook.message-catch.alias-hit name={} correlationKey={}", req.name(), businessKey);
            }
        }
        if (parked == null && !signal) {


            parked = sqsEngine.findMessageEventExecutionByStartKey(req.name(), businessKey);
            if (parked != null) {
                log.info("webhook.message-catch.start-key-hit name={} correlationKey={}", req.name(), businessKey);
            }
        }
        if (parked == null) {
            log.info("webhook.{}.no-match name={} businessKey={}",
                    signal ? "signal-catch" : "message-catch", req.name(), businessKey);
            return WebhookEventOutcome.notCorrelated(req.eventType());
        }
        String targetNodeId;
        boolean boundaryKind = req.eventType() == SqEventType.BOUNDARY_MESSAGE
                || req.eventType() == SqEventType.BOUNDARY_SIGNAL;
        if (boundaryKind) {
            targetNodeId = resolveTargetNodeId(req);
        } else {
            // The addressed node is the PARKED one — derive the seal/fan-out target from its
            // activityId, never from req.nodeId(): the intra-engine sender bridge puts the
            // SENDER's node id there, and sealing/fanning out against a node that does not exist
            // in the receiver's definition aborts the correlation (surfaced by audit C6).
            targetNodeId = stripElementPrefix(parked.getActivityId());
            if (StringUtils.isBlank(targetNodeId)) {
                targetNodeId = req.nodeId();
            }
        }
        String executionId = parked.getId();
        String processInstanceId = parked.getProcessInstanceId();
        // Seal under the RECEIVER's real businessKey, not the caller-supplied key: on the
        // alias-correlation path the supplied workflowInstanceId is the authored correlation key,

        // On the businessKey path this
        // re-derivation is a no-op.
        String sealBusinessKey = StringUtils.defaultIfBlank(sqsEngine.findBusinessKey(processInstanceId),
                businessKey);
        SealResult seal = sealMsg(req, sealBusinessKey, targetNodeId);
        Map<String, Object> refOnlyVars = seal.refOnlyVars();
        // The template's promised catch envelope (messageName/correlationKey/payload/payloadRef/…)
        // fans out to the event node's downstream BEFORE the trigger, so a fan-out failure aborts
        // the correlation cleanly — nothing engine-side has happened yet and the caller can retry
        // instead of losing the envelope downstream (same contract as writeTaskOutputs).
        if ((req.eventType() == SqEventType.MESSAGE_CATCH || req.eventType() == SqEventType.SIGNAL_CATCH)
                && seal.ref() != null) {
            fanOutEventEnvelope(req, sealBusinessKey, targetNodeId,
                    processDefinitionIdOf(processInstanceId), seal.ref());
        } else if (req.eventType() == SqEventType.BOUNDARY_MESSAGE
                && WebhookMessageNames.isHookMessageName(req.name())
                && req.msg() != null && !req.msg().isEmpty()) {
            sealIngressBoundaryPayload(req, sealBusinessKey);
        }


        if (!refOnlyVars.isEmpty()) {
            sqsEngine.setProcessVariables(processInstanceId, refOnlyVars);
        }
        if (signal) {
            sqsEngine.signalEventReceived(req.name(), executionId, refOnlyVars);
        } else {
            sqsEngine.messageEventReceived(req.name(), executionId, refOnlyVars);
        }
        log.info("webhook.{}.done name={} businessKey={} executionId={} processInstanceId={}",
                signal ? "signal-catch" : "message-catch", req.name(), businessKey, executionId, processInstanceId);
        return WebhookEventOutcome.correlated(processInstanceId, req.eventType());
    }

    /**
     * The plain DESIGNER node id whose INPUT-schema ports {@code msg} should feed, vithe SAME
     * {@code (executionId, targetNodeId)} history scan {@link ExecutionContextStore#loadInputsTargetingNode}
     * already uses for every ordinary node-to-node edge (it matches purely on that pair — never on who "produced"
     * the row, so a synthetic webhook source is indistinguishable from a real one). For a bare
     * {@code MESSAGE_START}/{@code MESSAGE_CATCH}/{@code SIGNAL_START}/{@code SIGNAL_CATCH} event this is the
     * addressed node itself: whatever the designer wires FROM that event node reads {@code msg}'s fields as its
     * normal inputs, exactly like any other producer.
     */
    private String resolveTargetNodeId(WebhookEventRequest req) {
        boolean boundary = req.eventType() == SqEventType.BOUNDARY_MESSAGE
                || req.eventType() == SqEventType.BOUNDARY_SIGNAL;
        if (!boundary || StringUtils.isBlank(req.workflowId())) {
            return req.nodeId();
        }
        return artifactStore.loadDeployedGraph(req.workflowId())
                .flatMap(graph -> findHostNode(graph.nodes(), req.nodeId()))
                .map(hostNode -> boundaryHandlerNodeId(hostNode, req))
                .orElse(req.nodeId());
    }

    private Optional<NodeDto> findHostNode(List<NodeDto> nodes, String nodeId) {
        if (nodes == null || StringUtils.isBlank(nodeId)) {
            return Optional.empty();
        }
        return nodes.stream().filter(n -> n != null && nodeId.equals(n.getId())).findFirst();
    }

    /**
     * The boundary's configured handler node id, or the host node id when none matches.
     */
    private String boundaryHandlerNodeId(NodeDto hostNode, WebhookEventRequest req) {
        boolean signal = req.eventType() == SqEventType.BOUNDARY_SIGNAL;
        for (ActivitiBoundaryEventHelper.PlannedBoundary planned : boundaryEventHelper.planBoundaries(hostNode)) {
            BoundaryEventConfig cfg = planned.config();
            String cfgName = signal
                    ? cfg.getSignalName()
                    : ActivitiBoundaryEventHelper.resolveMessageName(req.workflowId(), "", hostNode.getId(), cfg);
            if (Objects.equals(req.name(), cfgName) && StringUtils.isNotBlank(cfg.getTargetNodeId())) {
                return cfg.getTargetNodeId();
            }
        }
        // No configured handler for this exact name — the token falls through to the shared error
        // end (nothing downstream reads ports there); seal to the host node id anyway so the ref
        // exists for observability even though nothing currently consumes it.
        return hostNode.getId();
    }

    /**
     * Seals an ingress-node {@code BOUNDARY_MESSAGE} payload onto the boundary's handler edge
     * ({@code nodeId = hostNodeId}, {@code portId/portValue = boundaryId}) so downstream DuckTape
     * mappings against the inherited ingress output schema resolve immediately.
     */
    private void sealIngressBoundaryPayload(WebhookEventRequest req, String businessKey) {
        if (StringUtils.isAnyBlank(req.workflowId(), req.nodeId(), businessKey)) {
            return;
        }
        artifactStore.loadDeployedGraph(req.workflowId()).ifPresent(graph -> {
            NodeDto hostNode = findHostNode(graph.nodes(), req.nodeId()).orElse(null);
            if (hostNode == null) {
                return;
            }
            BoundaryEventConfig matchedCfg = null;
            for (ActivitiBoundaryEventHelper.PlannedBoundary planned : boundaryEventHelper.planBoundaries(hostNode)) {
                BoundaryEventConfig cfg = planned.config();
                if (cfg == null || cfg.getType() != BoundaryEventConfig.BoundaryEventType.MESSAGE) {
                    continue;
                }
                String resolved = ActivitiBoundaryEventHelper.resolveMessageName(
                        req.workflowId(), "", hostNode.getId(), cfg);
                if (Objects.equals(req.name(), resolved)) {
                    matchedCfg = cfg;
                    break;
                }
            }
            if (matchedCfg == null || StringUtils.isBlank(matchedCfg.getId()) || graph.edges() == null) {
                return;
            }
            EdgeDto handlerEdge = null;
            for (EdgeDto edge : graph.edges()) {
                if (edge != null && edge.getData() != null
                        && hostNode.getId().equals(edge.getSource())
                        && matchedCfg.getId().equals(StringUtils.trimToNull(edge.getData().getBoundaryEventId()))
                        && StringUtils.isNotBlank(edge.getTarget())) {
                    handlerEdge = edge;
                    break;
                }
            }
            if (handlerEdge == null) {
                return;
            }
            ExecutionContext boundaryCtx = new ExecutionContext();
            boundaryCtx.setWorkflowExecutionId(businessKey);
            boundaryCtx.setWorkflowId(req.workflowId());
            boundaryCtx.setCurrentNodeId(matchedCfg.getId());

            InOutValueDto out = new InOutValueDto();
            out.setNodeId(hostNode.getId());
            out.setPortId(matchedCfg.getId());
            out.setPortValue(matchedCfg.getId());
            out.setTargetNodeId(handlerEdge.getTarget());
            String tgtPortValue = parseHandlePortValue(handlerEdge.getTargetHandle());
            if (StringUtils.isNotBlank(tgtPortValue)) {
                out.setTargetPortValue(tgtPortValue);
                out.setTargetPortId(resolveTargetPortId(graph.nodes(), handlerEdge.getTarget(), tgtPortValue));
            }
            // If payload wraps a single port value map (e.g. {"output": {...}}), unwrap it for the
            // boundary's inherited output schema; otherwise publish the payload map directly.
            Object value = unwrapSinglePortMapIfApplicable(req.msg());
            out.setValue(value);

            executionContextStore.saveOutputs(executionContextStore.snapshot(boundaryCtx, null), List.of(out));
            log.info("webhook.boundary-ingress.sealed workflowId={} host={} boundary={} handler={}",
                    req.workflowId(), hostNode.getId(), matchedCfg.getId(), handlerEdge.getTarget());
        });
    }

    private static Object unwrapSinglePortMapIfApplicable(Map<String, Object> msg) {
        if (msg == null) {
            return Map.of();
        }
        if (msg.size() == 1) {
            Object only = msg.values().iterator().next();
            if (only instanceof Map<?, ?>) {
                return only;
            }
        }
        return msg;
    }

    private static String parseHandlePortValue(String handle) {
        if (StringUtils.isBlank(handle)) {
            return null;
        }
        int dash = handle.indexOf('-');
        return dash >= 0 && dash < handle.length() - 1 ? handle.substring(dash + 1).trim() : null;
    }

    private static String resolveTargetPortId(List<NodeDto> nodes, String targetNodeId, String portValue) {
        if (nodes == null || StringUtils.isAnyBlank(targetNodeId, portValue)) {
            return portValue;
        }
        for (NodeDto node : nodes) {
            if (node != null && targetNodeId.equals(node.getId()) && node.getData() != null) {
                PortDto port = node.getData().findPortByPortName(portValue);
                if (port != null && StringUtils.isNotBlank(port.getId())) {
                    return port.getId();
                }
            }
        }
        return portValue;
    }

    private static String stripElementPrefix(String activityId) {
        if (activityId == null) {
            return null;
        }
        if (activityId.startsWith(SqsEngine.NODE_ELEMENT_ID_PREFIX)) {
            return activityId.substring(SqsEngine.NODE_ELEMENT_ID_PREFIX.length());
        }
        if (activityId.startsWith("catch_")) {
            return activityId.substring("catch_".length());
        }
        return activityId;
    }

    /**
     * Seeds the graph-hydrated {@code studioquackRef} for a message-started run — the same shape
     * {@code StartRunCommandHandler} pins for a start-run, so the run's first dispatchable node resolves its
     * context (and its node definition off the hydrated graph) instead of parking forever on a null ref. The graph
     * rides the deploy store (REF-only run: the graph ships at deploy, not on the trigger).
     */
    private ProcessContextRef seedStartContext(String messageName, String businessKey, String startNodeId) {
        ProcessDefinition def = sqsEngine.findMessageStartDefinition(messageName);
        if (def == null || StringUtils.isBlank(businessKey)) {
            return null;
        }
        String workflowId = WorkflowGraph.workflowIdOf(def.getKey());
        if (workflowId == null) {
            return null;
        }
        ExecutionContext seedCtx = new ExecutionContext();
        seedCtx.setWorkflowExecutionId(businessKey);
        seedCtx.setWorkflowId(workflowId);
        seedCtx.setCurrentNodeId(startNodeId);
        seedCtx.setEnvId(ownEnvId);
        String artifactKey = workflowId + WorkflowGraph.namespaceSuffixOf(def.getKey());
        artifactStore.loadDeployedGraph(artifactKey).ifPresentOrElse(graph -> {
            seedCtx.setNodes(graph.nodes());
            seedCtx.setEdges(graph.edges());
        }, () -> log.warn("webhook.message-start.seed-no-graph workflowId={} — the deploy graph row "
                + "is missing; the run's first dispatchable node will park", workflowId));
        ProcessContextRef ref = executionContextStore.snapshot(seedCtx, null);
        log.info("webhook.message-start.seeded workflowId={} businessKey={} nodes={}",
                workflowId, businessKey,
                seedCtx.getNodes() == null ? 0 : seedCtx.getNodes().size());
        return ref;
    }

    /**
     * Seals {@code req.msg()} into the execution-context store as a synthetic pseudo-node's OUTPUTS targeting
     * {@code targetNodeId} — the exact mechanism every ordinary node-to-node edge already uses (matches purely on
     * {@code (executionId, dto.targetNodeId)}). One {@link InOutValueDto} per top-level {@code msg} field, keyed
     * by field name as both the port id and the target port id (the designer's input-schema port names are
     * expected to match the webhook payload's field names, the same convention any node's own input schema uses).
     *
     * @return the {@code {studioquackRef: <ref>}} map to pin on the engine plus the snapshot ref itself (the
     * envelope fan-out cites it as {@code payloadRef}); an empty result when {@code msg} is null/empty (nothing to
     * seal, nothing to pin)
     */
    private SealResult sealMsg(WebhookEventRequest req, String businessKey, String targetNodeId) {
        if (req.msg() == null || req.msg().isEmpty()) {
            return SealResult.empty();
        }
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setWorkflowId(req.workflowId());
        ctx.setCurrentNodeId(WEBHOOK_SOURCE_PREFIX + NanoIdUtils.randomNanoId());
        ProcessContextRef ref = executionContextStore.snapshot(ctx, null);

        List<InOutValueDto> outputs = new ArrayList<>();
        for (Map.Entry<String, Object> field : req.msg().entrySet()) {
            InOutValueDto dto = new InOutValueDto();
            dto.setNodeId(ctx.getCurrentNodeId());
            dto.setPortId(field.getKey());
            dto.setPortValue(field.getKey());
            dto.setValue(field.getValue());
            dto.setTargetNodeId(targetNodeId);
            dto.setTargetPortId(field.getKey());
            outputs.add(dto);
        }
        executionContextStore.saveOutputs(ref, outputs);

        Map<String, Object> refOnlyVars = new HashMap<>(1);
        refOnlyVars.put(EngineVariableNames.STUDIOQUACK_REF, ref);
        RefOnlyVariableGuard.assertRefOnly(refOnlyVars);
        log.info("webhook.msg-sealed businessKey={} targetNodeId={} fields={} nodeRefId={}",
                businessKey, targetNodeId, outputs.size(), ref.getCurrentNodeRefId());
        return new SealResult(refOnlyVars, ref);
    }

    /**
     * Seals a broadcast signal's payload into ONE parked subscriber's execution-context store, under the
     * SUBSCRIBER's own businessKey and targeting its catch node — the same REF-only seal the webhook ingress
     * performs, but initiated by the intra-engine signal bridge ({@code SignalSenderExecutionListener}) before the
     * native broadcast wakes the subscriber. The returned ref is deliberately discarded: the subscriber already
     * carries its own {@code studioquackRef}, and the sealed rows are keyed by businessKey, readable through it.
     */
    public void sealBroadcastPayload(String businessKey, String targetNodeId, String subscriberProcessInstanceId,
                                     String signalName, Map<String, Object> msg) {
        if (msg == null || msg.isEmpty() || StringUtils.isBlank(businessKey)
                || StringUtils.isBlank(targetNodeId)) {
            return;
        }
        WebhookEventRequest req = new WebhookEventRequest(SqEventType.SIGNAL_CATCH, null, targetNodeId,
                businessKey, signalName, msg, WebhookEventRequest.SOURCE_BRIDGE);
        SealResult seal = sealMsg(req, businessKey, targetNodeId);
        try {
            fanOutEventEnvelope(req, businessKey, targetNodeId,
                    processDefinitionIdOf(subscriberProcessInstanceId), seal.ref());
        } catch (Exception ex) {
            log.warn("signal bridge envelope fan-out failed node={} signal={}: {}",
                    targetNodeId, signalName, ex.toString());
        }
    }


    // C1 — the catch/start envelope the FE templates promise on the OUT port
    // (`message` / `signal_payload`), fanned out to the event node's downstream.


    /**
     * The result of {@link #sealMsg}: the REF-only vars to pin plus the snapshot ref.
     */
    private record SealResult(Map<String, Object> refOnlyVars, ProcessContextRef ref) {
        static SealResult empty() {
            return new SealResult(Map.of(), null);
        }
    }

    /**
     * Message-start leg of the envelope fan-out. The instance already started when this runs, so a failure here
     * cannot abort anything — it is logged at ERROR (a lost envelope is a data-flow defect the operator must see)
     * rather than thrown (a throw would invite a retry that double-starts the instance).
     */
    private void fanOutStartEnvelope(WebhookEventRequest req, String businessKey, String eventNodeId,
                                     String processInstanceId, ProcessContextRef payloadRef) {
        if (payloadRef == null || StringUtils.isBlank(processInstanceId)) {
            return;
        }
        try {
            ProcessInstance instance = sqsEngine.processInstances()
                    .processInstanceId(processInstanceId).singleResult();
            if (instance == null) {
                log.error("webhook.message-start.envelope-lost node={} processInstanceId={}: "
                        + "the started instance is not queryable", eventNodeId, processInstanceId);
                return;
            }
            fanOutEventEnvelope(req, businessKey, eventNodeId, instance.getProcessDefinitionId(),
                    payloadRef);
        } catch (Exception ex) {
            log.error("webhook.message-start.envelope-lost node={} processInstanceId={}: {}",
                    eventNodeId, processInstanceId, ex.toString());
        }
    }

    /**
     * Writes the template-promised envelope as the event node's sealed OUTPUTS addressed to every downstream node
     * — the same fan-out row shape {@code writeTaskOutputs}/
     * {@code MessageSenderExecutionListener#writeEmitResult} use, so the next node's ordinary input scan reads
     * {@code message.messageName} / {@code signal_payload.payload} like any other producer's port value. The event
     * node's own outgoing flows are read off the DEPLOYED model — in the timeout shape the catch element's
     * outgoing flows are exactly the {@code message}-port edges (the timer branch hangs off the synthesized timer
     * catch), so no port filtering is needed.
     *
     * @throws IllegalStateException when the deployed model or the event element is unreadable — the catch-path
     * caller invokes this BEFORE triggering, so the throw aborts the correlation cleanly and the caller can retry
     * instead of losing the envelope
     */
    private void fanOutEventEnvelope(WebhookEventRequest req, String businessKey, String eventNodeId,
                                     String processDefinitionId, ProcessContextRef payloadRef) {
        if (payloadRef == null || StringUtils.isBlank(eventNodeId)
                || StringUtils.isBlank(processDefinitionId)) {
            return;
        }
        List<String> downstreamIds = downstreamNodeIds(processDefinitionId, eventNodeId);
        if (downstreamIds.isEmpty()) {
            return;
        }
        boolean signal = req.eventType() == SqEventType.SIGNAL_CATCH
                || req.eventType() == SqEventType.SIGNAL_START
                || req.eventType() == SqEventType.BOUNDARY_SIGNAL;
        String portValue = signal ? SIGNAL_OUT_PORT : MESSAGE_OUT_PORT;
        Map<String, Object> envelope = signal
                ? signalEnvelope(req)
                : messageEnvelope(req, businessKey, payloadRef);

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(businessKey);
        ctx.setWorkflowId(req.workflowId());
        ctx.setCurrentNodeId(eventNodeId);
        ProcessContextRef writeRef = executionContextStore.snapshot(ctx, null);

        List<InOutValueDto> outputs = new ArrayList<>(downstreamIds.size());
        boolean hookFanOutWritten = false;
        if (!signal && WebhookMessageNames.isHookMessageName(req.name())
                && StringUtils.isNotBlank(req.workflowId()) && req.msg() != null) {
            hookFanOutWritten = appendHookEdgeOutputs(req, eventNodeId, outputs);
        }
        if (!hookFanOutWritten) {
            for (String downstreamId : downstreamIds) {
                InOutValueDto dto = new InOutValueDto();
                dto.setNodeId(eventNodeId);
                dto.setPortId(portValue);
                dto.setPortValue(portValue);
                dto.setTargetNodeId(downstreamId);
                dto.setTargetPortId(portValue);
                dto.setTargetPortValue(portValue);
                dto.setValue(envelope);
                outputs.add(dto);
            }
        }
        executionContextStore.saveOutputs(writeRef, outputs);
        log.info("event.envelope-fanned node={} port={} downstream={} businessKey={}",
                eventNodeId, portValue, downstreamIds.size(), businessKey);
    }

    /**
     * Fans out a parameter-driven ingress node's emitted payload onto the node's actual outgoing
     * graph edges ({@code sourceHandle = OUTPUT-<srcPort>}, {@code targetHandle = INPUT-<tgtPort>})
     * so downstream DuckTape mappings and {@code getInputValuesByPortValue} receive the node's own
     * declared OUTPUT port values verbatim.
     */
    private boolean appendHookEdgeOutputs(WebhookEventRequest req, String eventNodeId,
                                          List<InOutValueDto> outputs) {
        Optional<DeployedArtifactStore.DeployedGraph> graphOpt =
                artifactStore.loadDeployedGraph(req.workflowId());
        if (graphOpt.isEmpty() || graphOpt.get().edges() == null) {
            return false;
        }
        DeployedArtifactStore.DeployedGraph graph = graphOpt.get();
        int added = 0;
        for (EdgeDto edge : graph.edges()) {
            if (edge == null || !eventNodeId.equals(edge.getSource()) || StringUtils.isBlank(edge.getTarget())) {
                continue;
            }
            if (edge.getData() != null && StringUtils.isNotBlank(edge.getData().getBoundaryEventId())) {
                continue;
            }
            String srcPort = StringUtils.defaultIfBlank(parseHandlePortValue(edge.getSourceHandle()), MESSAGE_OUT_PORT);
            String tgtPort = StringUtils.defaultIfBlank(parseHandlePortValue(edge.getTargetHandle()), srcPort);
            Object portVal = req.msg().containsKey(srcPort) ? req.msg().get(srcPort) : req.msg();

            InOutValueDto dto = new InOutValueDto();
            dto.setNodeId(eventNodeId);
            dto.setPortId(srcPort);
            dto.setPortValue(srcPort);
            dto.setTargetNodeId(edge.getTarget());
            dto.setTargetPortValue(tgtPort);
            dto.setTargetPortId(resolveTargetPortId(graph.nodes(), edge.getTarget(), tgtPort));
            dto.setValue(portVal);
            outputs.add(dto);
            added++;
        }
        return added > 0;
    }

    /**
     * The message catch/start OUT-port envelope, exactly the fields the FE templates declare: {@code messageName},
     * {@code correlationKey} (the key the sender addressed — the run's businessKey or the receiver-authored shared
     * alias), {@code payload} (verbatim), {@code payloadRef} + {@code envelopeMeta} (the sealed snapshot row the
     * payload rides on), {@code receivedAt}, {@code messageId}, plus provenance: {@code source} is the request's
     * own ({@code webhook} for external ingress, {@code bridge} for an intra-engine delivery); {@code senderTier}
     * is {@code external} for webhook ingress and {@code sqs} for a local bridge delivery.
     */
    private Map<String, Object> messageEnvelope(WebhookEventRequest req, String businessKey,
                                                ProcessContextRef payloadRef) {
        String now = OffsetDateTime.now(ZoneOffset.UTC).toString();
        boolean bridge = WebhookEventRequest.SOURCE_BRIDGE.equals(req.source());
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("messageName", req.name());
        envelope.put("correlationKey", StringUtils.defaultIfBlank(req.workflowInstanceId(), businessKey));
        envelope.put("payload", req.msg());
        envelope.put("payloadRef", payloadRef.getCurrentNodeRefId());


        String sealAlg = executionContextCrypto.sealAlgorithm();
        Map<String, Object> envelopeMeta = new LinkedHashMap<>();
        envelopeMeta.put("envelopeId", payloadRef.getCurrentNodeRefId());
        envelopeMeta.put("sealAlg", sealAlg == null ? "none" : sealAlg);
        envelopeMeta.put("sealedAt", now);
        envelope.put("envelopeMeta", envelopeMeta);
        envelope.put("receivedAt", now);
        envelope.put("messageId", "msg_" + NanoIdUtils.randomNanoId());
        envelope.put("source", bridge ? WebhookEventRequest.SOURCE_BRIDGE : WebhookEventRequest.SOURCE_WEBHOOK);
        envelope.put("senderTier", bridge ? "sqs" : "external");
        return envelope;
    }

    /**
     * The signal catch/start OUT-port envelope. F3 unified the name field on {@code signalName} (the catch
     * template used to spell it {@code name}); only {@code signalName} is written.
     */
    private Map<String, Object> signalEnvelope(WebhookEventRequest req) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("signalName", req.name());
        envelope.put("payload", req.msg());
        return envelope;
    }

    /**
     * The process definition id of a live instance. Activiti 9's plain {@link Execution} does not expose it, so it
     * is resolved through the instance query.
     */
    private String processDefinitionIdOf(String processInstanceId) {
        ProcessInstance instance = sqsEngine.processInstances()
                .processInstanceId(processInstanceId).singleResult();
        if (instance == null) {
            throw new IllegalStateException(
                    "event-envelope: process instance not queryable: " + processInstanceId);
        }
        return instance.getProcessDefinitionId();
    }

    /**
     * The designer node ids directly downstream of an event node, resolved off the deployed model.
     */
    private List<String> downstreamNodeIds(String processDefinitionId, String eventNodeId) {        BpmnModel model = sqsEngine.readDeployedModel(processDefinitionId);
        if (model == null) {
            throw new IllegalStateException(
                    "event-envelope: deployed model unreadable for " + processDefinitionId);
        }
        // For SEND_AND_WAIT compound nodes, the catch element is catch_<id> (node_<id> is the Phase-1
        // async service task whose outgoing flow goes to wait_<id>, not the downstream nodes).
        FlowElement fe = model.getFlowElement("catch_" + eventNodeId);
        if (fe == null) {
            fe = model.getFlowElement(SqsEngine.NODE_ELEMENT_ID_PREFIX + eventNodeId);
        }
        if (!(fe instanceof FlowNode flowNode)) {
            throw new IllegalStateException(
                    "event-envelope: event element node_" + eventNodeId + " missing in " + processDefinitionId);
        }
        if (flowNode.getOutgoingFlows() == null) {
            return List.of();
        }
        return flowNode.getOutgoingFlows().stream()
                .map(SequenceFlow::getTargetRef)
                .filter(StringUtils::isNotBlank)
                .map(target -> StringUtils.removeStart(target, SqsEngine.NODE_ELEMENT_ID_PREFIX))
                .distinct()
                .collect(Collectors.toList());
    }
}
