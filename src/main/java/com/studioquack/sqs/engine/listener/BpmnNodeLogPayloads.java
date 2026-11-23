package com.studioquack.sqs.engine.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.BranchEvaluation;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.GatewayDecision;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Per-node-type DuckTales I/O + echoes payload builder for engine-native BPMN nodes.
 * These nodes never run a sandbox, so {@code language} is always {@code "bpmn"} and
 * {@code containerId}/{@code exitCode} stay null. Heavy inline file bytes are stripped
 * (same 18 MiB cap as {@code ScriptDispatchDelegate}) and {@link Redactor} runs before
 * the record leaves this class.
 */
@Component
public class BpmnNodeLogPayloads {

    public static final String LANGUAGE = "bpmn";

    /** Cap copied from ScriptDispatchDelegate — do not call that class. */
    private static final int MAX_INLINE_DATA_CHARS = 18_000_000;

    /**
     * Authored catalog bpmnTypes (case-folded). The JSON catalog currently has 35 unique
     * types; membership is case-insensitive.
     */
    private static final Set<String> ENGINE_NATIVE_BPMN_TYPES = Set.of(
            "exclusive.gateway",
            "inclusive.gateway",
            "parallel.gateway",
            "event.based.gateway",
            "complex.gateway",
            "start.event",
            "timer.start.event",
            "message.start.event",
            "signal.start.event",
            "conditional.start.event",
            "multiple.start.event",
            "parallel.multiple.start.event",
            "end.event",
            "terminate.end.event",
            "error.end.event",
            "escalation.end.event",
            "compensate.end.event",
            "cancel.end.event",
            "message.end.event",
            "signal.end.event",
            "timer.intermediate.catch.event",
            "message.intermediate.catch.event",
            "signal.intermediate.catch.event",
            "conditional.intermediate.catch.event",
            "link.intermediate.catch.event",
            "multiple.intermediate.catch.event",
            "parallel.multiple.intermediate.catch.event",
            "none.intermediate.throw.event",
            "message.intermediate.throw.event",
            "signal.intermediate.throw.event",
            "escalation.intermediate.throw.event",
            "compensate.intermediate.throw.event",
            "link.intermediate.throw.event",
            "business.rule.task",
            "call.activity");

    private static final String[] TIMER_KEYS = {"timerCycle", "timerDate", "timeDuration", "value", "type"};
    private static final String[] MESSAGE_KEYS = {"messageRef", "messageName", "name"};
    private static final String[] SIGNAL_KEYS = {"signalRef", "signalName", "name"};
    private static final String[] CONDITION_KEYS = {"condition", "activationCondition", "expression"};
    private static final String[] ERROR_KEYS = {"errorRef", "errorCode", "code", "name"};
    private static final String[] ESCALATION_KEYS = {"escalationRef", "name", "code"};
    private static final String[] LINK_KEYS = {"linkName", "linkRef", "name"};
    private static final String[] ACTIVITY_KEYS = {"activityRef"};
    private static final String[] PROCESS_KEYS = {"calledElement", "processId"};
    private static final String[] RULE_REF_KEYS = {"ruleRef"};
    private static final String[] DECISION_KEY_KEYS = {"decisionKey"};
    private static final String[] RESULT_VAR_KEYS = {"resultVariable"};

    private final Redactor redactor;
    private final ObjectMapper objectMapper;

    public BpmnNodeLogPayloads(Redactor redactor, ObjectMapper objectMapper) {
        this.redactor = Objects.requireNonNull(redactor, "redactor");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    /** The authored bpmnTypes. Case-insensitive membership. */
    public static boolean isEngineNativeBpmnType(String bpmnType) {
        if (bpmnType == null || bpmnType.isBlank()) {
            return false;
        }
        return ENGINE_NATIVE_BPMN_TYPES.contains(bpmnType.trim().toLowerCase(Locale.ROOT));
    }

    /**
     * Build a terminal SandboxLogRecord. Never throws. Never inlines heavy file bytes
     * (strip data fields &gt; 18_000_000 chars). Run Redactor.redactTree on input/output
     * and Redactor.redact on stdout/stderr/errorMessage.
     *
     * <p>incomingMsg = upstream node output passthrough (already collected by the caller).
     * decision = GatewayDecisionRecorder.latest(...) or null.
     * status = COMPLETED / FAILED / CANCELED
     */
    public SandboxLogRecord buildTerminal(
            String workflowExecutionId,
            String nodeId,
            String nodeName,
            String bpmnType,
            String status,
            Long durationMs,
            Object incomingMsg,
            NodeDto node,
            GatewayDecision decision,
            String errorMessage) {
        try {
            Object rawOutput = buildOutput(bpmnType, incomingMsg, node, decision);
            Object input = redactHeavyFilePayloads(incomingMsg);
            Object output = redactHeavyFilePayloads(rawOutput);
            String stdout = buildStdout(bpmnType, incomingMsg, node, decision);
            String failureKind = isCompleted(status) ? "NONE" : "ENGINE";
            return new SandboxLogRecord(
                    workflowExecutionId,
                    nodeId,
                    nodeName,
                    status,
                    null,
                    durationMs,
                    LANGUAGE,
                    null,
                    redactor.redact(stdout),
                    redactor.redact(null),
                    redactor.redact(errorMessage),
                    redactor.redactTree(input),
                    redactor.redactTree(output),
                    null,
                    null,
                    failureKind,
                    null);
        } catch (Exception first) {
            // Last-ditch: still emit a record; never throw into the engine listener.
            try {
                String failureKind = isCompleted(status) ? "NONE" : "ENGINE";
                return new SandboxLogRecord(
                        workflowExecutionId,
                        nodeId,
                        nodeName,
                        status,
                        null,
                        durationMs,
                        LANGUAGE,
                        null,
                        redactor.redact("bpmn: " + nullToEmpty(bpmnType)),
                        null,
                        redactor.redact(errorMessage),
                        redactor.redactTree(incomingMsg),
                        redactor.redactTree(incomingMsg),
                        null,
                        null,
                        failureKind,
                        null);
            } catch (Exception ignored) {
                return new SandboxLogRecord(
                        workflowExecutionId, nodeId, nodeName, status,
                        null, durationMs, LANGUAGE, null,
                        null, null, null, null, null, null, null,
                        isCompleted(status) ? "NONE" : "ENGINE",
                        null);
            }
        }
    }

    /**
     * The {@code user.task} terminal row, written when the decision lands: {@code input} is Task
     * Input as the pipeline resolved it (the Values the recipient saw), {@code output} is Task Output
     * as fanned out ({@code decision}). Same redaction and heavy-file strip as every BPMN row.
     * Never throws.
     */
    public SandboxLogRecord buildUserTaskTerminal(String workflowExecutionId, String nodeId, String nodeName,
                                                  Long durationMs, Object input, Object output, String decision) {
        try {
            return new SandboxLogRecord(
                    workflowExecutionId, nodeId, nodeName, "COMPLETED", null, durationMs, LANGUAGE, null,
                    redactor.redact("user task: " + nullToEmpty(decision)), redactor.redact(null), redactor.redact(null),
                    redactor.redactTree(redactHeavyFilePayloads(input)),
                    redactor.redactTree(redactHeavyFilePayloads(output)),
                    null, null, "NONE", null);
        } catch (Exception e) {
            return new SandboxLogRecord(
                    workflowExecutionId, nodeId, nodeName, "COMPLETED", null, durationMs, LANGUAGE, null,
                    null, null, null, null, null, null, null, "NONE", null);
        }
    }

    /** Human echoes line only (also used by tests). Never throws. */
    public String buildStdout(String bpmnType, Object incomingMsg, NodeDto node,
                              GatewayDecision decision) {
        try {
            String type = normalize(bpmnType);
            String fallback = type.isEmpty() ? "unknown" : type;
            return switch (type) {
                case "exclusive.gateway" -> exclusiveStdout(node, decision);
                case "inclusive.gateway" -> inclusiveStdout(node, decision);
                case "parallel.gateway" -> "parallel: forked/joined; passthrough";
                case "event.based.gateway" -> "event-based: waiting for first event";
                case "complex.gateway" -> "complex: activation condition evaluated";
                case "start.event" -> "none start";
                case "timer.start.event" -> "timer start: " + configOrFallback(node, fallback, TIMER_KEYS);
                case "message.start.event" -> "message start: " + configOrFallback(node, fallback, MESSAGE_KEYS);
                case "signal.start.event" -> "signal start: " + configOrFallback(node, fallback, SIGNAL_KEYS);
                case "conditional.start.event" -> "conditional start: " + configOrFallback(node, fallback, CONDITION_KEYS);
                case "multiple.start.event" -> "multiple start";
                case "parallel.multiple.start.event" -> "parallel-multiple start";
                case "end.event" -> "none end";
                case "terminate.end.event" -> "terminate end";
                case "error.end.event" -> "error end: " + configOrFallback(node, fallback, ERROR_KEYS);
                case "escalation.end.event" -> "escalation end: " + configOrFallback(node, fallback, ESCALATION_KEYS);
                case "compensate.end.event" -> "compensate end";
                case "cancel.end.event" -> "cancel end";
                case "message.end.event" -> "message end: " + configOrFallback(node, fallback, MESSAGE_KEYS);
                case "signal.end.event" -> "signal end: " + configOrFallback(node, fallback, SIGNAL_KEYS);
                case "timer.intermediate.catch.event" -> "caught timer: " + configOrFallback(node, fallback, TIMER_KEYS);
                case "message.intermediate.catch.event" -> "caught message: " + configOrFallback(node, fallback, MESSAGE_KEYS);
                case "signal.intermediate.catch.event" -> "caught signal: " + configOrFallback(node, fallback, SIGNAL_KEYS);
                case "conditional.intermediate.catch.event" ->
                        "caught condition: " + configOrFallback(node, fallback, CONDITION_KEYS);
                case "link.intermediate.catch.event" -> "caught link: " + configOrFallback(node, fallback, LINK_KEYS);
                case "multiple.intermediate.catch.event" -> "caught multiple";
                case "parallel.multiple.intermediate.catch.event" -> "caught parallel-multiple";
                case "none.intermediate.throw.event" -> "threw none";
                case "message.intermediate.throw.event" -> "threw message: " + configOrFallback(node, fallback, MESSAGE_KEYS);
                case "signal.intermediate.throw.event" -> "threw signal: " + configOrFallback(node, fallback, SIGNAL_KEYS);
                case "escalation.intermediate.throw.event" ->
                        "threw escalation: " + configOrFallback(node, fallback, ESCALATION_KEYS);
                case "compensate.intermediate.throw.event" ->
                        "threw compensate: " + configOrFallback(node, fallback, ACTIVITY_KEYS);
                case "link.intermediate.throw.event" -> "threw link: " + configOrFallback(node, fallback, LINK_KEYS);
                case "business.rule.task" -> "business-rule: " + configOrFallback(node, fallback, RULE_REF_KEYS);
                case "call.activity" -> "call-activity: " + configOrFallback(node, fallback, PROCESS_KEYS);
                default -> "bpmn: " + (bpmnType == null ? "" : bpmnType);
            };
        } catch (Exception ignored) {
            return "bpmn: " + (bpmnType == null ? "" : bpmnType);
        }
    }

    // --- output shapes -------------------------------------------------------

    private Object buildOutput(String bpmnType, Object incomingMsg, NodeDto node,
                               GatewayDecision decision) {
        String type = normalize(bpmnType);
        String fallback = type.isEmpty() ? "unknown" : type;
        return switch (type) {
            case "exclusive.gateway", "inclusive.gateway", "parallel.gateway",
                 "event.based.gateway", "complex.gateway" -> gatewayOutput(incomingMsg, node, decision);
            case "start.event" -> incomingMsg != null ? incomingMsg : new LinkedHashMap<>();
            case "timer.start.event", "timer.intermediate.catch.event" ->
                    singleton("timer", configOrFallback(node, fallback, TIMER_KEYS));
            case "message.start.event", "message.intermediate.catch.event",
                 "message.intermediate.throw.event", "message.end.event" ->
                    singleton("message", configOrFallback(node, fallback, MESSAGE_KEYS));
            case "signal.start.event", "signal.intermediate.catch.event",
                 "signal.intermediate.throw.event", "signal.end.event" ->
                    singleton("signal", configOrFallback(node, fallback, SIGNAL_KEYS));
            case "conditional.start.event", "conditional.intermediate.catch.event" ->
                    singleton("condition", configOrFallback(node, fallback, CONDITION_KEYS));
            case "error.end.event" -> singleton("error", configOrFallback(node, fallback, ERROR_KEYS));
            case "escalation.end.event", "escalation.intermediate.throw.event" ->
                    singleton("escalation", configOrFallback(node, fallback, ESCALATION_KEYS));
            case "link.intermediate.catch.event", "link.intermediate.throw.event" ->
                    singleton("link", configOrFallback(node, fallback, LINK_KEYS));
            case "compensate.intermediate.throw.event" ->
                    singleton("activity", configOrFallback(node, fallback, ACTIVITY_KEYS));
            case "business.rule.task" -> businessRuleOutput(node, fallback);
            case "call.activity" -> singleton("processId", configOrFallback(node, fallback, PROCESS_KEYS));
            case "end.event" -> incomingMsg;
            default -> incomingMsg; // remaining starts/ends/throws publish the passthrough
        };
    }

    /** Gateways always publish {condition, msg} so DuckTales has a stable shape. */
    private Map<String, Object> gatewayOutput(Object incomingMsg, NodeDto node, GatewayDecision decision) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("condition", winningCondition(node, decision));
        out.put("msg", incomingMsg);
        return out;
    }

    private Map<String, Object> businessRuleOutput(NodeDto node, String fallback) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ruleRef", configOrFallback(node, fallback, RULE_REF_KEYS));
        out.put("decisionKey", configOrFallback(node, fallback, DECISION_KEY_KEYS));
        out.put("resultVariable", configOrFallback(node, fallback, RESULT_VAR_KEYS));
        return out;
    }

    private static Map<String, Object> singleton(String key, String value) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put(key, value);
        return out;
    }

    // --- gateway echoes ------------------------------------------------------

    private String exclusiveStdout(NodeDto node, GatewayDecision decision) {
        if (decision == null) {
            return "activated: " + configOrDefault(node) + " (evaluated-condition-unavailable)";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("activated: ").append(winningCondition(node, decision));
        BranchEvaluation winner = winningEval(decision);
        List<BranchEvaluation> evals = decision.evaluations();
        if (evals == null) {
            return sb.toString();
        }
        for (BranchEvaluation ev : evals) {
            if (ev == null || ev == winner) {
                continue;
            }
            // Default pass: empty/unconditioned branch is implied by activated: default.
            if (decision.defaultTaken()
                    && (ev.conditionExpression() == null || ev.conditionExpression().isBlank())) {
                continue;
            }
            sb.append('\n');
            sb.append("not-taken: ").append(nullToEmpty(ev.conditionExpression()));
            sb.append(" → ").append(ev.result());
            if (ev.error() != null && !ev.error().isBlank()) {
                sb.append(" error=").append(ev.error());
            }
        }
        return sb.toString();
    }

    private String inclusiveStdout(NodeDto node, GatewayDecision decision) {
        if (decision == null) {
            return "inclusive: " + configOrDefault(node) + " (evaluated-condition-unavailable)";
        }
        List<String> taken = takenExpressions(decision);
        StringBuilder sb = new StringBuilder();
        sb.append("inclusive: took ").append(taken.size()).append(" branch(es)");
        for (String expr : taken) {
            sb.append('\n').append(expr);
        }
        return sb.toString();
    }

    /**
     * Winning condition text: {@code dead-end} / {@code default} / the selected
     * expression. Never resolved payload values (REF-only).
     */
    private String winningCondition(NodeDto node, GatewayDecision decision) {
        if (decision != null) {
            if (decision.deadEnd()) {
                return "dead-end";
            }
            if (decision.defaultTaken()) {
                return "default";
            }
            BranchEvaluation winner = winningEval(decision);
            if (winner != null && winner.conditionExpression() != null && !winner.conditionExpression().isBlank()) {
                return winner.conditionExpression();
            }
            return "default";
        }
        return configOrDefault(node);
    }

    private static BranchEvaluation winningEval(GatewayDecision decision) {
        if (decision == null || decision.defaultTaken() || decision.deadEnd()
                || decision.evaluations() == null) {
            return null;
        }
        String selected = decision.selectedTargetNodeId();
        if (selected != null && !selected.isBlank()) {
            for (BranchEvaluation ev : decision.evaluations()) {
                if (ev != null && selected.equals(ev.targetNodeId())) {
                    return ev;
                }
            }
        }
        for (BranchEvaluation ev : decision.evaluations()) {
            if (ev != null && ev.result()) {
                return ev;
            }
        }
        return null;
    }

    /** Inclusive: every true branch; default counts as one taken path; dead-end counts as none. */
    private static List<String> takenExpressions(GatewayDecision decision) {
        List<String> taken = new java.util.ArrayList<>();
        if (decision.deadEnd()) {
            return taken;
        }
        if (decision.evaluations() != null) {
            for (BranchEvaluation ev : decision.evaluations()) {
                if (ev != null && ev.result()) {
                    taken.add(ev.conditionExpression() == null ? "" : ev.conditionExpression());
                }
            }
        }
        if (taken.isEmpty() && decision.defaultTaken()) {
            taken.add("default");
        }
        return taken;
    }

    // --- config lookup (never NPE) -------------------------------------------

    private String configOrDefault(NodeDto node) {
        String v = firstConfig(node, CONDITION_KEYS);
        return (v == null || v.isBlank()) ? "default" : v;
    }

    private String configOrFallback(NodeDto node, String fallback, String... shortCodes) {
        String v = firstConfig(node, shortCodes);
        if (v != null && !v.isBlank()) {
            return v;
        }
        return (fallback == null || fallback.isBlank()) ? "unknown" : fallback;
    }

    /** First non-blank configurationParameter among aliases; null when none. */
    private static String firstConfig(NodeDto node, String... shortCodes) {
        if (node == null || node.getData() == null || shortCodes == null) {
            return null;
        }
        List<ParameterDto> params = node.getData().getConfigurationParameters();
        if (params == null || params.isEmpty()) {
            return null;
        }
        for (String code : shortCodes) {
            if (code == null || code.isBlank()) {
                continue;
            }
            for (ParameterDto p : params) {
                if (p == null || p.getShortCode() == null) {
                    continue;
                }
                if (code.equalsIgnoreCase(p.getShortCode())) {
                    String v = TemplateDto.stringifyParameterValue(p.getValue());
                    if (v != null && !v.isBlank()) {
                        return v;
                    }
                }
            }
        }
        return null;
    }

    // --- heavy-file strip (copied from ScriptDispatchDelegate; do not call it) -

    private Object redactHeavyFilePayloads(Object payload) {
        if (payload == null || objectMapper == null) {
            return payload;
        }
        try {
            Object copy = objectMapper.convertValue(payload, Object.class);
            stripDataFields(copy);
            return copy;
        } catch (Exception e) {
            return null; // prefer empty logs over a timed-out multi-MB push
        }
    }

    @SuppressWarnings("unchecked")
    private static void stripDataFields(Object node) {
        if (node instanceof Map<?, ?> map) {
            Map<Object, Object> m = (Map<Object, Object>) map;
            Object data = m.get("data");
            if (data instanceof String s && s.length() > MAX_INLINE_DATA_CHARS) {
                m.put("data", "<omitted " + s.length() + " chars>");
            }
            for (Object value : m.values()) {
                stripDataFields(value);
            }
        } else if (node instanceof List<?> list) {
            for (Object element : list) {
                stripDataFields(element);
            }
        }
    }

    private static boolean isCompleted(String status) {
        return status != null && "COMPLETED".equalsIgnoreCase(status.trim());
    }

    private static String normalize(String bpmnType) {
        return bpmnType == null ? "" : bpmnType.trim().toLowerCase(Locale.ROOT);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
