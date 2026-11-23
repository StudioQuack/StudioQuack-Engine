package com.studioquack.sqs.engine.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.BranchEvaluation;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.GatewayDecision;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Payload builder for engine-native BPMN DuckTales panels: membership, gateway echoes,
 * heavy-file strip, redaction, and null-safety.
 */
class BpmnNodeLogPayloadsTest {

    /** Unique bpmnType strings from agent refactor/bpmn_nodes/*.json (excl. the combined dump). */
    private static final List<String> AUTHORED_BPMN_TYPES = List.of(
            "business.rule.task",
            "call.activity",
            "cancel.end.event",
            "compensate.end.event",
            "compensate.intermediate.throw.event",
            "complex.gateway",
            "conditional.intermediate.catch.event",
            "conditional.start.event",
            "end.event",
            "error.end.event",
            "escalation.end.event",
            "escalation.intermediate.throw.event",
            "event.based.gateway",
            "exclusive.gateway",
            "inclusive.gateway",
            "link.intermediate.catch.event",
            "link.intermediate.throw.event",
            "message.end.event",
            "message.intermediate.catch.event",
            "message.intermediate.throw.event",
            "message.start.event",
            "multiple.intermediate.catch.event",
            "multiple.start.event",
            "none.intermediate.throw.event",
            "parallel.gateway",
            "parallel.multiple.intermediate.catch.event",
            "parallel.multiple.start.event",
            "signal.end.event",
            "signal.intermediate.catch.event",
            "signal.intermediate.throw.event",
            "signal.start.event",
            "start.event",
            "terminate.end.event",
            "timer.intermediate.catch.event",
            "timer.start.event");

    private static final String JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abcDEF_ghi-JKL";
    private static final String WINNING_EXPR = "@ai_agent.main_output.final_answer == ''";

    private final BpmnNodeLogPayloads payloads =
            new BpmnNodeLogPayloads(new Redactor(), new ObjectMapper());

    @Test
    @DisplayName("isEngineNativeBpmnType is true for every authored catalog bpmnType")
    void membership_coversAllAuthoredTypes() {
        assertThat(AUTHORED_BPMN_TYPES).hasSize(35);
        for (String type : AUTHORED_BPMN_TYPES) {
            assertThat(BpmnNodeLogPayloads.isEngineNativeBpmnType(type))
                    .as(type)
                    .isTrue();
            assertThat(BpmnNodeLogPayloads.isEngineNativeBpmnType(type.toUpperCase(Locale.ROOT)))
                    .as(type + " (upper)")
                    .isTrue();
        }
        assertThat(BpmnNodeLogPayloads.isEngineNativeBpmnType(null)).isFalse();
        assertThat(BpmnNodeLogPayloads.isEngineNativeBpmnType("")).isFalse();
        assertThat(BpmnNodeLogPayloads.isEngineNativeBpmnType("service.task")).isFalse();
    }

    @Test
    @DisplayName("exclusive.gateway with a decision renders activated + not-taken lines")
    void exclusive_withDecision() {
        GatewayDecision decision = new GatewayDecision(
                "run-1", "gw-1", "XOR",
                List.of(
                        new BranchEvaluation("n1", WINNING_EXPR, true, null),
                        new BranchEvaluation("n2", "@other == 1", false, null),
                        new BranchEvaluation("n3", "@broken", false, "parse error")),
                "n1", false, false, 1L);

        String stdout = payloads.buildStdout("exclusive.gateway", Map.of("k", "v"),
                node("exclusive.gateway"), decision);

        assertThat(stdout).isEqualTo(
                "activated: " + WINNING_EXPR + "\n"
                        + "not-taken: @other == 1 → false\n"
                        + "not-taken: @broken → false error=parse error");

        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "gw-1", "XOR", "exclusive.gateway", "COMPLETED", 12L,
                Map.of("k", "v"), node("exclusive.gateway"), decision, null);
        assertThat(rec.stdout()).startsWith("activated: " + WINNING_EXPR);
        assertThat(rec.stdout()).contains("not-taken: @other == 1 → false");
        assertGatewayShape(rec.output(), WINNING_EXPR, Map.of("k", "v"));
    }

    @Test
    @DisplayName("exclusive.gateway with null decision marks evaluation unavailable")
    void exclusive_nullDecision() {
        NodeDto node = nodeWithParam("exclusive.gateway", "condition", "$(@cart.total) > 10");
        String stdout = payloads.buildStdout("exclusive.gateway", null, node, null);
        assertThat(stdout).isEqualTo("activated: $(@cart.total) > 10 (evaluated-condition-unavailable)");

        String noParam = payloads.buildStdout("exclusive.gateway", null, node("exclusive.gateway"), null);
        assertThat(noParam).isEqualTo("activated: default (evaluated-condition-unavailable)");
    }

    @Test
    @DisplayName("gateway output is always {condition, msg}")
    void gateway_outputShape() {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("orderId", "O-1");
        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "gw-1", "XOR", "exclusive.gateway", "COMPLETED", 1L,
                msg, nodeWithParam("exclusive.gateway", "condition", "x > 1"), null, null);
        assertGatewayShape(rec.output(), "x > 1", msg);
        assertThat(rec.input()).isEqualTo(msg);
    }

    @Test
    @DisplayName("heavy file data field over 18_000_000 chars is stripped")
    void heavyFile_dataFieldStripped() {
        String huge = "H".repeat(18_000_001);
        Map<String, Object> incoming = new LinkedHashMap<>();
        incoming.put("fileName", "big.bin");
        incoming.put("data", huge);

        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "n-1", "Start", "start.event", "COMPLETED", 1L,
                incoming, node("start.event"), null, null);

        @SuppressWarnings("unchecked")
        Map<String, Object> in = (Map<String, Object>) rec.input();
        assertThat(in.get("data")).isEqualTo("<omitted 18000001 chars>");
        assertThat(in.get("fileName")).isEqualTo("big.bin");
        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) rec.output();
        assertThat(out.get("data")).isEqualTo("<omitted 18000001 chars>");
    }

    @Test
    @DisplayName("Redactor scrubs a JWT-looking string in incomingMsg")
    void redactor_appliedToIncomingJwt() {
        Map<String, Object> incoming = Map.of("token", JWT);
        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "n-1", "Start", "start.event", "COMPLETED", 1L,
                incoming, node("start.event"), null, null);

        assertThat(String.valueOf(rec.input())).doesNotContain(JWT);
        assertThat(String.valueOf(rec.input())).contains("[redacted-jwt]");
        assertThat(String.valueOf(rec.output())).contains("[redacted-jwt]");
    }

    @Test
    @DisplayName("never throws on null node / null incomingMsg")
    void neverThrows_onNulls() {
        assertThatCode(() -> payloads.buildTerminal(
                "run-1", "n-1", "X", "exclusive.gateway", "COMPLETED", null,
                null, null, null, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> payloads.buildTerminal(
                null, null, null, null, null, null, null, null, null, null))
                .doesNotThrowAnyException();
        assertThatCode(() -> payloads.buildStdout(null, null, null, null))
                .doesNotThrowAnyException();

        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "n-1", "X", "start.event", "COMPLETED", 0L,
                null, null, null, null);
        assertThat(rec.language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
        assertThat(rec.containerId()).isNull();
        assertThat(rec.exitCode()).isNull();
        assertThat(rec.appliedInputs()).isNull();
        assertThat(rec.postureNote()).isNull();
        assertThat(rec.failureKind()).isEqualTo("NONE");
        assertThat(rec.stdout()).isEqualTo("none start");
        assertThat(rec.output()).isInstanceOf(Map.class);
    }

    @Test
    @DisplayName("FAILED and CANCELED classify as ENGINE; COMPLETED as NONE")
    void failureKind_byStatus() {
        assertThat(payloads.buildTerminal("r", "n", "N", "end.event", "FAILED", 1L,
                null, null, null, "boom").failureKind()).isEqualTo("ENGINE");
        assertThat(payloads.buildTerminal("r", "n", "N", "end.event", "CANCELED", 1L,
                null, null, null, null).failureKind()).isEqualTo("ENGINE");
        assertThat(payloads.buildTerminal("r", "n", "N", "end.event", "COMPLETED", 1L,
                null, null, null, null).failureKind()).isEqualTo("NONE");
    }

    @Test
    @DisplayName("unknown bpmnType still emits the minimum bar")
    void unknownType_minimumBar() {
        Map<String, Object> msg = Map.of("x", 1);
        SandboxLogRecord rec = payloads.buildTerminal(
                "r", "n", "N", "not.a.real.type", "COMPLETED", 1L,
                msg, null, null, null);
        assertThat(rec.input()).isEqualTo(msg);
        assertThat(rec.output()).isEqualTo(msg);
        assertThat(rec.stdout()).isEqualTo("bpmn: not.a.real.type");
        assertThat(rec.language()).isEqualTo("bpmn");
    }

    @Test
    @DisplayName("inclusive.gateway lists taken expressions")
    void inclusive_withDecision() {
        GatewayDecision decision = new GatewayDecision(
                "run-1", "gw-2", "OR",
                List.of(
                        new BranchEvaluation("a", "@a == 1", true, null),
                        new BranchEvaluation("b", "@b == 2", true, null),
                        new BranchEvaluation("c", "@c == 3", false, null)),
                "a", false, false, 1L);
        String stdout = payloads.buildStdout("inclusive.gateway", null, node("inclusive.gateway"), decision);
        assertThat(stdout).isEqualTo("inclusive: took 2 branch(es)\n@a == 1\n@b == 2");
    }

    @Test
    @DisplayName("exclusive defaultTaken renders activated: default plus full not-taken list")
    void exclusive_defaultTaken_distinctFromDeadEnd() {
        String cond = "@ai_agent.main_output.final_answer == \" \"";
        GatewayDecision decision = new GatewayDecision(
                "run-1", "gw-1", "XOR",
                List.of(
                        new BranchEvaluation("emptyTarget", null, false, null),
                        new BranchEvaluation("condTarget", cond, false, null)),
                "emptyTarget", true, false, 1L);

        String stdout = payloads.buildStdout("exclusive.gateway", Map.of("k", "v"),
                node("exclusive.gateway"), decision);

        assertThat(stdout).isEqualTo("activated: default\nnot-taken: " + cond + " → false");
        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "gw-1", "XOR", "exclusive.gateway", "COMPLETED", 12L,
                Map.of("k", "v"), node("exclusive.gateway"), decision, null);
        assertGatewayShape(rec.output(), "default", Map.of("k", "v"));
        assertThat(rec.stdout()).startsWith("activated: default");
        assertThat(rec.stdout()).contains("not-taken: " + cond + " → false");
        assertThat(rec.stdout()).doesNotContain("activated: dead-end");
    }

    @Test
    @DisplayName("exclusive deadEnd renders activated: dead-end plus full not-taken list")
    void exclusive_deadEnd_distinctFromDefault() {
        String cond = "@ai_agent.main_output.final_answer == \" \"";
        GatewayDecision decision = new GatewayDecision(
                "run-1", "gw-1", "XOR",
                List.of(new BranchEvaluation("condTarget", cond, false, null)),
                com.studioquack.sqs.runtime.GatewayRouteResolver.DEAD_END_SENTINEL,
                false, true, 1L);

        String stdout = payloads.buildStdout("exclusive.gateway", Map.of("k", "v"),
                node("exclusive.gateway"), decision);

        assertThat(stdout).isEqualTo("activated: dead-end\nnot-taken: " + cond + " → false");
        SandboxLogRecord rec = payloads.buildTerminal(
                "run-1", "gw-1", "XOR", "exclusive.gateway", "FAILED", 12L,
                Map.of("k", "v"), node("exclusive.gateway"), decision, "no outgoing flow matched — condition miss");
        assertGatewayShape(rec.output(), "dead-end", Map.of("k", "v"));
        assertThat(rec.stdout()).startsWith("activated: dead-end");
        assertThat(rec.stdout()).contains("not-taken: " + cond + " → false");
        assertThat(rec.stdout()).doesNotContain("activated: default");
    }

    @Test
    @DisplayName("inclusive dead-end counts as zero taken; default still counts as one")
    void inclusive_deadEndVsDefault() {
        String cond = "@x == 1";
        GatewayDecision dead = new GatewayDecision(
                "run-1", "gw-2", "OR",
                List.of(new BranchEvaluation("a", cond, false, null)),
                com.studioquack.sqs.runtime.GatewayRouteResolver.DEAD_END_SENTINEL,
                false, true, 1L);
        assertThat(payloads.buildStdout("inclusive.gateway", null, node("inclusive.gateway"), dead))
                .isEqualTo("inclusive: took 0 branch(es)");

        GatewayDecision def = new GatewayDecision(
                "run-1", "gw-2", "OR",
                List.of(
                        new BranchEvaluation("empty", null, false, null),
                        new BranchEvaluation("a", cond, false, null)),
                "empty", true, false, 1L);
        assertThat(payloads.buildStdout("inclusive.gateway", null, node("inclusive.gateway"), def))
                .isEqualTo("inclusive: took 1 branch(es)\ndefault");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "start.event", "timer.start.event", "message.start.event", "signal.start.event",
            "conditional.start.event", "multiple.start.event", "parallel.multiple.start.event",
            "end.event", "terminate.end.event", "error.end.event", "escalation.end.event",
            "compensate.end.event", "cancel.end.event", "message.end.event", "signal.end.event",
            "timer.intermediate.catch.event", "message.intermediate.catch.event",
            "signal.intermediate.catch.event", "conditional.intermediate.catch.event",
            "link.intermediate.catch.event", "multiple.intermediate.catch.event",
            "parallel.multiple.intermediate.catch.event",
            "none.intermediate.throw.event", "message.intermediate.throw.event",
            "signal.intermediate.throw.event", "escalation.intermediate.throw.event",
            "compensate.intermediate.throw.event", "link.intermediate.throw.event",
            "business.rule.task", "call.activity",
            "parallel.gateway", "event.based.gateway", "complex.gateway"
    })
    @DisplayName("every authored type produces a non-blank stdout and never throws")
    void everyType_emitsStdout(String bpmnType) {
        SandboxLogRecord rec = payloads.buildTerminal(
                "r", "n", "N", bpmnType, "COMPLETED", 1L,
                Map.of("passthrough", true), node(bpmnType), null, null);
        assertThat(rec.stdout()).isNotBlank();
        assertThat(rec.language()).isEqualTo("bpmn");
        assertThat(rec.containerId()).isNull();
        assertThat(rec.exitCode()).isNull();
        assertThat(rec.input()).isNotNull();
    }

    @Test
    @DisplayName("timer / message / business-rule / call-activity publish typed output")
    void typedOutputs() {
        NodeDto timer = nodeWithParam("timer.start.event", "timerCycle", "R/PT1H");
        assertThat(payloads.buildStdout("timer.start.event", null, timer, null))
                .isEqualTo("timer start: R/PT1H");
        assertThat(map(payloads.buildTerminal("r", "n", "N", "timer.start.event", "COMPLETED", 1L,
                null, timer, null, null).output())).containsEntry("timer", "R/PT1H");

        NodeDto msg = nodeWithParam("message.start.event", "messageRef", "OrderPlaced");
        assertThat(payloads.buildStdout("message.start.event", null, msg, null))
                .isEqualTo("message start: OrderPlaced");

        NodeDto rule = nodeWithParams("business.rule.task",
                "ruleRef", "discount", "decisionKey", "dk", "resultVariable", "rv");
        SandboxLogRecord ruleRec = payloads.buildTerminal(
                "r", "n", "N", "business.rule.task", "COMPLETED", 1L, Map.of("f", 1), rule, null, null);
        assertThat(ruleRec.stdout()).isEqualTo("business-rule: discount");
        assertThat(map(ruleRec.output()))
                .containsEntry("ruleRef", "discount")
                .containsEntry("decisionKey", "dk")
                .containsEntry("resultVariable", "rv");

        NodeDto call = nodeWithParam("call.activity", "calledElement", "child-proc");
        assertThat(payloads.buildStdout("call.activity", null, call, null))
                .isEqualTo("call-activity: child-proc");
        assertThat(map(payloads.buildTerminal("r", "n", "N", "call.activity", "COMPLETED", 1L,
                null, call, null, null).output())).containsEntry("processId", "child-proc");
    }

    @Test
    @DisplayName("errorMessage is redacted")
    void errorMessage_redacted() {
        SandboxLogRecord rec = payloads.buildTerminal(
                "r", "n", "N", "error.end.event", "FAILED", 1L,
                null, null, null, "token=" + JWT);
        assertThat(rec.errorMessage()).doesNotContain(JWT);
        assertThat(rec.errorMessage()).contains("[redacted-jwt]");
        assertThat(rec.failureKind()).isEqualTo("ENGINE");
    }

    // --- helpers -------------------------------------------------------------

    private static NodeDto node(String bpmnType) {
        TemplateDto data = new TemplateDto();
        data.setBpmnType(bpmnType);
        data.setLabel(bpmnType);
        data.setConfigurationParameters(new ArrayList<>());
        NodeDto n = new NodeDto();
        n.setId("node-" + bpmnType);
        n.setData(data);
        return n;
    }

    private static NodeDto nodeWithParam(String bpmnType, String shortCode, String value) {
        return nodeWithParams(bpmnType, shortCode, value);
    }

    private static NodeDto nodeWithParams(String bpmnType, String... kv) {
        NodeDto n = node(bpmnType);
        List<ParameterDto> params = new ArrayList<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            ParameterDto p = new ParameterDto();
            p.setShortCode(kv[i]);
            p.setValue(kv[i + 1]);
            params.add(p);
        }
        n.getData().setConfigurationParameters(params);
        return n;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return (Map<String, Object>) o;
    }

    private static void assertGatewayShape(Object output, String condition, Object msg) {
        assertThat(output).isInstanceOf(LinkedHashMap.class);
        Map<String, Object> m = map(output);
        assertThat(m).containsKeys("condition", "msg");
        assertThat(m.get("condition")).isEqualTo(condition);
        assertThat(m.get("msg")).isEqualTo(msg);
    }
}
