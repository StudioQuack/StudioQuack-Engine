package com.studioquack.sqs.execution.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.sqs.execution.TransformFixtures;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.pipeline.PreparedInbound;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.helperchan.SandboxHelperScriptProvider;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Edge-transform pre-step: match/mismatch, hard-fail codes, sanitizer, JOLT skip. */
class EdgeTransformPreStepTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private SandboxExecutionPort sandbox;
    private GenerateScriptService generateScriptService;
    private SandboxLogCallback logCallback;
    private EdgeTransformPreStep preStep;
    private final AtomicInteger sandboxCalls = new AtomicInteger();

    @BeforeEach
    void setUp() {
        sandbox = mock(SandboxExecutionPort.class);
        logCallback = mock(SandboxLogCallback.class);
        generateScriptService = new GenerateScriptService(mapper, new SandboxHelperScriptProvider());
        preStep = new EdgeTransformPreStep(generateScriptService, sandbox, mapper, logCallback, 30_000L);
        sandboxCalls.set(0);
    }

    @Test
    void mismatch_script_output_becomes_consuming_node_inbound() {
        Map<String, Object> inbound = Map.of("name", "Ada");
        Fixture fx = fixture("javascript", "python", "target.fullName = source.name", inbound);

        when(sandbox.execute(any())).thenAnswer(inv -> {
            sandboxCalls.incrementAndGet();
            SandboxJobRequest job = inv.getArgument(0);
            assertEquals(EgressPolicy.NONE, job.egress());
            assertTrue(job.limits().wallMs() > 0 && job.limits().wallMs() <= 30_000L);
            String stdout = "SQ_EDGE_TRANSFORM_RESULT:{\"fullName\":\"Ada\"}\n";
            return SandboxJobResponse.success(0, stdout, "",
                    Map.of(EdgeTransformPreStep.RESULT_PORT, Map.of("fullName", "Ada")),
                    12L, "c1");
        });

        EdgeTransformPreStep.Result result = preStep.run(
                fx.ctx, fx.prepared, SandboxLanguage.JAVASCRIPT, 60_000L, "d1");

        assertFalse(result.failed());
        assertEquals(1, sandboxCalls.get());
        assertFalse(result.useCombinedBake());
        assertEquals(Map.of("fullName", "Ada"), fx.ctx.getCurrentNode().getInputList().get(0).getValue());
        assertEquals(Map.of("fullName", "Ada"), result.prepared().inputVariables().get("in"));
        assertTrue(result.prepared().daemonDirectives().stream().noneMatch(
                s -> "script".equalsIgnoreCase(s.getKind())));
        ArgumentCaptor<SandboxJobRequest> cap = ArgumentCaptor.forClass(SandboxJobRequest.class);
        verify(sandbox).execute(cap.capture());
        assertTrue(cap.getValue().source().contains("target.fullName = source.name")
                || cap.getValue().source().contains("_TransformSource"));
        assertFalse(cap.getValue().source().contains("JVBERi0")); // no inline bytes leaked via unrelated path
        // Synthetic standard node identity on the normal dispatch path.
        assertEquals(EdgeTransformPreStep.syntheticNodeId("A", "B"), cap.getValue().currentNodeId());
        assertNotNull(cap.getValue().executionContext());
        assertTrue(cap.getValue().executionContext().containsKey("currentNode"));
        verify(logCallback, atLeastOnce()).reportStarted(any());
        verify(logCallback, atLeastOnce()).report(any());
    }

    @Test
    void script_edge_strips_sibling_jolt_so_neither_side_runs() {
        // Legacy/malformed dual directive: script MUST win; jolt must not remain for APPLY.
        TransformationSpecDto script = TransformationSpecDto.builder()
                .kind("script").language("python").script("target.y = source.x")
                .sourceNodeId("A").targetNodeId("B")
                .sourcePortShortCode("out").targetPortShortCode("in").direction("input")
                .build();
        TransformationSpecDto jolt = TransformationSpecDto.builder()
                .kind("jolt")
                .sourceNodeId("A").targetNodeId("B")
                .sourcePortShortCode("out").targetPortShortCode("in").direction("input")
                .joltSpec(List.of(Map.of("operation", "shift", "spec", Map.of("x", "y"))))
                .build();
        Map<String, Object> inbound = Map.of("x", 1);
        Map<String, Object> vars = new LinkedHashMap<>();
        vars.put("in", inbound);
        vars.put(TransformDirectiveApplier.DIRECTIVE_KEY, List.of(jolt, script));
        PreparedInbound prepared = new PreparedInbound(vars, List.of(jolt, script), Map.of(), Set.of());
        ExecutionContext ctx = new ExecutionContext();
        NodeDto node = TransformFixtures.simpleNode("B", "in", "out");
        node.setInputList(new ArrayList<>(List.of(
                TransformFixtures.input("A", "out", "B", "in", inbound))));
        ctx.setCurrentNode(node);

        EdgeTransformPreStep.Result result = preStep.run(ctx, prepared, SandboxLanguage.PYTHON, 0L, "d1");

        assertFalse(result.failed());
        assertTrue(result.useCombinedBake());
        assertTrue(result.prepared().daemonDirectives().stream().noneMatch(
                s -> "jolt".equalsIgnoreCase(s.getKind())),
                "jolt sibling must be stripped so it cannot run before or after the sandbox");
        assertTrue(result.prepared().daemonDirectives().stream().noneMatch(
                s -> "script".equalsIgnoreCase(s.getKind())),
                "consumed script directives must not re-pending on B");
        verify(sandbox, never()).execute(any());
    }

    @Test
    void jolt_edge_does_not_invoke_pre_step_sandbox() {
        TransformationSpecDto jolt = TransformationSpecDto.builder()
                .kind("jolt")
                .sourceNodeId("A")
                .targetNodeId("B")
                .targetPortShortCode("in")
                .joltSpec(List.of(Map.of("operation", "shift", "spec", Map.of("x", "y"))))
                .build();
        Map<String, Object> vars = new LinkedHashMap<>();
        vars.put("in", Map.of("x", 1));
        vars.put(TransformDirectiveApplier.DIRECTIVE_KEY, List.of(jolt));
        PreparedInbound prepared = new PreparedInbound(vars, List.of(jolt), Map.of(), Set.of());
        ExecutionContext ctx = new ExecutionContext();
        NodeDto node = TransformFixtures.simpleNode("B", "in", "out");
        InOutValueDto entry = TransformFixtures.input("A", "out", "B", "in", Map.of("x", 1));
        node.setInputList(new ArrayList<>(List.of(entry)));
        ctx.setCurrentNode(node);

        EdgeTransformPreStep.Result result = preStep.run(ctx, prepared, SandboxLanguage.PYTHON, 0L, "d1");

        assertFalse(result.failed());
        assertFalse(result.useCombinedBake());
        verify(sandbox, never()).execute(any());
        assertEquals(1, result.prepared().daemonDirectives().size());
        assertEquals("jolt", result.prepared().daemonDirectives().get(0).getKind());
    }

    @Test
    void failing_transform_hard_fails_without_main_dispatch() {
        Fixture fx = fixture("javascript", "python", "raise RuntimeError('boom')", Map.of("a", 1));
        when(sandbox.execute(any())).thenReturn(SandboxJobResponse.failure(
                1, "", "boom", Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                "sandbox exited non-zero", 5L, "c1", SandboxJobResponse.FailureKind.SCRIPT));

        EdgeTransformPreStep.Result result = preStep.run(
                fx.ctx, fx.prepared, SandboxLanguage.JAVASCRIPT, 0L, "d1");

        assertTrue(result.failed());
        assertEquals(EdgeTransformPreStep.ERROR_FAILED, result.errorCode());
        assertTrue(result.errorMessage().startsWith("edge-transform "));
        assertFalse(result.errorMessage().contains("boom")); // message only, no payload/script detail dump
        verify(sandbox).execute(any());
    }

    @Test
    void timeout_maps_to_EDGE_TRANSFORM_TIMEOUT() {
        Fixture fx = fixture("javascript", "python", "target.x = source.a", Map.of("a", 1));
        when(sandbox.execute(any())).thenReturn(SandboxJobResponse.failure(
                -1, "", "", Map.of(), SandboxJobResponse.ERROR_DEADLINE,
                "sandbox wall-clock deadline exceeded", 30_000L, "c1",
                SandboxJobResponse.FailureKind.CONTAINER));

        EdgeTransformPreStep.Result result = preStep.run(
                fx.ctx, fx.prepared, SandboxLanguage.JAVASCRIPT, 0L, "d1");

        assertTrue(result.failed());
        assertEquals(EdgeTransformPreStep.ERROR_TIMEOUT, result.errorCode());
    }

    @Test
    void empty_result_maps_to_EDGE_TRANSFORM_EMPTY() {
        Fixture fx = fixture("javascript", "python", "pass", Map.of("a", 1));
        when(sandbox.execute(any())).thenReturn(SandboxJobResponse.success(
                0, "SQ_EDGE_TRANSFORM_RESULT:null\n", "", Map.of(), 3L, "c1"));

        EdgeTransformPreStep.Result result = preStep.run(
                fx.ctx, fx.prepared, SandboxLanguage.JAVASCRIPT, 0L, "d1");

        assertTrue(result.failed());
        assertEquals(EdgeTransformPreStep.ERROR_EMPTY, result.errorCode());
    }

    @Test
    void language_match_uses_combined_bake_without_extra_acquire() {
        Fixture fx = fixture("python", "python", "target.fullName = source.name", Map.of("name", "Ada"));

        EdgeTransformPreStep.Result result = preStep.run(
                fx.ctx, fx.prepared, SandboxLanguage.PYTHON, 0L, "d1");

        assertFalse(result.failed());
        assertTrue(result.useCombinedBake());
        assertEquals(1, result.matchedBakes().size());
        verify(sandbox, never()).execute(any());
        assertTrue(result.prepared().daemonDirectives().stream().noneMatch(
                s -> "script".equalsIgnoreCase(s.getKind())));

        var built = generateScriptService.buildCombinedTransformAndNodeSource(
                Map.of("currentNode", Map.of("inputList", List.of(
                        Map.of("portValue", "in", "targetPortValue", "in", "value", Map.of("name", "Ada"))))),
                "python",
                "print('main')",
                result.matchedBakes());
        assertTrue(built.source().contains("target.fullName = source.name"));
        assertTrue(built.source().contains("print('main')"));
        assertTrue(built.source().contains("_sq_patch_edge_transform_into_ctx")
                || built.source().contains("_TransformSource"));
    }

    @Test
    void contentBlock_inline_data_stripped_before_bake() {
        String ref = "sq-file:edge-prestep-1";
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("fileId", ref);
        block.put("data", "JVBERi0xLjQK" + "B".repeat(32));
        block.put("content", "should-not-bake");
        block.put("base64", "should-not-bake");
        Map<String, Object> inbound = new LinkedHashMap<>();
        inbound.put("doc", block);

        Fixture fx = fixture("javascript", "python", "target.doc = source.doc", inbound);
        when(sandbox.execute(any())).thenAnswer(inv -> {
            SandboxJobRequest job = inv.getArgument(0);
            // Inline bytes must never appear in the baked source (payload keys stripped).
            assertFalse(job.source().contains("JVBERi0"));
            assertFalse(job.source().contains("should-not-bake"));
            // Ref may only exist inside Base64 JSON — decode the transform-input placeholder.
            assertTrue(bakedTransformInputContainsRef(job.source(), ref),
                    "sanitized bake must carry sq-file ref, not inline bytes");
            return SandboxJobResponse.success(0,
                    "SQ_EDGE_TRANSFORM_RESULT:" + mapper.writeValueAsString(Map.of("doc", Map.of("fileId", ref, "source", "sq-file"))) + "\n",
                    "", Map.of(), 4L, "c1");
        });

        EdgeTransformPreStep.Result result = preStep.run(
                fx.ctx, fx.prepared, SandboxLanguage.JAVASCRIPT, 0L, "d1");

        assertFalse(result.failed());
        verify(sandbox).execute(any());
        @SuppressWarnings("unchecked")
        Map<String, Object> mergedDoc = (Map<String, Object>)
                ((Map<?, ?>) result.prepared().inputVariables().get("in")).get("doc");
        assertNotNull(mergedDoc);
        assertEquals(ref, mergedDoc.get("fileId"));
        assertNull(mergedDoc.get("data"));
    }

    @Test
    void mapCombinedFailureCode_timeout_and_empty() {
        SandboxJobResponse timeout = SandboxJobResponse.failure(
                -1, "", "", Map.of(), SandboxJobResponse.ERROR_DEADLINE, "deadline", 1L, "c",
                SandboxJobResponse.FailureKind.CONTAINER);
        assertEquals(EdgeTransformPreStep.ERROR_TIMEOUT, preStep.mapCombinedFailureCode(timeout));

        SandboxJobResponse empty = SandboxJobResponse.failure(
                1, "", "EDGE_TRANSFORM_EMPTY: transform target was never written", Map.of(),
                SandboxJobResponse.ERROR_SANDBOX_FAILED, "failed", 1L, "c",
                SandboxJobResponse.FailureKind.SCRIPT);
        assertEquals(EdgeTransformPreStep.ERROR_EMPTY, preStep.mapCombinedFailureCode(empty));
    }

    /* ---- fixtures ---- */

    private Fixture fixture(String nodeLang, String transformLang, String script,
                            Map<String, Object> inboundValue) {
        TransformationSpecDto spec = TransformationSpecDto.builder()
                .kind("script")
                .language(transformLang)
                .script(script)
                .sourceNodeId("A")
                .targetNodeId("B")
                .sourcePortShortCode("out")
                .targetPortShortCode("in")
                .direction("input")
                .build();
        Map<String, Object> vars = new LinkedHashMap<>();
        vars.put("in", inboundValue);
        vars.put(TransformDirectiveApplier.DIRECTIVE_KEY, List.of(spec));
        PreparedInbound prepared = new PreparedInbound(vars, List.of(spec), Map.of(), Set.of());

        ExecutionContext ctx = new ExecutionContext();
        NodeDto node = TransformFixtures.simpleNode("B", "in", "out");
        if (node.getData() != null) {
            TemplateScriptDto ts = new TemplateScriptDto();
            ts.setLanguage(nodeLang);
            ts.setScript("print('node-main')");
            node.getData().setTemplateScript(ts);
        }
        InOutValueDto entry = TransformFixtures.input("A", "out", "B", "in", inboundValue);
        node.setInputList(new ArrayList<>(List.of(entry)));
        ctx.setCurrentNode(node);
        ctx.setEnvId("env-1");
        ctx.setWorkflowExecutionId("run-1");
        return new Fixture(ctx, prepared, spec);
    }

    private record Fixture(ExecutionContext ctx, PreparedInbound prepared, TransformationSpecDto spec) {
    }

    /** True when the Base64 transform-input blob (not the whole source) contains the file ref. */
    private boolean bakedTransformInputContainsRef(String source, String ref) throws Exception {
        // Python: _SQ_TRANSFORM_INPUT_B64 = "<b64>"
        // JS:     var _SQ_TRANSFORM_INPUT_B64 = "<b64>";
        String marker = "_SQ_TRANSFORM_INPUT_B64 = \"";
        int i = source.indexOf(marker);
        if (i < 0) {
            marker = "_SQ_TRANSFORM_INPUT_B64 = '";
            i = source.indexOf(marker);
        }
        if (i < 0) {
            return false;
        }
        int start = i + marker.length();
        char quote = source.charAt(start - 1);
        int end = source.indexOf(quote, start);
        if (end <= start) {
            return false;
        }
        String b64 = source.substring(start, end);
        String json = new String(java.util.Base64.getDecoder().decode(b64),
                java.nio.charset.StandardCharsets.UTF_8);
        return json.contains(ref) && json.contains("sq-file")
                && !json.contains("JVBERi0") && !json.contains("should-not-bake");
    }
}
