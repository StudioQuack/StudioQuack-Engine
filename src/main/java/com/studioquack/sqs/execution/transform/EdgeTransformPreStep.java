package com.studioquack.sqs.execution.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.pipeline.PreparedInbound;
import com.studioquack.sqs.execution.transform.GenerateScriptService.EdgeTransformBake;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.status.SandboxLogCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * S6 / — inbound script-transform pre-step on the consuming node.
 *
 * <p>Mechanism (PO): synthesise a <b>standard transform node</b> per wire
 * ({@code edge-transform {source}→{target}}) with one INPUT (A's sanitized outbound) and one
 * OUTPUT (B's target {@link PortDto} schema), then run it through the normal sandbox dispatch /
 * sandbox-log path. Restricted transform helper still exposes only {@code source}/{@code target}.
 *
 * <ul>
 * <li>Language match with B → {@link GenerateScriptService#buildCombinedTransformAndNodeSource}
 * (1 acquire / 1 exec); script directives stripped; Duck Tales row mirrored for the edge id.</li>
 * <li>Language mismatch → one warm-pool hop per distinct transform language via
 * {@link SandboxExecutionPort} with {@link EgressPolicy#NONE}, then merge into B.</li>
 * </ul>
 *
 * <p>Logs carry edge ids + message only — never script body, payload, file bytes, or secrets.
 * JOLT is excluded both before and after the transform sandbox for script edges.
 */
@Slf4j
@Component
public class EdgeTransformPreStep {

    public static final String ERROR_FAILED = "EDGE_TRANSFORM_FAILED";
    public static final String ERROR_TIMEOUT = "EDGE_TRANSFORM_TIMEOUT";
    public static final String ERROR_EMPTY = "EDGE_TRANSFORM_EMPTY";
    public static final String RESULT_PORT = "__sq_edge_transform__";
    public static final String RESULT_STDOUT_PREFIX = "SQ_EDGE_TRANSFORM_RESULT:";
    public static final String SYNTHETIC_NODE_PREFIX = "edge-transform ";
    public static final String SYNTHETIC_IN_PORT = "transform_in";
    public static final String SYNTHETIC_OUT_PORT = "transform_out";

    private final GenerateScriptService generateScriptService;
    private final SandboxExecutionPort sandbox;
    private final ObjectMapper objectMapper;
    private final SandboxLogCallback sandboxLogCallback;
    private final long edgeTransformWallMs;

    public EdgeTransformPreStep(GenerateScriptService generateScriptService,
                                SandboxExecutionPort sandbox,
                                ObjectMapper objectMapper,
                                SandboxLogCallback sandboxLogCallback,
                                @Value("${sqs.sandbox.edge-transform-wall-ms:30000}") long edgeTransformWallMs) {
        this.generateScriptService = generateScriptService;
        this.sandbox = sandbox;
        this.objectMapper = objectMapper;
        this.sandboxLogCallback = sandboxLogCallback;
        this.edgeTransformWallMs = edgeTransformWallMs > 0 ? edgeTransformWallMs : 30_000L;
    }

    /**
     * Result of the pre-step. On failure {@link #failed()} is true and B must not run its main
     * script. On success {@link #prepared()} has script directives stripped; when language-match
     * bakes are present the caller must use {@code buildCombinedTransformAndNodeSource}.
     */
    public record Result(
            PreparedInbound prepared,
            List<EdgeTransformBake> matchedBakes,
            String errorCode,
            String errorMessage) {

        public boolean failed() {
            return errorCode != null;
        }

        public boolean useCombinedBake() {
            return matchedBakes != null && !matchedBakes.isEmpty();
        }

        static Result ok(PreparedInbound prepared, List<EdgeTransformBake> matchedBakes) {
            return new Result(prepared,
                    matchedBakes == null ? List.of() : List.copyOf(matchedBakes),
                    null, null);
        }

        static Result fail(PreparedInbound prepared, String code, String message) {
            return new Result(prepared, List.of(), code, message);
        }
    }

    /**
     * Collects {@code kind=script} specs, runs mismatch hops, prepares match bakes, strips
     * consumed script directives. Never resolves secrets.
     *
     * @param nodeWallMs node wall budget (0 = unbounded → use edge-transform wall alone)
     */
    public Result run(ExecutionContext ctx,
                      PreparedInbound prepared,
                      SandboxLanguage nodeLanguage,
                      long nodeWallMs,
                      String dispatchId) {
        if (prepared == null) {
            return Result.ok(PreparedInbound.empty(), List.of());
        }
        // APPLY-path JOLT exclusion: drop any jolt sibling that shares a script-covered edge so
        // nothing can sneak a JVM JOLT pass around the transform sandbox (collector already omits
        // jolt for non-jolt edges; this is the belt for legacy/malformed dual directives).
        PreparedInbound joltStripped = stripJoltCoveredByScripts(prepared);
        List<TransformationSpecDto> scriptSpecs = collectScriptSpecs(joltStripped);
        if (scriptSpecs.isEmpty()) {
            return Result.ok(joltStripped, List.of());
        }
        prepared = joltStripped;

        String nodeLangWire = nodeLanguage == null ? "" : normalizeLang(nodeLanguage.wire());
        Map<String, List<PendingEdge>> byLang = new LinkedHashMap<>();
        for (TransformationSpecDto spec : scriptSpecs) {
            PendingEdge pending = toPending(ctx, prepared, spec);
            if (pending == null) {
                String src = spec.getSourceNodeId();
                String tgt = spec.getTargetNodeId();
                log.warn("edge-transform {}→{}: missing inbound value", src, tgt);
                return Result.fail(prepared, ERROR_FAILED,
                        "edge-transform " + src + "→" + tgt + ": missing inbound value");
            }
            if (StringUtils.isBlank(pending.userScript())) {
                log.warn("edge-transform {}→{}: empty script", pending.sourceNodeId(), pending.targetNodeId());
                return Result.fail(prepared, ERROR_FAILED,
                        "edge-transform " + pending.sourceNodeId() + "→" + pending.targetNodeId()
                                + ": empty script");
            }
            if (!isWarmableTransformLanguage(pending.language())) {
                log.warn("edge-transform {}→{}: language not warm-poolable",
                        pending.sourceNodeId(), pending.targetNodeId());
                return Result.fail(prepared, ERROR_FAILED,
                        "edge-transform " + pending.sourceNodeId() + "→" + pending.targetNodeId()
                                + ": language not warm-poolable");
            }
            byLang.computeIfAbsent(pending.language(), k -> new ArrayList<>()).add(pending);
        }

        List<EdgeTransformBake> matchedBakes = new ArrayList<>();
        Set<TransformationSpecDto> consumed = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<String, Object> mergedPortValues = new LinkedHashMap<>();

        for (Map.Entry<String, List<PendingEdge>> entry : byLang.entrySet()) {
            String transformLang = entry.getKey();
            List<PendingEdge> edges = entry.getValue();
            boolean match = !transformLang.isEmpty() && transformLang.equals(nodeLangWire);
            if (match) {
                for (PendingEdge e : edges) {
                    matchedBakes.add(new EdgeTransformBake(
                            e.sanitizedInbound(), e.userScript(),
                            e.sourceNodeId(), e.targetNodeId(), e.targetPortKey()));
                    consumed.add(e.spec());
                }
                continue;
            }

            // Language mismatch — one hop per distinct language (coalesced).
            Result hop = runMismatchHop(ctx, dispatchId, transformLang, edges, nodeWallMs);
            if (hop.failed()) {
                return hop;
            }
            for (PendingEdge e : edges) {
                Object value = hop.prepared().inputVariables().get(e.targetPortKey());
                if (isEmptyResult(value)) {
                    log.warn("edge-transform {}→{}: empty result", e.sourceNodeId(), e.targetNodeId());
                    return Result.fail(prepared, ERROR_EMPTY,
                            "edge-transform " + e.sourceNodeId() + "→" + e.targetNodeId()
                                    + ": empty result");
                }
                mergedPortValues.put(e.targetPortKey(), value);
                applyMergedValue(ctx, e, value);
                consumed.add(e.spec());
            }
        }

        PreparedInbound next = stripConsumedScripts(prepared, consumed, mergedPortValues);
        return Result.ok(next, matchedBakes);
    }

    /**
     * Maps a combined-bake (language-match) B-job failure onto edge-transform codes when the
     * failure originated in the transform prelude.
     */
    public String mapCombinedFailureCode(SandboxJobResponse response) {
        if (response == null || response.ok()) {
            return null;
        }
        if (SandboxJobResponse.ERROR_DEADLINE.equals(response.errorCode())) {
            return ERROR_TIMEOUT;
        }
        String detail = StringUtils.defaultString(response.errorMessage())
                + "\n" + StringUtils.defaultString(response.stderr())
                + "\n" + StringUtils.defaultString(response.stdout());
        if (detail.contains(ERROR_EMPTY) || detail.contains("transform target was never written")) {
            return ERROR_EMPTY;
        }
        if (detail.contains("TransformMissingPathError") || detail.contains("missing path")) {
            return ERROR_FAILED;
        }
        // Transform prelude hard-fail — treat non-zero / typed sandbox failure as edge-transform.
        return ERROR_FAILED;
    }

    /* ---- mismatch hop ---- */

    private Result runMismatchHop(ExecutionContext ctx,
                                  String dispatchId,
                                  String transformLang,
                                  List<PendingEdge> edges,
                                  long nodeWallMs) {
        SandboxLanguage lang = SandboxLanguage.fromWire(transformLang);
        if (lang == null) {
            PendingEdge first = edges.get(0);
            log.warn("edge-transform {}→{}: unsupported language",
                    first.sourceNodeId(), first.targetNodeId());
            return Result.fail(PreparedInbound.empty(), ERROR_FAILED,
                    "edge-transform " + first.sourceNodeId() + "→" + first.targetNodeId()
                            + ": unsupported language");
        }
        long wallMs = nodeWallMs > 0
                ? Math.min(nodeWallMs, edgeTransformWallMs)
                : edgeTransformWallMs;
        String jobId = (dispatchId == null ? "et" : dispatchId) + "-et-"
                + transformLang + "-" + UUID.randomUUID().toString().substring(0, 8);

        GenerateScriptService.BuildSourceResult built;
        try {
            if (edges.size() == 1) {
                PendingEdge e = edges.get(0);
                built = generateScriptService.buildEdgeTransformSource(
                        e.sanitizedInbound(), transformLang, e.userScript());
            } else {
                built = buildCoalescedMismatchSource(transformLang, edges);
            }
        } catch (Exception ex) {
            PendingEdge first = edges.get(0);
            log.warn("edge-transform {}→{}: bake failed ({})",
                    first.sourceNodeId(), first.targetNodeId(), ex.getClass().getSimpleName());
            return Result.fail(PreparedInbound.empty(), ERROR_FAILED,
                    "edge-transform " + first.sourceNodeId() + "→" + first.targetNodeId()
                            + ": bake failed");
        }

        // Synthetic standard node — one representative identity for the hop (first edge); per-edge
        // Duck Tales rows are mirrored after the hop with each wire's IO.
        PendingEdge firstEdge = edges.get(0);
        NodeDto synthetic = buildSyntheticTransformNode(ctx, firstEdge, transformLang);
        Map<String, Object> syntheticCtx = syntheticExecutionContext(ctx, synthetic);
        Map<String, Object> syntheticInputs = Map.of(SYNTHETIC_IN_PORT, firstEdge.sanitizedInbound());

        SandboxJobRequest job = SandboxJobRequest.builder(jobId, lang, built.source())
                .egress(EgressPolicy.NONE)
                .limits(SandboxLimits.of(0, wallMs))
                .envId(ctx.getEnvId())
                .workflowExecutionId(ctx.getWorkflowExecutionId())
                .currentNodeId(synthetic.getId())
                .executionContext(syntheticCtx)
                .inputVariables(syntheticInputs)
                .jobMode("system")
                .build();

        reportSyntheticStarted(ctx, synthetic, transformLang);

        SandboxJobResponse response;
        try {
            response = sandbox.execute(job);
        } catch (Exception ex) {
            PendingEdge first = edges.get(0);
            log.warn("edge-transform {}→{}: dispatch error ({})",
                    first.sourceNodeId(), first.targetNodeId(), ex.getClass().getSimpleName());
            reportSyntheticLog(ctx, first, transformLang, null, first.sanitizedInbound(), null,
                    false, -1, 0L, null, "dispatch error");
            return Result.fail(PreparedInbound.empty(), ERROR_FAILED,
                    "edge-transform " + first.sourceNodeId() + "→" + first.targetNodeId()
                            + ": dispatch error");
        }

        if (response == null || !response.ok()) {
            reportSyntheticLog(ctx, firstEdge, transformLang, response,
                    firstEdge.sanitizedInbound(), null, false,
                    response == null ? -1 : response.exitCode(),
                    response == null ? 0L : response.wallMs(),
                    response == null ? null : response.containerId(),
                    response == null ? null : response.errorMessage());
            return mapHopFailure(edges.get(0), response);
        }

        Map<String, Object> portHits = new LinkedHashMap<>();
        if (edges.size() == 1) {
            Object value = extractResult(response);
            if (isEmptyResult(value)) {
                PendingEdge e = edges.get(0);
                log.warn("edge-transform {}→{}: empty result", e.sourceNodeId(), e.targetNodeId());
                return Result.fail(PreparedInbound.empty(), ERROR_EMPTY,
                        "edge-transform " + e.sourceNodeId() + "→" + e.targetNodeId()
                                + ": empty result");
            }
            portHits.put(edges.get(0).targetPortKey(), value);
            reportSyntheticLog(ctx, edges.get(0), transformLang, response,
                    edges.get(0).sanitizedInbound(), value, true,
                    response.exitCode(), response.wallMs(), response.containerId(), null);
        } else {
            Object coalesced = extractResult(response);
            if (!(coalesced instanceof Map<?, ?> map) || map.isEmpty()) {
                PendingEdge e = edges.get(0);
                log.warn("edge-transform {}→{}: empty result", e.sourceNodeId(), e.targetNodeId());
                reportSyntheticLog(ctx, e, transformLang, response,
                        e.sanitizedInbound(), null, false,
                        response.exitCode(), response.wallMs(), response.containerId(), "empty result");
                return Result.fail(PreparedInbound.empty(), ERROR_EMPTY,
                        "edge-transform " + e.sourceNodeId() + "→" + e.targetNodeId()
                                + ": empty result");
            }
            for (PendingEdge e : edges) {
                Object value = map.get(e.targetPortKey());
                if (isEmptyResult(value)) {
                    log.warn("edge-transform {}→{}: empty result", e.sourceNodeId(), e.targetNodeId());
                    reportSyntheticLog(ctx, e, transformLang, response,
                            e.sanitizedInbound(), null, false,
                            response.exitCode(), response.wallMs(), response.containerId(), "empty result");
                    return Result.fail(PreparedInbound.empty(), ERROR_EMPTY,
                            "edge-transform " + e.sourceNodeId() + "→" + e.targetNodeId()
                                    + ": empty result");
                }
                portHits.put(e.targetPortKey(), value);
                reportSyntheticLog(ctx, e, transformLang, response,
                        e.sanitizedInbound(), value, true,
                        response.exitCode(), response.wallMs(), response.containerId(), null);
            }
        }

        // Temporary PreparedInbound carrier for port hits (caller merges into ctx).
        return Result.ok(new PreparedInbound(portHits, List.of(), Map.of(), Set.of()), List.of());
    }

    /**
     * After a language-match combined B job succeeds, mirror one Duck Tales / sandbox-log row per
     * edge using the synthetic wire identity (no extra acquire — transform ran inside B).
     */
    public void reportMatchedTransformLogs(ExecutionContext ctx,
                                           List<EdgeTransformBake> matchedBakes,
                                           String transformLanguage,
                                           SandboxJobResponse bResponse) {
        if (matchedBakes == null || matchedBakes.isEmpty() || ctx == null) {
            return;
        }
        String lang = normalizeLang(transformLanguage);
        boolean ok = bResponse != null && bResponse.ok();
        for (EdgeTransformBake bake : matchedBakes) {
            Object out = null;
            if (ok && ctx.getCurrentNode() != null && ctx.getCurrentNode().getInputList() != null) {
                for (InOutValueDto entry : ctx.getCurrentNode().getInputList()) {
                    String key = StringUtils.firstNonEmpty(entry.getTargetPortValue(), entry.getPortValue());
                    if (bake.targetPortKey() != null && bake.targetPortKey().equals(key)) {
                        out = entry.getValue();
                        break;
                    }
                }
            }
            PendingEdge edge = new PendingEdge(null, lang, bake.targetPortKey(), bake.inboundValue(),
                    bake.userScript(), bake.sourceNodeId(), bake.targetNodeId());
            reportSyntheticLog(ctx, edge, lang, bResponse, bake.inboundValue(), out, ok,
                    bResponse == null ? -1 : bResponse.exitCode(),
                    bResponse == null ? 0L : bResponse.wallMs(),
                    bResponse == null ? null : bResponse.containerId(),
                    ok ? null : (bResponse == null ? null : bResponse.errorMessage()));
        }
    }

    private Result mapHopFailure(PendingEdge edge, SandboxJobResponse response) {
        String src = edge.sourceNodeId();
        String tgt = edge.targetNodeId();
        if (response != null && SandboxJobResponse.ERROR_DEADLINE.equals(response.errorCode())) {
            log.warn("edge-transform {}→{}: timeout", src, tgt);
            return Result.fail(PreparedInbound.empty(), ERROR_TIMEOUT,
                    "edge-transform " + src + "→" + tgt + ": timeout");
        }
        String detail = response == null ? "null response"
                : StringUtils.defaultString(response.errorMessage())
                + "\n" + StringUtils.defaultString(response.stderr());
        if (detail.contains(ERROR_EMPTY) || detail.contains("transform target was never written")) {
            log.warn("edge-transform {}→{}: empty result", src, tgt);
            return Result.fail(PreparedInbound.empty(), ERROR_EMPTY,
                    "edge-transform " + src + "→" + tgt + ": empty result");
        }
        log.warn("edge-transform {}→{}: failed", src, tgt);
        return Result.fail(PreparedInbound.empty(), ERROR_FAILED,
                "edge-transform " + src + "→" + tgt + ": failed");
    }

    /**
     * Multi-edge same-language mismatch: one acquire, sequential transforms, one result map
     * posted on {@link #RESULT_PORT} / stdout marker.
     */
    private GenerateScriptService.BuildSourceResult buildCoalescedMismatchSource(
            String language, List<PendingEdge> edges) {
        // Reuse single-edge bake for the first edge's helper shell, then replace the user-script
        // slot with a multi-edge runner that rebinds source/target per edge.
        PendingEdge first = edges.get(0);
        String helper = generateScriptService.buildEdgeTransformSource(
                first.sanitizedInbound(), language, "#placeholder").source();
        // Placeholder already interpolated away — find user-script region markers.
        String startMark = "# === user script injected below (GenerateScriptService) ===";
        String endMark = "# === end user script ===";
        int start = helper.indexOf(startMark);
        int end = helper.indexOf(endMark);
        if (start < 0 || end < 0 || end <= start) {
            // JS markers
            startMark = "// === user script injected below (GenerateScriptService) ===";
            endMark = "// === end user script ===";
            start = helper.indexOf(startMark);
            end = helper.indexOf(endMark);
        }
        if (start < 0 || end < 0 || end <= start) {
            // Fall back: one bake (first edge only) — still better than N acquires for the rest.
            return generateScriptService.buildEdgeTransformSource(
                    first.sanitizedInbound(), language, first.userScript());
        }
        boolean python = isPython(language);
        StringBuilder multi = new StringBuilder();
        if (python) {
            multi.append("_sq_coalesced = {}\n");
            for (PendingEdge e : edges) {
                String b64 = generateScriptService.bakeJsonBase64(e.sanitizedInbound());
                multi.append("\n_sq_inbound = _sq_json.loads(_sq_b64.b64decode('")
                        .append(b64).append("').decode('utf-8'))\n");
                multi.append("source = _TransformSource(_sq_inbound)\n");
                multi.append("target = _TransformTarget()\n");
                multi.append(e.userScript()).append('\n');
                multi.append("_sq_coalesced[").append(jsonString(e.targetPortKey()))
                        .append("] = target._result_or_empty()\n");
            }
            multi.append("_sq_post_transform_result(_sq_coalesced)\n");
            // Prevent the template footer from posting again / raising EMPTY.
            multi.append("target.replace(_sq_coalesced)\n");
        } else {
            multi.append("var _sqCoalesced = {};\n");
            for (PendingEdge e : edges) {
                String b64 = generateScriptService.bakeJsonBase64(e.sanitizedInbound());
                multi.append("\n_sqInbound = JSON.parse(Buffer.from('")
                        .append(b64).append("', 'base64').toString('utf-8'));\n");
                multi.append("var source = _sqMakeSource(_sqInbound);\n");
                multi.append("var target = _sqMakeTarget();\n");
                multi.append(e.userScript()).append('\n');
                multi.append("_sqCoalesced[").append(jsonString(e.targetPortKey()))
                        .append("] = target._resultOrEmpty();\n");
            }
            multi.append("_sqPostTransformResult(_sqCoalesced);\n");
            multi.append("target.replace(_sqCoalesced);\n");
        }
        String source = helper.substring(0, start + startMark.length())
                + "\n" + multi + helper.substring(end);
        // Suppress duplicate footer post by leaving target written (footer still posts — ok,
        // same coalesced object). Return with empty contextBase64 marker.
        return new GenerateScriptService.BuildSourceResult(source, "");
    }

    /* ---- collect / merge helpers ---- */

    private List<TransformationSpecDto> collectScriptSpecs(PreparedInbound prepared) {
        List<TransformationSpecDto> out = new ArrayList<>();
        List<TransformationSpecDto> specs = prepared.daemonDirectives();
        if (specs == null || specs.isEmpty()) {
            Object raw = prepared.inputVariables() == null
                    ? null : prepared.inputVariables().get(TransformDirectiveApplier.DIRECTIVE_KEY);
            if (raw instanceof List<?> list) {
                for (Object o : list) {
                    if (o instanceof TransformationSpecDto s && isScript(s)) {
                        out.add(s);
                    }
                }
            }
            return out;
        }
        for (TransformationSpecDto s : specs) {
            if (isScript(s)) {
                out.add(s);
            }
        }
        return out;
    }

    private static boolean isScript(TransformationSpecDto s) {
        return s != null && "script".equalsIgnoreCase(s.getKind());
    }

    private PendingEdge toPending(ExecutionContext ctx, PreparedInbound prepared,
                                  TransformationSpecDto spec) {
        String lang = normalizeLang(spec.getLanguage());
        String portKey = targetPortKey(spec, ctx);
        Object inbound = findInboundValue(ctx, prepared, spec, portKey);
        if (inbound == null) {
            return null;
        }
        Object sanitized = FileRefSanitizer.stripInlineFileBytes(deepCopy(inbound));
        return new PendingEdge(spec, lang, portKey == null ? "" : portKey, sanitized,
                StringUtils.defaultString(spec.getScript()),
                spec.getSourceNodeId(), spec.getTargetNodeId());
    }

    private Object findInboundValue(ExecutionContext ctx, PreparedInbound prepared,
                                    TransformationSpecDto spec, String portKey) {
        NodeDto node = ctx.getCurrentNode();
        if (node != null && node.getInputList() != null) {
            for (InOutValueDto entry : node.getInputList()) {
                if (inputListEntryMatches(entry, spec)) {
                    return entry.getValue();
                }
            }
        }
        if (portKey != null && prepared.inputVariables() != null) {
            return prepared.inputVariables().get(portKey);
        }
        return null;
    }

    private static boolean inputListEntryMatches(InOutValueDto entry, TransformationSpecDto spec) {
        if (entry == null || spec == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getSourceNodeId())
                && !spec.getSourceNodeId().equals(entry.getNodeId())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getTargetNodeId())
                && !spec.getTargetNodeId().equals(entry.getTargetNodeId())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getSourcePortShortCode())
                && !spec.getSourcePortShortCode().equals(entry.getPortValue())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getTargetPortShortCode())
                && !spec.getTargetPortShortCode().equals(entry.getTargetPortValue())) {
            return false;
        }
        return true;
    }

    private String targetPortKey(TransformationSpecDto spec, ExecutionContext ctx) {
        if (StringUtils.isNotEmpty(spec.getTargetPortShortCode())) {
            return spec.getTargetPortShortCode();
        }
        NodeDto node = ctx.getCurrentNode();
        if (node != null && node.getInputList() != null) {
            for (InOutValueDto entry : node.getInputList()) {
                if (inputListEntryMatches(entry, spec)) {
                    return StringUtils.firstNonEmpty(entry.getTargetPortValue(), entry.getPortValue());
                }
            }
        }
        return null;
    }

    private void applyMergedValue(ExecutionContext ctx, PendingEdge edge, Object value) {
        NodeDto node = ctx.getCurrentNode();
        if (node == null || node.getInputList() == null) {
            return;
        }
        for (InOutValueDto entry : node.getInputList()) {
            if (inputListEntryMatches(entry, edge.spec())) {
                entry.setValue(value);
                return;
            }
            String key = StringUtils.firstNonEmpty(entry.getTargetPortValue(), entry.getPortValue());
            if (edge.targetPortKey().equals(key)) {
                entry.setValue(value);
                return;
            }
        }
    }

    private PreparedInbound stripConsumedScripts(PreparedInbound prepared,
                                                 Set<TransformationSpecDto> consumed,
                                                 Map<String, Object> mergedPortValues) {
        List<TransformationSpecDto> remaining = new ArrayList<>();
        if (prepared.daemonDirectives() != null) {
            for (TransformationSpecDto s : prepared.daemonDirectives()) {
                if (s != null && !consumed.contains(s)) {
                    remaining.add(s);
                }
            }
        }
        Map<String, Object> vars = new LinkedHashMap<>(
                prepared.inputVariables() == null ? Map.of() : prepared.inputVariables());
        vars.putAll(mergedPortValues);
        if (remaining.isEmpty()) {
            vars.remove(TransformDirectiveApplier.DIRECTIVE_KEY);
        } else {
            vars.put(TransformDirectiveApplier.DIRECTIVE_KEY, remaining);
        }
        return new PreparedInbound(vars, remaining,
                prepared.functionPortTransforms(), prepared.beAppliedSpecs());
    }

    Object extractResult(SandboxJobResponse response) {
        if (response == null) {
            return null;
        }
        Object fromPort = response.outputVariables() == null
                ? null : response.outputVariables().get(RESULT_PORT);
        if (fromPort != null) {
            // Helper-channel may leave stringified JSON.
            return coerceJson(fromPort);
        }
        String stdout = response.stdout();
        if (stdout == null || stdout.isEmpty()) {
            return null;
        }
        Object last = null;
        for (String rawLine : stdout.split("\n", -1)) {
            String line = rawLine.endsWith("\r") ? rawLine.substring(0, rawLine.length() - 1) : rawLine;
            if (!line.startsWith(RESULT_STDOUT_PREFIX)) {
                continue;
            }
            String json = line.substring(RESULT_STDOUT_PREFIX.length()).trim();
            try {
                last = objectMapper.readValue(json, Object.class);
            } catch (Exception ignore) {
                // ignore malformed marker lines
            }
        }
        return last;
    }

    private Object coerceJson(Object value) {
        if (value instanceof String s) {
            String t = s.trim();
            if ((t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"))
                    || (t.startsWith("\"") && t.endsWith("\""))) {
                try {
                    return objectMapper.readValue(t, Object.class);
                } catch (Exception ignore) {
                    return value;
                }
            }
        }
        return value;
    }

    private static boolean isEmptyResult(Object value) {
        if (value == null) {
            return true;
        }
        return value instanceof String s && s.isBlank();
    }

    private Object deepCopy(Object value) {
        try {
            return objectMapper.readValue(objectMapper.writeValueAsBytes(value), Object.class);
        } catch (Exception e) {
            return value;
        }
    }

    private String jsonString(String value) {
        try {
            return objectMapper.writeValueAsString(value == null ? "" : value);
        } catch (Exception e) {
            return "\"\"";
        }
    }

    private static String normalizeLang(String language) {
        if (language == null) {
            return "";
        }
        String l = language.trim().toLowerCase();
        return switch (l) {
            case "py", "python3" -> "python";
            case "js", "node", "nodejs" -> "javascript";
            case "ts" -> "typescript";
            default -> l;
        };
    }

    private static boolean isPython(String language) {
        String l = normalizeLang(language);
        return "python".equals(l);
    }

    /**
 * Edge-transform jobs are always script-language jobs, so the four-language
 * {@link WarmPool#warmableLanguages} list remains the right gate even though the pool itself
 * is now image-keyed (/). A custom/entrypoint image is never an edge-transform target.
 */
    private static boolean isWarmableTransformLanguage(String language) {
        String l = normalizeLang(language);
        if (l.isEmpty()) {
            return false;
        }
        return WarmPool.warmableLanguages().contains(l);
    }

    /** Stable synthetic node id/label: {@code edge-transform A→B}. */
    public static String syntheticNodeId(String sourceNodeId, String targetNodeId) {
        return SYNTHETIC_NODE_PREFIX
                + StringUtils.defaultString(sourceNodeId) + "→"
                + StringUtils.defaultString(targetNodeId);
    }

    /**
     * Builds the synthetic standard transform node: INPUT = sanitized A value; OUTPUT port carries
     * B's target {@link PortDto} schema (from B's template ports — same port ConnectionBuilder
     * resolves). Restricted helper binds {@code source}/{@code target} to these ports' attributes.
     */
    NodeDto buildSyntheticTransformNode(ExecutionContext ctx, PendingEdge edge, String language) {
        String id = syntheticNodeId(edge.sourceNodeId(), edge.targetNodeId());
        PortDto bTargetPort = resolveConsumingTargetPort(ctx, edge);
        PortDto inPort = new PortDto();
        inPort.setId("et_in");
        inPort.setName(SYNTHETIC_IN_PORT);
        inPort.setValue(SYNTHETIC_IN_PORT);
        inPort.setType(PortType.INPUT);
        PortDto outPort = new PortDto();
        outPort.setId("et_out");
        outPort.setName(SYNTHETIC_OUT_PORT);
        outPort.setValue(SYNTHETIC_OUT_PORT);
        outPort.setType(PortType.OUTPUT);
        if (bTargetPort != null && bTargetPort.getSchema() != null) {
            outPort.setSchema(bTargetPort.getSchema());
        }
        TemplateDto template = new TemplateDto();
        template.setBpmnType("custom.sandbox");
        template.setJobType("custom-script-executor");
        template.setNodeType("serviceTask");
        template.setLabel(id);
        template.setPorts(List.of(inPort, outPort));
        TemplateScriptDto script = new TemplateScriptDto();
        script.setLanguage(language);
        script.setScript(edge.userScript());
        template.setTemplateScript(script);

        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        InOutValueDto in = new InOutValueDto();
        in.setNodeId(edge.sourceNodeId());
        in.setTargetNodeId(id);
        in.setPortValue(edge.spec() == null ? null : edge.spec().getSourcePortShortCode());
        in.setTargetPortValue(SYNTHETIC_IN_PORT);
        in.setValue(edge.sanitizedInbound());
        node.setInputList(new ArrayList<>(List.of(in)));
        node.setOutputList(new ArrayList<>());
        return node;
    }

    private PortDto resolveConsumingTargetPort(ExecutionContext ctx, PendingEdge edge) {
        NodeDto b = ctx == null ? null : ctx.getCurrentNode();
        if (b == null || b.getData() == null || StringUtils.isBlank(edge.targetPortKey())) {
            return null;
        }
        PortDto byValue = b.getData().findPortByPortValue(edge.targetPortKey());
        if (byValue != null) {
            return byValue;
        }
        return b.getData().findPortByPortName(edge.targetPortKey());
    }

    private Map<String, Object> syntheticExecutionContext(ExecutionContext parent, NodeDto synthetic) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("workflowExecutionId", parent == null ? null : parent.getWorkflowExecutionId());
        map.put("envId", parent == null ? null : parent.getEnvId());
        map.put("currentNodeId", synthetic.getId());
        try {
            map.put("currentNode", objectMapper.convertValue(synthetic, Map.class));
        } catch (Exception e) {
            map.put("currentNode", Map.of("id", synthetic.getId()));
        }
        return map;
    }

    private void reportSyntheticStarted(ExecutionContext ctx, NodeDto synthetic, String language) {
        if (sandboxLogCallback == null || ctx == null || synthetic == null) {
            return;
        }
        try {
            sandboxLogCallback.reportStarted(new SandboxLogCallback.SandboxStartRecord(
                    ctx.getWorkflowExecutionId(), synthetic.getId(),
                    synthetic.getData() == null ? synthetic.getId() : synthetic.getData().getLabel(),
                    language));
        } catch (Exception e) {
            log.debug("edge-transform: sandbox-started mirror skipped ({})", e.toString());
        }
    }

    private void reportSyntheticLog(ExecutionContext ctx,
                                    PendingEdge edge,
                                    String language,
                                    SandboxJobResponse response,
                                    Object input,
                                    Object output,
                                    boolean ok,
                                    int exitCode,
                                    long wallMs,
                                    String containerId,
                                    String errorMessage) {
        if (sandboxLogCallback == null || ctx == null || edge == null) {
            return;
        }
        try {
            String nodeId = syntheticNodeId(edge.sourceNodeId(), edge.targetNodeId());
            String failureKind = ok ? "NONE"
                    : (response == null || response.failureKind() == null
                    ? SandboxJobResponse.FailureKind.SCRIPT.name()
                    : response.failureKind().name());
            // IO only — never attach stdout/stderr bodies that might echo script/payload.
            // No postureNote: edge-transform hops run air-gapped (EgressPolicy.NONE), so the
            // host's overlay-egress enforcement state does not apply to them.
            sandboxLogCallback.report(new SandboxLogCallback.SandboxLogRecord(
                    ctx.getWorkflowExecutionId(),
                    nodeId,
                    nodeId,
                    ok ? "COMPLETED" : "FAILED",
                    exitCode,
                    wallMs,
                    language,
                    containerId,
                    null,
                    null,
                    errorMessage,
                    input == null ? null : List.of(Map.of("portValue", SYNTHETIC_IN_PORT, "value", input)),
                    output == null ? null : List.of(Map.of("portValue", SYNTHETIC_OUT_PORT, "value", output)),
                    null,
                    null,
                    failureKind,
                    null));
        } catch (Exception e) {
            log.debug("edge-transform: sandbox-log mirror skipped ({})", e.toString());
        }
    }

    /**
     * Removes {@code kind=jolt} directives that share an edge key with any {@code kind=script}
     * directive — APPLY-path complement to S1's collector omit.
     */
    PreparedInbound stripJoltCoveredByScripts(PreparedInbound prepared) {
        if (prepared == null || prepared.daemonDirectives() == null || prepared.daemonDirectives().isEmpty()) {
            return prepared == null ? PreparedInbound.empty() : prepared;
        }
        Set<String> scriptEdges = new HashSet<>();
        for (TransformationSpecDto s : prepared.daemonDirectives()) {
            if (isScript(s)) {
                scriptEdges.add(edgeKey(s));
            }
        }
        if (scriptEdges.isEmpty()) {
            return prepared;
        }
        List<TransformationSpecDto> kept = new ArrayList<>();
        boolean removedJolt = false;
        for (TransformationSpecDto s : prepared.daemonDirectives()) {
            if (s == null) {
                continue;
            }
            if ("jolt".equalsIgnoreCase(s.getKind()) && scriptEdges.contains(edgeKey(s))) {
                removedJolt = true;
                continue;
            }
            kept.add(s);
        }
        if (!removedJolt) {
            return prepared;
        }
        Map<String, Object> vars = new LinkedHashMap<>(
                prepared.inputVariables() == null ? Map.of() : prepared.inputVariables());
        if (kept.isEmpty()) {
            vars.remove(TransformDirectiveApplier.DIRECTIVE_KEY);
        } else {
            vars.put(TransformDirectiveApplier.DIRECTIVE_KEY, kept);
        }
        return new PreparedInbound(vars, kept,
                prepared.functionPortTransforms(), prepared.beAppliedSpecs());
    }

    private static String edgeKey(TransformationSpecDto s) {
        return StringUtils.defaultString(s.getSourceNodeId()) + "|"
                + StringUtils.defaultString(s.getTargetNodeId()) + "|"
                + StringUtils.defaultString(s.getSourcePortShortCode()) + "|"
                + StringUtils.defaultString(s.getTargetPortShortCode()) + "|"
                + StringUtils.defaultString(s.getDirection());
    }

    private record PendingEdge(
            TransformationSpecDto spec,
            String language,
            String targetPortKey,
            Object sanitizedInbound,
            String userScript,
            String sourceNodeId,
            String targetNodeId) {
    }
}
