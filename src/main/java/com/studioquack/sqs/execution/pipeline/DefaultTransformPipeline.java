package com.studioquack.sqs.execution.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.common.utils.flow.PortSchemaFilter;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.FileHandleRider;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.PinnedRowFolder;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Applies inbound transforms before a node runs and fans outputs out along its edges.
 */
@Slf4j
@Component
public class DefaultTransformPipeline implements TransformPipeline {

    private static final String FUNCTION_PORT_TRANSFORMS_KEY = "__sq_function_port_transforms__";
    private static final String APPLIED_INPUTS_KEY = "__sq_applied_inputs__";
    private static final Set<String> DAEMON_META_KEYS = Set.of(
            "stdout", "stderr", "exitCode",
            "$stdout", "$stderr", "$exitCode",
            APPLIED_INPUTS_KEY);

    private final ConnectionBuilder connectionBuilder;
    private final TransformationCollector transformationCollector;
    private final JoltTransformService joltTransformService;
    private final ObjectMapper objectMapper;
    private final com.studioquack.sqs.files.ParameterSpiller parameterSpiller;

    public DefaultTransformPipeline(ConnectionBuilder connectionBuilder,
                                    TransformationCollector transformationCollector,
                                    JoltTransformService joltTransformService,
                                    ObjectMapper objectMapper,
                                    com.studioquack.sqs.files.ParameterSpiller parameterSpiller) {
        this.connectionBuilder = connectionBuilder;
        this.transformationCollector = transformationCollector;
        this.joltTransformService = joltTransformService;
        this.objectMapper = objectMapper;
        this.parameterSpiller = parameterSpiller;
    }

    @Override
    public PreparedInbound prepareInbound(ExecutionContext ctx) {
        if (ctx == null) {
            return PreparedInbound.empty();
        }
        ctx.createCurrentNode();
        NodeDto currentNode = ctx.getCurrentNode();
        if (currentNode == null) {
            log.debug("prepareInbound: no currentNode resolved (nodeId={}); nothing to prepare",
                    ctx.getCurrentNodeId());
            return PreparedInbound.empty();
        }

        // ---- Phase 1: connection maps + pre-collect, BEFORE clearNodeAndEdges (Invariant 1) ----
        if (CollectionUtils.isNotEmpty(ctx.getNodes()) && CollectionUtils.isNotEmpty(ctx.getEdges())) {
            connectionBuilder.buildConnections(currentNode, ctx.getNodes(), ctx.getEdges());
        }
        collectInboundTransformSpecs(ctx);
        collectFunctionPortTransforms(ctx);
        // Needs the graph: which wires ride an edge whose DuckTape screen has mapping rows.
        Set<String> authoredWires = transformationCollector.authoredWireKeys(ctx);
        ctx.clearNodeAndEdges();

        // ---- Phase 2: pre-bake inbound JOLT on inputList BEFORE the __SQ_CONTEXT__ bake (Invariant 2) ----
        Set<InOutValueDto> shaped = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<TransformationSpecDto> beApplied = applyInboundJoltSpecsBeSide(ctx, shaped);

        // The screen defines the message: on an authored edge a wire with no rows of its own
        // contributes nothing — never its raw payload beside the fields the screen mapped.
        blankUnmappedAuthoredWires(ctx, authoredWires, shaped);

        // DuckTape Add-Node: a pinned source's row was JOLTed into the consuming port's shape on
        // its own (its spec is keyed on that source). Fold it into the message of the connection
        // line it was pinned on — connection lines decide the message count, attribute mappings
        // never do — so N lines still yield N messages and each carries every field its screen mapped.
        foldPinnedRows(ctx);

        // Flatten inputList → port map AFTER the pre-bake (Invariant 12).
        Map<String, Object> inputVariables = buildInputVariables(ctx);

        // Strip the control plane-applied + unmatched JOLT from the daemon-bound directive (Invariants 3, 4).
        List<TransformationSpecDto> daemonDirectives =
                attachTransformationDirective(ctx, inputVariables, beApplied);
        // Function-port lanes — emitted here exactly once, the single owner (Invariant 10).
        attachFunctionPortTransforms(ctx, inputVariables);

        return new PreparedInbound(inputVariables, daemonDirectives,
                ctx.getFunctionPortTransforms() == null ? Collections.emptyMap() : ctx.getFunctionPortTransforms(),
                beApplied);
    }

    @Override
    public PreparedInbound filterInboundAgainstPortSchemas(ExecutionContext ctx, PreparedInbound prepared) {
        if (prepared == null) {
            return PreparedInbound.empty();
        }
        if (ctx == null) {
            return prepared;
        }
        NodeDto node = ctx.getCurrentNode();
        if (node == null || CollectionUtils.isEmpty(node.getInputList())) {
            ctx.setDroppedInputParameters(Collections.emptyList());
            return prepared;
        }
        Set<String> dropped = new TreeSet<>();
        for (InOutValueDto entry : node.getInputList()) {
            String portKey = StringUtils.firstNonEmpty(entry.getTargetPortValue(), entry.getPortValue());
            if (StringUtils.isEmpty(portKey)) {
                continue;
            }
            // repair: a row folded from the operator's apply-inputs override is exempt from
            // schema filtering — every field on it is operator-supplied by definition, so it must
            // reach the node and the record, never be stripped / reported as an unknown parameter.
            if (entry.isOperatorOverride()) {
                continue;
            }
            Object value = entry.getValue();
            PortDto port = node.getData() == null ? null : node.getData().findPortByPortName(portKey);
            if (port == null || CollectionUtils.isEmpty(port.getSchema()) || value == null) {
                continue;
            }
            try {
                Object filtered = PortSchemaFilter.filter(port.getSchema(), value);
                // Schema children (kind/mime_type/source/data/name) omit fileId; ride the
                // pre-filter handle back onto map-shaped file values. PortSchemaFilter stays
                // ClientFileStore-free (common-module / control plane-copy drift).
                filtered = FileHandleRider.rideHandles(value, filtered);
                collectDroppedTopLevelKeys(dropped, portKey, value, filtered);
                entry.setValue(filtered);
            } catch (Exception e) {
                log.warn("Inbound port-schema filter failed node={} port={}; keeping unfiltered: {}",
                        node.getId(), portKey, e.getMessage());
            }
        }
        ctx.setDroppedInputParameters(dropped.isEmpty() ? Collections.emptyList() : new ArrayList<>(dropped));

        Map<String, Object> merged = new LinkedHashMap<>(buildInputVariables(ctx));
        for (Map.Entry<String, Object> e : prepared.inputVariables().entrySet()) {
            String key = e.getKey();
            if (key != null && (key.startsWith("__sq_") || DAEMON_META_KEYS.contains(key))) {
                merged.put(key, e.getValue());
            }
        }
        return new PreparedInbound(merged, prepared.daemonDirectives(),
                prepared.functionPortTransforms(), prepared.beAppliedSpecs());
    }

    /** Records {@code portKey.paramName} for top-level map keys removed by {@link PortSchemaFilter}. */
    private static void collectDroppedTopLevelKeys(Set<String> dropped, String portKey,
                                                     Object before, Object after) {
        if (!(before instanceof Map<?, ?> beforeMap)) {
            return;
        }
        Set<String> afterKeys = new java.util.HashSet<>();
        if (after instanceof Map<?, ?> afterMap) {
            for (Object k : afterMap.keySet()) {
                if (k != null) {
                    afterKeys.add(String.valueOf(k));
                }
            }
        }
        for (Object k : beforeMap.keySet()) {
            if (k == null) {
                continue;
            }
            String param = String.valueOf(k);
            if (!afterKeys.contains(param)) {
                dropped.add(portKey + "." + param);
            }
        }
    }

    /* ---- Phase 1 pre-collect (best-effort; failures are warnings, Invariant 11) ---- */

    private void collectInboundTransformSpecs(ExecutionContext ctx) {
        try {
            ctx.setInboundTransformSpecs(transformationCollector.collectForCurrentNode(ctx));
        } catch (Exception e) {
            log.warn("Inbound transform spec pre-collection failed node={}: {}",
                    ctx.getCurrentNodeIdSafe(), e.getMessage());
            ctx.setInboundTransformSpecs(null);
        }
    }

    private void collectFunctionPortTransforms(ExecutionContext ctx) {
        try {
            ctx.setFunctionPortTransforms(transformationCollector.collectFunctionPortTransforms(ctx));
        } catch (Exception e) {
            log.warn("Function-port transform pre-collection failed node={}: {}",
                    ctx.getCurrentNodeIdSafe(), e.getMessage());
            ctx.setFunctionPortTransforms(null);
        }
    }

    /* ---- Phase 2 pre-bake ---- */

    /**
     * Applies each inbound JOLT spec in place onto the matching {@code currentNode.inputList}
     * entry's value, BEFORE the context is baked. Returns the identity set of specs that
     * matched at least one entry so {@link #attachTransformationDirective} can strip them by
     * IdentityHashMap identity (Invariant 3). Best-effort — a JOLT failure is logged and the
     * value is left untouched (Invariant 11).
     */
    /**
     * @param shaped receives (by identity) every inputList entry at least one JOLT spec shaped, so the
     *               caller can tell an unmapped wire on an authored screen from a shaped one
     */
    private Set<TransformationSpecDto> applyInboundJoltSpecsBeSide(ExecutionContext ctx, Set<InOutValueDto> shaped) {
        Set<TransformationSpecDto> applied = Collections.newSetFromMap(new IdentityHashMap<>());
        NodeDto node = ctx.getCurrentNode();
        if (node == null) {
            return applied;
        }
        List<TransformationSpecDto> specs = ctx.getInboundTransformSpecs();
        if (CollectionUtils.isEmpty(specs)) {
            return applied;
        }
        List<InOutValueDto> inputList = node.getInputList();
        if (CollectionUtils.isEmpty(inputList)) {
            return applied;
        }
        //when any inbound script covers an edge, never JVM-JOLT that wire (script replaces
        // the mapping entirely — before AND after the transform sandbox).
        Set<String> scriptCovered = scriptCoveredEdgeKeys(specs);
        for (TransformationSpecDto spec : specs) {
            if (spec == null || !"jolt".equalsIgnoreCase(spec.getKind())) {
                continue;
            }
            if (scriptCovered.contains(edgeKey(spec))) {
                continue;
            }
            if (CollectionUtils.isEmpty(spec.getJoltSpec())) {
                continue;
            }
            // Only the inbound "input" lane targets currentNode.inputList; skip explicit outbound.
            if (StringUtils.isNotEmpty(spec.getDirection()) && !"input".equalsIgnoreCase(spec.getDirection())) {
                continue;
            }
            boolean matchedAny = false;
            for (InOutValueDto entry : inputList) {
                if (!inputListEntryMatchesSpec(entry, spec)) {
                    continue;
                }
                try {
                    entry.setValue(joltTransformService.transform(entry.getValue(), spec.getJoltSpec()));
                    matchedAny = true;
                    shaped.add(entry);
                } catch (Exception e) {
                    log.warn("BE-side JOLT failed node={} edge={}->{}: {}",
                            node.getId(), spec.getSourceNodeId(), spec.getTargetNodeId(), e.getMessage());
                }
            }
            if (matchedAny) {
                applied.add(spec);
            }
        }
        return applied;
    }

    /**
     * The screen defines the message. A wire row that rides an authored edge ({@code producer|port}
     * in {@code authoredWires}) but that no spec shaped — the author connected other tiles on that
     * screen and left this wire's attributes alone — becomes an empty object, so the pinned fold
     * lands the screen's fields on a clean message instead of beside the wire's raw payload.
     * Pinned rows and wires on untouched edges (legacy raw pass-through) are left alone.
     */
    private void blankUnmappedAuthoredWires(ExecutionContext ctx, Set<String> authoredWires,
                                            Set<InOutValueDto> shaped) {
        NodeDto node = ctx.getCurrentNode();
        if (authoredWires == null || authoredWires.isEmpty() || node == null
                || CollectionUtils.isEmpty(node.getInputList())) {
            return;
        }
        Map<String, String> pinned = ctx.getPinnedInputCarriers() == null ? Map.of() : ctx.getPinnedInputCarriers();
        for (InOutValueDto entry : node.getInputList()) {
            if (entry == null || shaped.contains(entry)
                    || pinned.containsKey(ForeignEdgeInputResolver.bindingKey(entry))) {
                continue;
            }
            String port = StringUtils.firstNonEmpty(entry.getTargetPortValue(), entry.getPortValue());
            if (StringUtils.isEmpty(entry.getNodeId()) || StringUtils.isEmpty(port)
                    || !authoredWires.contains(entry.getNodeId() + "|" + port)) {
                continue;
            }
            entry.setValue(new LinkedHashMap<String, Object>());
            log.info("pipeline.authored-screen node={} wire={} port={}: no mapping rows for this wire on an "
                    + "authored screen — its raw payload is not passed through", node.getId(), entry.getNodeId(), port);
        }
    }

    /**
     * One DuckTape screen → one message: every pinned (Add-Node / Globals) row is folded into the
     * message of the connection line it was pinned on, after both carry the port's shape. Anchor
     * rules and the N-lines-N-messages guarantee live on {@link PinnedRowFolder}.
     */
    private void foldPinnedRows(ExecutionContext ctx) {
        NodeDto node = ctx.getCurrentNode();
        if (node == null) {
            return;
        }
        PinnedRowFolder.Result result = PinnedRowFolder.fold(
                node.getInputList(), ctx.getPinnedInputCarriers(), node.getId());
        if (result.changed()) {
            node.setInputList(result.rows());
        }
    }

    /** Conservative match — an unset spec field is "any" (mirrors control plane + Go). */
    private boolean inputListEntryMatchesSpec(InOutValueDto entry, TransformationSpecDto spec) {
        if (entry == null || spec == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getSourceNodeId()) && !spec.getSourceNodeId().equals(entry.getNodeId())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getTargetNodeId()) && !spec.getTargetNodeId().equals(entry.getTargetNodeId())) {
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

    private boolean hasMatchingInputEntry(ExecutionContext ctx, TransformationSpecDto spec) {
        NodeDto node = ctx.getCurrentNode();
        if (node == null || CollectionUtils.isEmpty(node.getInputList())) {
            return false;
        }
        for (InOutValueDto entry : node.getInputList()) {
            if (inputListEntryMatchesSpec(entry, spec)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Flattens {@code currentNode.inputList} into the port map the helper SDK exposes,
     * keyed {@code targetPortValue || portValue} (Invariant 12). Always mutable so the
     * directive attach can inject reserved keys.
     */
    private Map<String, Object> buildInputVariables(ExecutionContext ctx) {
        Map<String, Object> out = new LinkedHashMap<>();
        NodeDto node = ctx.getCurrentNode();
        if (node == null || CollectionUtils.isEmpty(node.getInputList())) {
            return out;
        }
        for (InOutValueDto in : node.getInputList()) {
            String key = StringUtils.firstNonEmpty(in.getTargetPortValue(), in.getPortValue());
            if (StringUtils.isEmpty(key)) {
                continue;
            }
            out.put(key, in.getValue());
        }
        return out;
    }

    /**
 * Emits the daemon-bound {@code __sq_transformation__} directive, EXCLUDING specs already
 * applied the control plane-side (Invariant 3, IdentityHashMap identity) and inbound JOLT specs that
 * match no {@code inputList} entry (Invariant 4 — an unmatched inbound JOLT on the wire
 * would trigger the daemon's whole-ctx fallback and wipe {@code currentNode}/
 * {@code connectionList}). Script specs and unmatched-removed JOLT flow through.
 *
 * @return the spec list left for the sandbox (also written under the reserved key).
 */
    private List<TransformationSpecDto> attachTransformationDirective(
            ExecutionContext ctx, Map<String, Object> inputVariables, Set<TransformationSpecDto> beApplied) {
        List<TransformationSpecDto> specs = ctx.getInboundTransformSpecs();
        if (CollectionUtils.isEmpty(specs)) {
            return Collections.emptyList();
        }
        List<TransformationSpecDto> forDaemon = new ArrayList<>(specs.size());
        for (TransformationSpecDto s : specs) {
            if (s == null) {
                continue;
            }
            if (beApplied != null && beApplied.contains(s)) {
                continue; // already applied the control plane-side — never re-ship (double-apply guard)
            }
            if ("jolt".equalsIgnoreCase(s.getKind())) {
                if (StringUtils.isNotEmpty(s.getDirection()) && !"input".equalsIgnoreCase(s.getDirection())) {
                    continue;
                }
                if (!hasMatchingInputEntry(ctx, s)) {
                    continue; // no matching input → don't put an unmatched JOLT on the wire
                }
            }
            forDaemon.add(s);
        }
        if (!forDaemon.isEmpty()) {
            inputVariables.put(TransformDirectiveApplier.DIRECTIVE_KEY, forDaemon);
        }
        return forDaemon;
    }

    private void attachFunctionPortTransforms(ExecutionContext ctx, Map<String, Object> inputVariables) {
        Map<String, ?> map = ctx.getFunctionPortTransforms();
        if (map != null && !map.isEmpty()) {
            inputVariables.put(FUNCTION_PORT_TRANSFORMS_KEY, map);
        }
    }

    /* ================================================================== */
    /* Output handling — port fan-out + schema filter ONLY (Invariant 14). */
    /* ================================================================== */

    @Override
    public void finalizeOutputs(ExecutionContext ctx, Map<String, Object> sandboxOutputs) {
        if (ctx == null || sandboxOutputs == null || sandboxOutputs.isEmpty()) {
            return;
        }
        NodeDto currentNode = ctx.getCurrentNode();
        if (currentNode == null) {
            return;
        }
        // The applied-inputs overlay is diagnostic only — remove it so it is never fanned out.
        sandboxOutputs.remove(APPLIED_INPUTS_KEY);
        applyOutputs(ctx, sandboxOutputs);
    }

    private void applyOutputs(ExecutionContext ctx, Map<String, Object> outputs) {
        NodeDto currentNode = ctx.getCurrentNode();
        List<InOutValueDto> outputList = new ArrayList<>();
        for (Map.Entry<String, Object> entry : outputs.entrySet()) {
            String portValue = entry.getKey();
            if (portValue == null || portValue.startsWith("$")) {
                continue;
            }
            if (DAEMON_META_KEYS.contains(portValue)) {
                continue;
            }
            // Schema filter drops fields outside the declared port shape — NO JOLT here.
            // Spill the RAW envelope first so file ContentBlocks become handles on the helper
            // return path even when a stale deployed schema strips them from the wire value.
            Object raw = dedupeFileFields(entry.getValue());
            // repair: a port whose value came from the operator's apply-inputs OUTPUT
            // override (Skip & Continue) is exempt — operator-supplied fields must fan out.
            Object filtered = isOperatorOverridePort(ctx, portValue)
                    ? raw
                    : filterOutputAgainstPortSchema(currentNode, portValue, raw);
            Object sizedRaw = parameterSpiller.spillIfOversized(raw, portValue, ctx.getWorkflowExecutionId());
            // Filtered value still fans out to downstream wires (Invariant 14). A stale schema
            // that matches none of the live keys yields {} here; unfilteredValue keeps the payload.
            Object sized = parameterSpiller.spillIfOversized(filtered, portValue, ctx.getWorkflowExecutionId());
            outputList.addAll(fanOut(currentNode, portValue, sized, sizedRaw));
        }
        // Data-flow checkpoint (shape only): which ports the sandbox returned and how many wire
        // DTOs survived fan-out. receivedPorts non-empty + fannedOut=0 is the schema-filter /
        // fan-out drop tell; receivedPorts empty means the transport delivered nothing.
        if (log.isInfoEnabled()) {
            log.info("pipeline.outputs node={} receivedPorts={} fannedOut={}",
                    currentNode.getId(), outputs.keySet(), outputList.size());
        }
        // Preserve bridge-set outputs when the sandbox returned only metadata.
        if (outputList.isEmpty() && CollectionUtils.isNotEmpty(currentNode.getOutputList())) {
            return;
        }
        currentNode.setOutputList(outputList);
    }

    /**
     * Dedupe the double-published file list in LLM-style envelopes: older in-container node-library
     * builds copy the generated files into BOTH the dedicated file field ({@code outputFiles}) and
     * the {@code result} map ({@code result.output_files}), so run-log panels render the same file
     * twice and the spiller writes it once per copy. Current libraries strip at the source; this
     * keeps envelopes from still-running older library containers idempotent. Only strips when the
     * sibling file field is a non-empty list — a {@code result} carrying the ONLY copy is untouched.
     */
    static Object dedupeFileFields(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return value;
        }
        Object files = map.get("outputFiles");
        if (!(files instanceof List<?> list) || list.isEmpty()) {
            files = map.get("files");
            if (!(files instanceof List<?> list2) || list2.isEmpty()) {
                return value;
            }
        }
        Object result = map.get("result");
        if (!(result instanceof Map<?, ?> resultMap)
                || (!resultMap.containsKey("output_files") && !resultMap.containsKey("outputFiles"))) {
            return value;
        }
        Map<String, Object> stripped = new LinkedHashMap<>();
        resultMap.forEach((k, v) -> {
            if (k != null && !"output_files".equals(k.toString()) && !"outputFiles".equals(k.toString())) {
                stripped.put(k.toString(), v);
            }
        });
        Map<String, Object> copy = new LinkedHashMap<>();
        map.forEach((k, v) -> copy.put(k == null ? null : k.toString(), v));
        copy.put("result", stripped);
        return copy;
    }

    /** Skip &amp; Continue fan-out: {@code portValue} was supplied by the operator's output override. */
    private static boolean isOperatorOverridePort(ExecutionContext ctx, String portValue) {
        return ctx != null && ctx.getOperatorOverrideOutputPorts() != null
                && ctx.getOperatorOverrideOutputPorts().contains(portValue);
    }

    private Object filterOutputAgainstPortSchema(NodeDto currentNode, String portValue, Object value) {
        if (currentNode == null || currentNode.getData() == null) {
            return value;
        }
        PortDto port = currentNode.getData().findPortByPortName(portValue);
        if (port == null || CollectionUtils.isEmpty(port.getSchema())) {
            return value;
        }
        try {
            return PortSchemaFilter.filter(port.getSchema(), value);
        } catch (Exception e) {
            log.warn("Output port-schema filter failed node={} port={}; emitting unfiltered: {}",
                    currentNode.getId(), portValue, e.getMessage());
            return value;
        }
    }

    /** One {@link InOutValueDto} per downstream wire; a fallback seed when the port is unwired. */
    private List<InOutValueDto> fanOut(NodeDto currentNode, String portValue, Object value,
                                       Object unfiltered) {
        List<InOutValueDto> out = new ArrayList<>();
        InOutValueDto seed = new InOutValueDto(portValue, valueToWire(value));
        seed.setNodeId(currentNode.getId());
        seed.setPortId(currentNode.findPortIdByPortValue(portValue));
        // Helper harvest reads this when targetPortValue is an internal end-sub wire and/or
        // the filtered map is {}. Downstream nodes still consume {@code value} only.
        seed.setUnfilteredValue(valueToWire(unfiltered));

        List<ConnectionDto> connections = currentNode.findConnectionListByPortValue(portValue);
        if (CollectionUtils.isEmpty(connections)) {
            out.add(seed);
            return out;
        }
        for (ConnectionDto connection : connections) {
            for (ConnectionDetailDto detail : connection.getConnectionDetailList()) {
                InOutValueDto clone = seed.clone();
                clone.setTargetNodeId(detail.getNodeId());
                PortDto target = detail.getPortDto();
                if (target != null) {
                    clone.setTargetPortId(target.getId());
                    clone.setTargetPortValue(target.getValue());
                }
                clone.setGateway("gateway".equals(connection.getNodeType()));
                out.add(clone);
            }
        }
        return out;
    }

    /** Structure-preserving wire form (no stringify of Maps/Lists); tolerant JSON reparse of strings. */
    private Object valueToWire(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?> || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof CharSequence cs) {
            String raw = cs.toString();
            String trimmed = raw.trim();
            if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                    || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                try {
                    return objectMapper.readValue(trimmed, Object.class);
                } catch (Exception jacksonErr) {
                    String repaired = trimmed.replace("\\'", "\u0001").replace("'", "\"").replace("\u0001", "'");
                    try {
                        return objectMapper.readValue(repaired, Object.class);
                    } catch (Exception ignore) {
                        return raw;
                    }
                }
            }
            return raw;
        }
        try {
            return objectMapper.convertValue(value, Object.class);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private static Set<String> scriptCoveredEdgeKeys(List<TransformationSpecDto> specs) {
        Set<String> out = new java.util.HashSet<>();
        if (CollectionUtils.isEmpty(specs)) {
            return out;
        }
        for (TransformationSpecDto s : specs) {
            if (s != null && "script".equalsIgnoreCase(s.getKind())) {
                out.add(edgeKey(s));
            }
        }
        return out;
    }

    private static String edgeKey(TransformationSpecDto s) {
        return StringUtils.defaultString(s.getSourceNodeId()) + "|"
                + StringUtils.defaultString(s.getTargetNodeId()) + "|"
                + StringUtils.defaultString(s.getSourcePortShortCode()) + "|"
                + StringUtils.defaultString(s.getTargetPortShortCode()) + "|"
                + StringUtils.defaultString(s.getDirection());
    }
}
