package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.SubPiInvocation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DuckTape "Add Node" delivery. A mapping row pinned on edge B→C can name a source node A that
 * has <b>no</b> INPUT/OUTPUT/FUNCTION wire into C (the Add-Node tile). A's outputs are then never
 * fanned out towards C, so the plain {@code loadInputsTargetingNode} scan — which only sees
 * entries addressed {@code targetNodeId = C} — delivers nothing for A, and A's mapping rows die
 * unmatched.
 *
 * <p>The fix applied here mirrors the sequential contract: when B completes and C is triggered,
 * A's persisted port payload is re-addressed onto C's input list as an extra message, exactly as
 * if a wire A→C existed. The per-source JOLT groups emitted by
 * {@link TransformationCollector#collectForCurrentNode} then shape A's message off A's payload,
 * and the helper SDK exposes every message on the port as a list
 * ({@code getInputValuesByPortValue}) — the same multi-connection shape {@code ai agent.py}
 * already merges.
 *
 * <p>Two virtual source cards — "Workflow Globals" ({@code $workflowGlobals}) and "Project
 * Globals" ({@code $projectGlobals}) — never hit the store: their rows name global variable keys
 * via {@code sourcePath}, and each (virtual source, target port) group materializes ONE message
 * whose value is the {@code {key: value}} map read off the sealed context's variable maps.
 *
 * <p>A pinned node that lives inside a function-port sub-workflow has no parent-scope outputs.
 * Those misses are resolved through the sub-PI invocation index: the inbound edge's source is
 * the caller, its child executions (and nested children, BFS, cycle-safe, cap 256) are scanned
 * for the pinned node only, and every invocation's matched port value is merged into ONE
 * list-wrapped message — even a single invocation — so the consumer sees N runs as a list.
 * Parent-scope hits stay unwrapped.
 *
 * <p>Scope guards:
 * <ul>
 *   <li>Delivery is language-agnostic: the raw pinned message rides onto C's input list whatever
 *       the edge's effective transform language is — JOLT shaping, when configured, stays the
 *       collector's job.</li>
 *   <li>Only nodes explicitly pinned by a mapping row are read; nothing else on the graph can
 *       leak template data into this connection scope.</li>
 *   <li>A producer that never ran (or failed — failed nodes persist no outputs) delivers
 *       nothing; the consuming node then runs without that message, never with debris.</li>
 * </ul>
 *
 * <p>Stateless static helper: both dispatch delegates ({@code ScriptDispatchDelegate},
 * {@code DoclingDelegate}) call it right after the upstream-input load, before
 * {@code prepareInbound} / the inbound pre-bake, passing the sealed context's global variable
 * maps straight through.
 */
public final class ForeignEdgeInputResolver {

    private static final Logger log = LoggerFactory.getLogger(ForeignEdgeInputResolver.class);

    /** Reserved {@code sourceNodeId} the FE persists for the virtual "Workflow Globals" card. */
    private static final String WORKFLOW_GLOBALS_ID = "$workflowGlobals";
    /** Reserved {@code sourceNodeId} the FE persists for the virtual "Project Globals" card. */
    private static final String PROJECT_GLOBALS_ID = "$projectGlobals";
    /** Synthetic port identity carried by every materialized globals message. */
    private static final String GLOBALS_PORT_ID = "$globals";
    private static final String GLOBALS_PORT_VALUE = "GLOBALS";
    /** Hard cap on BFS-visited child executions per (parent, caller) walk. */
    private static final int SUB_PI_VISIT_CAP = 256;

    private ForeignEdgeInputResolver() {
    }

    /**
     * Stable identity of an input wire (source node/port → target node/port), mirroring
     * {@code H2ExecutionContextStore#bindingKey}. {@code value} is deliberately excluded so the same
     * wire from two sources dedups to one entry. Shared by the dispatch merge and the pipeline's
     * pinned-row fold so both sides name a row the same way.
     */
    public static String bindingKey(InOutValueDto dto) {
        return dto.getNodeId() + "|" + dto.getPortId() + "|" + dto.getPortValue()
                + "|" + dto.getTargetNodeId() + "|" + dto.getTargetPortId();
    }

    /**
     * What the resolver delivered plus, per delivered row (by identity), the <b>carrier</b>: the
     * source node id of the connection line whose DuckTape screen the tile was pinned on. A pinned
     * tile enriches THAT line's message and no other — with two real wires into one port (A→C and
     * B→C) a tile pinned on the B→C screen must fold into B's message, never A's.
     */
    public record Resolution(List<InOutValueDto> rows, Map<InOutValueDto, String> carrierByRow) {
        public static Resolution empty() {
            return new Resolution(List.of(), Map.of());
        }
    }

    /**
     * {@code bindingKey → carrier} for the rows in {@code merged} that are (by identity) rows this
     * resolver delivered. A pinned row the dispatch merge dropped in favour of an existing wire with
     * the same key is not pinned any more — its key would otherwise fold the primary wire into itself.
     */
    public static Map<String, String> pinnedCarriers(List<InOutValueDto> merged, Resolution resolution) {
        if (merged == null || merged.isEmpty() || resolution == null || resolution.carrierByRow().isEmpty()) {
            return Map.of();
        }
        Map<String, String> carriers = new LinkedHashMap<>();
        for (InOutValueDto row : merged) {
            if (row == null) {
                continue;
            }
            String carrier = resolution.carrierByRow().get(row);
            if (carrier != null) {
                carriers.put(bindingKey(row), carrier);
            }
        }
        return carriers;
    }

    /** One foreign mapping row plus the edge that carries it (the edge owns the target port). */
    private record ForeignRow(EdgeDto edge, EdgeTransformDto row) {
    }

    /**
     * Resolves the extra input messages pinned-node outputs (and the virtual global cards)
     * contribute to {@code consumingNodeId}.
     *
     * @param store             the sealed execution-context store (read-only here)
     * @param ref               the consuming node's context ref (provides the execution id)
     * @param consumingNodeId   the node about to run (C in the example above)
     * @param graphEdges        the deployed graph edges captured before {@code prepareInbound}
     * @param workflowVariables the sealed context's workflow globals (may be null)
     * @param projectVariables  the sealed context's project globals (may be null)
     * @return re-addressed clones of the pinned nodes' persisted outputs plus one materialized
     *         message per (virtual global source, target port) — never live references
     */
    public static List<InOutValueDto> resolve(ExecutionContextStore store, ProcessContextRef ref,
                                              String consumingNodeId, List<EdgeDto> graphEdges,
                                              Map<String, Object> workflowVariables,
                                              Map<String, Object> projectVariables) {
        return resolveWithCarriers(store, ref, consumingNodeId, graphEdges, workflowVariables, projectVariables)
                .rows();
    }

    /** As {@link #resolve}, and names each delivered row's carrier (see {@link Resolution}). */
    public static Resolution resolveWithCarriers(ExecutionContextStore store, ProcessContextRef ref,
                                                 String consumingNodeId, List<EdgeDto> graphEdges,
                                                 Map<String, Object> workflowVariables,
                                                 Map<String, Object> projectVariables) {
        if (store == null || ref == null || StringUtils.isBlank(consumingNodeId)
                || StringUtils.isBlank(ref.getExecutionId()) || CollectionUtils.isEmpty(graphEdges)) {
            return Resolution.empty();
        }
        Map<InOutValueDto, String> carrierByRow = new java.util.IdentityHashMap<>();

        // 1) Foreign mapping rows on this node's inbound edges — any transform language. Rows
        //    sourced from the virtual global cards are split off here: they read the sealed
        //    variable maps, never the persisted node outputs.
        List<ForeignRow> foreignRows = new ArrayList<>();
        List<ForeignRow> globalRows = new ArrayList<>();
        Set<String> foreignNodeIds = new LinkedHashSet<>();
        for (EdgeDto edge : graphEdges) {
            if (edge == null || !consumingNodeId.equals(edge.getTarget())) {
                continue;
            }
            EdgeDataDto data = edge.getData();
            if (data == null || CollectionUtils.isEmpty(data.getEdgeTransforms())) {
                continue;
            }
            for (EdgeTransformDto row : data.getEdgeTransforms()) {
                if (row == null) {
                    continue;
                }
                String src = row.getSourceNodeId();
                if (StringUtils.isBlank(src) || src.equals(edge.getSource())) {
                    continue; // the edge's own source arrives over the real wire
                }
                if (StringUtils.isNotEmpty(row.getTargetNodeId()) && !consumingNodeId.equals(row.getTargetNodeId())) {
                    continue; // other tab/lane rows (e.g. FUNCTION response lane)
                }
                if (WORKFLOW_GLOBALS_ID.equals(src) || PROJECT_GLOBALS_ID.equals(src)) {
                    globalRows.add(new ForeignRow(edge, row));
                    continue;
                }
                foreignRows.add(new ForeignRow(edge, row));
                foreignNodeIds.add(src);
            }
        }
        if (foreignRows.isEmpty() && globalRows.isEmpty()) {
            return Resolution.empty();
        }

        List<InOutValueDto> out = new ArrayList<>();

        // 2) One store scan, crypto-opened only for the pinned nodes (latest-wins, retry-folded).
        //    Skipped entirely when only virtual global rows target this node.
        if (!foreignRows.isEmpty()) {
            Map<String, List<InOutValueDto>> producedByNode =
                    store.collectOutputsByNode(ref.getExecutionId(), foreignNodeIds);

            // 3) One message per (foreign node, source port, edge target port): the entry carries
            //    the producing port's whole payload so the per-source JOLT group shapes every
            //    mapped row off it. Re-addressed onto the consuming node so spec matching + the
            //    SDK's targetPortValue filter see it as an ordinary inbound wire.
            Set<String> delivered = new HashSet<>();
            for (ForeignRow fr : foreignRows) {
                List<InOutValueDto> produced = producedByNode.get(fr.row().getSourceNodeId());
                if (CollectionUtils.isEmpty(produced)) {
                    continue;
                }
                InOutValueDto match = findProducedPortEntry(produced, fr.row());
                if (match == null) {
                    continue;
                }
                String targetPortValue = extractShortCode(fr.edge().getTargetHandle());
                String dedupKey = fr.row().getSourceNodeId() + '|' + match.getPortId() + '|' + targetPortValue;
                if (!delivered.add(dedupKey)) {
                    continue;
                }
                InOutValueDto clone = match.clone();
                clone.setTargetNodeId(consumingNodeId);
                clone.setTargetPortId(fr.row().getTargetPortId());
                clone.setTargetPortValue(targetPortValue);
                clone.setGateway(false);
                out.add(clone);
                carrierByRow.put(clone, fr.edge().getSource());
            }

            // 4) Missed pinned nodes (null/empty in parent scope) resolve via the function-port
            //    invocation index. A parent-scope producer is a HIT even when a particular row
            //    fails port matching — that node never falls through here. Soft-fail: a store
            //    exception must not erase what the parent-scope loop already delivered.
            Set<String> missedNodeIds = missedPinnedNodeIds(foreignNodeIds, producedByNode);
            if (!missedNodeIds.isEmpty()) {
                try {
                    out.addAll(resolveMissedViaSubPiIndex(store, ref, consumingNodeId,
                            foreignRows, missedNodeIds, delivered, carrierByRow));
                } catch (RuntimeException ignored) {
                    log.warn("foreign-edge: sub-pi index walk failed execution={} missed={}",
                            ref.getExecutionId(), missedNodeIds.size());
                }
            }
        }

        out.addAll(materializeGlobalsMessages(globalRows, consumingNodeId, workflowVariables, projectVariables,
                carrierByRow));
        return new Resolution(out, carrierByRow);
    }

    /**
     * Pinned ids whose parent-scope collect returned null or empty. A non-empty parent list is a
     * HIT for every row of that node — even rows that later fail {@link #findProducedPortEntry}.
     */
    private static Set<String> missedPinnedNodeIds(Set<String> foreignNodeIds,
                                                   Map<String, List<InOutValueDto>> producedByNode) {
        Set<String> missed = new LinkedHashSet<>();
        for (String nodeId : foreignNodeIds) {
            if (CollectionUtils.isEmpty(producedByNode.get(nodeId))) {
                missed.add(nodeId);
            }
        }
        return missed;
    }

    /**
     * Walks the invocation index for each missed-node row and delivers one list-wrapped message
     * per (node, matched port, edge target port). Parent-scope {@code delivered} keys are reused
     * so an index hit never duplicates a parent-scope delivery.
     */
    private static List<InOutValueDto> resolveMissedViaSubPiIndex(ExecutionContextStore store,
                                                                 ProcessContextRef ref,
                                                                 String consumingNodeId,
                                                                 List<ForeignRow> foreignRows,
                                                                 Set<String> missedNodeIds,
                                                                 Set<String> delivered,
                                                                 Map<InOutValueDto, String> carrierByRow) {
        List<InOutValueDto> out = new ArrayList<>();
        Map<String, List<String>> subtreeByCaller = new HashMap<>();
        for (ForeignRow fr : foreignRows) {
            String pinnedId = fr.row().getSourceNodeId();
            if (!missedNodeIds.contains(pinnedId)) {
                continue;
            }
            String callerNodeId = fr.edge().getSource();
            String cacheKey = ref.getExecutionId() + '|' + callerNodeId;
            List<String> childExecIds = subtreeByCaller.computeIfAbsent(cacheKey,
                    k -> collectCallerSubtree(store, ref.getExecutionId(), callerNodeId));
            List<InOutValueDto> matches = new ArrayList<>();
            for (String childId : childExecIds) {
                Map<String, List<InOutValueDto>> byNode =
                        store.collectOutputsByNode(childId, Set.of(pinnedId));
                List<InOutValueDto> produced = byNode == null ? null : byNode.get(pinnedId);
                if (CollectionUtils.isEmpty(produced)) {
                    continue;
                }
                InOutValueDto match = findProducedPortEntry(produced, fr.row());
                if (match == null) {
                    continue;
                }
                matches.add(match);
            }
            if (matches.isEmpty()) {
                continue;
            }
            InOutValueDto first = matches.get(0);
            String targetPortValue = extractShortCode(fr.edge().getTargetHandle());
            String dedupKey = fr.row().getSourceNodeId() + '|' + first.getPortId() + '|' + targetPortValue;
            if (!delivered.add(dedupKey)) {
                continue;
            }
            InOutValueDto wrapped = listWrappedClone(first, matches, consumingNodeId, fr.row().getTargetPortId(),
                    targetPortValue);
            out.add(wrapped);
            carrierByRow.put(wrapped, fr.edge().getSource());
        }
        return out;
    }

    /**
     * BFS over the invocation index. Seed = parent's invocations whose {@code callerNodeId}
     * equals the edge source; nested levels include every child (no caller filter). Cycles
     * are skipped; the walk stops at {@link #SUB_PI_VISIT_CAP}.
     */
    private static List<String> collectCallerSubtree(ExecutionContextStore store,
                                                     String parentExecutionId,
                                                     String callerNodeId) {
        List<String> included = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        ArrayDeque<String> queue = new ArrayDeque<>();
        boolean capHit = enqueueInvocations(store.subPiInvocationsOf(parentExecutionId),
                callerNodeId, true, visited, included, queue);
        while (!capHit && !queue.isEmpty()) {
            capHit = enqueueInvocations(store.subPiInvocationsOf(queue.poll()),
                    null, false, visited, included, queue);
        }
        if (capHit) {
            log.warn("foreign-edge: sub-pi index visit cap parent={} visited={}",
                    parentExecutionId, visited.size());
        }
        return included;
    }

    /**
     * Offers each invocation's child execution id into {@code visited}/{@code included}/{@code queue}.
     *
     * @return true when the visit cap was hit (caller must stop expanding)
     */
    private static boolean enqueueInvocations(List<SubPiInvocation> invocations,
                                              String callerNodeId,
                                              boolean filterByCaller,
                                              Set<String> visited,
                                              List<String> included,
                                              ArrayDeque<String> queue) {
        if (invocations == null) {
            return false;
        }
        for (SubPiInvocation inv : invocations) {
            if (inv == null || StringUtils.isBlank(inv.childExecutionId())) {
                continue;
            }
            if (filterByCaller && !StringUtils.equals(callerNodeId, inv.callerNodeId())) {
                continue;
            }
            if (visited.contains(inv.childExecutionId())) {
                continue;
            }
            visited.add(inv.childExecutionId());
            included.add(inv.childExecutionId());
            if (visited.size() >= SUB_PI_VISIT_CAP) {
                return true;
            }
            queue.add(inv.childExecutionId());
        }
        return false;
    }

    /**
     * Clones the first invocation's matched entry as the carrier and list-wraps every
     * invocation's value (always a list, even of size 1). Values are deep-copied so the
     * store's live trees are never aliased onto the inbound list.
     */
    private static InOutValueDto listWrappedClone(InOutValueDto first,
                                                  List<InOutValueDto> matches,
                                                  String consumingNodeId,
                                                  String targetPortId,
                                                  String targetPortValue) {
        boolean anyUnfiltered = false;
        for (InOutValueDto match : matches) {
            if (match.getUnfilteredValue() != null) {
                anyUnfiltered = true;
                break;
            }
        }
        List<Object> wrappedValues = new ArrayList<>(matches.size());
        for (InOutValueDto match : matches) {
            wrappedValues.add(deepCopy(match.getValue()));
        }
        List<Object> wrappedUnfiltered = new ArrayList<>(matches.size());
        if (anyUnfiltered) {
            for (InOutValueDto match : matches) {
                Object raw = match.getUnfilteredValue() != null
                        ? match.getUnfilteredValue() : match.getValue();
                wrappedUnfiltered.add(deepCopy(raw));
            }
        } else {
            // Separate list instance — do not alias {@code wrappedValues}.
            for (InOutValueDto match : matches) {
                wrappedUnfiltered.add(deepCopy(match.getValue()));
            }
        }
        InOutValueDto clone = first.clone();
        clone.setValue(wrappedValues);
        clone.setUnfilteredValue(wrappedUnfiltered);
        clone.setTargetNodeId(consumingNodeId);
        clone.setTargetPortId(targetPortId);
        clone.setTargetPortValue(targetPortValue);
        clone.setGateway(false);
        return clone;
    }

    /**
     * Structural copy: {@code Map} → {@code LinkedHashMap}, {@code List} → {@code ArrayList}.
     * Scalars and other types are returned as-is. {@link InOutValueDto#clone()} is shallow —
     * without this, list-wrapped payloads would still alias the store.
     */
    private static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> map) {
            LinkedHashMap<Object, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                copy.put(e.getKey(), deepCopy(e.getValue()));
            }
            return copy;
        }
        if (value instanceof List<?> list) {
            ArrayList<Object> copy = new ArrayList<>(list.size());
            for (Object item : list) {
                copy.add(deepCopy(item));
            }
            return copy;
        }
        return value;
    }

    /**
     * Virtual global cards: folds every (virtual source, edge target port) group of rows into ONE
     * message whose value is a {@code {globalKey: value}} map — one entry per row
     * {@code sourcePath}, read off the matching sealed variable map. Keys with no sealed value
     * are simply absent (never a null placeholder), so a renamed/deleted global degrades to a
     * no-op mapping instead of poisoning the payload.
     */
    private static List<InOutValueDto> materializeGlobalsMessages(List<ForeignRow> globalRows,
                                                                  String consumingNodeId,
                                                                  Map<String, Object> workflowVariables,
                                                                  Map<String, Object> projectVariables,
                                                                  Map<InOutValueDto, String> carrierByRow) {
        if (globalRows.isEmpty()) {
            return List.of();
        }
        // Group by (virtual source, target port): N key rows pinned on one port fold into ONE message.
        Map<String, List<ForeignRow>> byTargetPort = new LinkedHashMap<>();
        for (ForeignRow fr : globalRows) {
            String groupKey = fr.row().getSourceNodeId() + '|' + fr.row().getTargetPortId() + '|'
                    + extractShortCode(fr.edge().getTargetHandle());
            byTargetPort.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(fr);
        }
        List<InOutValueDto> out = new ArrayList<>();
        for (List<ForeignRow> group : byTargetPort.values()) {
            ForeignRow first = group.get(0);
            String virtualId = first.row().getSourceNodeId();
            Map<String, Object> source = WORKFLOW_GLOBALS_ID.equals(virtualId)
                    ? workflowVariables : projectVariables;
            Map<String, Object> keyed = new LinkedHashMap<>();
            for (ForeignRow fr : group) {
                String key = fr.row().getSourcePath();
                if (StringUtils.isBlank(key) || source == null || !source.containsKey(key)) {
                    continue;
                }
                keyed.put(key, source.get(key));
            }
            InOutValueDto msg = new InOutValueDto();
            msg.setNodeId(virtualId);
            msg.setPortId(GLOBALS_PORT_ID);
            msg.setPortValue(GLOBALS_PORT_VALUE);
            msg.setTargetNodeId(consumingNodeId);
            msg.setTargetPortId(first.row().getTargetPortId());
            msg.setTargetPortValue(extractShortCode(first.edge().getTargetHandle()));
            msg.setValue(keyed);
            msg.setGateway(false);
            out.add(msg);
            carrierByRow.put(msg, first.edge().getSource());
        }
        return out;
    }

    /**
     * Finds the producer's fanned-out entry for the mapped port. Port id is the durable identity
     * the FE persists on the mapping row; the port-value/short-code pair is only a fallback for
     * rows authored before port ids rode the payload (the runtime-mutated row short code is NOT
     * trustworthy here — {@code ConnectionBuilder} rewrites it from the edge's own source handle).
     * Final fallback: a producer with EXACTLY ONE fanned-out entry is unambiguous, so it delivers
     * even when neither identity matches (e.g. a stale row after the producer's template swap).
     */
    private static InOutValueDto findProducedPortEntry(List<InOutValueDto> produced, EdgeTransformDto row) {
        if (StringUtils.isNotEmpty(row.getSourcePortId())) {
            for (InOutValueDto dto : produced) {
                if (dto != null && row.getSourcePortId().equals(dto.getPortId())) {
                    return dto;
                }
            }
        }
        if (StringUtils.isNotEmpty(row.getSourcePortShortCode())) {
            for (InOutValueDto dto : produced) {
                if (dto != null && row.getSourcePortShortCode().equals(dto.getPortValue())) {
                    return dto;
                }
            }
        }
        return produced.size() == 1 ? produced.get(0) : null;
    }

    private static String extractShortCode(String handle) {
        return (handle != null && handle.split("-").length == 2) ? handle.split("-")[1] : null;
    }
}
