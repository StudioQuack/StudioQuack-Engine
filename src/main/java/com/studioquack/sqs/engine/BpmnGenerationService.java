package com.studioquack.sqs.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.bpmn.FeelToJuelTranslator;
import com.studioquack.sqs.engine.expression.GatewayConditionCarriers;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy;
import com.studioquack.sqs.engine.strategy.ActivitiFlowNodeStrategy.GeneratedNode;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ExclusiveGateway;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.Gateway;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.InclusiveGateway;
import org.activiti.bpmn.model.IntermediateCatchEvent;
import org.activiti.bpmn.model.LinkEventDefinition;
import org.activiti.bpmn.model.ParallelGateway;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.StartEvent;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Builds Activiti BPMN for a workflow graph and its function-port sub-processes.
 * Gateway conditions use execution-local routing tokens; a gateway with no default flow fails closed.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BpmnGenerationService {

    /** BPMN target namespace stamped on every generated model. */
    public static final String TARGET_NAMESPACE = "http://studioquack.com/bpmn";

    private final List<ActivitiFlowNodeStrategy> strategies; // Spring-ordered (@Order); fallback last
    private final FeelToJuelTranslator feelToJuelTranslator;
    private final ActivitiBoundaryEventHelper boundaryEventHelper;

    /**
     * Dedicated mapper for the deterministic graph-signature JSON. A vanilla instance is used
     * (not the Spring bean) so the signature is stable regardless of app-wide Jackson config.
     */
    private final ObjectMapper signatureMapper = new ObjectMapper();

    /**
 * Convenience facade for neighbours (deploy command): generate the main process BPMN,
 * every function-port sub-process BPMN, and the {@code v6} graph signature in one call.
 *
 * @return {@link GeneratedBpmn} — {@code mainXml}, {@code subProcessXmls} (rootId → xml),
 * {@code graphSignature}.
 */
    public GeneratedBpmn generate(String workflowId, String workflowName,
                                  List<NodeDto> nodes, List<EdgeDto> edges) {
        return generate(workflowId, workflowName, nodes, edges, "");
    }

    /**
     * @param namespaceSuffix appended to every generated process key so a Designer Run's throwaway
     *                        deployment cannot become a newer version of the published one. Empty for
     *                        a published deployment. Deliberately NOT folded into the graph signature:
     *                        the signature identifies the graph, not the deployment it landed in.
     */
    public GeneratedBpmn generate(String workflowId, String workflowName,
                                  List<NodeDto> nodes, List<EdgeDto> edges, String namespaceSuffix) {
        String mainXml = generateBpmnXml(workflowId, workflowName, nodes, edges, namespaceSuffix);
        Map<String, String> subProcessXmls =
                generateSubProcessBpmnXmls(workflowName, nodes, edges, namespaceSuffix);
        String signature = computeGraphSignature(workflowName, nodes, edges);
        return new GeneratedBpmn(mainXml, subProcessXmls, signature);
    }

    public String generateBpmnXml(String workflowId, String workflowName,
                                  List<NodeDto> nodes, List<EdgeDto> edges) {
        return generateBpmnXml(workflowId, workflowName, nodes, edges, "");
    }

    public String generateBpmnXml(String workflowId, String workflowName,
                                  List<NodeDto> nodes, List<EdgeDto> edges, String namespaceSuffix) {
        boundaryEventHelper.resolveBoundaryTargets(nodes, edges);

        BpmnModel model = new BpmnModel();
        model.setTargetNamespace(TARGET_NAMESPACE);

        Process process = new Process();
        process.setId("Process_" + workflowId + nullToEmpty(namespaceSuffix));
        process.setName(StringUtils.defaultIfBlank(workflowName, workflowId));
        process.setExecutable(true);
        model.addProcess(process);

        List<EdgeDto> edgeList = edges == null ? Collections.emptyList() : edges;
        Map<String, NodeDto> nodeById = new LinkedHashMap<>();
        for (NodeDto node : nodes) {
            nodeById.put(node.getId(), node);
        }
        Map<String, GeneratedNode> generatedByNodeId = new LinkedHashMap<>();
        Map<String, String> bpmnTypeByNodeId = new LinkedHashMap<>();
        /** Nodes deliberately left out of the main process because no main-flow edge reaches them. */
        List<String> droppedUnreachable = new ArrayList<>();

        // Nodes that belong to a function-port sub-process run ON-DEMAND as their own
        // Process_SubProc_<root> (invoked by the caller's script via studioquack.execute() ->
        // startExecutionForFunction), NEVER in the main control flow. They — and the FUNCTION /
        // VARIABLE_PROVIDER "special" edges that attach them — must therefore be excluded from the
        // main process, exactly as generateSubProcessBpmnXmls() excludes them from each other.
        Set<String> subProcessNodes = collectFunctionSubProcessNodes(nodes, edgeList);

        // Main control-flow edges only: drop special edges, boundary-event edges, ASSOCIATION
        // edges (visual-only) and any edge touching a sub-process node. What remains is the
        // sequential/gateway skeleton.
        List<EdgeDto> processEdges = edgeList.stream()
                .filter(e -> !isSpecialConnection(e, nodeById))
                .filter(e -> !subProcessNodes.contains(e.getSource())
                        && !subProcessNodes.contains(e.getTarget()))
                .toList();
        // Boundary + ASSOCIATION: no sequence flow, no fork/join degree.
        List<EdgeDto> mainEdges = processEdges.stream()
                .filter(e -> !isBoundaryConnection(e) && !isAssociationConnection(e))
                .toList();
        // A boundary handler hangs off the boundary event, not off the host's exit, so its edge must
        // NOT become a sequence flow — that would give the host a second unconditional outgoing flow
        // and reach the handler on every run. The handler node itself must still be emitted.
        // ASSOCIATION is visual-only: it must not keep a node "reachable" for main-flow emission.
        Set<String> connectedByMainEdge = new HashSet<>();
        for (EdgeDto e : processEdges) {
            if (isAssociationConnection(e)) {
                continue;
            }
            connectedByMainEdge.add(e.getSource());
            connectedByMainEdge.add(e.getTarget());
        }

        // 1) Emit main-flow nodes via their strategy. A node that reaches the main flow ONLY
        //    through a special edge would become an unreachable async task, so we drop it unless it
        //    is a structural start/end (always kept so Activiti can resolve the initial/terminal).
        // 1a) Required-param gate BEFORE any strategy runs: a strategy may itself throw on an
        //     invalid param (the signal/message registry refuses blank names), but the gate's
        //     aggregated violation names every offending node and parameter at once.
        List<String> requiredParamViolations = new ArrayList<>();
        for (NodeDto node : nodes) {
            if (subProcessNodes.contains(node.getId())) {
                continue;
            }
            if (!isDuckbotBoundaryNode(node, "start") && !isDuckbotBoundaryNode(node, "end")
                    && !connectedByMainEdge.contains(node.getId())
                    && !isStructuralNode(effectiveBpmnType(node))) {
                continue; // dropped as unreachable below — no gate for a node that never generates
            }
            collectRequiredParamViolations(node, requiredParamViolations);
        }
        throwOnRequiredParamViolations(requiredParamViolations);

        for (NodeDto node : nodes) {
            if (subProcessNodes.contains(node.getId())) {
                continue;
            }
            if (isDuckbotBoundaryNode(node, "start")) {
                GeneratedNode generated = createDuckbotStartTask(model, process, node, resolveLabel(node));
                generatedByNodeId.put(node.getId(), generated);
                bpmnTypeByNodeId.put(node.getId(), "__sq_duckbot_start_task__");
                continue;
            }
            if (isDuckbotBoundaryNode(node, "end")) {
                GeneratedNode generated = createDuckbotEndTask(model, process, node, resolveLabel(node));
                generatedByNodeId.put(node.getId(), generated);
                bpmnTypeByNodeId.put(node.getId(), "__sq_duckbot_end_task__");
                continue;
            }
            String bpmnType = effectiveBpmnType(node);
            if (!connectedByMainEdge.contains(node.getId()) && !isStructuralNode(bpmnType)) {
                // Dropping this node is correct — it would otherwise be an unreachable async task that
                // parks a token forever. Saying so is the point: the node simply never appears in any
                // run log, which reads as "my converter did nothing" with no way to tell that the
                // graph, not the converter, is why. Name it so the author can see the wiring is
                // main-flow-less (only a function/variable-provider or boundary edge touches it).
                droppedUnreachable.add(node.getId() + "(" + bpmnType + ")");
                continue;
            }
            generatedByNodeId.put(node.getId(), selectStrategy(bpmnType).create(model, process, node, resolveLabel(node)));
            bpmnTypeByNodeId.put(node.getId(), bpmnType);
        }

        // 1b) Link events pair off-edge: Activiti wires the pair by ELEMENT id (throw.target,
        //     catch.sources) while the designer authors only the shared link `name`. Resolve the
        //     pairing here — an unpaired or duplicated link is an authoring error, said loudly.
        pairLinkEvents(process, nodes, generatedByNodeId, bpmnTypeByNodeId);

        // 1c) An event-based gateway may only feed intermediate catch events — Activiti rejects
        //     any other target late, at deploy, with an engine-internal message. Name the nodes.
        validateEventBasedGatewayTargets(mainEdges, bpmnTypeByNodeId);

        // 2) Auto parallel gateways: a non-gateway node with >1 main-flow outgoing edge fans out
        //    through a ParallelGateway fork, and one with >1 incoming edge merges through a
        //    ParallelGateway join. Without this Activiti treats N unconditional outgoing flows as an
        //    uncontrolled fork and N incoming flows as no join, so a converging node (an agent fed by
        //    the Duckbot start AND a Docling converter in parallel) fires on the FIRST token instead
        //    of waiting for every branch. wireEdge is unchanged — it routes through the wiring handles.
        Map<String, GeneratedNode> wiring =
                insertParallelGateways(process, mainEdges, generatedByNodeId, bpmnTypeByNodeId);

        // 3) Wire main-flow edges: exit(source) -> entry(target), with gateway conditions/default.
        Map<String, Integer> wiredOutDegree = wiredOutDegree(mainEdges, wiring);
        for (EdgeDto edge : mainEdges) {
            wireEdge(process, edge, wiring, bpmnTypeByNodeId, nodeById, wiredOutDegree);
        }
        ensureSyntheticDefaultOnConditionedOnlyGateways(process);
        boundaryEventHelper.repairDanglingBoundaryRoutes(process);
        warnOnDanglingPortExits(process, generatedByNodeId);

        // Any deployed workflow can be a call.activity TARGET: pin a child's terminal failure onto
        // the parent (the propagation listener on the call activity re-raises it). No-op for runs
        // without a super execution. The function sub-PI completion listener stays sub-process-only.
        attachChildFailureListenerToEndEvents(process);

        byte[] xml = new BpmnXMLConverter().convertToXML(model);
        String result = new String(xml, StandardCharsets.UTF_8);
        // At INFO: this is the only place that can answer "why was my node never dispatched?" — the
        // run logs can only show what the generated process contains, so the shape has to be recorded
        // at deploy time, once, rather than inferred from an absence later.
        log.info("activiti.bpmn.generated process={} mainNodes={}/{} mainEdges={}/{} subProcessNodes={} "
                        + "parallelGateways={} bytes={}",
                process.getId(), generatedByNodeId.size(), nodes.size(),
                mainEdges.size(), edgeList.size(), subProcessNodes.size(),
                countParallelGateways(process), result.length());
        if (!droppedUnreachable.isEmpty()) {
            log.warn("activiti.bpmn.generated process={} left {} node(s) OUT of the main flow because no "
                            + "main-flow edge reaches them: {} — they will never be dispatched. Connect them "
                            + "to the control flow if they are meant to run.",
                    process.getId(), droppedUnreachable.size(), droppedUnreachable);
        }
        return result;
    }

    /**
     * Duckbot start nodes are logical workflow-boundary adapters, not passive BPMN events. Their
     * bridge runs inside the dispatch delegate, so Activiti must execute them as a task. We wrap the
     * task with a synthetic none StartEvent to keep a valid BPMN initial element.
     */
    private GeneratedNode createDuckbotStartTask(BpmnModel model, Process process, NodeDto node, String label) {
        StartEvent start = new StartEvent();
        start.setId("start_duck_" + node.getId());
        start.setName("Start " + label);
        process.addFlowElement(start);

        GeneratedNode task = selectStrategy("__sq_forced_task__").create(model, process, node, label);
        process.addFlowElement(newFlow("flow_start_duck_" + node.getId(), start.getId(), task.entryElementId()));
        return task;
    }

    /**
     * Duckbot end nodes must execute their bridge to persist the assistant/application response. A
     * plain EndEvent would skip that bridge, so emit a task and then a synthetic EndEvent.
     */
    private GeneratedNode createDuckbotEndTask(BpmnModel model, Process process, NodeDto node, String label) {
        GeneratedNode task = selectStrategy("__sq_forced_task__").create(model, process, node, label);
        EndEvent end = new EndEvent();
        end.setId("end_duck_" + node.getId());
        end.setName("End " + label);
        process.addFlowElement(end);
        process.addFlowElement(newFlow("flow_end_duck_" + node.getId(), task.exitElementId(), end.getId()));
        return new GeneratedNode(task.entryElementId(), end.getId());
    }

    private boolean isDuckbotBoundaryNode(NodeDto node, String nodeType) {
        TemplateDto template = node == null ? null : node.getData();
        if (template == null) {
            return false;
        }
        String category = template.getCategory();
        boolean duckbotCategory = StringUtils.isNotBlank(category)
                && ("conversion".equalsIgnoreCase(category) || "application".equalsIgnoreCase(category));
        return duckbotCategory && nodeType.equalsIgnoreCase(template.getNodeType());
    }

    /**
     * All nodes that belong to a function-port sub-process — the union of every function root's
     * reachable set (BFS over NON-special edges, so nested function ports naturally fall into their
     * own root's set). These are compiled by {@link #generateSubProcessBpmnXmls} into standalone
     * {@code Process_SubProc_<root>} definitions and MUST NOT also appear in the main process.
     */
    private Set<String> collectFunctionSubProcessNodes(List<NodeDto> nodes, List<EdgeDto> edges) {
        Map<String, NodeDto> nodeById = new LinkedHashMap<>();
        for (NodeDto n : nodes) {
            nodeById.put(n.getId(), n);
        }
        // A node the main flow can reach is part of the main flow, full stop. The BFS below only stops
        // at special edges, which is enough for nesting but not for convergence: as soon as a helper's
        // reachable set touches a node that the start can also reach — a helper returning into the node
        // that owns its function port, for instance — it used to sweep the rest of the graph into the
        // sub-process, and those nodes silently disappeared from the main process along with their
        // edges. A whole branch (Docling converting a file and feeding the result back to an agent)
        // vanished that way with no error: the deploy reported fewer main nodes and zero gateways, and
        // the branch simply never dispatched.
        Set<String> mainFlow = collectMainFlowNodes(nodes, edges, nodeById);
        Set<String> subNodes = new HashSet<>();
        for (String rootId : findFunctionRoots(edges)) {
            if (!nodeById.containsKey(rootId) || mainFlow.contains(rootId)) {
                continue;
            }
            Set<String> reachable = collectReachableNodes(rootId, nodeById, edges);
            reachable.removeAll(mainFlow);
            subNodes.addAll(reachable);
        }
        return subNodes;
    }

    /**
     * Everything the main control flow can reach: BFS from every start-like node (a structural start
     * event or a Duckbot start boundary) over non-special, non-boundary edges.
     *
     * <p>This is the fence that keeps a function sub-process from claiming main-flow nodes. Without it
     * the two sets overlap and the main process loses whichever nodes the helper's traversal happened
     * to reach first.
     */
    private Set<String> collectMainFlowNodes(List<NodeDto> nodes, List<EdgeDto> edges,
                                             Map<String, NodeDto> nodeById) {
        // A function root is start-like in the parent graph on purpose (generateSingleSubProcess
        // task-ifies it), so it must never seed the main flow — otherwise the fence would claim the
        // helper's own chain and empty its sub-process.
        Set<String> functionRoots = findFunctionRoots(edges);
        Set<String> mainFlow = new HashSet<>();
        for (NodeDto node : nodes) {
            if (node == null || node.getId() == null || functionRoots.contains(node.getId())
                    || !isMainFlowEntry(node)) {
                continue;
            }
            for (String reached : collectReachableNodes(node.getId(), nodeById, edges)) {
                mainFlow.add(reached);
            }
        }
        return mainFlow;
    }

    /** A node the engine may start the main process at: a structural start event or a Duckbot start. */
    private boolean isMainFlowEntry(NodeDto node) {
        if (isDuckbotBoundaryNode(node, "start")) {
            return true;
        }
        String bpmnType = node.getData() == null ? null : node.getData().getBpmnType();
        String nodeType = node.getData() == null ? null : node.getData().getNodeType();
        return isStartLike(bpmnType) || "start".equalsIgnoreCase(nodeType);
    }

    /**
     * A node whose effective type is a structural start or end. Such nodes are kept in the main
     * process even without a main-flow edge, so Activiti can always resolve the initial element.
     */
    private boolean isStructuralNode(String bpmnType) {
        if (bpmnType == null) {
            return false;
        }
        String t = bpmnType.toLowerCase(java.util.Locale.ROOT);
        return t.equals("start") || t.endsWith("start.event") || t.endsWith("end.event");
    }

    /**
     * Out-degree over the edges that will ACTUALLY be wired (both endpoints resolved). A gateway's
     * authored outgoing edges can shrink to one when a sibling target was dropped as unreachable or
     * moved into a sub-process — Activiti forbids a condition on a single-outgoing gateway, so
     * {@link #wireEdge} counts wired flows and, when the sole exit is conditioned, adds a synthetic
     * error default instead of dropping the condition.
     */
    private Map<String, Integer> wiredOutDegree(List<EdgeDto> edges,
                                                Map<String, GeneratedNode> generatedByNodeId) {
        Map<String, Integer> outDegree = new HashMap<>();
        for (EdgeDto e : edges) {
            if (generatedByNodeId.containsKey(e.getSource()) && generatedByNodeId.containsKey(e.getTarget())) {
                outDegree.merge(e.getSource(), 1, Integer::sum);
            }
        }
        return outDegree;
    }

    private void wireEdge(Process process, EdgeDto edge,
                          Map<String, GeneratedNode> generatedByNodeId,
                          Map<String, String> bpmnTypeByNodeId,
                          Map<String, NodeDto> nodeById,
                          Map<String, Integer> wiredOutDegree) {
        GeneratedNode from = generatedByNodeId.get(edge.getSource());
        GeneratedNode to = generatedByNodeId.get(edge.getTarget());
        if (from == null || to == null) {
            log.debug("Skipping edge {}: unresolved source={} or target={}",
                    edge.getId(), edge.getSource(), edge.getTarget());
            return;
        }

        // Event-choice nodes (message catch with a timeout) route each outbound edge from the
        // element that owns the edge's source port; a blank port-exit means the node's own
        // synthesis already terminates that branch (cancel/error) and the authored edge is moot.
        String sourcePortValue = portValue(edge.getSourceHandle());
        String sourceExitId = from.exitForPort(sourcePortValue);
        if (StringUtils.isBlank(sourceExitId)) {
            log.warn("Skipping edge {}: port '{}' of source={} is terminated by the node's own "
                    + "synthesis (e.g. onTimeoutBehavior cancel/error) — the authored edge is moot",
                    edge.getId(), sourcePortValue, edge.getSource());
            return;
        }

        SequenceFlow flow = new SequenceFlow(sourceExitId, to.entryElementId());
        // BPMN element ids must be XML NCNames. FE edge ids are UUIDs like "9fb19140-…" which start
        // with a digit — an ILLEGAL NCName. A static letter prefix + light sanitisation of the
        // remainder guarantees a valid, stable, collision-free id.
        String rawFlowId = StringUtils.defaultIfBlank(edge.getId(),
                edge.getSource() + "_" + edge.getTarget());
        flow.setId(uniqueFlowId(process, toNcName("flow_", rawFlowId)));
        process.addFlowElement(flow);

        //every FE-accepted carrier compiles to branch-identity JUEL (daemon evaluates
        // $(…)/@… upstream and sets route__<gwId>). Leftover FEEL/JUEL (`=` / `${`) still
        // goes through FeelToJuelTranslator. A filled condition always wins over
        // isDefaultFlow / sole-exit defaulting; blank still means default.
        if (isConditionalGateway(bpmnTypeByNodeId.get(edge.getSource()))) {
            String authored = GatewayConditionCarriers.authoredExpression(
                    edge, nodeById.get(edge.getSource()));
            int outDegree = wiredOutDegree.getOrDefault(edge.getSource(), 0);
            if (StringUtils.isNotBlank(authored)) {
                // CONDITION WINS: never mark this flow default (even if isDefaultFlow or sole exit).
                applyAuthoredCondition(flow, authored, edge);
                if (outDegree <= 1) {
                    // Activiti SequenceflowValidator rejects a condition on a single-outgoing
                    // XOR/OR. Keep the authored JUEL and add a synthetic default → error end
                    // so a false condition fails the run instead of silently passing.
                    addSyntheticConditionMissDefault(process, from.exitElementId());
                }
            } else {
                // Pure blank / explicit-default-without-condition: unconditional default.
                markAsDefaultFlow(process, from.exitElementId(), flow.getId());
            }
        }
    }

    /**
 * Compiles {@code authored} onto {@code flow}. Payload values never enter the expression —
 * carriers become a branch-identity {@code route__} compare; leftover FEEL/JUEL is
 * translated under the fail-closed route grammar. A condition that cannot be compiled fails
 * the deploy: an edge left without its condition would be unconditioned, i.e. always taken.
 *
 * @throws IllegalArgumentException when the condition is outside the safe grammar or does not parse
 */
    private void applyAuthoredCondition(SequenceFlow flow, String authored, EdgeDto edge) {
        String juel;
        if (GatewayConditionCarriers.leftoverFeelOrJuel(authored)) {
            juel = feelToJuelTranslator.translateCondition(authored);
        } else {
            // Engine compares the pre-resolved route token — never the authored expression.
            // The variable name is JUEL-sanitised (FE node ids are hyphenated UUIDs, which
            // are NOT legal inside a ${…} identifier) and matches the runtime write side.
            String targetId = StringUtils.defaultString(edge.getTarget());
            juel = "${" + EngineVariableNames.routeTokenVariableName(edge.getSource())
                    + " == '" + escapeJuelSingleQuoted(targetId) + "'}";
        }
        flow.setConditionExpression(requireValidJuel(juel, edge));
    }

    /**
     * Adds an unconditional default sequence flow from {@code gatewayElementId} to the shared
     * sandbox-error end. Used when the only authored XOR/OR exit carries a condition: Activiti
     * requires a second, condition-less default, and taking it is the loud miss (historic
     * activity on {@link ActivitiBpmnElementHelper#SHARED_ERROR_END_ID}). Does not steal an
     * existing default.
     */
    private void addSyntheticConditionMissDefault(Process process, String gatewayElementId) {
        FlowElement gateway = process.getFlowElement(gatewayElementId);
        if (!(gateway instanceof Gateway g) || g.getDefaultFlow() != null) {
            return;
        }
        boundaryEventHelper.getOrCreateSharedErrorEnd(process);
        String flowId = toNcName("flow_", gatewayElementId + "_condition_miss");
        if (process.getFlowElement(flowId) == null) {
            SequenceFlow miss = new SequenceFlow(gatewayElementId, ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
            miss.setId(flowId);
            // Visible on the historic activity list so a dead-end is diagnosable.
            miss.setName("condition miss");
            process.addFlowElement(miss);
        }
        g.setDefaultFlow(flowId);
        log.warn("activiti.bpmn.condition-miss-default gateway={} flow={} — no unconditioned default; "
                        + "false / no-match takes the shared error end",
                gatewayElementId, flowId);
    }

    /**
     * Exclusive/inclusive gateways with every authored exit conditioned have no BPMN default.
     * Add the synthetic error-end default so a token of {@code __dead_end__} (all JUEL
     * compares false) takes the error end instead of Activiti throwing no-outgoing-flow.
     * No-ops when a default is already present (blank edge or prior sole-exit insert).
     */
    private void ensureSyntheticDefaultOnConditionedOnlyGateways(Process process) {
        List<String> choiceGatewayIds = new ArrayList<>();
        for (FlowElement el : process.getFlowElements()) {
            if (el instanceof ExclusiveGateway || el instanceof InclusiveGateway) {
                choiceGatewayIds.add(el.getId());
            }
        }
        for (String gatewayElementId : choiceGatewayIds) {
            addSyntheticConditionMissDefault(process, gatewayElementId);
        }
    }

    /**
     * Generation-stage guarantee that no invalid BPMN reaches deploy: returns {@code expression}
     * only when it is a non-blank {@code ${…}} that JUEL can parse — the exact check Activiti's
     * {@code SequenceflowValidator} runs at deploy time — and otherwise fails the deploy with the
     * offending edge named.
     */
    private String requireValidJuel(String expression, EdgeDto edge) {
        String trimmed = expression == null ? "" : expression.trim();
        if (trimmed.isEmpty() || !trimmed.startsWith("${") || !trimmed.endsWith("}")) {
            throw new IllegalArgumentException("gateway condition on edge " + edge.getId()
                    + " is not a ${…} expression: " + abbreviate(trimmed));
        }
        try {
            jakarta.el.ExpressionFactory.newInstance().createValueExpression(
                    new org.activiti.core.el.juel.util.SimpleContext(), trimmed, Object.class);
            return trimmed;
        } catch (Exception parseFailure) {
            throw new IllegalArgumentException("gateway condition on edge " + edge.getId()
                    + " does not parse as JUEL: " + abbreviate(trimmed) + " — " + parseFailure.getMessage(),
                    parseFailure);
        }
    }

    private static String abbreviate(String value) {
        return value.length() <= 120 ? value : value.substring(0, 120) + "…";
    }

    private boolean isConditionalGateway(String bpmnType) {
        return "exclusive.gateway".equalsIgnoreCase(bpmnType)
                || "inclusive.gateway".equalsIgnoreCase(bpmnType);
    }

    /**
     * Inserts {@link ParallelGateway} fork/join elements around emitted main-flow nodes and returns
     * the wiring handles {@link #wireEdge} should route edges through.
     *
     * <ul>
     *   <li><b>Fork</b> — a non-gateway node with more than one main-flow OUTGOING edge gets a fork
     *       after its exit ({@code node.exit -> fork}); outgoing edges then leave the fork. A task
     *       with N unconditional outgoing flows is already an implicit BPMN split, so this makes the
     *       existing semantics explicit — every branch starts together.</li>
     *   <li><b>Join</b> — a non-gateway node with more than one main-flow INCOMING edge gets a join
     *       before its entry ({@code join -> node.entry}) ONLY when those branches are genuinely
     *       parallel (see {@link #isParallelConvergence}); incoming edges then land on the join, so
     *       the node runs only after EVERY branch arrives. This is the fix for an agent that was
     *       fed by the Duckbot start AND a Docling converter in parallel yet fired on the first
     *       token, leaving the converter branch stranded.</li>
     * </ul>
     *
     * <p>Authored gateways are never wrapped — they already express their own fork/join semantics.
     * A convergence whose branches come from a choice gateway (exclusive / inclusive / event-based)
     * is left as plain sequence flows: only one branch emits a token there, so a parallel join would
     * wait forever. That keeps conditional merges behaving exactly as before.
     */
    private Map<String, GeneratedNode> insertParallelGateways(Process process,
                                                              List<EdgeDto> mainEdges,
                                                              Map<String, GeneratedNode> generatedByNodeId,
                                                              Map<String, String> bpmnTypeByNodeId) {
        Map<String, Integer> outDegree = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, List<String>> successors = new HashMap<>();
        Map<String, List<String>> predecessors = new HashMap<>();
        for (EdgeDto edge : mainEdges) {
            String source = edge.getSource();
            String target = edge.getTarget();
            if (!generatedByNodeId.containsKey(source) || !generatedByNodeId.containsKey(target)) {
                continue;
            }
            outDegree.merge(source, 1, Integer::sum);
            inDegree.merge(target, 1, Integer::sum);
            successors.computeIfAbsent(source, k -> new ArrayList<>()).add(target);
            predecessors.computeIfAbsent(target, k -> new ArrayList<>()).add(source);
        }

        Map<String, GeneratedNode> wiring = new LinkedHashMap<>(generatedByNodeId);
        for (Map.Entry<String, GeneratedNode> entry : generatedByNodeId.entrySet()) {
            String nodeId = entry.getKey();
            GeneratedNode gen = entry.getValue();
            if (isGateway(bpmnTypeByNodeId.get(nodeId))) {
                continue;
            }
            String entryId = gen.entryElementId();
            String exitId = gen.exitElementId();

            if (inDegree.getOrDefault(nodeId, 0) > 1
                    && isParallelConvergence(nodeId, predecessors, successors, outDegree, bpmnTypeByNodeId)) {
                String joinId = toNcName("gwjoin_", nodeId);
                addParallelGateway(process, joinId, "Join " + nodeId);
                process.addFlowElement(newFlow(toNcName("flow_join_", nodeId), joinId, gen.entryElementId()));
                entryId = joinId;
            }
            if (outDegree.getOrDefault(nodeId, 0) > 1 && !gen.hasPortSpecificExits()) {
                // A port-routed node (message catch with a timeout branch) is an event CHOICE,
                // not a fan-out: its exits leave from different elements and must never fork.
                String forkId = toNcName("gwfork_", nodeId);
                addParallelGateway(process, forkId, "Fork " + nodeId);
                process.addFlowElement(newFlow(toNcName("flow_fork_", nodeId), gen.exitElementId(), forkId));
                exitId = forkId;
            }
            if (!entryId.equals(gen.entryElementId()) || !exitId.equals(gen.exitElementId())) {
                wiring.put(nodeId, new GeneratedNode(entryId, exitId, gen.exitElementIdByPortValue()));
            }
        }
        return wiring;
    }

    /**
     * True when {@code node}'s multiple incoming branches are guaranteed to all carry a token — i.e.
     * a real parallel join, not an exclusive/inclusive merge that would deadlock a {@link ParallelGateway}.
     *
     * <p>The branches must share a nearest common ancestor that is a parallel divergence point (an
     * authored {@code parallel.gateway}, or a non-gateway node with more than one outgoing edge — the
     * fork we synthesise), and no choice gateway may sit between that fork and this node (a choice
     * there could suppress one branch). Anything less certain is left as plain sequence flows.
     */
    private boolean isParallelConvergence(String node,
                                          Map<String, List<String>> predecessors,
                                          Map<String, List<String>> successors,
                                          Map<String, Integer> outDegree,
                                          Map<String, String> bpmnTypeByNodeId) {
        List<String> preds = predecessors.getOrDefault(node, Collections.emptyList());
        if (preds.size() < 2) {
            return false;
        }
        Set<String> common = null;
        for (String predecessor : preds) {
            Set<String> branch = backwardReachable(predecessor, predecessors);
            branch.add(predecessor);
            if (common == null) {
                common = new HashSet<>(branch);
            } else {
                common.retainAll(branch);
            }
        }
        if (common == null) {
            return false;
        }
        common.remove(node);
        String fork = null;
        int nearest = Integer.MAX_VALUE;
        for (String candidate : common) {
            int distance = distance(candidate, node, successors);
            if (distance > 0 && distance < nearest) {
                nearest = distance;
                fork = candidate;
            }
        }
        if (fork == null) {
            return false;
        }
        String forkType = bpmnTypeByNodeId.get(fork);
        boolean parallelFork = "parallel.gateway".equalsIgnoreCase(forkType)
                || (!isGateway(forkType) && outDegree.getOrDefault(fork, 0) > 1);
        if (!parallelFork) {
            return false;
        }
        // A choice gateway strictly between the fork and this node could divert a branch away.
        Set<String> between = forwardReachable(fork, successors);
        between.retainAll(backwardReachable(node, predecessors));
        between.remove(fork);
        between.remove(node);
        for (String mid : between) {
            if (isBranchingGateway(bpmnTypeByNodeId.get(mid))) {
                return false;
            }
        }
        return true;
    }

    /** Ancestors of {@code start} over main-flow edges (excludes {@code start}); cycle-safe. */
    private Set<String> backwardReachable(String start, Map<String, List<String>> predecessors) {
        Set<String> seen = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        for (String p : predecessors.getOrDefault(start, Collections.emptyList())) {
            if (seen.add(p)) {
                queue.add(p);
            }
        }
        while (!queue.isEmpty()) {
            for (String p : predecessors.getOrDefault(queue.poll(), Collections.emptyList())) {
                if (seen.add(p)) {
                    queue.add(p);
                }
            }
        }
        return seen;
    }

    /** Descendants of {@code start} over main-flow edges (excludes {@code start}); cycle-safe. */
    private Set<String> forwardReachable(String start, Map<String, List<String>> successors) {
        Set<String> seen = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        for (String s : successors.getOrDefault(start, Collections.emptyList())) {
            if (seen.add(s)) {
                queue.add(s);
            }
        }
        while (!queue.isEmpty()) {
            for (String s : successors.getOrDefault(queue.poll(), Collections.emptyList())) {
                if (seen.add(s)) {
                    queue.add(s);
                }
            }
        }
        return seen;
    }

    /** Shortest hop count from {@code from} to {@code to} over main-flow edges, or -1 if unreachable. */
    private int distance(String from, String to, Map<String, List<String>> successors) {
        if (from.equals(to)) {
            return 0;
        }
        Map<String, Integer> dist = new HashMap<>();
        Deque<String> queue = new ArrayDeque<>();
        dist.put(from, 0);
        queue.add(from);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            int d = dist.get(current);
            for (String next : successors.getOrDefault(current, Collections.emptyList())) {
                if (!dist.containsKey(next)) {
                    if (next.equals(to)) {
                        return d + 1;
                    }
                    dist.put(next, d + 1);
                    queue.add(next);
                }
            }
        }
        return -1;
    }

    /** Fork/join gateways the generator inserted — zero here means nothing runs in parallel. */
    private static int countParallelGateways(Process process) {
        int count = 0;
        for (org.activiti.bpmn.model.FlowElement element : process.getFlowElements()) {
            if (element instanceof ParallelGateway) {
                count++;
            }
        }
        return count;
    }

    private void addParallelGateway(Process process, String id, String name) {
        ParallelGateway gateway = new ParallelGateway();
        gateway.setId(id);
        gateway.setName(name);
        process.addFlowElement(gateway);
    }

    /** Any authored gateway type — already expresses its own fork/join, so never wrapped. */
    private boolean isGateway(String bpmnType) {
        if (bpmnType == null) {
            return false;
        }
        return bpmnType.toLowerCase(java.util.Locale.ROOT).contains("gateway");
    }

    /** A choice gateway (exclusive / inclusive / event-based): emits at most one outgoing token. */
    private boolean isBranchingGateway(String bpmnType) {
        return "exclusive.gateway".equalsIgnoreCase(bpmnType)
                || "inclusive.gateway".equalsIgnoreCase(bpmnType)
                || "event.based.gateway".equalsIgnoreCase(bpmnType);
    }

    /** Escapes {@code '} inside a JUEL single-quoted literal (branch target id). */
    private String escapeJuelSingleQuoted(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "\\'");
    }

    /** The only legal targets of an event-based gateway (BPMN: intermediate catch events). */
    private static final Set<String> EVENT_GATEWAY_TARGET_TYPES = Set.of(
            "message.intermediate.catch.event",
            "signal.intermediate.catch.event",
            "timer.intermediate.catch.event");

    private void validateEventBasedGatewayTargets(List<EdgeDto> mainEdges,
                                                  Map<String, String> bpmnTypeByNodeId) {
        for (EdgeDto edge : mainEdges) {
            if (!"event.based.gateway".equalsIgnoreCase(bpmnTypeByNodeId.get(edge.getSource()))) {
                continue;
            }
            String targetType = bpmnTypeByNodeId.get(edge.getTarget());
            if (targetType == null
                    || !EVENT_GATEWAY_TARGET_TYPES.contains(targetType.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException(
                        "event.based.gateway node=" + edge.getSource() + " has an outgoing edge to "
                                + edge.getTarget() + "(" + targetType + ") — an event-based gateway "
                                + "may only feed intermediate catch events (message/signal/timer)");
            }
        }
    }

    /**
     * Blank edge ids fall back to {@code flow_<src>_<tgt>} — two edges between the same pair
     * (or two authored ids that NCName-sanitise alike) would collide and the deploy would
     * fail late on Activiti's duplicate-id error. Suffix until the id is free in the process.
     */
    private String uniqueFlowId(Process process, String baseId) {
        String candidate = baseId;
        for (int suffix = 2; process.getFlowElement(candidate) != null; suffix++) {
            candidate = baseId + "_" + suffix;
        }
        return candidate;
    }

    private void markAsDefaultFlow(Process process, String gatewayId, String flowId) {
        FlowElement gateway = process.getFlowElement(gatewayId);
        if (gateway instanceof Gateway g) {
            String previous = g.getDefaultFlow();
            if (StringUtils.isNotBlank(previous) && !previous.equals(flowId)) {
                // Multiple blank outgoings on one XOR/OR: the LAST one written becomes the
                // default, the earlier ones stay unconditional (always taken). That makes
                // routing depend on edge order — surface it instead of staying silent.
                log.warn("gateway {} has more than one blank outgoing flow: default flow "
                        + "re-assigned {} -> {}; the earlier flow is now unconditional "
                        + "(always taken). Give every non-default outgoing a condition.",
                        gatewayId, previous, flowId);
            }
            g.setDefaultFlow(flowId);
        }
    }

    private String resolveLabel(NodeDto node) {
        TemplateDto template = node.getData();
        if (template != null && StringUtils.isNotBlank(template.getLabel())) {
            return template.getLabel();
        }
        if (template != null && StringUtils.isNotBlank(template.getName())) {
            return template.getName();
        }
        return node.getId();
    }

    private ActivitiFlowNodeStrategy selectStrategy(String bpmnType) {
        return strategies.stream()
                .filter(s -> s.supports(bpmnType))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No ActivitiFlowNodeStrategy for bpmnType=" + bpmnType));
    }

    // ---------------------------------------------------------------------
    // Deploy-time required-parameter validation (audit item 3).
    // ---------------------------------------------------------------------

    /**
     * Collects one violation per blank REQUIRED configuration parameter of an emitted node. The
     * node's template snapshot carries the {@code required} flags, so the daemon can fail the
     * deploy loudly instead of letting the runtime invent its own value (the signal/message
     * registry used to mint a UUID name for a blank {@code name}, and the deployed flow then
     * never matched what the author configured). A bound secret placeholder
     * ({@code {"$secretRef":…}}) stringifies to non-blank JSON and satisfies the requirement.
     */
    private void collectRequiredParamViolations(NodeDto node, List<String> violations) {
        TemplateDto data = node == null ? null : node.getData();
        if (data == null || data.getConfigurationParameters() == null) {
            return;
        }
        for (ParameterDto param : data.getConfigurationParameters()) {
            if (param == null || !param.isRequired()) {
                continue;
            }
            if (StringUtils.isBlank(TemplateDto.stringifyParameterValue(param.getValue()))) {
                violations.add("node '" + resolveLabel(node) + "' (" + effectiveBpmnType(node)
                        + "): required parameter '" + param.getShortCode() + "' is blank");
            }
        }
    }

    /** Fails the deploy with every violation listed — never one-at-a-time whack-a-mole. */
    private void throwOnRequiredParamViolations(List<String> violations) {
        if (!violations.isEmpty()) {
            throw new IllegalArgumentException("deploy validation failed — " + violations.size()
                    + " required parameter(s) blank: [" + String.join("; ", violations) + "]");
        }
    }

    /**
     * Wires BPMN link throw/catch pairs. The pair is an off-page connector: the designer authors
     * only the shared link {@code name}, no sequence flow joins them. Activiti 9 parses
     * {@code LinkEventDefinition} but never executes the jump (the throw behavior is the default
     * pass-through and the definition is decorative), so the jump is materialised here as a real
     * sequence flow from each throw to the paired catch — exactly what a BPMN link event stands
     * for. Several throws may share one link name; the catch must be unique per name. Anything
     * unpaired or ambiguous fails the deploy loudly, like a blank required parameter.
     */
    private void pairLinkEvents(Process process, Collection<NodeDto> nodes,
                                Map<String, GeneratedNode> generatedByNodeId,
                                Map<String, String> bpmnTypeByNodeId) {
        Map<String, List<String>> throwElementIdsByLinkName = new LinkedHashMap<>();
        Map<String, String> catchElementIdByLinkName = new LinkedHashMap<>();
        List<String> duplicateCatchNames = new ArrayList<>();
        for (NodeDto node : nodes) {
            String bpmnType = bpmnTypeByNodeId.get(node.getId());
            if (bpmnType == null) {
                continue; // dropped as unreachable, or compiled into a function sub-process
            }
            boolean isThrow = "link.intermediate.throw.event".equalsIgnoreCase(bpmnType);
            boolean isCatch = "link.intermediate.catch.event".equalsIgnoreCase(bpmnType);
            if (!isThrow && !isCatch) {
                continue;
            }
            TemplateDto data = node.getData();
            String linkName = data == null ? null : data.findParameterValueByShortCode("name");
            if (StringUtils.isBlank(linkName)) {
                continue; // the required-parameter gate already names it
            }
            GeneratedNode generated = generatedByNodeId.get(node.getId());
            if (generated == null) {
                continue;
            }
            if (isThrow) {
                throwElementIdsByLinkName.computeIfAbsent(linkName, k -> new ArrayList<>())
                        .add(generated.entryElementId());
            } else if (catchElementIdByLinkName.put(linkName, generated.entryElementId()) != null) {
                duplicateCatchNames.add(linkName);
            }
        }
        if (throwElementIdsByLinkName.isEmpty() && catchElementIdByLinkName.isEmpty()) {
            return;
        }
        List<String> violations = new ArrayList<>();
        for (String name : duplicateCatchNames) {
            violations.add("link '" + name + "' has more than one catch event"
                    + " — a link name identifies exactly one catch");
        }
        for (Map.Entry<String, List<String>> entry : throwElementIdsByLinkName.entrySet()) {
            String catchElementId = catchElementIdByLinkName.get(entry.getKey());
            if (catchElementId == null || duplicateCatchNames.contains(entry.getKey())) {
                violations.add("link throw '" + entry.getKey() + "' has no paired catch event");
                continue;
            }
            for (String throwElementId : entry.getValue()) {
                // The catch's LinkEventDefinition requires a `source` (Activiti validation);
                // the jump itself rides the materialised sequence flow.
                FlowElement catchElement = process.getFlowElement(catchElementId);
                ((LinkEventDefinition) ((IntermediateCatchEvent) catchElement)
                        .getEventDefinitions().get(0)).addSource(throwElementId);
                process.addFlowElement(newFlow("flow_link_" + throwElementId,
                        throwElementId, catchElementId));
            }
        }
        for (String name : catchElementIdByLinkName.keySet()) {
            if (!throwElementIdsByLinkName.containsKey(name)) {
                violations.add("link catch '" + name + "' has no paired throw event");
            }
        }
        if (!violations.isEmpty()) {
            throw new IllegalArgumentException("deploy validation failed — " + violations.size()
                    + " link event pairing problem(s): [" + String.join("; ", violations) + "]");
        }
    }

    /**
     * Standard BPMN types with NO engine strategy and NO safe structural remap. All are
     * {@code published:false} in the template catalog, so only an API-crafted graph can carry
     * them — and for those the generic nodeType remap would be a silent lie: complex.gateway is
     * NOT an exclusive gateway (its activationCondition would be dropped), cancel.end.event is
     * NOT a none end (there is no transaction subprocess to cancel), and the compensate throw/end
     * events are NOT executable here at all — the engine has no compensation boundary events,
     * handlers or event-subprocesses, so a compensate throw was a no-op whose {@code activityRef}
     * never even matched (the authored id lacks the {@code node_} prefix the strategies mint).
     * The conditional/multiple start events are NOT plain start events either: remapped to
     * {@code start.event} a conditional start fires IMMEDIATELY without its condition ever being
     * evaluated, and the intermediate conditional/multiple catches would degrade into a sandbox
     * service task. A business.rule.task is not executable either: the daemon ships no DMN/rules
     * engine, so the emitted BusinessRuleTask's ruleNames could never resolve and the token failed
     * at runtime. Fail the deploy loudly instead (old-audit M2, deep-audit M2/M3).
     */
    private static final Set<String> UNSUPPORTED_STANDARD_TYPES = Set.of(
            "complex.gateway", "cancel.end.event",
            "compensate.intermediate.throw.event", "compensate.end.event",
            "conditional.start.event", "multiple.start.event", "parallel.multiple.start.event",
            "conditional.intermediate.catch.event", "multiple.intermediate.catch.event",
            "parallel.multiple.intermediate.catch.event",
            "business.rule.task");

    /**
     * Resolves the effective strategy key for a node. When a node's {@code bpmnType} has NO specific
     * strategy (it would fall to the sandbox fallback) yet its {@code nodeType} is structural, we
     * honour the structural role so a start/end node always becomes the right BPMN element.
     * Standard types in {@link #UNSUPPORTED_STANDARD_TYPES} never reach the remap — they fail.
     */
    private String effectiveBpmnType(NodeDto node) {
        TemplateDto data = node.getData();
        String bpmnType = data == null ? null : data.getBpmnType();
        if (hasSpecificStrategy(bpmnType)) {
            return bpmnType;
        }
        if (bpmnType != null && UNSUPPORTED_STANDARD_TYPES.stream().anyMatch(bpmnType::equalsIgnoreCase)) {
            throw new IllegalArgumentException("node '" + resolveLabel(node) + "' (" + bpmnType
                    + ") is not supported by the engine: no strategy exists and the structural remap"
                    + " would silently change its semantics. Remove or replace the node.");
        }
        String nodeType = data == null ? null : data.getNodeType();
        if ("start".equalsIgnoreCase(nodeType)) {
            return "start.event";
        }
        if ("end".equalsIgnoreCase(nodeType)) {
            return "end.event";
        }
        if ("gateway".equalsIgnoreCase(nodeType)) {
            return "exclusive.gateway";
        }
        return bpmnType; // no structural signal → sandbox ServiceTask fallback
    }

    /**
     * True when a NON-fallback strategy handles {@code bpmnType}. The catch-all fallback is the only
     * strategy that returns {@code true} for an arbitrary sentinel, so we exclude any strategy
     * matching that sentinel.
     */
    private boolean hasSpecificStrategy(String bpmnType) {
        if (StringUtils.isBlank(bpmnType)) {
            return false;
        }
        final String neverMatch = "__sq_never_match__";
        return strategies.stream()
                .anyMatch(s -> !s.supports(neverMatch) && s.supports(bpmnType));
    }

    // =====================================================================
    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    // Function-port sub-process generation (Process_SubProc_<rootId>).
    // A "function root" is the INPUT/FUNCTION target of a FUNCTION-port edge. Each root compiles to
    // a standalone process. collectReachableNodes stops at FUNCTION/VARIABLE_PROVIDER ("special")
    // edges, so a nested function port is naturally excluded from its parent's set and gets its OWN
    // sub-process — a flat scan over all roots therefore handles nesting correctly (no recursion).
    // =====================================================================

    /** @return {@code rootNodeId -> sub-process BPMN XML} for every function-port root. */
    public Map<String, String> generateSubProcessBpmnXmls(String workflowName,
                                                          List<NodeDto> nodes,
                                                          List<EdgeDto> edges) {
        return generateSubProcessBpmnXmls(workflowName, nodes, edges, "");
    }

    /** @param namespaceSuffix see {@link #generate(String, String, List, List, String)}. */
    public Map<String, String> generateSubProcessBpmnXmls(String workflowName,
                                                          List<NodeDto> nodes,
                                                          List<EdgeDto> edges,
                                                          String namespaceSuffix) {
        Map<String, String> result = new LinkedHashMap<>();
        if (nodes == null || edges == null) {
            return result;
        }
        boundaryEventHelper.resolveBoundaryTargets(nodes, edges);

        Map<String, NodeDto> nodeById = new LinkedHashMap<>();
        nodes.forEach(n -> nodeById.put(n.getId(), n));

        for (String rootId : findFunctionRoots(edges)) {
            if (result.containsKey(rootId) || !nodeById.containsKey(rootId)) {
                continue;
            }
            try {
                result.put(rootId,
                        generateSingleSubProcess(workflowName, rootId, nodeById, edges, namespaceSuffix));
            } catch (Exception ex) {
                // Loud deploy failure: a swallowed sub-process deploys the main flow WITHOUT
                // Process_SubProc_<rootId>, and the run only fails when studioquack.execute()
                // targets the missing key — the worst possible time to surface a generation bug.
                throw new IllegalStateException("sub-process generation failed for function root '"
                        + rootId + "' in workflow '" + workflowName + "': " + ex.getMessage(), ex);
            }
        }
        return result;
    }

    private String generateSingleSubProcess(String workflowName, String rootId,
                                            Map<String, NodeDto> nodeById, List<EdgeDto> allEdges,
                                            String namespaceSuffix) {
        // Same fence as collectFunctionSubProcessNodes: a main-flow node belongs to the main process and
        // must not be duplicated into this sub-process, or one node would be compiled into two
        // definitions and dispatched twice.
        Set<String> reachable = collectReachableNodes(rootId, nodeById, allEdges);
        reachable.removeAll(collectMainFlowNodes(new ArrayList<>(nodeById.values()), allEdges, nodeById));
        reachable.add(rootId);
        List<EdgeDto> subEdges = allEdges.stream()
                .filter(e -> reachable.contains(e.getSource()) && reachable.contains(e.getTarget()))
                .filter(e -> !isSpecialConnection(e, nodeById))
                .filter(e -> !isBoundaryConnection(e) && !isAssociationConnection(e))
                .toList();

        BpmnModel model = new BpmnModel();
        model.setTargetNamespace(TARGET_NAMESPACE);
        Process process = new Process();
        process.setId("Process_SubProc_" + rootId + nullToEmpty(namespaceSuffix));
        process.setName(StringUtils.defaultIfBlank(workflowName, rootId) + "_SubProc_" + rootId);
        process.setExecutable(true);
        model.addProcess(process);

        // Synthetic start → the (task-ified) root node.
        StartEvent start = new StartEvent();
        start.setId("start_sub_" + rootId);
        start.setName("Start Sub-Process");
        process.addFlowElement(start);

        Map<String, GeneratedNode> generated = new LinkedHashMap<>();
        Map<String, String> effectiveBpmnType = new LinkedHashMap<>();
        List<String> subRequiredParamViolations = new ArrayList<>();
        for (String nid : reachable) {
            NodeDto node = nodeById.get(nid);
            if (node == null) {
                continue;
            }
            // A Duckbot end reached through a function port is still a boundary adapter: its
            // bridge (answer persist / thinking render) runs inside the dispatch delegate, so it
            // must compile to a task exactly like in the main process. A plain EndEvent would
            // complete the sub-process without ever running the bridge — the delivery vanishes.
            // The sub-process's synthetic end_sub_ terminates the task, so no end_duck_ is needed.
            if (isDuckbotBoundaryNode(node, "end")) {
                collectRequiredParamViolations(node, subRequiredParamViolations);
                generated.put(nid, selectStrategy("__sq_forced_task__")
                        .create(model, process, node, resolveLabel(node)));
                effectiveBpmnType.put(nid, "__sq_duckbot_end_task__");
                continue;
            }
            String bpmnType = node.getData() == null ? null : node.getData().getBpmnType();
            String nodeType = node.getData() == null ? null : node.getData().getNodeType();
            // The root (a "start" in the parent graph) must become a task here, not a StartEvent —
            // force the fallback sandbox strategy for start-like nodes.
            boolean startLike = isStartLike(bpmnType) || "start".equalsIgnoreCase(nodeType);
            String effective = startLike ? "__sq_forced_task__" : effectiveBpmnType(node);
            collectRequiredParamViolations(node, subRequiredParamViolations);
            generated.put(nid, selectStrategy(effective).create(model, process, node, resolveLabel(node)));
            effectiveBpmnType.put(nid, effective);
        }
        throwOnRequiredParamViolations(subRequiredParamViolations);

        // Link pairs inside a function sub-process resolve exactly like in the main process.
        List<NodeDto> subNodes = reachable.stream()
                .map(nodeById::get)
                .filter(java.util.Objects::nonNull)
                .toList();
        pairLinkEvents(process, subNodes, generated, effectiveBpmnType);

        // Auto parallel gateways — the same fork/join synthesis the main process gets (step 2 of
        // generate): without it a converging node inside a function sub-process fired on the FIRST
        // arriving token instead of waiting for every branch, and a multi-outgoing node was an
        // uncontrolled fork.
        Map<String, GeneratedNode> wiring =
                insertParallelGateways(process, subEdges, generated, effectiveBpmnType);

        GeneratedNode rootGen = wiring.get(rootId);
        if (rootGen != null) {
            process.addFlowElement(newFlow("flow_start_" + rootId, start.getId(), rootGen.entryElementId()));
        }

        Map<String, Integer> wiredOutDegree = wiredOutDegree(subEdges, wiring);
        for (EdgeDto edge : subEdges) {
            wireEdge(process, edge, wiring, effectiveBpmnType, nodeById, wiredOutDegree);
        }
        ensureSyntheticDefaultOnConditionedOnlyGateways(process);

        // Terminal nodes (no outgoing sub-edge, not already an end) → synthetic end.
        Set<String> sources = new HashSet<>();
        subEdges.forEach(e -> sources.add(e.getSource()));
        EndEvent end = null;
        for (String nid : reachable) {
            if (sources.contains(nid) || isEndLike(effectiveBpmnType.get(nid))) {
                continue;
            }
            GeneratedNode g = generated.get(nid);
            if (g == null) {
                continue;
            }
            if (end == null) {
                end = new EndEvent();
                end.setId("end_sub_" + rootId);
                end.setName("End Sub-Process");
                process.addFlowElement(end);
            }
            process.addFlowElement(newFlow("flow_" + nid + "_end", wiring.get(nid).exitElementId(), end.getId()));
        }

        boundaryEventHelper.repairDanglingBoundaryRoutes(process);

        // Signal the parent (function-port caller) when the sub-process ends.
        attachCompletionListenerToEndEvents(process);

        return new String(new BpmnXMLConverter().convertToXML(model), StandardCharsets.UTF_8);
    }

    /**
     * Attaches the {@code end} {@code ${subProcessCompletionListener}} execution listener to every
     * {@link EndEvent} of a sub-process, so the parent's parked future resolves by businessKey when
     * the sub-process finishes (success end or shared error end) — and the {@code end}
     * {@code ${callActivityChildFailureListener}}, so a {@code call.activity} child reaching a
     * failure end pins its error code onto the parent for the propagation listener to re-raise.
     */
    private void attachCompletionListenerToEndEvents(Process process) {
        for (FlowElement element : process.getFlowElements()) {
            if (!(element instanceof EndEvent endEvent)) {
                continue;
            }
            List<ActivitiListener> listeners = new ArrayList<>();
            if (endEvent.getExecutionListeners() != null) {
                listeners.addAll(endEvent.getExecutionListeners());
            }
            listeners.add(endListener("${subProcessCompletionListener}"));
            listeners.add(endListener("${callActivityChildFailureListener}"));
            endEvent.setExecutionListeners(listeners);
        }
    }

    /**
     * Main-process variant: attaches only the {@code ${callActivityChildFailureListener}} (the
     * sub-PI completion listener is function-port machinery and stays sub-process-only).
     */
    private void attachChildFailureListenerToEndEvents(Process process) {
        for (FlowElement element : process.getFlowElements()) {
            if (!(element instanceof EndEvent endEvent)) {
                continue;
            }
            List<ActivitiListener> listeners = new ArrayList<>();
            if (endEvent.getExecutionListeners() != null) {
                listeners.addAll(endEvent.getExecutionListeners());
            }
            listeners.add(endListener("${callActivityChildFailureListener}"));
            endEvent.setExecutionListeners(listeners);
        }
    }

    private static ActivitiListener endListener(String delegateExpression) {
        ActivitiListener listener = new ActivitiListener();
        listener.setEvent("end");
        listener.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation(delegateExpression);
        return listener;
    }

    /** Function roots = INPUT/FUNCTION targets of FUNCTION-port edges. */
    private Set<String> findFunctionRoots(List<EdgeDto> edges) {
        Set<String> roots = new HashSet<>();
        for (EdgeDto e : edges) {
            String sourceType = portType(e.getSourceHandle());
            String targetType = portType(e.getTargetHandle());
            if ("FUNCTION".equalsIgnoreCase(sourceType)
                    && ("INPUT".equalsIgnoreCase(targetType) || "FUNCTION".equalsIgnoreCase(targetType))) {
                roots.add(e.getTarget());
            }
        }
        return roots;
    }

    /** BFS from {@code startId} over NON-special (sequence-flow) edges only. */
    private Set<String> collectReachableNodes(String startId, Map<String, NodeDto> nodeById, List<EdgeDto> edges) {
        Set<String> visited = new HashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(startId);
        visited.add(startId);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            for (EdgeDto e : edges) {
                if (current.equals(e.getSource()) && !isSpecialConnection(e, nodeById) && visited.add(e.getTarget())) {
                    queue.add(e.getTarget());
                }
            }
        }
        return visited;
    }

    /** A special connection touches a FUNCTION / VARIABLE_PROVIDER port (from the handle prefix). */
    private boolean isSpecialConnection(EdgeDto edge, Map<String, NodeDto> nodeById) {
        return isSpecialPort(portType(edge.getSourceHandle())) || isSpecialPort(portType(edge.getTargetHandle()));
    }

    /**
     * A boundary connection leaves a boundary event rather than the node's own exit; the generated
     * boundary owns its outgoing flow, so this edge must not also be wired as a sequence flow.
     */
    private boolean isBoundaryConnection(EdgeDto edge) {
        return edge.getData() != null && StringUtils.isNotBlank(edge.getData().getBoundaryEventId());
    }

    /**
 *ASSOCIATION edges are documentation on the canvas — no sequence flow, no fork/join
 * degree, no transform. Same exclusion lane as {@link #isBoundaryConnection}.
 */
    private boolean isAssociationConnection(EdgeDto edge) {
        return edge.getData() != null
                && "ASSOCIATION".equalsIgnoreCase(edge.getData().getFlowType());
    }

    private boolean isSpecialPort(String portType) {
        return "FUNCTION".equalsIgnoreCase(portType) || "VARIABLE_PROVIDER".equalsIgnoreCase(portType);
    }

    /** Handle wire format is {@code "{PORTTYPE}-{portValue}"}; return the PORTTYPE prefix. */
    private String portType(String handle) {
        if (StringUtils.isBlank(handle)) {
            return null;
        }
        int dash = handle.indexOf('-');
        return dash > 0 ? handle.substring(0, dash).trim() : handle.trim();
    }

    /** Handle wire format is {@code "{PORTTYPE}-{portValue}"}; return the portValue half. */
    private String portValue(String handle) {
        if (StringUtils.isBlank(handle)) {
            return null;
        }
        int dash = handle.indexOf('-');
        return dash > 0 ? StringUtils.trimToNull(handle.substring(dash + 1)) : null;
    }

    /**
     * A port-routed exit (message-catch timeout branch under {@code onTimeoutBehavior=route}) whose
     * element ends up with no outgoing flow absorbs the token quietly — name it at deploy time so
     * the author either wires the Timeout port or picks a terminal behaviour (cancel/error).
     * Blank mappings are deliberate swallows and are not checked.
     */
    private void warnOnDanglingPortExits(Process process, Map<String, GeneratedNode> generatedByNodeId) {
        for (GeneratedNode gen : generatedByNodeId.values()) {
            gen.exitElementIdByPortValue().forEach((port, elementId) -> {
                if (StringUtils.isBlank(elementId)) {
                    return;
                }
                if (process.getFlowElement(elementId) instanceof FlowNode flowNode
                        && flowNode.getOutgoingFlows().isEmpty()) {
                    log.warn("activiti.bpmn.generated process={} port-routed exit {} (port='{}') has no "
                                    + "outgoing flow — a token taking that branch ends there. Wire the port "
                                    + "or choose a terminal behaviour.",
                            process.getId(), elementId, port);
                }
            });
        }
    }

    private boolean isStartLike(String bpmnType) {
        return "start.event".equalsIgnoreCase(bpmnType) || "start".equalsIgnoreCase(bpmnType);
    }

    private boolean isEndLike(String bpmnType) {
        return bpmnType != null && bpmnType.toLowerCase(java.util.Locale.ROOT).endsWith("end.event");
    }

    private SequenceFlow newFlow(String id, String sourceRef, String targetRef) {
        SequenceFlow flow = new SequenceFlow(sourceRef, targetRef);
        flow.setId(id);
        return flow;
    }

    /**
     * Builds an XML-NCName-safe id from a letter-starting {@code prefix} plus a raw token. NCName
     * rules: first char must be a letter or '_', remaining chars may be letters, digits, '.', '-'
     * or '_'. FE ids are UUIDs (digit-first, hyphenated) so a static letter prefix + light
     * sanitisation of the remainder guarantees a valid, stable, collision-free id.
     */
    private String toNcName(String prefix, String raw) {
        String safeRemainder = (raw == null ? "" : raw).replaceAll("[^A-Za-z0-9_.-]", "_");
        return prefix + safeRemainder;
    }

    /** Exposed for callers that want the route-token variable name convention. */
    public static String routeTokenName(String nodeId) {
        return EngineVariableNames.routeTokenVariableName(nodeId);
    }

    // =====================================================================
    // Graph signature (bpmn-signature:v8) — the control plane
    // WorkflowExecutionServiceImpl.computeGraphSignature shape. Nodes and edges are serialised and
    // sorted so the hash is order-independent; the workflow name is folded in. Only BPMN-shaping
    // fields are hashed (runtime enrichment such as connectionList/inputList/outputList must not
    // force a redeploy). Returns null on any failure, which the caller treats as a cache miss.
    // =====================================================================

    /**
     * @return a stable SHA-256 hex signature of the workflow graph ({@code bpmn-signature:v8}), or
     * {@code null} on any failure (treated by callers as a cache miss → deploy, never a false hit).
     */
    public String computeGraphSignature(String name, List<NodeDto> nodes, List<EdgeDto> edges) {
        try {
            // Resolved here too: callers compute the signature before OR after generating, and a
            // signature that moved depending on which ran first would thrash the deploy-reuse cache.
            boundaryEventHelper.resolveBoundaryTargets(nodes, edges);

            List<Map<String, Object>> nodeShapes = new ArrayList<>();
            if (nodes != null) {
                for (NodeDto n : nodes) {
                    nodeShapes.add(toBpmnNodeSignature(n));
                }
            }
            nodeShapes.sort(Comparator.comparing(m -> String.valueOf(m.get("id"))));
            List<Map<String, Object>> edgeShapes = new ArrayList<>();
            if (edges != null) {
                for (EdgeDto e : edges) {
                    edgeShapes.add(toBpmnEdgeSignature(e));
                }
            }
            edgeShapes.sort(Comparator.comparing(m -> String.join("|",
                    String.valueOf(m.get("id")),
                    String.valueOf(m.get("source")),
                    String.valueOf(m.get("target")),
                    String.valueOf(m.get("sourceHandle")),
                    String.valueOf(m.get("targetHandle")))));
            // v7: a node's boundaryEventConfigs now compile to real boundary events, so the same
            // graph emits different XML than a v6 signature was computed over — without the bump a
            // cached v6 signature would still match and the daemon would keep running the old XML.
            // Do NOT change the emitted BPMN without a version bump.
            // v8: function-rooted Duckbot ends compile to a forced task (was a plain EndEvent) —
            // same graph, different sub-process XML, so cached v7 deployments must redeploy.
            // v9: the node shape now hashes configuration parameter VALUES (was a presence
            // boolean), so a value-only edit (message name, correlationKey, timer expression)
            // busts the reuse cache instead of silently keeping the stale deployed XML.
            // Do NOT change the emitted BPMN without a version bump.
            String canonical = "bpmn-signature:v9|"
                    + (name == null ? "" : name)
                    + "|nodes=" + signatureMapper.writeValueAsString(nodeShapes)
                    + "|edges=" + signatureMapper.writeValueAsString(edgeShapes);
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception ex) {
            log.warn("graph signature computation failed; treating as cache miss: {}", ex.getMessage());
            return null;
        }
    }

    private Map<String, Object> toBpmnNodeSignature(NodeDto node) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (node == null) {
            return out;
        }
        out.put("id", node.getId());
        out.put("type", node.getType());
        out.put("parentId", node.getParentId());

        TemplateDto data = node.getData();
        if (data != null) {
            Map<String, Object> template = new LinkedHashMap<>();
            template.put("id", data.getId());
            template.put("bpmnType", data.getBpmnType());
            template.put("nodeType", data.getNodeType());
            template.put("category", data.getCategory());
            template.put("jobType", data.getJobType());
            template.put("label", data.getLabel());
            template.put("rootId", data.getRootId());
            template.put("ports", data.getPorts());
            // v9: configuration parameter VALUES are BPMN-shaping (a message event's name /
            // correlationKey, a timer's expression, …) — the old hasConfigurationParameters
            // boolean let a value-only edit reuse the stale deployed XML (the tt4 manual-test
            // failure: key removed, new version deployed, daemon kept the filtered start).
            // Hashed as shortCode→value rows; UI-only ParameterDto fields stay out.
            List<Map<String, Object>> configParams = new ArrayList<>();
            if (data.getConfigurationParameters() != null) {
                for (ParameterDto p : data.getConfigurationParameters()) {
                    if (p == null) {
                        continue;
                    }
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("shortCode", p.getShortCode());
                    row.put("value", p.getValue());
                    configParams.add(row);
                }
            }
            template.put("configurationParameters", configParams);
            template.put("boundaryEventConfigs", data.getBoundaryEventConfigs());
            out.put("data", template);
        }
        return out;
    }

    private Map<String, Object> toBpmnEdgeSignature(EdgeDto edge) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (edge == null) {
            return out;
        }
        out.put("id", edge.getId());
        out.put("source", edge.getSource());
        out.put("sourceHandle", edge.getSourceHandle());
        out.put("target", edge.getTarget());
        out.put("targetHandle", edge.getTargetHandle());
        out.put("type", edge.getType());
        if (edge.getData() != null) {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("boundaryEventId", edge.getData().getBoundaryEventId());
            //flow kind / typed condition / default flag must bust deploy-reuse.
            data.put("flowType", edge.getData().getFlowType());
            data.put("condition", edge.getData().getCondition());
            data.put("isDefaultFlow", edge.getData().getIsDefaultFlow());
            out.put("data", data);
        }
        return out;
    }

    /**
     * Result of {@link #generate}: the main process BPMN, every function-port sub-process BPMN
     * (keyed by root node id), and the deterministic {@code bpmn-signature:v8} graph signature.
     */
    public record GeneratedBpmn(String mainXml, Map<String, String> subProcessXmls, String graphSignature) {
    }
}
