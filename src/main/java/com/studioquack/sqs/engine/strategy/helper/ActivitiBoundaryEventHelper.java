package com.studioquack.sqs.engine.strategy.helper;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.BoundaryEventConfig.BoundaryEventType;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.Activity;
import org.activiti.bpmn.model.ActivitiListener;
import org.activiti.bpmn.model.BoundaryEvent;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EndEvent;
import org.activiti.bpmn.model.ErrorEventDefinition;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.ImplementationType;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.SignalEventDefinition;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Compiles a node's {@link TemplateDto#getBoundaryEventConfigs()} into real Activiti
 * {@code <boundaryEvent attachedToRef=…>} elements. The four types the designer offers are emitted:
 *
 * <table><caption>configured boundary → emitted element</caption>
 *   <tr><th>type</th><th>event definition</th><th>host activity</th></tr>
 *   <tr><td>{@code ERROR}</td><td>{@code errorEventDefinition} with {@code errorRef=Error_&lt;code&gt;}, or no {@code errorRef} when the code is blank (Activiti catch-all)</td><td>{@code errgate_<id>}</td></tr>
 *   <tr><td>{@code TIMER}</td><td>{@code timerEventDefinition} (duration / date / cycle)</td><td>{@code wait_<id>}</td></tr>
 *   <tr><td>{@code MESSAGE}</td><td>{@code messageEventDefinition messageRef=Message_&lt;name&gt;}</td><td>{@code wait_<id>}</td></tr>
 *   <tr><td>{@code SIGNAL}</td><td>{@code signalEventDefinition signalRef=Signal_&lt;name&gt;}</td><td>{@code wait_<id>}</td></tr>
 *   <tr><td>{@code ESCALATION}</td><td>{@code signalEventDefinition signalRef=Signal___sq_esc_&lt;code&gt;}</td><td>{@code wait_<id>}</td></tr>
 * </table>
 *
 * <p><b>Escalation rides the signal lane.</b> Activiti 9 has no escalation model, so an
 * ESCALATION boundary is emitted as a signal boundary subscribed to the reserved name
 * {@code __sq_esc_<escalationCode>} ({@link EngineVariableNames#ESCALATION_SIGNAL_PREFIX}); the
 * escalation throw/end listener fires it instance-scoped. The reserved prefix is rejected for
 * user-authored signal names, so a real signal can never trip an escalation boundary.
 *
 * <p><b>Why the host differs.</b> The sandbox pattern is
 * {@code node_X (async ServiceTask) → wait_X (ReceiveTask) → errgate_X (ServiceTask)}. Only
 * {@code errgate_X} can raise the {@code BpmnError} an error boundary catches, so ERROR stays there.
 * Everything else must attach to {@code wait_X}: {@code node_X} is {@code activiti:async} and is left
 * within milliseconds of dispatching the container, and {@code errgate_X} runs only after the result
 * is already back — a boundary on either would have no window in which to fire. {@code wait_X} is the
 * element the process instance is genuinely parked on for the whole sandbox execution, so an
 * interrupting boundary there abandons the step (and skips the error gate, so a timeout is never
 * re-read as a typed sandbox error), while a non-interrupting one leaves the parked
 * {@code ReceiveTask} untouched and the sandbox result can still resume it.
 *
 * <p><b>Routing.</b> The handler comes from the boundary's own designer edge — the one whose
 * {@code data.boundaryEventId} equals the config's {@code id} and whose {@code source} is the host
 * node — resolved by {@link #resolveBoundaryTargets} before generation and read back off
 * {@link BoundaryEventConfig#getTargetNodeId()}. It routes to {@code node_<targetNodeId>}; with no
 * target at all the boundary falls back to {@link ActivitiBpmnElementHelper#SHARED_ERROR_END_ID},
 * exactly like a per-code error boundary without a handler.
 *
 * <p><b>Fired-event data.</b> Every emitted boundary carries a {@code start} execution listener
 * ({@code ${sqBoundaryEventDelegate}}) that publishes what the event can hand to the node it triggers
 * — the fields the designer maps from the boundary port's output schema. The element id is the join
 * key between the two halves, so it is minted in exactly one place ({@link #boundaryElementId}) and
 * read back through {@link #resolveFired(List, String)}.
 *
 * <p><b>Bounds.</b> Boundary configs are authored by a user, so a graph must not be able to turn one
 * step into an unbounded scheduler load: at most {@link #MAX_BOUNDARIES_PER_NODE} boundaries are
 * emitted per node, and a timer cycle is normalised to a bounded, rate-limited repeat
 * ({@link ActivitiTimerDefinitionFactory#boundedTimerCycle}). Anything that does not parse is
 * dropped rather than deployed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ActivitiBoundaryEventHelper {

    /**
     * Upper bound on boundary events emitted for one node. Each one is a persistent timer job or
     * event subscription per run, so an unbounded config list would multiply engine load by the
     * number of runs.
     */
    public static final int MAX_BOUNDARIES_PER_NODE = 16;

    private final ActivitiSignalMessageRegistry registry;
    private final ActivitiMessageReceiverConfigurator receiverConfigurator;

    /**
 * Populates {@link BoundaryEventConfig#getTargetNodeId} on every boundary config from the
 * designer edge that leaves it: the edge whose {@code data.boundaryEventId} equals the config's
 * {@code id} and whose {@code source} is the host node. Idempotent, so it may run once per public
 * generator entry point without the result depending on the call order.
 *
 * <p>The edge is the ONLY dependable source. {@code targetNodeId} is a {@code transient} field
 * that the stored workflow document does not persist, so a config read back from the stored workflow
 * document always arrives with it null even though the designer set it — whereas
 * {@code EdgeDataDto.boundaryEventId} is an ordinary persisted field. A value that IS present is
 * therefore kept only as a fallback hint, for payloads that never went through the store.
 */
    public void resolveBoundaryTargets(List<NodeDto> nodes, List<EdgeDto> edges) {
        if (nodes == null || nodes.isEmpty() || edges == null || edges.isEmpty()) {
            return;
        }
        Map<BoundaryEdgeKey, String> handlerByBoundary = new HashMap<>();
        for (EdgeDto edge : edges) {
            if (edge == null || edge.getData() == null
                    || StringUtils.isAnyBlank(edge.getSource(), edge.getTarget())) {
                continue;
            }
            String boundaryEventId = StringUtils.trimToNull(edge.getData().getBoundaryEventId());
            if (boundaryEventId == null) {
                continue;
            }
            BoundaryEdgeKey key = new BoundaryEdgeKey(edge.getSource(), boundaryEventId);
            String previous = handlerByBoundary.putIfAbsent(key, edge.getTarget());
            if (previous != null) {
                log.warn("node={} boundary={} has more than one outgoing edge — keeping {} and ignoring {}.",
                        edge.getSource(), boundaryEventId, previous, edge.getTarget());
            }
        }
        if (handlerByBoundary.isEmpty()) {
            return;
        }
        for (NodeDto node : nodes) {
            TemplateDto template = node == null ? null : node.getData();
            List<BoundaryEventConfig> configs = template == null ? null : template.getBoundaryEventConfigs();
            if (configs == null || configs.isEmpty()) {
                continue;
            }
            for (BoundaryEventConfig cfg : configs) {
                String boundaryEventId = cfg == null ? null : StringUtils.trimToNull(cfg.getId());
                if (boundaryEventId == null) {
                    continue;
                }
                String handler = handlerByBoundary.get(new BoundaryEdgeKey(node.getId(), boundaryEventId));
                if (handler != null) {
                    cfg.setTargetNodeId(handler);
                }
            }
        }
    }

    /** A boundary event is identified by its host node plus its id; ids are unique per node only. */
    private record BoundaryEdgeKey(String hostNodeId, String boundaryEventId) {
    }

    /**
     * Emits every configured boundary of {@code node}.
     *
     * @param errorHostId activity an ERROR boundary attaches to (the error gate)
     * @param waitHostId  activity a TIMER/MESSAGE/SIGNAL boundary attaches to (the wait state), or
     *                    {@code null} when the node has no wait state — those types are then dropped
     */
    public void attachConfiguredBoundaries(BpmnModel model, Process process, NodeDto node,
                                           String errorHostId, String waitHostId) {
        for (PlannedBoundary planned : planBoundaries(node)) {
            emit(model, process, node.getId(), planned, errorHostId, waitHostId);
        }
    }

    /**
     * One configured boundary paired with the BPMN element id it is emitted as. The plan is the single
     * owner of the id ⇄ config mapping: the generator emits from it and
     * {@link #resolveFired(List, String)} reads a fired element id back through it, so the two can
     * never disagree about which config an element belongs to.
     */
    public record PlannedBoundary(String elementId, BoundaryEventConfig config) {
    }

    /**
     * The boundaries of {@code node} that will actually be emitted, in emission order, each with its
     * element id. Configs that cannot produce an element (an unparseable timer, a nameless
     * message/signal, a type this tier does not emit) are dropped here, so they consume neither an
     * index nor a slot of {@link #MAX_BOUNDARIES_PER_NODE}. A blank ERROR code is emittable — it is
     * the user catch-all.
     */
    public List<PlannedBoundary> planBoundaries(NodeDto node) {
        TemplateDto template = node == null ? null : node.getData();
        List<BoundaryEventConfig> configs = template == null ? null : template.getBoundaryEventConfigs();
        if (configs == null || configs.isEmpty()) {
            return List.of();
        }
        String nid = node.getId();
        List<PlannedBoundary> plan = new ArrayList<>();
        Map<BoundaryEventType, Integer> nextIndex = new EnumMap<>(BoundaryEventType.class);
        for (BoundaryEventConfig cfg : configs) {
            if (!isEmittable(nid, cfg)) {
                continue;
            }
            if (plan.size() >= MAX_BOUNDARIES_PER_NODE) {
                log.warn("node={} boundaryEventConfigs truncated at {} — remaining configs dropped.",
                        nid, MAX_BOUNDARIES_PER_NODE);
                break;
            }
            int index = nextIndex.getOrDefault(cfg.getType(), 0);
            nextIndex.put(cfg.getType(), index + 1);
            plan.add(new PlannedBoundary(boundaryElementId(cfg.getType(), nid, index), cfg));
        }
        return plan;
    }

    /**
     * Whether {@code cfg} produces a BPMN element. It decides on the config alone, so the runtime
     * reaches the same verdict for a deployed graph as the generator did.
     */
    private boolean isEmittable(String nid, BoundaryEventConfig cfg) {
        if (cfg == null || cfg.getType() == null) {
            return false;
        }
        return switch (cfg.getType()) {
            // Blank Error Code is an Activiti catch-all (no errorRef) routed to the wired handler.
            case ERROR -> true;
            case TIMER -> timerDefinition(nid, cfg) != null;
            case MESSAGE -> StringUtils.isNotBlank(cfg.getMessageName());
            case SIGNAL -> StringUtils.isNotBlank(cfg.getSignalName());
            case ESCALATION -> StringUtils.isNotBlank(cfg.getEscalationCode());
            // Anything outside the five modelled types is not emitted (COMPENSATION/CONDITIONAL
            // were dropped from BoundaryEventType — see the enum's javadoc for the rejection reasons).
            default -> false;
        };
    }

    /** The BPMN element id a boundary of {@code type} is emitted as; {@code null} for an unemitted type. */
    public static String boundaryElementId(BoundaryEventType type, String nodeId, int index) {
        if (type == null) {
            return null;
        }
        return switch (type) {
            case ERROR -> "errcode_" + nodeId + "_" + index;
            case TIMER -> "btimer_" + nodeId + "_" + index;
            case MESSAGE -> "bmsg_" + nodeId + "_" + index;
            case SIGNAL -> "bsig_" + nodeId + "_" + index;
            case ESCALATION -> "besc_" + nodeId + "_" + index;
            default -> null;
        };
    }

    /**
     * The host node id + config of the boundary emitted as {@code elementId}. This is how the runtime
     * turns a fired boundary back into what it was configured as.
     */
    public FiredBoundary resolveFired(List<NodeDto> nodes, String elementId) {
        if (nodes == null || StringUtils.isBlank(elementId)) {
            return null;
        }
        for (NodeDto node : nodes) {
            if (node == null || StringUtils.isBlank(node.getId())) {
                continue;
            }
            for (PlannedBoundary planned : planBoundaries(node)) {
                if (elementId.equals(planned.elementId())) {
                    return new FiredBoundary(node, planned.config());
                }
            }
        }
        return null;
    }

    /**
     * Runtime twin of {@link #planBoundaries}' verdict for an ERROR config (always emittable):
     * whether {@code node} authors an ERROR boundary whose code — trimmed the way
     * {@link #attachTypedError} trims it — equals {@code errorCode}. The dispatch delegates'
     * operator-skip short-circuit uses this to route {@code SQ_SKIP} exactly like a raised
     * {@code BpmnError} would. The {@link #MAX_BOUNDARIES_PER_NODE} truncation is NOT replicated:
     * a node with 16 earlier boundaries is pathological, and emission order does not change which
     * codes are authored.
     */
    public static boolean hasAuthoredErrorBoundary(NodeDto node, String errorCode) {
        TemplateDto template = node == null ? null : node.getData();
        List<BoundaryEventConfig> configs = template == null ? null : template.getBoundaryEventConfigs();
        if (configs == null || errorCode == null) {
            return false;
        }
        for (BoundaryEventConfig cfg : configs) {
            if (cfg != null && cfg.getType() == BoundaryEventType.ERROR
                    && errorCode.equals(StringUtils.trimToNull(cfg.getErrorCode()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when {@link #planBoundaries} includes an ERROR config whose errorCode is blank after
     * trim. That planned boundary is the user catch-all; the system {@code err_<node>} must not
     * also be emitted on the same activity.
     */
    public boolean hasUserCatchAllError(NodeDto node) {
        for (PlannedBoundary planned : planBoundaries(node)) {
            BoundaryEventConfig cfg = planned.config();
            if (cfg != null && cfg.getType() == BoundaryEventType.ERROR
                    && StringUtils.isBlank(cfg.getErrorCode())) {
                return true;
            }
        }
        return false;
    }

    /** A boundary that fired, resolved back to its host node and its authored config. */
    public record FiredBoundary(NodeDto hostNode, BoundaryEventConfig config) {
    }

    private void emit(BpmnModel model, Process process, String nid, PlannedBoundary planned,
                      String errorHostId, String waitHostId) {
        BoundaryEventConfig cfg = planned.config();
        BoundaryEventType type = cfg.getType();
        if (type == BoundaryEventType.ERROR) {
            attachTypedError(model, process, errorHostId, nid, planned.elementId(), cfg);
            return;
        }
        if (StringUtils.isBlank(waitHostId)) {
            log.debug("node={} boundaryEventConfig type={} dropped — the node has no wait state to "
                    + "attach a non-error boundary to.", nid, type);
            return;
        }
        switch (type) {
            case TIMER -> attachTimer(process, waitHostId, nid, planned.elementId(), cfg);
            case MESSAGE -> attachMessage(model, process, waitHostId, nid, planned.elementId(), cfg);
            case SIGNAL -> attachSignal(model, process, waitHostId, nid, planned.elementId(), cfg);
            case ESCALATION -> attachEscalation(model, process, waitHostId, nid, planned.elementId(), cfg);
            default -> log.debug("node={} boundaryEventConfig type={} is not emitted on this tier.", nid, type);
        }
    }

    // =====================================================================
    // ERROR — the sandbox typed-error path.
    // =====================================================================

    /**
     * Attaches ONE interrupting error boundary. A non-blank {@code cfg.errorCode} becomes a typed
     * {@code errorRef}; a blank/null code is an Activiti catch-all (no {@code errorRef}, no
     * {@code model.addError}) routed to the wired handler. The system {@code err_<node>} is
     * suppressed when this catch-all is planned — two catch-alls on one activity are forbidden.
     */
    private void attachTypedError(BpmnModel model, Process process, String hostId, String nid,
                                  String elementId, BoundaryEventConfig cfg) {
        String code = StringUtils.trimToNull(cfg.getErrorCode());
        ErrorEventDefinition def = new ErrorEventDefinition();
        if (code != null) {
            String errorRef = "Error_" + sanitizeToken(code);
            // addError(errorRef, errorName, errorCode) — the boundary's errorRef resolves to this code.
            model.addError(errorRef, code, code);
            def.setErrorRef(errorRef);
        }
        // BPMN forbids a non-interrupting error boundary, so `interrupting` is not honoured here.
        BoundaryEvent boundary = newBoundary(process, elementId, hostId, true);
        boundary.addEventDefinition(def);
        process.addFlowElement(boundary);
        route(process, boundary, cfg.getTargetNodeId());
        log.debug("node={} error boundary {} code={} interrupting=true", nid, boundary.getId(),
                code == null ? "(catch-all)" : code);
    }

    // =====================================================================
    // TIMER / MESSAGE / SIGNAL / ESCALATION — boundaries on the sandbox wait state.
    // =====================================================================

    private void attachTimer(Process process, String hostId, String nid, String elementId,
                             BoundaryEventConfig cfg) {
        BoundaryEvent boundary = newBoundary(process, elementId, hostId, cfg.isInterrupting());
        boundary.addEventDefinition(timerDefinition(nid, cfg));
        process.addFlowElement(boundary);
        route(process, boundary, cfg.getTargetNodeId());
        log.debug("node={} timer boundary {} interrupting={}", nid, boundary.getId(), cfg.isInterrupting());
    }

    private void attachMessage(BpmnModel model, Process process, String hostId, String nid,
                               String elementId, BoundaryEventConfig cfg) {
        String messageName = StringUtils.trimToNull(cfg.getMessageName());
        MessageEventDefinition def = new MessageEventDefinition();
        def.setMessageRef(registry.getOrCreateMessage(model, messageName));
        BoundaryEvent boundary = newBoundary(process, elementId, hostId, cfg.isInterrupting());
        boundary.addEventDefinition(def);
        String correlationKey = StringUtils.trimToNull(cfg.getMessageCorrelationKey());
        if (correlationKey != null) {
            // Same contract as the message catch node: the authored key rides the deployed model
            // as sq:receiverConfig, and the pin bridge on the HOST (a listener on the boundary
            // element itself never fires at park time) writes the __sq_msgcorr_<name> variable
            // the shared-key correlation fallback queries.
            receiverConfigurator.emitCorrelationKeyConfig(boundary, correlationKey);
            receiverConfigurator.attachCorrelationPinListener(process.getFlowElement(hostId));
        }
        process.addFlowElement(boundary);
        route(process, boundary, cfg.getTargetNodeId());
        log.debug("node={} message boundary {} name={} interrupting={}",
                nid, boundary.getId(), messageName, cfg.isInterrupting());
    }

    private void attachSignal(BpmnModel model, Process process, String hostId, String nid,
                              String elementId, BoundaryEventConfig cfg) {
        String signalName = StringUtils.trimToNull(cfg.getSignalName());
        SignalEventDefinition def = new SignalEventDefinition();
        // The signal stays globally scoped: Activiti's SignalEventReceivedCmd delivers only to
        // globally scoped subscriptions, so a processInstance-scoped boundary signal could never be
        // reached by the daemon's correlation path.
        def.setSignalRef(registry.getOrCreateSignal(model, signalName));
        BoundaryEvent boundary = newBoundary(process, elementId, hostId, cfg.isInterrupting());
        boundary.addEventDefinition(def);
        process.addFlowElement(boundary);
        route(process, boundary, cfg.getTargetNodeId());
        log.debug("node={} signal boundary {} name={} interrupting={}",
                nid, boundary.getId(), signalName, cfg.isInterrupting());
    }

    /**
     * Escalation boundary = a signal boundary on the reserved {@code __sq_esc_<code>} name (see
     * the class javadoc). The throw/end listener fires that name instance-scoped, so the
     * escalation reaches exactly this run's matching boundaries; {@code interrupting} maps to the
     * boundary's native {@code cancelActivity}.
     */
    private void attachEscalation(BpmnModel model, Process process, String hostId, String nid,
                                  String elementId, BoundaryEventConfig cfg) {
        String escalationCode = StringUtils.trimToNull(cfg.getEscalationCode());
        SignalEventDefinition def = new SignalEventDefinition();
        // Globally scoped like a real signal boundary: Activiti's SignalEventReceivedCmd delivers
        // only to globally scoped subscriptions. Instance scoping is the sender's job — it fires
        // per execution of THIS process instance, never the engine-wide broadcast.
        def.setSignalRef(registry.getOrCreateEngineSignal(model,
                EngineVariableNames.ESCALATION_SIGNAL_PREFIX + escalationCode));
        BoundaryEvent boundary = newBoundary(process, elementId, hostId, cfg.isInterrupting());
        boundary.addEventDefinition(def);
        process.addFlowElement(boundary);
        route(process, boundary, cfg.getTargetNodeId());
        log.debug("node={} escalation boundary {} code={} interrupting={}",
                nid, boundary.getId(), escalationCode, cfg.isInterrupting());
    }

    /** @return the timer definition for {@code cfg}, or {@code null} when the config is unusable. */
    private TimerEventDefinition timerDefinition(String nid, BoundaryEventConfig cfg) {
        String value = StringUtils.trimToNull(cfg.getTimerValue());
        if (value == null) {
            log.warn("node={} TIMER boundary dropped — config.timerValue is blank.", nid);
            return null;
        }
        String timerType = StringUtils.trimToEmpty(cfg.getTimerType()).toLowerCase(Locale.ROOT);
        TimerEventDefinition def = new TimerEventDefinition();
        switch (timerType) {
            case "date" -> {
                if (!ActivitiTimerDefinitionFactory.isIsoDateTime(value)) {
                    log.warn("node={} TIMER boundary dropped — timerValue '{}' is not an ISO-8601 instant.",
                            nid, value);
                    return null;
                }
                def.setTimeDate(value);
            }
            case "cycle" -> {
                String bounded = ActivitiTimerDefinitionFactory.boundedTimerCycle(value);
                if (bounded == null) {
                    log.warn("node={} TIMER boundary dropped — timerValue '{}' is not a boundable "
                            + "R[n]/<ISO duration> cycle.", nid, value);
                    return null;
                }
                if (!bounded.equals(value)) {
                    log.info("node={} timer cycle '{}' bounded to '{}'.", nid, value, bounded);
                }
                def.setTimeCycle(bounded);
            }
            // Blank or unrecognised timerType is authored as a duration, matching the timer catch node.
            default -> {
                if (!ActivitiTimerDefinitionFactory.isPositiveIsoDuration(value)) {
                    log.warn("node={} TIMER boundary dropped — timerValue '{}' is not a positive "
                            + "ISO-8601 duration.", nid, value);
                    return null;
                }
                def.setTimeDuration(value);
            }
        }
        return def;
    }

    // =====================================================================
    // Shared element plumbing.
    // =====================================================================

    /** Idempotently creates the single shared sandbox-error end event on the process. */
    public EndEvent getOrCreateSharedErrorEnd(Process process) {
        FlowElement existing = process.getFlowElement(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
        if (existing instanceof EndEvent endEvent) {
            return endEvent;
        }
        EndEvent errorEnd = new EndEvent();
        errorEnd.setId(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
        errorEnd.setName("Sandbox error");
        // Every arrival is a failure by construction, but only the error-gate path carries a typed
        // __sq_error_code sentinel — a BpmnError caught by a boundary (e.g. a call.activity child's
        // propagated failure) lands here with none and would be misreported as COMPLETED. The
        // listener stamps a fallback sentinel when the token carries no typed code.
        ActivitiListener sentinel = new ActivitiListener();
        sentinel.setEvent(ExecutionListener.EVENTNAME_START);
        sentinel.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        sentinel.setImplementation("${sharedErrorEndSentinelListener}");
        List<ActivitiListener> listeners = new ArrayList<>();
        listeners.add(sentinel);
        errorEnd.setExecutionListeners(listeners);
        process.addFlowElement(errorEnd);
        return errorEnd;
    }

    /**
     * Re-points any boundary flow whose target was never emitted (a configured {@code targetNodeId}
     * naming a node the main process dropped) at the shared error end. An unresolved
     * {@code targetRef} is an unresolved IDREF, which fails schema validation on deploy and would
     * make the whole workflow undeployable.
     */
    public void repairDanglingBoundaryRoutes(Process process) {
        List<SequenceFlow> dangling = new ArrayList<>();
        for (FlowElement element : process.getFlowElements()) {
            if (element instanceof SequenceFlow flow
                    && process.getFlowElement(flow.getSourceRef()) instanceof BoundaryEvent
                    && process.getFlowElement(flow.getTargetRef()) == null) {
                dangling.add(flow);
            }
        }
        // Collected first: getOrCreateSharedErrorEnd adds to the very collection being iterated.
        for (SequenceFlow flow : dangling) {
            log.warn("boundary flow {} targets missing element '{}' — routed to {} instead.",
                    flow.getId(), flow.getTargetRef(), ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
            getOrCreateSharedErrorEnd(process);
            flow.setTargetRef(ActivitiBpmnElementHelper.SHARED_ERROR_END_ID);
        }
    }

    /**
     * @param cancelActivity written to {@code bpmn:boundaryEvent/@cancelActivity} for every
     *                       definition except {@code errorEventDefinition} (always interrupting)
     */
    private BoundaryEvent newBoundary(Process process, String id, String hostId, boolean cancelActivity) {
        BoundaryEvent boundary = new BoundaryEvent();
        boundary.setId(id);
        boundary.setAttachedToRefId(hostId);
        // Activiti's BpmnXMLConverter writes the mandatory bpmn:boundaryEvent/@attachedToRef from
        // getAttachedToRef() (the resolved Activity OBJECT), NOT from attachedToRefId (the String).
        // Without the object reference the attribute is omitted and schema validation fails on deploy.
        FlowElement host = process.getFlowElement(hostId);
        if (host instanceof Activity activity) {
            boundary.setAttachedToRef(activity);
        } else {
            throw new IllegalStateException("Cannot attach boundary '" + id + "': host element '" + hostId
                    + "' is " + (host == null ? "missing"
                    : "not an Activity (" + host.getClass().getSimpleName() + ")"));
        }
        boundary.setCancelActivity(cancelActivity);
        return boundary;
    }

    /**
     * The listener that publishes what a fired boundary can hand to the node it triggers
     * ({@code eventId}, {@code firedAt}, the type's own fields), so a mapping drawn from the boundary
     * port has real values to read.
     *
     * <p>It rides on the boundary's outgoing sequence flow as a {@code take} listener, not on the
     * boundary event itself: Activiti activates a boundary through its event handler rather than the
     * ordinary activity-start operation, so an {@code ExecutionListener} on the boundary element is
     * never invoked. Taking the flow is the first hook the engine does run, and the flow's
     * {@code sourceRef} is the boundary's element id — which is what identifies the config that fired.
     */
    private ActivitiListener firePublisher() {
        ActivitiListener listener = new ActivitiListener();
        listener.setEvent(ExecutionListener.EVENTNAME_TAKE);
        listener.setImplementationType(ImplementationType.IMPLEMENTATION_TYPE_DELEGATEEXPRESSION);
        listener.setImplementation("${sqBoundaryEventDelegate}");
        return listener;
    }

    private void route(Process process, BoundaryEvent boundary, String targetNodeId) {
        String targetRefId = StringUtils.isNotBlank(targetNodeId)
                ? "node_" + targetNodeId
                : ActivitiBpmnElementHelper.SHARED_ERROR_END_ID;
        if (ActivitiBpmnElementHelper.SHARED_ERROR_END_ID.equals(targetRefId)) {
            getOrCreateSharedErrorEnd(process);
        }
        SequenceFlow flow = new SequenceFlow(boundary.getId(), targetRefId);
        flow.setId("flow_" + boundary.getId() + "_route");
        flow.setExecutionListeners(new ArrayList<>(List.of(firePublisher())));
        process.addFlowElement(flow);
    }

    /** BPMN ids must be XML NCNames, and a config value is authored freely. */
    private String sanitizeToken(String raw) {
        return raw.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
