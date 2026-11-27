package com.studioquack.sqs.channel.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelKemIkm;
import com.studioquack.sqs.channel.net.NetAgentClient;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes BE-pushed {@link DaemonCommand}s to the registered {@link DaemonCommandHandler} for their type and
 * turns the outcome into a wire {@link DaemonCommandResult}. The BE keeps re-shipping a command on every poll
 * until it sees a result for that {@code commandId}, and with the overlay up it pushes down the tunnel while
 * ALSO leaving the queued copy.
 */
@Slf4j
@Component
public class DaemonCommandRegistry {

    /**
     * Distinct wire error when a command arrives without a valid MAC after hybrid enrolment.
     */
    public static final String ERROR_COMMAND_UNAUTHENTICATED = "Error_CommandUnauthenticated";

    /**
     * Distinct wire error when the heartbeat lane refuses a non-emergency type.
     */
    public static final String ERROR_EMERGENCY_ONLY_CHANNEL = "Error_EmergencyOnlyChannel";

    /**
     * Enumerated emergency command types permitted on the heartbeat lane. Wire type strings are exact;
     * {@code vm-reset} is reserved for a later handler.
     */
    public static final Set<String> HEARTBEAT_EMERGENCY_TYPES = Set.of(
            "restart", "status", "net-reconnect", "vm-reset",
            "update-daemon", "update-preview");

    /**
     * Read-only reverse log-pull commands permitted over HMAC-signed HTTPS heartbeat fallback when the WireGuard
     * overlay is unreachable (e.g. corporate firewalls blocking UDP).
     */
    public static final Set<String> HEARTBEAT_LOG_PULL_TYPES = Set.of(
            "run-logs", "engine-logs");

    private static final ObjectMapper PAYLOAD_JSON = new ObjectMapper();

    /**
     * Engine-management commands that must never queue behind a wedged handler on the heartbeat command lane: they
     * run on the dispatcher's immediate lane instead of the serial one. The serial lane preserves execution order
     * for workload commands (deploy → start-run), but a handler that blocks (a wedged Podman/Postgres call after a
     * host sleep) must not delay the very commands that recover the engine — {@code net-reconnect} above all,
     * since it is the only path that re-establishes the overlay the blocked lane depends on.
     */
    public static final Set<String> IMMEDIATE_LANE_TYPES = Set.of(
            "restart", "net-reconnect", "vm-reset", "update-daemon", "update-preview",
            "ingest-webhook-key-event");

    /**
     * True when {@code type} must run on the immediate lane (see {@link #IMMEDIATE_LANE_TYPES}).
     */
    public static boolean isImmediateLaneType(String type) {
        return type != null && IMMEDIATE_LANE_TYPES.contains(type);
    }

    /**
     * Execution commands whose overlay HTTP hop must return as soon as they are queued. A slow {@code startLatest}
     * (first node) on the HTTPS thread used to burn the backend's 30s command-response budget and get the tunnel
     * classified as dead while the run was already going.
     */
    public static boolean isAcceptAndRunType(String type) {
        return "deploy".equalsIgnoreCase(type) || "start-run".equalsIgnoreCase(type);
    }

    /**
     * Immutable type → handler map built once at startup.
     */
    private final Map<String, DaemonCommandHandler> handlers;

    /**
     * CommandId → in-flight-or-completed execution, claimed atomically so only one thread runs it.
     */
    private final Map<String, Dispatch> resultCache = new ConcurrentHashMap<>();

    /**
     * How long a completed result is remembered so a re-ship re-reports instead of re-executing.
     */
    private final long dedupTtlMs;

    /**
     * Live hybrid IKM for command MAC verification. {@code null} only in offline unit tests — those still accept
     * unsigned commands.
     */
    private final ChannelKemIkm channelKemIkm;

    /**
     * Crash-safe backing of {@link #resultCache}: a result is persisted before its claim completes, and a
     * post-restart registry answers a re-shipped command from disk instead of executing it again ({@code restart}
     * / {@code update-daemon} must never run twice). {@code null} in the offline unit-test forms.
     */
    private final DurableCommandResultStore resultStore;

    /**
     * True when non-emergency types must be refused on {@link #INBOUND_HEARTBEAT} (see the class javadoc). Derived
     * once at construction — not a runtime toggle.
     */
    private final boolean emergencyOnlyHeartbeat;

    /**
     * How long a duplicate delivery waits for the execution already in progress. Commands finish in well under a
     * second; this only has to outlast the slowest handler, and timing out here would report a command as
     * unexecuted while it is in fact running.
     */
    private static final long DUPLICATE_WAIT_MS = 120_000L;

    /**
     * The claim on a {@code commandId}, inserted BEFORE the handler runs. A completed-result cache is not enough:
     * the backend deliberately keeps the queued copy of a command it also pushes down the tunnel, so with
     * WireGuard up both transports deliver the same {@code commandId} within milliseconds of each other.
     */
    private record Dispatch(java.util.concurrent.CompletableFuture<DaemonCommandResult> result,
                            long startedAtMs) {
    }

    /**
     * Collects every {@link DaemonCommandHandler} Spring bean. Duplicate types are a wiring bug (two subsystems
     * claiming the same command) and fail fast at startup.
     */
    @org.springframework.beans.factory.annotation.Autowired
    public DaemonCommandRegistry(List<DaemonCommandHandler> handlerBeans,
                                 @Value("${sqs.channel.net.enabled:true}") boolean netEnabled,
                                 ObjectProvider<NetAgentClient> netAgent,
                                 ObjectProvider<ChannelKemIkm> kemIkm,
                                 ObjectProvider<DurableCommandResultStore> resultStore) {
        this(handlerBeans, 604_800_000L,
                emergencyOnlyHeartbeat(netEnabled, netAgent != null && netAgent.getIfAvailable() != null),
                kemIkm == null ? null : kemIkm.getIfAvailable(),
                resultStore == null ? null : resultStore.getIfAvailable());
    }

    /**
     * True when the private network is enabled and its data plane is installed.
     */
    static boolean emergencyOnlyHeartbeat(boolean netEnabled, boolean dataPlanePresent) {
        return netEnabled && dataPlanePresent;
    }

    /**
     * Offline form used by the unit tests: emergency-only gate OFF.
     */
    public DaemonCommandRegistry(List<DaemonCommandHandler> handlerBeans) {
        this(handlerBeans, 604_800_000L, false);
    }

    DaemonCommandRegistry(List<DaemonCommandHandler> handlerBeans, long dedupTtlMs) {
        this(handlerBeans, dedupTtlMs, false);
    }

    /**
     * Package-visible test/offline form: set {@code emergencyOnlyHeartbeat} directly. Prefer this over flippable
     * config when unit-testing the heartbeat emergency set.
     */
    DaemonCommandRegistry(List<DaemonCommandHandler> handlerBeans, long dedupTtlMs,
                          boolean emergencyOnlyHeartbeat) {
        this(handlerBeans, dedupTtlMs, emergencyOnlyHeartbeat, null);
    }

    public DaemonCommandRegistry(List<DaemonCommandHandler> handlerBeans, long dedupTtlMs,
                                 boolean emergencyOnlyHeartbeat, ChannelKemIkm channelKemIkm) {
        this(handlerBeans, dedupTtlMs, emergencyOnlyHeartbeat, channelKemIkm, null);
    }

    /**
     * Full form: as {@link #DaemonCommandRegistry(List, long, boolean, ChannelKemIkm)} plus the durable result
     * store (nullable — offline tests pass {@code null} and keep the in-memory-only behaviour).
     */
    public DaemonCommandRegistry(List<DaemonCommandHandler> handlerBeans, long dedupTtlMs,
                                 boolean emergencyOnlyHeartbeat, ChannelKemIkm channelKemIkm,
                                 DurableCommandResultStore resultStore) {
        Map<String, DaemonCommandHandler> map = new HashMap<>();
        for (DaemonCommandHandler h : handlerBeans) {
            String type = h.type();
            if (type == null || type.isBlank()) {
                throw new IllegalStateException(
                        "DaemonCommandHandler " + h.getClass().getName() + " returned a blank type()");
            }
            DaemonCommandHandler prev = map.putIfAbsent(type, h);
            if (prev != null) {
                throw new IllegalStateException("Duplicate DaemonCommandHandler for type '" + type + "': "
                        + prev.getClass().getName() + " and " + h.getClass().getName());
            }
        }
        this.handlers = Map.copyOf(map);
        this.dedupTtlMs = dedupTtlMs;
        this.emergencyOnlyHeartbeat = emergencyOnlyHeartbeat;
        this.channelKemIkm = channelKemIkm;
        this.resultStore = resultStore;
        log.info("channel: DaemonCommandRegistry ready — {} handler(s) for types {} (emergencyOnlyHeartbeat={})",
                handlers.size(), handlers.keySet(), emergencyOnlyHeartbeat);
    }

    /**
     * True when a handler is registered for {@code type}.
     */
    public boolean hasHandler(String type) {
        return handlers.containsKey(type);
    }

    /**
     * The channel a command arrived on, reported in the {@code [TRANSPORT]} line.
     */
    public static final String INBOUND_OVERLAY = "WIREGUARD_OVERLAY";
    public static final String INBOUND_HEARTBEAT = "HEARTBEAT_QUEUE";

    /**
     * Dispatches one command. Returns the wire result to ship on the next heartbeat, or {@link Optional#empty()}
     * only when {@code command} is malformed (null / blank id) and cannot be reported.
     */
    public Optional<DaemonCommandResult> dispatch(DaemonCommand command) {
        return dispatch(command, INBOUND_HEARTBEAT);
    }

    /**
     * @param inboundTransport which channel delivered this command. Both callers converge here, so without it the
     * log cannot distinguish a command the backend pushed down the tunnel from one the daemon collected on its
     * next beat — and that distinction is the whole point of preferring WireGuard.
     */
    public Optional<DaemonCommandResult> dispatch(DaemonCommand command, String inboundTransport) {
        if (command == null || command.commandId() == null || command.commandId().isBlank()) {
            log.warn("channel: dropping command with no commandId ({})", command);
            return Optional.empty();
        }
        Optional<DaemonCommandResult> refused = refuseIfNotAdmitted(command, inboundTransport);
        if (refused.isPresent()) {
            return refused;
        }
        sweepExpired();


        // Emergency-gate refusal: the gate stops heartbeat copies from EXECUTING, it must not
        // deny the backend the true outcome of a finished command. Only completed claims are
        // served here — an in-progress claim falls through to the gate, because waiting on it
        // could park the serial heartbeat lane behind a running handler.
        Dispatch completed = resultCache.get(command.commandId());
        if (completed != null && completed.result().isDone()) {
            return awaitClaimed(command, inboundTransport, completed);
        }
        // Post-restart re-report: the in-memory claim map dies with the process, so a command the
        // backend still re-ships — its result was lost with the old process, the classic case
        // being restart / update-daemon — is answered from the durable store instead of
        // executing again. Sits BEFORE the emergency-only gate for the same reason the in-memory
        // completed-claim check does: the gate stops executions, not true outcomes.
        Dispatch stored = rehydrate(command.commandId());
        if (stored != null) {
            return awaitClaimed(command, inboundTransport, stored);
        }
        // : the heartbeat lane is emergency-only; refuse before claim so the overlay
        // redelivery of the same commandId can still run it. Read-only log-pull commands and
        // zero-PHI ephemeral none-start/none-end drafts are also admitted over HMAC-verified
        // HTTPS heartbeat so UDP-blocked networks do not cause false integrity failures.
        if (emergencyOnlyHeartbeat
                && INBOUND_HEARTBEAT.equals(inboundTransport)
                && !isHeartbeatLanePermitted(command)) {
            return Optional.of(refuseHeartbeatEmergencyOnly(command, inboundTransport));
        }

        Dispatch mine = new Dispatch(new java.util.concurrent.CompletableFuture<>(),
                System.currentTimeMillis());
        Dispatch existing = resultCache.putIfAbsent(command.commandId(), mine);
        if (existing != null) {
            // Already claimed — either finished earlier (re-ship) or running right now on the other
            // transport. Either way the handler must not run a second time.
            return awaitClaimed(command, inboundTransport, existing);
        }

        long startNanos = System.nanoTime();
        DaemonCommandResult result;
        try {
            result = execute(command);
        } catch (RuntimeException | Error ex) {
            // execute() converts handler failures itself, so reaching here is a registry bug — but a
            // duplicate parked on this future must never hang because of one.
            mine.result().complete(DaemonCommandResult.of(command,
                    DaemonCommandOutcome.failed("dispatch failed: " + ex)));
            throw ex;
        }
        // Persist BEFORE completing the claim: a duplicate parked on this future must never be
        // answered with a result the process has not yet fsynced. High-frequency polls stay
        // in-memory-only — their loss just costs one cheap re-execution.
        persist(command.type(), result);
        mine.result().complete(result);
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        if (!isHighFrequencyPoll(command.type())) {
            // Skipped for status/list polls, which arrive every few seconds and would drown the file.
            log.info("[TRANSPORT] leg=daemon.command.received transport={} id={} type={} handlerMs={} status={}",
                    inboundTransport, command.commandId(), command.type(), elapsedMs, result.status());
        }
        return Optional.of(result);
    }

    /**
     * Completed result for {@code commandId}, or empty when unknown / still running. Overlay accept-and-run uses
     * this so a re-ship of a finished command re-reports the cached outcome instead of another ACCEPT.
     */
    public Optional<DaemonCommandResult> peekCompleted(String commandId) {
        if (commandId == null || commandId.isBlank()) {
            return Optional.empty();
        }
        Dispatch claimed = resultCache.get(commandId);
        if (claimed != null && claimed.result().isDone()) {
            try {
                return Optional.ofNullable(claimed.result().getNow(null));
            } catch (RuntimeException ex) {
                return Optional.empty();
            }
        }
        // Post-restart: the terminal result may only exist on disk.
        if (resultStore == null) {
            return Optional.empty();
        }
        try {
            return resultStore.load(commandId);
        } catch (RuntimeException ex) {
            return Optional.empty();
        }
    }

    /**
     * Rebuilds a completed claim from the durable store after a restart, so the caller re-reports the stored
     * result through the same {@link #awaitClaimed} path as any duplicate. A concurrent in-memory claim always
     * wins the race (it is the fresher truth).
     */
    private Dispatch rehydrate(String commandId) {
        if (resultStore == null) {
            return null;
        }
        Optional<DaemonCommandResult> stored;
        try {
            stored = resultStore.load(commandId);
        } catch (RuntimeException ex) {
            log.warn("channel: command-result store read failed id={}: {}", commandId, ex.getMessage());
            return null;
        }
        if (stored.isEmpty()) {
            return null;
        }
        Dispatch rehydrated = new Dispatch(
                java.util.concurrent.CompletableFuture.completedFuture(stored.get()),
                System.currentTimeMillis());
        Dispatch raced = resultCache.putIfAbsent(commandId, rehydrated);
        return raced == null ? rehydrated : raced;
    }

    /**
     * Persists a terminal result; failures degrade to the pre-store behaviour, never break dispatch.
     */
    private void persist(String type, DaemonCommandResult result) {
        if (resultStore == null || isHighFrequencyPoll(type)) {
            return;
        }
        try {
            resultStore.record(result);
        } catch (RuntimeException ex) {
            log.warn("channel: command-result store append failed id={}: {}",
                    result.commandId(), ex.getMessage());
        }
    }

    /**
     * Command-MAC gate used by both {@link #dispatch} and overlay accept-and-run. Overlay {@code deploy}/
     * {@code start-run} must not HTTP-ACCEPT before this returns empty: the client source is public, so a missing
     * MAC is not an implementation secret.
     */
    public Optional<DaemonCommandResult> refuseIfNotAdmitted(DaemonCommand command, String inboundTransport) {
        if (command == null || command.commandId() == null || command.commandId().isBlank()) {
            return Optional.empty();
        }
        if (!commandMacOk(command)) {
            return Optional.of(refuseUnauthenticated(command, inboundTransport));
        }
        return Optional.empty();
    }

    /**
     * Tolerated forward clock skew between BE and daemon for {@code issuedAt}.
     */
    static final long ISSUED_AT_FUTURE_SKEW_MS = 5 * 60_000L;

    private boolean commandMacOk(DaemonCommand command) {
        if (channelKemIkm == null) {
            return true;
        }
        byte[] ikm = channelKemIkm.ikmOrNull();
        if (ikm == null) {
            log.warn("channel: refusing command while ChannelKemIkm has no IKM id={} type={}",
                    command.commandId(), command.type());
            return false;
        }
        boolean ok = EnvelopeCrypto.verifyCommandMac(
                ikm, channelKemIkm.envId(), command.commandId(), command.type(),
                command.issuedAt(), command.payload(), command.mac());
        if (!ok) {
            log.warn("channel: refusing unsigned or mismatched command MAC id={} type={}",
                    command.commandId(), command.type());
            return false;
        }
        // Replay window: the dedup cache is in-memory, so a captured signed command would
        // run again after a restart. issuedAt is under the MAC, so an attacker cannot
        // refresh it; bound its age to the same TTL the dedup cache uses.
        if (!issuedAtFresh(command.issuedAt())) {
            log.warn("channel: refusing stale or unstamped signed command id={} type={} issuedAt={}",
                    command.commandId(), command.type(), command.issuedAt());
            return false;
        }
        return true;
    }

    private boolean issuedAtFresh(String issuedAt) {
        if (issuedAt == null || issuedAt.isBlank()) {
            return false;
        }
        long issued;
        try {
            issued = java.time.Instant.parse(issuedAt).toEpochMilli();
        } catch (java.time.format.DateTimeParseException ex) {
            return false;
        }
        long now = System.currentTimeMillis();
        return issued <= now + ISSUED_AT_FUTURE_SKEW_MS && now - issued <= dedupTtlMs;
    }

    private DaemonCommandResult refuseUnauthenticated(DaemonCommand command, String inboundTransport) {
        log.warn("[TRANSPORT] leg=daemon.command.refused transport={} id={} type={} reason=unauthenticated-mac",
                inboundTransport, command.commandId(), command.type());
        return DaemonCommandResult.of(command,
                DaemonCommandOutcome.failed(ERROR_COMMAND_UNAUTHENTICATED
                        + ": command MAC missing, invalid, or issuedAt stale"));
    }

    /**
     * Refuses a non-emergency command on the heartbeat emergency lane. Deliberately not recorded in
     * {@link #resultCache}: caching it would claim the {@code commandId} for the refusal, so the same command
     * arriving later over the overlay would re-report "refused" instead of running — a temporary channel policy
     * must not permanently claim the id.
     */
    private DaemonCommandResult refuseHeartbeatEmergencyOnly(DaemonCommand command,
                                                             String inboundTransport) {
        log.info("[TRANSPORT] leg=daemon.command.refused transport={} id={} type={} reason=emergency-only "
                        + "(delivered over the private network instead)",
                inboundTransport, command.commandId(), command.type());
        return DaemonCommandResult.of(command,
                DaemonCommandOutcome.failed(ERROR_EMERGENCY_ONLY_CHANNEL
                        + ": type '" + command.type()
                        + "' is delivered over the private network, not the heartbeat emergency lane"));
    }

    /**
     * Reports the result of an execution this thread did not win, without re-running the handler. With the overlay
     * up this is the ordinary path, not an error: the backend pushes down the tunnel AND leaves the queued copy,
     * so the same command legitimately arrives twice.
     */
    private Optional<DaemonCommandResult> awaitClaimed(DaemonCommand command, String inboundTransport,
                                                       Dispatch claimed) {
        try {
            DaemonCommandResult result = claimed.result()
                    .get(DUPLICATE_WAIT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!isHighFrequencyPoll(command.type())) {
                log.info("[TRANSPORT] leg=daemon.command.duplicate transport={} id={} type={} status={} "
                                + "(already dispatched on the other transport; not re-executed)",
                        inboundTransport, command.commandId(), command.type(), result.status());
            }
            return Optional.of(result);
        } catch (java.util.concurrent.TimeoutException ex) {
            // Still running after two minutes. Report nothing rather than guess: delivery is
            // at-least-once, so the next re-ship finds the finished result in this same claim.
            log.warn("channel: command id={} type={} still executing after {}ms on the other transport "
                    + "— not reporting a result yet", command.commandId(), command.type(), DUPLICATE_WAIT_MS);
            return Optional.empty();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (java.util.concurrent.ExecutionException ex) {
            log.warn("channel: command id={} type={} failed on the other transport: {}",
                    command.commandId(), command.type(), ex.toString());
            return Optional.empty();
        }
    }

    /**
     * Runs the handler (or synthesises an unknown-type failure) exactly once.
     */
    private DaemonCommandResult execute(DaemonCommand command) {
        DaemonCommandHandler handler = handlers.get(command.type());
        if (handler == null) {
            log.warn("channel: no handler for command type '{}' (id={}) — reporting failed",
                    command.type(), command.commandId());
            return DaemonCommandResult.of(command,
                    DaemonCommandOutcome.failed("unknown command type '" + command.type() + "'"));
        }
        try {
            DaemonCommandOutcome outcome = handler.handle(command);
            if (outcome == null) {
                outcome = DaemonCommandOutcome.failed(
                        "handler for '" + command.type() + "' returned a null outcome");
            }
            // Mirror the backend's queue-side gating: `status` / `list-jobs` / `list-models` are UI polls
            // that arrive every few seconds, so a per-execution INFO line for them is pure volume.
            // Non-ok outcome is always worth seeing, whatever the type.
            if (isHighFrequencyPoll(command.type()) && outcome.succeeded()) {
                log.debug("channel: command executed id={} type={} status={}",
                        command.commandId(), command.type(), outcome.status());
            } else {
                log.info("channel: command executed id={} type={} status={}",
                        command.commandId(), command.type(), outcome.status());
            }
            return DaemonCommandResult.of(command, outcome);
        } catch (Exception ex) {
            log.warn("channel: command handler threw id={} type={}: {}",
                    command.commandId(), command.type(), ex.toString());
            return DaemonCommandResult.of(command,
                    DaemonCommandOutcome.failed(ex.getMessage() == null ? ex.toString() : ex.getMessage()));
        }
    }

    /**
     * Command types the FE polls on a timer rather than issuing on a user action. Their successful execution
     * carries no signal; only a failure does.
     */
    public static boolean isHighFrequencyPoll(String type) {
        return "status".equalsIgnoreCase(type)
                || "list-jobs".equalsIgnoreCase(type)
                || "list-models".equalsIgnoreCase(type)
                || "engine-logs".equalsIgnoreCase(type);
    }

    /**
     * Heartbeat-lane admission: emergency recovery commands, read-only log pulls ({@code run-logs} /
     * {@code engine-logs}), and zero-PHI ephemeral none-start/none-end drafts (deploy + start-run) may fall back
     * to the HMAC-signed HTTPS heartbeat channel when the private-network overlay is unreachable (e.g. corporate
     * firewalls blocking UDP). Customer scripts, secrets, and PHI remain strictly overlay-only.
     */
    static boolean isHeartbeatLanePermitted(DaemonCommand command) {
        if (command == null || command.type() == null) {
            return false;
        }
        String type = command.type();
        if (HEARTBEAT_EMERGENCY_TYPES.contains(type) || HEARTBEAT_LOG_PULL_TYPES.contains(type)) {
            return true;
        }
        if ("deploy".equals(type)) {
            return isZeroPhiEphemeralBpmnDeploy(command.payload());
        }
        if ("start-run".equals(type)) {
            return isZeroPhiDraftStartRun(command.payload());
        }
        return false;
    }

    private static boolean isZeroPhiEphemeralBpmnDeploy(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            return false;
        }
        try {
            JsonNode root = PAYLOAD_JSON.readTree(rawPayload);
            if (!root.path("ephemeral").asBoolean(false)) {
                return false;
            }
            if (root.path("draftToken").asText("").isBlank()) {
                return false;
            }
            if (!isAbsentOrEmpty(root.get("nodeScripts"))
                    || !isAbsentOrEmpty(root.get("nodeImages"))
                    || !isAbsentOrEmpty(root.get("globalParams"))
                    || !isAbsentOrEmpty(root.get("projectVariables"))) {
                return false;
            }
            JsonNode nodes = root.get("nodes");
            if (nodes == null || !nodes.isArray() || nodes.isEmpty() || nodes.size() > 4) {
                return false;
            }
            for (JsonNode n : nodes) {
                JsonNode data = n.get("data");
                if (data == null || !data.isObject()) {
                    return false;
                }
                String bpmnType = data.path("bpmnType").asText("").trim().toLowerCase(java.util.Locale.ROOT);
                if (!"start.event".equals(bpmnType) && !"end.event".equals(bpmnType)) {
                    return false;
                }
                if (!isAbsentOrEmpty(data.get("templateScript"))
                        || !isAbsentOrEmpty(data.get("configurationParameters"))
                        || !data.path("jobType").asText("").isBlank()) {
                    return false;
                }
            }
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static boolean isZeroPhiDraftStartRun(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            return false;
        }
        try {
            JsonNode root = PAYLOAD_JSON.readTree(rawPayload);
            String processKey = root.path("processKey").asText("");
            if (!processKey.contains("__draft_")) {
                return false;
            }
            if (!isAbsentOrEmpty(root.get("seedVars"))) {
                return false;
            }
            JsonNode seed = root.get("seedContext");
            if (seed != null && !seed.isNull()) {
                if (!seed.isObject()) {
                    return false;
                }
                if (!isAbsentOrEmpty(seed.get("nodes"))
                        || !isAbsentOrEmpty(seed.get("edges"))
                        || !isAbsentOrEmpty(seed.get("workflowVariables"))
                        || !isAbsentOrEmpty(seed.get("projectVariables"))
                        || !isAbsentOrEmpty(seed.get("inputData"))
                        || !isAbsentOrEmpty(seed.get("agenticInfo"))) {
                    return false;
                }
            }
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static boolean isAbsentOrEmpty(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return true;
        }
        if (node.isArray() || node.isObject()) {
            return node.isEmpty();
        }
        if (node.isTextual()) {
            return node.asText().isBlank();
        }
        return false;
    }

    private void sweepExpired() {
        if (resultCache.isEmpty()) {
            return;
        }
        long cutoff = System.currentTimeMillis() - dedupTtlMs;
        // Only completed claims are evictable — dropping one still running would let the next delivery
        // of that same commandId execute it a second time, which is the whole thing this guards.
        resultCache.entrySet().removeIf(e ->
                e.getValue().startedAtMs() < cutoff && e.getValue().result().isDone());
    }
}
