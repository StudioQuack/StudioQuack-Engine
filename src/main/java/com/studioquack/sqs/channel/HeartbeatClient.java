package com.studioquack.sqs.channel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.CommandLaneDispatcher;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandRegistry;
import com.studioquack.sqs.channel.command.DaemonCommandResult;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Daemon heartbeat client. {@code POST …/heartbeat?waitMs=<longPoll>} on the interval {@link HeartbeatCadence}
 * decides (fast + long-polled while a run is in flight, a slow keepalive once the WireGuard reverse channel is
 * carrying commands, a decaying poll when there is no reverse channel), carrying the resource snapshot +
 * queued acks/commandResults, and parsing the response's {@code commands[]} to dispatch each through the
 * {@link DaemonCommandRegistry} (the seam {@code deploy}/{@code start-run} handlers plug into).
 */
@Slf4j
public class HeartbeatClient {


    private static final String PROTOCOL_VERSION = "2026-05";

    /**
     * Supplies the per-environment base each beat, so an overlay repoint takes effect live.
     */
    private final Supplier<String> environmentBaseSupplier;
    /**
     * Optional resolver notified of transport success/failure for overlay auto-fallback (nullable).
     */
    private final BackendBaseResolver resolver;
    private final String bearer;
    private final String agentVersion;
    private final String instanceId;
    private final long generation;
    /**
     * Owns both the sleep between beats and the per-beat long-poll budget.
     */
    private final HeartbeatCadence cadence;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final HeartbeatResourceProvider resources;
    /**
     * The background-snapshot wrapper around {@link #resources} to shut down on stop (null when unwrapped).
     */
    private final HeartbeatResourceCache resourceCache;
    /**
     * Runs pushed commands off the beat thread; results land in {@link #pendingResults} + wake the loop.
     */
    private final CommandLaneDispatcher commandDispatcher;
    private final WgLivenessProvider wgLiveness;
    /**
     * Tamper-event source drained into every beat's {@code tamperEvents[]} (nullable — null on the back-compat
     * constructors used by the offline unit tests).
     */
    private final TamperEventReporter tamperEventReporter;
    private volatile com.studioquack.sqs.channel.net.NetTelemetry netTelemetry;
    /**
     * Invoked from the beat loop when the control plane answers a beat with a definitive
     * {@code 404 Error_EnvNotFound}: the engine was deleted while this daemon was alive and every uninstall
     * delivery path (overlay, heartbeat queue, redelivery) missed. The bootstrap wires this to the deleted-engine
     * path (disable host supervision + graceful exit) so a deleted engine never leaves a daemon running on the
     * host.
     */
    private volatile Runnable envGoneHandler;
    /**
     * Null in unit tests. When set, every beat reports whether new runs are accepted.
     */
    private volatile com.studioquack.sqs.engine.update.WorkIntakeGate workIntakeGate;

    private final ConcurrentLinkedQueue<DaemonCommandResult> pendingResults = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Ack> pendingAcks = new ConcurrentLinkedQueue<>();
    private final long startedAtMs = System.currentTimeMillis();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread loopThread;

    /**
     * Back-compat constructor with a FIXED environment base (no overlay repoint / no fallback reporting). Used by
     * the offline unit tests and any static-base caller; delegates to the supplier form with a constant base and
     * no resolver.
     */
    public HeartbeatClient(String environmentBase, String bearer, String agentVersion, String instanceId,
                           long generation, long longPollWaitMs, long periodMs,
                           ChannelHttpClients httpClients, ObjectMapper mapper,
                           DaemonCommandRegistry registry, HeartbeatResourceProvider resources,
                           WgLivenessProvider wgLiveness) {
        this(() -> environmentBase, null, bearer, agentVersion, instanceId, generation,
                HeartbeatCadence.alwaysResponsive(periodMs, longPollWaitMs),
                httpClients, mapper, registry, resources, wgLiveness);
    }

    /**
     * Constructor: the environment base is read from {@code environmentBaseSupplier} on EVERY beat (so an overlay
     * repoint takes effect on the next iteration), and each round-trip reports success/failure to {@code resolver}
     * so a wedged overlay auto-falls-back to the public base. Both may be a {@link BackendBaseResolver} (
     * {@code resolver::current} + {@code resolver}); {@code resolver} may be null to disable reporting.
     */
    public HeartbeatClient(Supplier<String> environmentBaseSupplier, BackendBaseResolver resolver,
                           String bearer, String agentVersion, String instanceId,
                           long generation, HeartbeatCadence cadence,
                           ChannelHttpClients httpClients, ObjectMapper mapper,
                           DaemonCommandRegistry registry, HeartbeatResourceProvider resources,
                           WgLivenessProvider wgLiveness) {
        this(environmentBaseSupplier, resolver, bearer, agentVersion, instanceId, generation, cadence,
                httpClients, mapper, registry, resources, wgLiveness, null);
    }

    /**
     * As above, plus the {@code tamperEventReporter} drained into every beat. May be null — a daemon without a
     * reporter simply omits {@code tamperEvents} from the body.
     */
    public HeartbeatClient(Supplier<String> environmentBaseSupplier, BackendBaseResolver resolver,
                           String bearer, String agentVersion, String instanceId,
                           long generation, HeartbeatCadence cadence,
                           ChannelHttpClients httpClients, ObjectMapper mapper,
                           DaemonCommandRegistry registry, HeartbeatResourceProvider resources,
                           WgLivenessProvider wgLiveness,
                           TamperEventReporter tamperEventReporter) {
        this.environmentBaseSupplier = environmentBaseSupplier;
        this.resolver = resolver;
        this.bearer = bearer;
        this.agentVersion = agentVersion;
        this.instanceId = instanceId;
        this.generation = generation;
        this.cadence = cadence == null
                ? HeartbeatCadence.alwaysResponsive(0L, 0L) : cadence;
        this.httpClients = httpClients;
        this.mapper = mapper;





        HeartbeatResourceProvider wrapped = HeartbeatResourceCache.wrap(resources);
        this.resources = wrapped;
        this.resourceCache = wrapped instanceof HeartbeatResourceCache cache ? cache : null;
        this.commandDispatcher = new CommandLaneDispatcher(registry, pendingResults::add,
                () -> beatNow("command-result-ready"));
        this.wgLiveness = wgLiveness;
        this.tamperEventReporter = tamperEventReporter;
    }

    private com.studioquack.sqs.engine.quota.NodeExecutionMeter nodeExecutionMeter;

    public void setNodeExecutionMeter(com.studioquack.sqs.engine.quota.NodeExecutionMeter nodeExecutionMeter) {
        this.nodeExecutionMeter = nodeExecutionMeter;
    }

    /**
     * Null in unit tests. A 2xx beat is the contact that keeps the engine out of offline mode.
     */
    private volatile com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    public void setExecutionHaltGuard(com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard) {
        this.executionHaltGuard = executionHaltGuard;
    }

    public void setNetTelemetry(com.studioquack.sqs.channel.net.NetTelemetry netTelemetry) {
        this.netTelemetry = netTelemetry;
    }

    public void setWorkIntakeGate(com.studioquack.sqs.engine.update.WorkIntakeGate workIntakeGate) {
        this.workIntakeGate = workIntakeGate;
    }

    /**
     * Wires the deleted-engine exit path (host-supervision disable + graceful exit).
     */
    public void setEnvGoneHandler(Runnable envGoneHandler) {
        this.envGoneHandler = envGoneHandler;
    }

    /**
     * Performs one heartbeat round-trip: ships queued acks/results + the resource snapshot, parses the response,
     * hands each pushed command to the command lanes (never runs one inline), and queues results for the next beat
     * as the lanes complete them. Returns the parsed response so callers/tests can inspect what was pushed.
     */
    public HeartbeatResponse beat() throws Exception {
        List<DaemonCommandResult> results = drain(pendingResults);
        List<Ack> acks = drain(pendingAcks);

        // transferBytesTotal rides INSIDE the resources map: the backend folds it into the
        // monthly bandwidth ledger via ingestFromResources(body.resources()). A top-level
        // sibling field would be silently dropped by the BE heartbeat reader.
        Map<String, Object> res = new java.util.HashMap<>(resources == null ? Map.of() : resources.resources());
        res.put("transferBytesTotal", com.studioquack.sqs.transport.TransferByteMeter.snapshotTotal());
        // The backend stores resources verbatim and judges this engine's silence against it.
        res.put("beatCadenceSeconds", cadence.maxBeatGapSeconds());
        res.put("daemonBuild", com.studioquack.sqs.engine.update.DaemonBuild.snapshot());
        res.put("diskGuard", com.studioquack.sqs.maintenance.DiskFullGuard.snapshot());
        long nodeDelta = nodeExecutionMeter == null ? 0L : nodeExecutionMeter.drainDelta();
        res.put(com.studioquack.sqs.engine.quota.NodeExecutionMeter.RESOURCE_DELTA_KEY, nodeDelta);
        if (netTelemetry != null) {
            res.put("net", netTelemetry.snapshot());
        }
        if (workIntakeGate != null) {
            res.put(com.studioquack.sqs.engine.update.WorkIntakeGate.RESOURCE_KEY, workIntakeGate.isAccepting());
        }
        if (executionHaltGuard != null && executionHaltGuard.isEnabled()) {
            res.put(com.studioquack.sqs.engine.halt.ExecutionHaltGuard.RESOURCE_KEY, executionHaltGuard.mode().name());
        }
        WgLivenessProvider.WgLiveness wg = wgLiveness == null
                ? WgLivenessProvider.WgLiveness.down() : wgLiveness.liveness();
        List<TamperEventReporter.TamperEvent> tamperEvents = tamperEventReporter == null
                ? List.of() : tamperEventReporter.drain();

        HeartbeatRequestBody body = new HeartbeatRequestBody(
                PROTOCOL_VERSION,
                agentVersion,
                emptyToNull(instanceId),
                generation > 0 ? generation : null,
                null,
                "execution",
                (System.currentTimeMillis() - startedAtMs) / 1000L,
                res,
                acks.isEmpty() ? null : acks,
                results.isEmpty() ? null : results,
                wg.up() ? Boolean.TRUE : null,
                wg.lastHandshakeUnix() > 0 ? wg.lastHandshakeUnix() : null,
                tamperEvents.isEmpty() ? null : tamperEvents);

        // Resolve the base for THIS beat (overlay when repointed + healthy, else public).
        String url = environmentBaseSupplier.get() + "/heartbeat";
        long waitMs = cadence.longPollWaitMs();
        if (waitMs > 0) {
            url += (url.contains("?") ? "&" : "?") + "waitMs=" + waitMs;
        }
        Duration timeout = Duration.ofMillis(waitMs > 0 ? waitMs + 15_000L : 30_000L);

        String payload = mapper.writeValueAsString(body);
        HttpResponse<String> resp;
        try {
            resp = ChannelHttp.postJson(httpClients.get(), url, bearer, payload, timeout);
        } catch (Exception ex) {
            if (nodeExecutionMeter != null && nodeDelta > 0L) {
                nodeExecutionMeter.restoreDelta(nodeDelta);
            }
            // A transport failure on the overlay base counts toward the fallback; a completed
            // round-trip is classified by its status below (gateway errors count too).
            if (resolver != null && ChannelHttp.isConnectionFailure(ex)) {
                resolver.reportFailure();
            }
            throw ex;
        }
        if (resolver != null) {
            resolver.reportResponse(resp.statusCode());
        }
        if (!ChannelHttp.isOk(resp.statusCode())) {
            if (nodeExecutionMeter != null && nodeDelta > 0L) {
                nodeExecutionMeter.restoreDelta(nodeDelta);
            }
            throw new HeartbeatException("heartbeat: status " + resp.statusCode()
                    + " body=" + ChannelHttp.trim(resp.body()), resp.statusCode(), resp.body());
        }
        // A 2xx beat is the proof a freshly updated build works end to end; it ends its probation.
        com.studioquack.sqs.engine.update.UpdateProbation.confirmControlPlaneContact();
        String rawBody = resp.body();
        if (rawBody == null || rawBody.isBlank()) {
            if (executionHaltGuard != null) {
                executionHaltGuard.onControlPlaneContact();
            }
            return new HeartbeatResponse(null, List.of(), null, null);
        }
        HeartbeatResponse parsed = mapper.readValue(rawBody, HeartbeatResponse.class);
        com.studioquack.sqs.engine.halt.ExecutionHaltGuard.Mode modeBefore =
                executionHaltGuard == null ? null : executionHaltGuard.mode();
        if (nodeExecutionMeter != null) {
            nodeExecutionMeter.applySnapshot(parsed.nodeQuota());
        }
        // After the snapshot: the guard reads the fresh allowance when it re-evaluates the mode.
        if (executionHaltGuard != null) {
            executionHaltGuard.onControlPlaneContact();
            if (modeBefore != com.studioquack.sqs.engine.halt.ExecutionHaltGuard.Mode.LIMITS_EXCEEDED
                    && executionHaltGuard.mode() == com.studioquack.sqs.engine.halt.ExecutionHaltGuard.Mode.LIMITS_EXCEEDED) {
                // Report LIMITS_EXCEEDED back promptly (e.g. reinstalled exhausted engine beat #1).
                cadence.noteActivity();
            }
        }
        // Always feed the cadence: null/false = unconfirmed inbound (fail-safe toward polling more).
        cadence.noteOverlayReachable(parsed.overlayReachable());
        // Same fail-safe for the liveness capability: only a backend that advertises it derives
        // liveness from its overlay polls, and only against such a backend may this daemon go silent.
        cadence.noteOverlayLiveness(parsed.overlayLiveness());
        dispatchCommands(parsed);
        handleDashboardReset(parsed.dashboardReset());
        return parsed;
    }

    /**
     * Hands each pushed command to the command lanes and returns at once — the beat thread never runs a handler.
     * Results are queued for the next beat by the lane's completion callback, which also wakes the loop so the
     * answer does not wait out a keepalive.
     */
    private void dispatchCommands(HeartbeatResponse resp) {
        if (resp.commands() == null || resp.commands().isEmpty()) {
            return;
        }
        for (DaemonCommand cmd : resp.commands()) {
            commandDispatcher.submit(cmd);
        }
        // Activity returns the fallback ladder to its floor so collected work is acknowledged promptly.
        cadence.noteActivity();
    }

    /**
     * The daemon is headless (no client dashboard), so a {@code dashboardReset} cannot be applied.
     */
    private void handleDashboardReset(DashboardReset reset) {
        if (reset == null) {
            return;
        }
        log.warn("channel/heartbeat: dashboardReset received but this daemon is headless — acking failed id={}",
                reset.commandId());
        pendingAcks.add(new Ack(reset.commandId(), "failed", "client dashboard not supported (headless daemon)"));
    }

    /**
     * Starts the background beat loop (idempotent).
     */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread t = new Thread(this::runLoop, "sqs-channel-heartbeat");
        t.setDaemon(true);
        this.loopThread = t;
        t.start();
        log.info("channel/heartbeat: loop started (cadence-driven; first beat reports enrolment)");
    }

    /**
     * The cadence policy this loop obeys — the seam a transition reports {@code sendNow} through.
     */
    public HeartbeatCadence cadence() {
        return cadence;
    }

    /**
     * Test seam: results the lanes have completed and the next beat will ship.
     */
    int pendingResultCount() {
        return pendingResults.size();
    }

    /**
     * Cuts the current cadence sleep short so a state transition is reported now. Setting the flag alone is not
     * enough: the loop only consults the cadence when it wakes, so a transition landing early in a long keepalive
     * sleep would be announced a full period late — the exact failure a long cadence introduces.
     */
    public void beatNow(String reason) {
        cadence.sendNow(reason);
        Thread t = loopThread;
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * Stops the background loop and the per-client lanes/collector (best-effort).
     */
    public void stop() {
        running.set(false);
        Thread t = loopThread;
        if (t != null) {
            t.interrupt();
        }
        commandDispatcher.shutdown();
        if (resourceCache != null) {
            resourceCache.shutdown();
        }
    }

    /**
     * Re-open delay once a beat produced a command result. A result ships on the NEXT beat, so the cadence is
     * bypassed rather than making the backend wait out a keepalive for the answer to the command it just pushed.
     */
    private static final long RESULT_REOPEN_MS = 10L;

    /**
     * Long-poll re-open delay: a beat that returned because its long-poll expired has to be re-opened promptly or
     * the window it was watching for commands has a gap in it. Only applies while the cadence is long-polling at
     * all.
     */
    private static final long LONG_POLL_REOPEN_MS = 250L;

    private void runLoop() {

        sleepQuietly(1_000L);
        while (running.get()) {
            long next;
            try {
                // OVERLAY_SILENT: no beat leaves the host while idle. Re-resolve on the stale-check clock;


                boolean hasNodeDelta = nodeExecutionMeter != null && nodeExecutionMeter.hasPendingDelta();
                if (cadence.mode() == HeartbeatCadence.Mode.OVERLAY_SILENT
                        && pendingResults.isEmpty()
                        && !hasNodeDelta) {
                    long silentCheck = cadence.nextSilentCheckMs();
                    if (silentCheck > 0) {
                        sleepQuietly(silentCheck);
                        continue;
                    }
                }
                boolean longPolling = cadence.beginBeat() > 0;
                beat();
                next = cadence.nextDelayMs();
                if (!pendingResults.isEmpty()) {
                    next = Math.min(next, RESULT_REOPEN_MS);
                } else if (nodeExecutionMeter != null && nodeExecutionMeter.hasPendingDelta()) {
                    // Activations landed while the previous beat was in flight — flush promptly so
                    // timer-triggered and manual runs update the backend quota ledger without lag.
                    next = Math.min(next, 2_000L);
                } else if (longPolling) {
                    next = Math.min(next, LONG_POLL_REOPEN_MS);
                }
            } catch (Exception ex) {
                if (ex instanceof HeartbeatException he && he.isEnvNotFound()) {
                    // The engine row is gone from the control plane — this daemon's
                    // enrolment identity can never work again. Take the deleted-engine
                    // exit path instead of beating (and failing) forever while the host
                    // keeps running a daemon whose engine vanished from the UI.
                    log.error("channel/heartbeat: engine unknown to the backend (404 "
                            + "Error_EnvNotFound) — taking the deleted-engine exit path");
                    running.set(false);
                    Runnable handler = envGoneHandler;
                    if (handler != null) {
                        try {
                            handler.run();
                        } catch (Exception handlerEx) {
                            log.warn("channel/heartbeat: env-gone handler failed: {}",
                                    handlerEx.toString());
                        }
                    }
                    break;
                }
                if (ex instanceof InterruptedException && running.get()) {
                    // A beatNow() interrupt surfaced inside the in-flight beat's HTTP send
                    // instead of the cadence sleep it was aimed at.
                    // Skip the WARN and the overlay-reachability reset, and re-beat at once
                    // exactly what beatNow asked for. The throw already cleared the interrupt
                    // status, so the next send is unaffected.
                    next = 0;
                } else {
                    if (running.get()) {
                        log.warn("channel/heartbeat: beat failed: {}", ex.toString());
                    }
                    // Clear any stale true confirmation so a transport failure cannot re-enter keepalive.
                    cadence.noteOverlayReachable(null);
                    next = cadence.nextDelayAfterFailedBeatMs();
                }
            }
            sleepQuietly(next);
        }
        log.info("channel/heartbeat: loop stopped");
    }

    private void sleepQuietly(long ms) {
        if (ms <= 0L) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            // Two callers interrupt this thread: stop() to end the loop, and beatNow() to shorten a
            // long cadence sleep. Restoring the flag in the second case would make the very next
            // HTTP call fail, so it is restored only when the loop is genuinely being torn down.
            if (!running.get()) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static <T> List<T> drain(ConcurrentLinkedQueue<T> queue) {
        List<T> out = new ArrayList<>();
        T item;
        while ((item = queue.poll()) != null) {
            out.add(item);
        }
        return out;
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    /**
     * Heartbeat request body — camelCase, nulls omitted (matches the BE {@code HeartbeatBody}).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record HeartbeatRequestBody(
            String heartbeatVersion,
            String agentVersion,
            String instanceId,
            Long generation,
            String instanceState,
            String instanceRole,
            Long uptimeSec,
            Map<String, Object> resources,
            List<Ack> acks,
            List<DaemonCommandResult> commandResults,
            Boolean wgOverlayUp,
            Long wgLastHandshakeUnix,
            List<TamperEventReporter.TamperEvent> tamperEvents) {
    }

    /**
     * Command acknowledgement ({commandId,status,error}) — matches the BE {@code Ack}.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Ack(String commandId, String status, String error) {
    }

    /**
     * Heartbeat response — {dashboardReset?, commands:[...], overlayReachable?, overlayLiveness?}.
     * {@code overlayReachable} is the backend confirming it recently reached THIS daemon over the overlay.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HeartbeatResponse(DashboardReset dashboardReset, List<DaemonCommand> commands,
                                    Boolean overlayReachable, Boolean overlayLiveness,
                                    Map<String, Object> nodeQuota) {
        public HeartbeatResponse(DashboardReset dashboardReset, List<DaemonCommand> commands,
                                 Boolean overlayReachable, Boolean overlayLiveness) {
            this(dashboardReset, commands, overlayReachable, overlayLiveness, null);
        }
    }

    /**
     * Dashboard-reset command block (parsed but not applied on the headless daemon).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record DashboardReset(String commandId, String username, String password) {
    }

    /**
     * Raised when a heartbeat returns a non-success status.
     */
    public static class HeartbeatException extends RuntimeException {
        private final int statusCode;
        private final String body;

        public HeartbeatException(String message) {
            this(message, 0, null);
        }

        public HeartbeatException(String message, int statusCode, String body) {
            super(message);
            this.statusCode = statusCode;
            this.body = body;
        }

        public int statusCode() {
            return statusCode;
        }

        /**
         * Definitive "this engine is gone from the control plane": the BE heartbeat endpoint answers 404 with
         * {@code Error_EnvNotFound} only when the env row (and with it the enrolment bearer) is deleted. A
         * proxy/deploy-window 404 carries no such body and stays transient — the daemon must never self-uninstall on
         * someone else's 404.
         */
        public boolean isEnvNotFound() {
            return statusCode == 404 && body != null && body.contains("Error_EnvNotFound");
        }
    }
}
