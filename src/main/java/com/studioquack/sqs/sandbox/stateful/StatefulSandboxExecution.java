package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.helperchan.HelperFailure;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.helperchan.HelperResult;
import com.studioquack.sqs.sandbox.helperchan.StreamSessionManager;
import com.studioquack.sqs.sandbox.oci.HelperBroker;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Executes ONE stateful-node dispatch against an already-running instance. This is the seam {@code
 * ScriptDispatchDelegate} calls INSTEAD of the ephemeral {@code PodmanSandboxExecution} when a
 * node targets a stateful image and a live {@code (env+imageRef)} instance is resolved.
 */
@Slf4j
@Component
public class StatefulSandboxExecution {

    /**
     * Refusal text for a dispatch that needs helper callbacks on an instance whose broker could not be
     * brought back. Says what died and what to do — the script would otherwise only see its own
     * transport error (curl exit 7) with no hint that the channel, not the LLM provider, was down.
     */
    private static final String BROKER_DOWN_MESSAGE =
            "the execution container's helper channel is down, so studioquack.execute/llm/connect "
                    + "cannot reach the engine. Restarting the instance did not recover it — the broker "
                    + "process is usually killed by host/VM memory pressure (exit 137). Free memory on "
                    + "the container host (or raise the Podman machine's RAM) and restart the instance.";

    private final HelperRequestRouter helperRouter;
    private final StatefulInstanceManager statefulInstances;
    private final StatefulLauncher launcher;
    private final Redactor redactor;
    private final ObjectMapper mapper;
    private final StatefulRuntimeProperties props;
    private final SubProcessCompletionRegistry subProcessRegistry;
    private final DispatchCancellationRegistry dispatchCancellationRegistry;
    private final StreamSessionManager streamSessions;

    /** Per-instance admission gate. Unlabeled images get 1 permit (FIFO). */
    private final ConcurrentHashMap<String, InstanceGate> instanceLocks = new ConcurrentHashMap<>();

    /**
     * Run ids with a dispatch currently in flight per instance. A function-port sub-PI child whose
     * ancestor chain contains one of these runs is RE-ENTRANT: its parent is parked inside the
     * container waiting for exactly this child, so queuing the child on the instance FIFO deadlocks
     * (parent holds the only permit until its wall-clock, child can never start — observed as a 300 s
     * "stateful request deadline exceeded" with a silent instance).
     */
    private final ConcurrentHashMap<String, Set<String>> inFlightRunsByInstance = new ConcurrentHashMap<>();

    /** One fair semaphore plus the permit count it was created with. */
    private record InstanceGate(Semaphore lock, int permits) {}

    /** Tests that do not exercise cancel bind the registry + stream manager as null. */
    public StatefulSandboxExecution(HelperRequestRouter helperRouter,
                                    StatefulInstanceManager statefulInstances,
                                    StatefulLauncher launcher,
                                    Redactor redactor,
                                    ObjectMapper mapper,
                                    StatefulRuntimeProperties props,
                                    SubProcessCompletionRegistry subProcessRegistry) {
        this(helperRouter, statefulInstances, launcher, redactor, mapper, props, subProcessRegistry,
                null, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public StatefulSandboxExecution(HelperRequestRouter helperRouter,
                                    StatefulInstanceManager statefulInstances,
                                    StatefulLauncher launcher,
                                    Redactor redactor,
                                    ObjectMapper mapper,
                                    StatefulRuntimeProperties props,
                                    SubProcessCompletionRegistry subProcessRegistry,
                                    @Nullable DispatchCancellationRegistry dispatchCancellationRegistry,
                                    @Nullable StreamSessionManager streamSessions) {
        this.helperRouter = helperRouter;
        this.statefulInstances = statefulInstances;
        this.launcher = launcher;
        this.redactor = redactor;
        this.mapper = mapper;
        this.props = props;
        this.subProcessRegistry = subProcessRegistry;
        this.dispatchCancellationRegistry = dispatchCancellationRegistry;
        this.streamSessions = streamSessions;
    }

    /** Runs {@code job} against the live {@code instance}. Never throws a job-level failure. */
    public SandboxJobResponse execute(SandboxJobRequest job, StatefulInstance instance) {
        // Re-entrancy: a function-port sub-PI child whose ancestor run is parked inside THIS
        // instance (waiting on exactly this child) bypasses the admission gate — queuing it on
        // the FIFO deadlocks parent and child until the parent's request wall-clock.
        String jobRunId = job.workflowExecutionId();
        Set<String> inFlight = jobRunId == null ? null : inFlightRunsByInstance.get(instance.instanceId());
        if (inFlight != null && subProcessRegistry.hasAncestorIn(jobRunId, inFlight)) {
            log.info("[SQ-6] stateful.dispatch job={} node={} instance={} — re-entrant sub-PI "
                            + "dispatch (ancestor run in flight on this instance); bypassing the FIFO",
                    job.jobId(), job.currentNodeId(), instance.instanceId());
            return executeQueued(job, instance);
        }
        int permits = permitsFor(instance);
        InstanceGate gate = instanceLocks.compute(instance.instanceId(), (k, existing) -> {
            if (existing == null) {
                return new InstanceGate(new Semaphore(permits, true), permits);
            }
            // Recycle of the same instanceId may change capability; replace only when idle.
            if (existing.permits() != permits
                    && existing.lock().availablePermits() == existing.permits()
                    && !existing.lock().hasQueuedThreads()) {
                return new InstanceGate(new Semaphore(permits, true), permits);
            }
            return existing;
        });
        Semaphore lock = gate.lock();
        int maxDepth = Math.max(0, props.getStatefulQueueMaxDepth());
        // Reject before parking when the waiter queue is already saturated (loud overload).
        // Queue length counts waiters only — in-flight holders occupy permits, not queue slots —
        // so this stays honest against both FIFO (1) and dispatch-capable (N) capacity.
        if (lock.getQueueLength() >= maxDepth) {
            return SandboxJobResponse.failure(-1, "", "", Map.of(), "SANDBOX_OVERLOADED",
                    "stateful instance queue full (instance=" + instance.instanceId()
                            + ", depth=" + maxDepth
                            + ") — dispatch rejected; retry the run or raise "
                            + "sqs.sandbox.oci.stateful-queue.max-depth",
                    0L, instance.containerId(), SandboxJobResponse.FailureKind.INFRA);
        }
        long waitTimeoutMs = Math.max(0L, props.getStatefulQueueWaitTimeoutMs());
        boolean acquired;
        try {
            if (job.streaming()) {
                // SSE-streaming nodes: the permit holder ahead may legitimately stream for minutes, so the 30 s
                // queue cap must not kill this wait — the run would fail with SANDBOX_OVERLOADED while tokens are
                // still flowing. The wait is unbounded but interruptible (run cancellation unwinds it), and the
                // max-depth reject above still bounds the queue.
                lock.acquire();
                acquired = true;
            } else {
                acquired = lock.tryAcquire(waitTimeoutMs, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return SandboxJobResponse.failure(-1, "", "", Map.of(),
                    SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "stateful dispatch interrupted while queued",
                    0L, instance.containerId(), SandboxJobResponse.FailureKind.ENGINE);
        }
        if (!acquired) {
            return SandboxJobResponse.failure(-1, "", "", Map.of(), "SANDBOX_OVERLOADED",
                    "timed out waiting " + waitTimeoutMs
                            + " ms for the stateful instance FIFO (instance="
                            + instance.instanceId() + ")",
                    0L, instance.containerId(), SandboxJobResponse.FailureKind.INFRA);
        }
        try {
            return executeQueued(job, instance);
        } finally {
            lock.release();
            // Idle iff every permit is back and nobody is waiting. Removing while another dispatch still holds
            // a permit would let the next compute mint a fresh N-permit semaphore and over-admit.
            instanceLocks.computeIfPresent(instance.instanceId(), (k, v) -> {
                if (v.lock() != lock) {
                    return v;
                }
                return (v.lock().availablePermits() < v.permits() || v.lock().hasQueuedThreads())
                        ? v : null;
            });
        }
    }

    /**
     * Unlabeled images stay at 1 (FIFO). Dispatch-capable images use the configured parallelism,
     * clamped to at least 1 so {@code 0} never means unbounded.
     */
    private int permitsFor(StatefulInstance instance) {
        if (!instance.dispatchCapable()) {
            return 1;
        }
        return Math.max(1, props.getStatefulDispatchParallelism());
    }

    private SandboxJobResponse executeQueued(SandboxJobRequest job, StatefulInstance instance) {
        long start = System.nanoTime();
        String helperToken = null;
        String admittedCid = null;
        String runId = job.workflowExecutionId();
        if (runId != null && !runId.isBlank()) {
            // Recorded for the whole invoke so a function-port child of THIS run can recognise
            // the re-entrant case and bypass the gate (see execute()).
            inFlightRunsByInstance
                    .computeIfAbsent(instance.instanceId(), k -> ConcurrentHashMap.newKeySet())
                    .add(runId);
        }
        try {
            // A dead helper broker (its process was killed — OOM shows up as exit 137) leaves the container
            // running but with no callback channel. Dispatching into it anyway made
            // studioquack.llm/execute/connect fail as an opaque connection error (curl exit 7), so bounce the
            // instance onto a fresh broker first and only then give up.
            StatefulInstance target = instance.isRunning() && instance.containerId() != null
                    ? instance
                    : instance.isTemporary() ? null
                    : statefulInstances.resolveRunning(instance.envId(), instance.imageRef()).orElse(null);
            if (target == null) {
                return notRoutable(instance, start);
            }
            if (!target.isTemporary() && !statefulInstances.brokerAlive(target)) {
                Recycled recycled = recycleForDeadBroker(job, target);
                if (recycled.instance() != null) {
                    target = recycled.instance();
                } else if (!target.isRunning() || target.containerId() == null) {
                    // The bounce stopped the old container and the replacement never came up:
                    // dispatching into the stopped one fails as an opaque launcher NPE.
                    String reason = "stateful instance " + target.instanceId()
                            + " could not be restarted: " + recycled.failure();
                    return SandboxJobResponse.failure(-1, "", reason, Map.of(),
                            SandboxJobResponse.ERROR_SANDBOX_FAILED, reason, elapsedMs(start),
                            target.containerId(), SandboxJobResponse.FailureKind.CONTAINER);
                }
                // else: the original container still runs (no broker by design, or the restart
                // was skipped) — serve on it without helper callbacks, as before.
            }
            // Count the container that is actually invoked (after recycle), not the queue wait.
            target = admitRouted(target);
            if (target == null) {
                return notRoutable(instance, start);
            }
            admittedCid = target.containerId();
            helperToken = registerPersistentHelperJob(job, target);
            if (dispatchCancellationRegistry != null) {
                if (helperToken != null) {
                    final String token = helperToken;
                    dispatchCancellationRegistry.bindHelperRevoke(job.jobId(),
                            () -> helperRouter.revoke(token));
                }
                final StatefulInstance abortTarget = target;
                dispatchCancellationRegistry.bindStatefulAbort(job.jobId(),
                        () -> abortThisRequest(abortTarget, job.jobId()));
            }
            boolean brokerUp = statefulInstances.brokerAlive(target);
            String helperSocket = brokerUp ? HelperBroker.SOCKET_PATH : null;
            if (!brokerUp && usesHelperCallbacks(job)) {
                // Naming the real cause beats letting the script report "couldn't connect".
                log.warn("[SQ-6] stateful.dispatch job={} instance={} — helper broker still down after a"
                                + " restart; refusing a dispatch that needs helper callbacks",
                        job.jobId(), target.instanceId());
                return SandboxJobResponse.failure(-1, "",
                        BROKER_DOWN_MESSAGE,
                        Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                        BROKER_DOWN_MESSAGE, elapsedMs(start),
                        target.containerId(), SandboxJobResponse.FailureKind.CONTAINER);
            }
            if (!brokerUp) {
                log.warn("[SQ-6] stateful.dispatch job={} instance={} — helper broker not alive; "
                                + "dispatch runs without helper callbacks (studioquack.execute/llm/connect)",
                        job.jobId(), target.instanceId());
            }
            String requestJson = buildRequestJson(job, helperToken, brokerUp, target.brokerNote());

            final String activityToken = helperToken;
            // Drive the launcher off the activity-based idle reaper (never the fixed request wall clock)
            // whenever the helper broker is up. A node that calls studioquack.execute()/llm parks INSIDE its
            // own request while the delegated sub-PI runs, so a fixed 300 s deadline fires "stateful request
            // deadline exceeded" mid-delegation even though real work is in flight.
            boolean idleReaper = job.streaming() || brokerUp;
            StatefulLauncher.LauncherOutcome outcome = launcher.invoke(
                    target, requestJson, helperSocket, helperToken, job.envId(),
                    idleReaper,
                    () -> helperRouter.lookup(activityToken)
                            .map(HelperJob::lastActivityNanos)
                            .orElseGet(System::nanoTime));
            return toResponse(job, target, outcome, helperToken, elapsedMs(start));
        } catch (Exception e) {
            // Redact BEFORE the log line: the raw exception text may embed a just-resolved secret literal, so
            // the daemon log gets the same scrubbed form as the response stderr.
            String diagnostics = redactor.redact(String.valueOf(e.getMessage()), job.secretLiterals());
            log.warn("stateful.dispatch.error job={} node={} instance={}: {}",
                    job.jobId(), job.currentNodeId(), instance.instanceId(),
                    e.getClass().getSimpleName() + ": " + diagnostics);
            return SandboxJobResponse.failure(-1, "",
                    diagnostics,
                    Map.of(), SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "stateful dispatch failed: " + e.getClass().getSimpleName(), elapsedMs(start),
                    instance.containerId(), SandboxJobResponse.FailureKind.CONTAINER);
        } finally {
            statefulInstances.endDispatch(admittedCid);
            if (runId != null && !runId.isBlank()) {
                inFlightRunsByInstance.computeIfPresent(instance.instanceId(), (k, runs) -> {
                    runs.remove(runId);
                    return runs.isEmpty() ? null : runs;
                });
            }
            // Revoke ONLY this request's token — the instance + any parallel requests stay up.
            if (helperToken != null) {
                helperRouter.revoke(helperToken);
            }
        }
    }

    /* ------------------------------------------------------------------ */
    /* helper registration + request body                                 */
    /* ------------------------------------------------------------------ */

    /** A dead-broker bounce: the instance to dispatch into, or null plus the reason none is running. */
    private record Recycled(StatefulInstance instance, String failure) {
    }

    /**
     * Admits the job into {@code instance} while it is still routed, else into whatever routing
     * points at now; null when nothing running admits it. A job resolves its instance before it
     * queues, so a library update can switch routing and drain the old container meanwhile.
     */
    private StatefulInstance admitRouted(StatefulInstance instance) {
        StatefulInstance candidate = instance;
        for (int attempt = 0; attempt < 2; attempt++) {
            if (candidate.isRunning() && statefulInstances.beginDispatch(candidate.containerId())) {
                return candidate;
            }
            if (instance.isTemporary()) {
                return null;
            }
            StatefulInstance routed = statefulInstances
                    .resolveRunning(instance.envId(), instance.imageRef()).orElse(null);
            if (routed == null || routed == candidate) {
                return null;
            }
            candidate = routed;
        }
        return null;
    }

    private SandboxJobResponse notRoutable(StatefulInstance instance, long start) {
        String reason = "stateful instance " + instance.instanceId()
                + " is not running — it was replaced or stopped while this dispatch waited; retry the node";
        return SandboxJobResponse.failure(-1, "", reason, Map.of(),
                SandboxJobResponse.ERROR_SANDBOX_FAILED, reason, elapsedMs(start),
                instance.containerId(), SandboxJobResponse.FailureKind.CONTAINER);
    }

    /**
     * Bounces a stateful instance whose helper-broker process is gone. Returns the running instance
     * afterwards — the fresh one, or the original when the manager skipped the bounce
     * (platform-managed instances such as Docling) and it is still up.
     */
    private Recycled recycleForDeadBroker(SandboxJobRequest job, StatefulInstance instance) {
        log.warn("[SQ-6] stateful.dispatch job={} instance={} — helper broker is dead; restarting the"
                        + " instance to restore studioquack.execute/llm/connect",
                job.jobId(), instance.instanceId());
        String failure = null;
        try {
            StatefulInstanceManager.RecycleResult r = statefulInstances.recycleForImage(
                    instance.envId(), instance.imageRef(), instance.containerId());
            if (r != null && !r.recycled()) {
                failure = r.message() != null && !r.message().isBlank() ? r.message()
                        : "restart status=" + r.status();
            }
        } catch (Exception e) {
            log.warn("[SQ-6] stateful.dispatch job={} instance={} — broker restart failed: {}",
                    job.jobId(), instance.instanceId(), e.toString());
            failure = e.toString();
        }
        var running = statefulInstances.resolveRunning(instance.envId(), instance.imageRef());
        if (running.isPresent()) {
            return new Recycled(running.get(), null);
        }
        return new Recycled(null, failure != null ? failure : "no running instance after restart");
    }

    /**
     * Whether this script would actually use the helper channel. Source probe rather than a declared
     * flag: a script that never calls back runs fine on a broker-less container, so only the callers
     * that WOULD hit the dead socket are refused instead of failing later as a connection error.
     */
    private static boolean usesHelperCallbacks(SandboxJobRequest job) {
        String source = job.source();
        return source != null
                && (source.contains("studioquack.llm")
                || source.contains("studioquack.execute")
                || source.contains("studioquack.connect"));
    }

    private String registerPersistentHelperJob(SandboxJobRequest job, StatefulInstance instance) {
        Map<String, Object> ec = job.executionContext() == null
                ? new LinkedHashMap<>() : new LinkedHashMap<>(job.executionContext());
        ec.putIfAbsent("workflowExecutionId", job.workflowExecutionId());
        ec.putIfAbsent("currentNodeId", job.currentNodeId());
        if (job.envId() != null) {
            ec.putIfAbsent("envId", job.envId());
        }
        bridgeFunctionPortTransforms(job, ec);
        HelperJob helperJob = HelperJob.builder(job.jobId(),
                        job.language() == null ? "" : job.language().wire())
                .executionContext(ec)
                .jobMode(job.jobMode())
                .persistent(true)
                .callerImageRef(instance.imageRef())
                .secretLiterals(job.secretLiterals())
                .build();
        return helperRouter.registerPersistent(helperJob);
    }

    /**
     * Cancel-step abort for this request: interrupt the blocking launcher exec, POST
     * {@code BOUNDARY_INTERRUPT} to the in-instance {@code ${SQ_APP_PATH}/cancel}, and abort any
     * in-flight {@code /invoke/stream} session for the same dispatch. Best-effort; never throws.
     */
    void abortThisRequest(StatefulInstance instance, String requestId) {
        try {
            launcher.abortInFlight();
        } catch (RuntimeException e) {
            log.debug("stateful.abort.interrupt skipped job={}: {}", requestId, e.toString());
        }
        if (streamSessions != null && requestId != null) {
            try {
                streamSessions.abortForDispatch(requestId);
            } catch (RuntimeException e) {
                log.debug("stateful.abort.stream skipped job={}: {}", requestId, e.toString());
            }
        }
        try {
            launcher.postBoundaryInterrupt(instance, requestId);
        } catch (RuntimeException e) {
            log.debug("stateful.abort.cancel-post skipped job={}: {}", requestId, e.toString());
        }
    }

    /**
     * Bridges the function-port transform directives onto the helper job's server-side execution
     * context so {@code studioquack.execute()} can apply the request/response JOLT + script I/O
     * mapping between the caller's FUNCTION port and the target sub-workflow.
     */
    private static void bridgeFunctionPortTransforms(SandboxJobRequest job, Map<String, Object> ec) {
        Map<String, Object> inputVariables = job.inputVariables();
        if (inputVariables == null) {
            return;
        }
        Object directives = inputVariables.get("__sq_function_port_transforms__");
        if (directives != null) {
            ec.putIfAbsent("__sq_function_port_transforms__", directives);
        }
    }

    /** The request body piped to the user's in-instance app (context + inputs + helper token). */
    private String buildRequestJson(SandboxJobRequest job, String helperToken,
                                    boolean brokerUp, String brokerNote) {
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("requestId", job.jobId());
        req.put("workflowExecutionId", job.workflowExecutionId());
        req.put("currentNodeId", job.currentNodeId());
        req.put("envId", job.envId());
        // Template identity for in-instance dispatch; bespokeDir only when files exist.
        if (job.templateId() != null && !job.templateId().isBlank()) {
            req.put("templateId", job.templateId());
        }
        if (job.bespokeDir() != null && !job.bespokeDir().isBlank()) {
            req.put("bespokeDir", job.bespokeDir());
        }
        req.put("executionContext", job.executionContext());
        req.put("inputVariables", job.inputVariables());
        if (helperToken != null) {
            req.put("helperToken", helperToken);
        }
        // BYO apps (any language) can name the cause when the helper channel is absent.
        if (!brokerUp) {
            String note = (brokerNote != null && !brokerNote.isBlank())
                    ? brokerNote
                    : "no helper broker in this container (image must ship python3)";
            req.put("helperBrokerAbsent", note);
        }
        try {
            return mapper.writeValueAsString(req);
        } catch (Exception e) {
            return "{}";
        }
    }

    /* ------------------------------------------------------------------ */
    /* output parsing + response                                          */
    /* ------------------------------------------------------------------ */

    private SandboxJobResponse toResponse(SandboxJobRequest job, StatefulInstance instance,
                                          StatefulLauncher.LauncherOutcome outcome, String helperToken,
                                          long wallMs) {
        String rawStdout = outcome.stdout();
        String rawStderr = outcome.stderr();
        Map<String, Object> outputs = mergeOutputs(helperToken, rawStdout);
        // Data-flow checkpoint (shape only): the launcher stdout IS the result transport, so its
        // byte count + the parsed port keys are the two facts that decide whether the caller sees
        // the node's outputs. An empty parsedPorts on a non-blank stdout is the truncation tell.
        log.info("[SQ-6] stateful.result job={} node={} exit={} stdoutBytes={} parsedPorts={}",
                job.jobId(), job.currentNodeId(), outcome.exitCode(),
                rawStdout == null ? -1 : rawStdout.length(), outputs.keySet());
        if (outcome.exitCode() == 0 && !outcome.timedOut() && outputs.isEmpty()
                && rawStdout != null && !rawStdout.isBlank()) {
            int cap = (int) Math.min(Integer.MAX_VALUE,
                    Math.max(1024, props.getStatefulResponseMaxBytes()));
            if (rawStdout.length() >= cap) {
                // Truncated mid-JSON by the transport cap: the app DID answer (a file-bearing envelope over the
                // cap), but the daemon kept only the first <cap> bytes, which never parse. Succeeding here would
                // return EMPTY outputs to the caller — the silent "provider produced files but the agent saw
                // nothing" defect.
                return SandboxJobResponse.failure(outcome.exitCode(), "",
                        redactor.redact(rawStderr, job.secretLiterals()), Map.of(),
                        "SANDBOX_RESPONSE_OVERFLOW",
                        "stateful app response exceeded the daemon transport cap ("
                                + cap + " bytes, sqs.sandbox.oci.stateful-response-max-bytes); "
                                + "the result envelope was truncated and cannot be recovered",
                        wallMs, instance.containerId(), SandboxJobResponse.FailureKind.CONTAINER);
            }
            if (!rawStdout.stripLeading().startsWith("{")) {
                // A successful app that answers non-JSON violates the stateful reply contract
                // (reply keys = output ports). Keep the soft empty-outputs behaviour for BYO
                // compatibility, but name it once so it is never a silent drop again.
                log.warn("[SQ-6] stateful.result job={} node={} — app response is not a JSON "
                                + "port-map ({} bytes); outputs treated as empty",
                        job.jobId(), job.currentNodeId(), rawStdout.length());
            }
        }
        // Scrub the just-in-time-resolved secret literals for THIS job alongside the pattern redaction,
        // so a resolved password/token echoed by the in-instance app never reaches the persisted log.
        java.util.Set<String> secretLiterals = job.secretLiterals();
        // Fold the job's llm-request summaries into the displayed stdout BEFORE redaction, so the
        // run's node log (Quack Logs containerLogs) shows what went to the model. mergeOutputs
        // already consumed the raw app response above, so output parsing is unaffected.
        String stdout = redactor.redact(appendLlmRequestNotes(rawStdout, helperToken), secretLiterals);
        String stderr = redactor.redact(rawStderr, secretLiterals);
        String containerId = instance.containerId();

        // SDK typed error (studioquack.fail) wins over the exit code, exactly like the ephemeral path.
        HelperFailure typed = typedFailure(helperToken);
        if (typed != null) {
            return SandboxJobResponse.failure(outcome.exitCode(), stdout, stderr, outputs,
                    typed.errorCode(), redactor.redact(typed.message(), secretLiterals), wallMs, containerId,
                    SandboxJobResponse.FailureKind.SCRIPT);
        }
        // Dispatcher-level failure: the in-instance dispatcher could not hand the request to a node (bad
        // body, unknown templateId, no handler, compile-on-miss failed, relay unreachable). It has no
        // trustworthy per-job token to POST /fail with, so it answers the reserved envelope
        // {"__sq_fail":{errorCode,message}} instead of a port map.
        HelperFailure dispatchFail = dispatchFailure(rawStdout);
        if (dispatchFail != null) {
            return SandboxJobResponse.failure(outcome.exitCode(), stdout, stderr, Map.of(),
                    dispatchFail.errorCode(), redactor.redact(dispatchFail.message(), secretLiterals),
                    wallMs, containerId, SandboxJobResponse.FailureKind.ENGINE);
        }
        if (outcome.timedOut()) {
            return SandboxJobResponse.failure(-1, stdout, stderr, outputs,
                    SandboxJobResponse.ERROR_DEADLINE,
                    "stateful request deadline exceeded", wallMs, containerId,
                    SandboxJobResponse.FailureKind.CONTAINER);
        }
        if (outcome.exitCode() != 0) {
            return SandboxJobResponse.failure(outcome.exitCode(), stdout, stderr, outputs,
                    SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "stateful launcher exited non-zero (" + outcome.exitCode() + ")", wallMs, containerId,
                    SandboxJobResponse.FailureKind.SCRIPT);
        }
        return SandboxJobResponse.success(outcome.exitCode(), stdout, stderr, outputs, wallMs, containerId);
    }

    private HelperFailure typedFailure(String helperToken) {
        if (helperToken == null) {
            return null;
        }
        return helperRouter.lookup(helperToken)
                .map(HelperJob::failure)
                .filter(HelperFailure::hasCode)
                .orElse(null);
    }

    /**
     * Appends the job's {@code kind:"llm-request"} summaries (recorded by the helper router)
     * to the displayed stdout as {@code [llm] …} lines — the only node-attributed surface that
     * reaches Quack Logs (containerLogs) without a wire-shape change. Content-free by
     * construction (model/params/message shape, never prompt bodies); ≤ 8 lines, ≤ 2 KiB each.
     */
    private String appendLlmRequestNotes(String stdout, String helperToken) {
        if (helperToken == null) {
            return stdout;
        }
        Optional<HelperJob> job = helperRouter.lookup(helperToken);
        if (job.isEmpty()) {
            return stdout;
        }
        String base = stdout == null ? "" : stdout;
        StringBuilder sb = null;
        int count = 0;
        for (Map<String, Object> entry : job.get().logs()) {
            if (!"llm-request".equals(entry.get("kind"))) {
                continue;
            }
            Object msg = entry.get("message");
            String text = msg == null ? "" : String.valueOf(msg);
            if (text.isBlank()) {
                continue;
            }
            if (sb == null) {
                sb = new StringBuilder(base);
            }
            sb.append(base.isEmpty() && count == 0 ? "" : "\n").append("[llm] ")
                    .append(text.length() > 2048 ? text.substring(0, 2048) + "…" : text);
            if (++count >= 8) {
                sb.append("\n[llm] further llm requests omitted");
                break;
            }
        }
        return sb == null ? stdout : sb.toString();
    }

    /** Helper-channel {@code /result} wins; the launcher's captured app response fills gaps. */
    private Map<String, Object> mergeOutputs(String helperToken, String rawStdout) {
        Map<String, Object> out = new LinkedHashMap<>();
        // Launcher stdout (the app's HTTP response) first, so the helper result overwrites on conflict.
        parseResponseBody(rawStdout).forEach(out::put);
        if (helperToken != null) {
            Optional<HelperJob> job = helperRouter.lookup(helperToken);
            if (job.isPresent() && job.get().result() != null) {
                HelperResult r = job.get().result();
                if (r.items() != null) {
                    for (HelperResult.ResultItem item : r.items()) {
                        if (item != null && item.port() != null) {
                            out.put(item.port(), item.value());
                        }
                    }
                }
                if ((r.items() == null || r.items().isEmpty()) && r.payload() != null) {
                    out.putAll(r.payload());
                }
            }
        }
        return out;
    }

    /**
     * Reserved key the in-instance dispatcher answers with for a protocol-level failure; mirrors
     * {@code StudioQuackDispatch.DISPATCH_FAIL_KEY} in the node library. Never a port name.
     */
    static final String DISPATCH_FAIL_KEY = "__sq_fail";

    /**
     * The dispatcher's {@code {"__sq_fail":{errorCode,message}}} envelope as a typed failure, or null
     * when the reply is an ordinary port map. A blank code still fails, as {@code DISPATCH_ERROR}:
     * the key alone already says "this is not a result".
     */
    HelperFailure dispatchFailure(String rawStdout) {
        Object raw = parseResponseBody(rawStdout).get(DISPATCH_FAIL_KEY);
        if (!(raw instanceof Map<?, ?> env)) {
            return null;
        }
        Object code = env.get("errorCode");
        Object message = env.get("message");
        String errorCode = code == null || String.valueOf(code).isBlank() ? "DISPATCH_ERROR" : String.valueOf(code);
        return new HelperFailure(errorCode, message == null ? errorCode : String.valueOf(message), null);
    }

    /** Parses the app's JSON-object response body into a port map (empty on non-object/blank). */
    private Map<String, Object> parseResponseBody(String body) {
        if (body == null || body.isBlank()) {
            return Map.of();
        }
        try {
            String trimmed = body.trim();
            if (!trimmed.startsWith("{")) {
                return Map.of();
            }
            return mapper.readValue(trimmed, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
