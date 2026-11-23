package com.studioquack.sqs.engine.halt;

import com.studioquack.sqs.engine.quota.NodeExecutionMeter;
import com.studioquack.sqs.engine.update.WorkIntakeGate;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Self-hosted execution halt: this engine executes only while the control plane can meter it.
 *
 * <ul>
 * <li>{@link Mode#OFFLINE} — no contact with the control plane for
 * {@code sqs.execution-halt.offline-after}, and from a cold boot until the first one. Contact is
 * a 2xx heartbeat, a pushed command, or an authenticated overlay request (a daemon in
 * {@code OVERLAY_SILENT} cadence stops beating while the control plane polls it over the overlay).</li>
 * <li>{@link Mode#LIMITS_EXCEEDED} — the monthly node allowance is spent: the heartbeat snapshot plus
 * the unflushed local count reach it ({@link NodeExecutionMeter#admitNewRun}), or the control plane
 * refused a log with {@link NodeExecutionMeter#ERROR_CODE}.</li>
 * </ul>
 *
 * <p>While halted every new start, message, signal and user-task decision is refused
 * ({@link #refusal}), and Activiti's async job acquisition is paused. Every designer node runs as an
 * async job, so active workflows stop at their next node boundary and timers stop firing; a node already
 * inside its sandbox finishes and its token parks on the next node's job. Jobs stay in the database, so
 * the runs continue where they stopped once the mode clears (contact is back, or the allowance renews).
 *
 * <p>Disabled without the secure channel ({@code sqs.channel.enabled=false}): there is no control plane
 * to meter against, and the engine config then starts the executor itself.
 */
@Slf4j
@Component
public class ExecutionHaltGuard {

    public enum Mode { ACTIVE, OFFLINE, LIMITS_EXCEEDED }

    public static final String OFFLINE_ERROR_CODE = "Error_EngineOffline";

    public static final String OFFLINE_MESSAGE =
            "The engine cannot reach StudioQuack and is in offline mode. Active runs are paused and new runs "
                    + "are refused until the connection is back.";

    /** Heartbeat {@code resources} key carrying {@link #mode()}. */
    public static final String RESOURCE_KEY = "executionHalt";

    private final NodeExecutionMeter meter;
    private final ObjectProvider<ProcessEngine> processEngine;
    private final ObjectProvider<WorkIntakeGate> intakeGate;
    private final boolean enabled;
    private final long offlineAfterMs;

    /** Epoch ms of the last 2xx heartbeat; 0 until the first one, so a cold boot starts OFFLINE. */
    private volatile long lastContactMs;
    /** A log refused for quota; the next beat's snapshot is authoritative again and clears it. */
    private volatile boolean quotaRefusedByBackend;
    private volatile Mode lastEnforced;
    private volatile boolean stopped;

    /** Pausing the executor waits for running jobs, so it never runs on a beat or engine thread. */
    private final ExecutorService enforcer = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "sqs-execution-halt");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean enforcePending = new AtomicBoolean();

    private final Clock clock;

    @Autowired
    public ExecutionHaltGuard(NodeExecutionMeter meter,
                              ObjectProvider<ProcessEngine> processEngine,
                              ObjectProvider<WorkIntakeGate> intakeGate,
                              @Value("${sqs.execution-halt.enabled:${sqs.channel.enabled:false}}") boolean enabled,
                              @Value("${sqs.execution-halt.offline-after:PT5M}") Duration offlineAfter) {
        this(meter, processEngine, intakeGate, enabled, offlineAfter, Clock.systemUTC());
    }

    ExecutionHaltGuard(NodeExecutionMeter meter,
                       ObjectProvider<ProcessEngine> processEngine,
                       ObjectProvider<WorkIntakeGate> intakeGate,
                       boolean enabled,
                       Duration offlineAfter,
                       Clock clock) {
        this.meter = meter;
        this.processEngine = processEngine;
        this.intakeGate = intakeGate;
        this.enabled = enabled;
        this.offlineAfterMs = Math.max(Duration.ofMinutes(1).toMillis(), offlineAfter.toMillis());
        this.clock = clock;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Mode mode() {
        if (!enabled) {
            return Mode.ACTIVE;
        }
        // Precedence: LIMITS_EXCEEDED > OFFLINE > ACTIVE once an exhausted allowance is known.
        // On cold boot (before any snapshot arrives) meter.admitNewRun() is true, so contact == 0L
        // keeps the engine fail-closed in OFFLINE until the control plane's first snapshot lands.
        if (quotaRefusedByBackend || (meter != null && !meter.admitNewRun())) {
            return Mode.LIMITS_EXCEEDED;
        }
        long contact = lastContactMs;
        if (contact == 0L || clock.millis() - contact > offlineAfterMs) {
            return Mode.OFFLINE;
        }
        return Mode.ACTIVE;
    }

    /** {@code null} when work is admitted, else {@code "<errorCode>: <message>"} for the refusal. */
    public String refusal() {
        return switch (mode()) {
            case ACTIVE -> null;
            case OFFLINE -> OFFLINE_ERROR_CODE + ": " + OFFLINE_MESSAGE;
            case LIMITS_EXCEEDED -> NodeExecutionMeter.ERROR_CODE + ": " + NodeExecutionMeter.EXHAUSTED_MESSAGE;
        };
    }

    /** Throws {@link ExecutionHaltedException} while halted. */
    public void requireActive() {
        String refusal = refusal();
        if (refusal != null) {
            throw new ExecutionHaltedException(refusal);
        }
    }

    /** A 2xx heartbeat landed; its quota snapshot is already applied to the meter. */
    public void onControlPlaneContact() {
        lastContactMs = clock.millis();
        quotaRefusedByBackend = false;
        requestEnforce();
    }

    /**
 * the control plane's "allowance spent" answer to a log POST. Keyed on the error code, not the status:
 * the log backfill limiter also answers 429.
 */
    public static boolean isQuotaRefusal(int status, String body) {
        return status == 429 && body != null && body.contains(NodeExecutionMeter.ERROR_CODE);
    }

    /**
 * An authenticated request reached the overlay listener. That is contact as much as a beat: a
 * daemon in {@code OVERLAY_SILENT} cadence stops beating while the control plane polls it over the
 * overlay. Unlike a beat it carries no quota snapshot, so it only extends contact a beat already
 * established: before the first beat the allowance is unknown and the engine stays OFFLINE.
 */
    public void onOverlayContact() {
        if (lastContactMs == 0L) {
            return;
        }
        lastContactMs = clock.millis();
        if (mode() != lastEnforced) {
            requestEnforce();
        }
    }

    /** A sandbox-log POST came back non-2xx. Only a quota refusal changes the mode. */
    public void observeLogRefusal(int status, String body) {
        if (!enabled || !isQuotaRefusal(status, body)) {
            return;
        }
        if (!quotaRefusedByBackend) {
            log.warn("execution-halt: backend refused a run log with {} — entering limits-exceeded mode",
                    NodeExecutionMeter.ERROR_CODE);
        }
        quotaRefusedByBackend = true;
        requestEnforce();
    }

    /** Called per node activation; enforces right away when that activation spent the allowance. */
    public void onNodeActivation() {
        if (enabled && mode() != Mode.ACTIVE) {
            requestEnforce();
        }
    }

    /** Catches the offline timeout, which no event signals. */
    @Scheduled(fixedDelayString = "${sqs.execution-halt.check-interval-ms:15000}",
            initialDelayString = "${sqs.execution-halt.check-interval-ms:15000}")
    public void tick() {
        if (enabled) {
            requestEnforce();
        }
    }

    public void requestEnforce() {
        if (stopped || !enforcePending.compareAndSet(false, true)) {
            return;
        }
        try {
            enforcer.execute(this::enforce);
        } catch (RuntimeException rejected) {
            enforcePending.set(false);
        }
    }

    @PreDestroy
    void shutdown() {
        stopped = true;
        enforcer.shutdownNow();
    }

    private void enforce() {
        enforcePending.set(false);
        if (stopped) {
            return;
        }
        try {
            Mode mode = mode();
            AsyncExecutor executor = asyncExecutor();
            if (executor == null) {
                return;
            }
            if (mode != Mode.ACTIVE && executor.isActive()) {
                // Waits for jobs already running; due timers and parked node jobs stay in the database.
                executor.shutdown();
            } else if (mode == Mode.ACTIVE && !executor.isActive() && !updateInProgress()) {
                executor.start();
            }
            if (mode != lastEnforced) {
                if (mode == Mode.ACTIVE) {
                    log.info("execution-halt: mode ACTIVE — new work admitted, job acquisition {}",
                            executor.isActive() ? "running" : "held by the in-place update");
                } else {
                    log.warn("execution-halt: mode {} — job acquisition paused, active runs stop at their next "
                            + "node, new work is refused", mode);
                }
                lastEnforced = mode;
            }
        } catch (RuntimeException ex) {
            log.warn("execution-halt: enforcement failed: {}", ex.toString());
        }
    }

    /** An in-place update owns the executor while it runs; it restarts or replaces this process itself. */
    private boolean updateInProgress() {
        WorkIntakeGate gate = intakeGate.getIfAvailable();
        return gate != null && gate.isUpdateInProgress();
    }

    private AsyncExecutor asyncExecutor() {
        ProcessEngine engine = processEngine.getIfAvailable();
        if (engine != null && engine.getProcessEngineConfiguration() instanceof ProcessEngineConfigurationImpl cfg) {
            return cfg.getAsyncExecutor();
        }
        return null;
    }
}
