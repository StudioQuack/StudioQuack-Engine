package com.studioquack.sqs.engine;

import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.repository.Deployment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Deletes the throwaway Activiti deployments produced by Designer Runs.
 *
 * <p><b>Why they pile up.</b> A run started from the designer canvas re-pushes the graph on every
 * click of Run, because the daemon must never execute a stale definition after a canvas edit. Each
 * push whose graph differs creates a new row in {@code ACT_RE_DEPLOYMENT} plus its
 * {@code ACT_RE_PROCDEF} / {@code ACT_GE_BYTEARRAY} children. Nothing ever removed them, so a single
 * afternoon of iterating on a workflow left hundreds of dead deployments in the client database.
 *
 * <p><b>How one is recognised.</b> By the {@link SqsEngine#EPHEMERAL_CATEGORY} stamped on the
 * deployment at creation. The marker lives on the deployment itself rather than in a side table, so
 * there is no second source of truth to reconcile and a daemon crash cannot orphan the bookkeeping.
 *
 * <p><b>Why a sweep and not the completion event.</b> The obvious trigger — {@code PROCESS_COMPLETED}
 * in {@code RunStatusEventBridge} — fires while the engine is still inside the completing command.
 * Deleting a deployment from there means issuing a nested engine command against rows the outer
 * transaction still holds. A sweep runs on its own thread with nothing in flight, which is both safer
 * and inherently crash-recoverable: whatever was missed is simply picked up on the next pass.
 *
 * <p><b>When a deployment is deleted.</b> Never while a process instance is running against it —
 * cascading a live run would strand it mid-flight. Beyond that, the two remaining states are treated
 * differently:
 * <ul>
 * <li><b>Its run has finished</b> (a historic instance exists, none running): delete IMMEDIATELY.
 * Promptness is not tidiness here. {@code start-run} resolves by process key, taking the LATEST
 * deployed version, so for as long as a spent draft deployment lingers it is the version a
 * published trigger would execute. Deleting as soon as the run ends is what keeps an unpublished
 * canvas draft out of a production trigger.</li>
 * <li><b>Its run has not started yet</b> (no instances at all): wait out {@code grace-minutes}.
 * the control plane pushes {@code deploy} and {@code start-run} as two separate commands, so a
 * perfectly live deployment briefly has zero instances; deleting there kills the run that is
 * about to start. The timer only has to cover that gap, plus any queueing delay.</li>
 * </ul>
 */
@Slf4j
@Component
public class EphemeralDeploymentReaper {

    private final SqsEngine sqsEngine;
    private final boolean enabled;
    private final Duration grace;

    public EphemeralDeploymentReaper(
            SqsEngine sqsEngine,
            @Value("${sqs.deployment.ephemeral-reap.enabled:true}") boolean enabled,
            @Value("${sqs.deployment.ephemeral-reap.grace-minutes:15}") long graceMinutes) {
        this.sqsEngine = sqsEngine;
        this.enabled = enabled;
        this.grace = Duration.ofMinutes(Math.max(1, graceMinutes));
    }

    /** Periodic sweep — the backstop that eventually clears everything, including post-crash leftovers. */
    @Scheduled(fixedDelayString = "${sqs.deployment.ephemeral-reap.interval-ms:300000}",
            initialDelayString = "${sqs.deployment.ephemeral-reap.initial-delay-ms:120000}")
    public void sweep() {
        reap("scheduled");
    }

    /**
     * Opportunistic reap right after a designer deploy. The deployment just created is younger than
     * the grace period and so is never a candidate; this only clears its predecessors, which keeps the
     * steady state at roughly one live ephemeral deployment per workflow between sweeps.
     */
    public void reapAfterDeploy() {
        reap("post-deploy");
    }

    /**
     * @return the number of deployments deleted. Never throws — reaping is housekeeping and must not
     *         fail the deploy that triggered it or wedge the scheduler.
     */
    public int reap(String trigger) {
        if (!enabled) {
            return 0;
        }
        try {
            List<Deployment> candidates = sqsEngine.findDeploymentsByCategory(SqsEngine.EPHEMERAL_CATEGORY);
            if (candidates.isEmpty()) {
                return 0;
            }
            Instant cutoff = Instant.now().minus(grace);
            int deleted = 0;
            int keptPending = 0;
            int keptRunning = 0;
            for (Deployment deployment : candidates) {
                // A live run always wins, regardless of age.
                if (sqsEngine.runningInstanceCount(deployment.getId()) > 0) {
                    keptRunning++;
                    continue;
                }
                // No live run. Has it ever had one? If yes the draft is spent — remove it now, before a
                // published trigger can resolve it as the latest version of this process key.
                boolean runFinished = sqsEngine.historicInstanceCount(deployment.getId()) > 0;
                if (!runFinished) {
                    boolean withinGrace = deployment.getDeploymentTime() != null
                            && deployment.getDeploymentTime().toInstant().isAfter(cutoff);
                    if (withinGrace) {
                        // Deployed but not yet started: this is the deploy -> start-run window.
                        keptPending++;
                        continue;
                    }
                    // Past the grace window with no run ever started — the run was abandoned, cancelled
                    // before start, or the daemon restarted between the two commands. Safe to drop.
                }
                if (sqsEngine.deleteDeployment(deployment.getId())) {
                    deleted++;
                }
            }
            if (deleted > 0 || keptRunning > 0) {
                log.info("ephemeral.deployment.reap trigger={} deleted={} keptPendingStart={} keptRunning={}",
                        trigger, deleted, keptPending, keptRunning);
            } else {
                log.debug("ephemeral.deployment.reap trigger={} deleted=0 keptPendingStart={}", trigger, keptPending);
            }
            return deleted;
        } catch (Exception e) {
            log.warn("ephemeral.deployment.reap failed trigger={} cause={}", trigger, e.toString());
            return 0;
        }
    }
}
