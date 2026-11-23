package com.studioquack.sqs.runtime;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Delivers a sandbox result onto its parked {@code ReceiveTask}, closing the resume race
 * (item 1). It is the single place that turns a {@code dispatchId} + result into a durable
 * resume, used by both the live {@code ScriptDispatchDelegate} and boot recovery (after
 * {@link DispatchExecutionRegistry#hydrateOnBoot} re-loads the wait-state, the same coordinator
 * re-delivers the result — no duplicate side effect).
 *
 * <p><b>How the race is removed.</b> The wait-state is registered <em>durably</em> in the delegate's
 * async {@code execute} — which commits atomically with the token parking at {@code wait_<node>} —
 * BEFORE the background sandbox task can produce a result. Delivery then does a <b>bounded
 * retry/backoff</b> ({@link #resolveParkedExecution}) that waits until the ReceiveTask is actually
 * parked before triggering. So a fast-returning script can no longer resume "before the token parks"
 * and can never see "execution not found / not waiting": if the wait-state row exists, the resumer
 * keeps polling until the token is parked (or the bounded budget elapses and the sweeper owns it).
 *
 * <p><b>Idempotent / no-dup.</b> Delivery claims the in-memory {@link DispatchExecutionRegistry.WaitState}
 * once; a second delivery for the same dispatch finds nothing (or the token is no longer parked) and
 * is a no-op. On resume it marks the durable row RESOLVED (REQUIRES_NEW).
 */
@Slf4j
@Component
public class DispatchResultCoordinator {

    private final DispatchExecutionRegistry registry;
    private final EngineTriggerService engineTriggerService;
    private final com.studioquack.sqs.engine.SqsEngine sqsEngine;
    private final DispatchCancellationRegistry cancellationRegistry;
    private final long resumePollMs;

    public DispatchResultCoordinator(DispatchExecutionRegistry registry,
                                     EngineTriggerService engineTriggerService,
                                     com.studioquack.sqs.engine.SqsEngine sqsEngine,
                                     DispatchCancellationRegistry cancellationRegistry,
                                     @Value("${sqs.sandbox.resume-poll-ms:30000}") long resumePollMs) {
        this.registry = registry;
        this.engineTriggerService = engineTriggerService;
        this.sqsEngine = sqsEngine;
        this.cancellationRegistry = cancellationRegistry;
        this.resumePollMs = resumePollMs;
    }

    /**
     * Delivers a successful result: resolves the parked execution (bounded retry), publishes the
     * gateway route tokens + REF-ONLY vars, triggers, and marks the wait-state RESOLVED.
     *
     * @return {@code true} when the token was resumed; {@code false} when the dispatch is unknown /
     * already resolved / not yet parked within budget (left PENDING for the sweeper).
     */
    public boolean deliverSuccess(String dispatchId, Map<String, Object> engineVars,
                                  Map<String, Object> routeTokens) {
        if (cancellationRegistry.isCancelled(dispatchId)) {
            log.debug("deliverSuccess: dispatchId={} cancelled ({}) — refusing resume",
                    dispatchId, cancellationRegistry.cancelReason(dispatchId));
            return false;
        }
        // Sandbox wait is over — do not let a racing cancel interrupt this H2 resume.
        cancellationRegistry.markEnginePhase(dispatchId);
        DispatchExecutionRegistry.WaitState state = registry.find(dispatchId).orElse(null);
        if (state == null) {
            log.debug("deliverSuccess: no wait-state for dispatchId={} (already resolved?) — no-op", dispatchId);
            return false;
        }
        String executionId = resolveParkedExecution(state);
        if (executionId == null) {
            log.warn("deliverSuccess: ReceiveTask {} for pi={} not parked within budget; leaving PENDING "
                    + "for the orphan sweeper.", state.waitActivityId(), state.processInstanceId());
            return false;
        }
        engineTriggerService.complete(executionId, engineVars, routeTokens);
        registry.complete(dispatchId);
        log.info("dispatch.resume.ok dispatchId={} node={} exec={}",
                dispatchId, state.nodeId(), executionId);
        return true;
    }

    /**
     * Delivers a failure: resolves the parked execution (bounded retry), stamps the error sentinel,
     * triggers (→ error gate → boundary → shared error end), and marks the wait-state RESOLVED.
     */
    public boolean deliverFailure(String dispatchId, String errorCode, String errorMessage) {
        return deliverFailure(dispatchId, errorCode, errorMessage, null, null);
    }

    /**
     * Delivers a classified failure: same as {@link #deliverFailure(String, String, String)} but also
     * stamps the {@link com.studioquack.sqs.engine.runtime.EngineVariableNames#ERROR_KIND}/{@link
     * com.studioquack.sqs.engine.runtime.EngineVariableNames#ERROR_EXIT_CODE} sentinels so the
     * boundary's structured ERROR payload carries them (see {@code EngineTriggerService#fail}).
     */
    public boolean deliverFailure(String dispatchId, String errorCode, String errorMessage,
                                  String failureKind, Integer exitCode) {
        if (cancellationRegistry.isCancelled(dispatchId)) {
            log.debug("deliverFailure: dispatchId={} cancelled ({}) — refusing resume",
                    dispatchId, cancellationRegistry.cancelReason(dispatchId));
            return false;
        }
        // Sandbox wait is over — do not let a racing cancel interrupt this H2 resume.
        cancellationRegistry.markEnginePhase(dispatchId);
        DispatchExecutionRegistry.WaitState state = registry.find(dispatchId).orElse(null);
        if (state == null) {
            log.debug("deliverFailure: no wait-state for dispatchId={} — no-op", dispatchId);
            return false;
        }
        String executionId = resolveParkedExecution(state);
        if (executionId == null) {
            log.warn("deliverFailure: ReceiveTask {} for pi={} not parked within budget; leaving PENDING.",
                    state.waitActivityId(), state.processInstanceId());
            return false;
        }
        engineTriggerService.fail(executionId, errorCode, errorMessage, failureKind, exitCode);
        registry.complete(dispatchId);
        log.info("dispatch.resume.fail dispatchId={} node={} code={} kind={} exec={}",
                dispatchId, state.nodeId(), errorCode, failureKind, executionId);
        return true;
    }

    /**
     * Bounded retry/backoff that returns the live execution id parked at {@code wait_<node>} once the
     * token has actually parked, or {@code null} if it does not park within the resume budget. This
     * is what makes a fast-returning script safe: it waits for the park instead of failing.
     */
    public String resolveParkedExecution(DispatchExecutionRegistry.WaitState state) {
        long deadline = System.currentTimeMillis() + resumePollMs;
        do {
            String executionId = sqsEngine.findUnfinishedWaitExecution(
                    state.processInstanceId(), state.waitActivityId());
            if (executionId != null) {
                return executionId;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return null;
            }
        } while (System.currentTimeMillis() < deadline);
        // last-chance read after the budget
        return sqsEngine.findUnfinishedWaitExecution(state.processInstanceId(), state.waitActivityId());
    }
}
