package com.studioquack.sqs.runtime;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.ActivitiObjectNotFoundException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Resumes a parked ReceiveTask when a sandbox job returns.
 * Success and failure both pass only the encrypted handle and routing tokens onto the engine.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EngineTriggerService {

    private final SqsEngine sqsEngine;

    /**
     * Resumes the ReceiveTask identified by {@code executionId} with a successful result. The
     * variable map MUST already be REF-ONLY ({@code studioquackRef} + whitelisted tokens); it is
     * re-asserted here as a defence-in-depth backstop.
     */
    public void complete(String executionId, Map<String, Object> refOnlyVariables) {
        complete(executionId, refOnlyVariables, null);
    }

    /**
 * Resumes the ReceiveTask with a successful result AND publishes the execution-local gateway
 * routing tokens ({@code route__<gatewayId>}) computed from the node's outputs, so a downstream
 * gateway's generated JUEL ({@code ${route__<gatewayId> == 'value'}}) resolves (item 5).
 *
 * <p><b>Ordering is the whole point.</b> Each route token is written with
 * {@link SqsEngine#setRouteToken} <b>before</b> {@link SqsEngine#trigger}. Because the token then
 * advances on the SAME execution (ReceiveTask → error gate → gateway) the local routing variable
 * is already in scope when the gateway evaluates — eliminating the "gateway sees no token" race.
 * The route tokens are non-PHI scalars and satisfy the REF-ONLY whitelist ({@code route__} prefix).
 */
    public void complete(String executionId, Map<String, Object> refOnlyVariables,
                         Map<String, Object> routeTokens) {
        Map<String, Object> vars = refOnlyVariables == null ? Map.of() : refOnlyVariables;
        RefOnlyVariableGuard.assertRefOnly(vars);
        if (routeTokens != null && !routeTokens.isEmpty()) {
            try {
                for (Map.Entry<String, Object> token : routeTokens.entrySet()) {
                    sqsEngine.setRouteToken(executionId, token.getKey(), token.getValue());
                }
                log.debug("engine.trigger.route executionId={} gateways={}", executionId, routeTokens.keySet());
            } catch (RuntimeException e) {
                if (isExecutionGone(e)) {
                    log.debug("engine.trigger.route: execution gone executionId={}: {}", executionId, e.toString());
                    return;
                }
                throw e;
            }
        }
        log.debug("engine.trigger.complete executionId={} refKeys={}", executionId, vars.keySet());
        triggerSwallowingGone(executionId, vars);
    }

    /**
 * Resumes the ReceiveTask with a failure: writes the {@code __sq_error_code} sentinel (+ the
 * non-PHI message) as REF-ONLY variables and triggers. The generated post-wait
 * {@code sqErrorGateDelegate} reads the sentinel and raises {@code BpmnError(code)}; a catch-all
 * interrupting boundary routes the token to {@code Event_SandboxErrorEnd}.
 *
 * @param executionId the parked ReceiveTask execution id
 * @param errorCode the typed error code driving the gate (never null/blank)
 * @param errorMessage a short, non-PHI failure detail (may be null)
 */
    public void fail(String executionId, String errorCode, String errorMessage) {
        fail(executionId, errorCode, errorMessage, null, null);
    }

    /**
     * Resumes the ReceiveTask with a failure AND stamps the structured classification the boundary's
     * ERROR handler needs: {@link EngineVariableNames#ERROR_KIND} (one of
     * {@code com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind}) and {@link
     * EngineVariableNames#ERROR_EXIT_CODE} (the sandbox process exit code, when one exists). Both are
     * non-PHI, whitelist-allowed sentinels (Mapping Invariant #15) — omitted entirely when {@code null}
     * so a daemon-side failure that never reached a container carries no misleading exit code.
     *
     * @param kind     coarse failure classification (e.g. {@code SCRIPT}/{@code CONTAINER}/{@code
     *                 INFRA}/{@code ENGINE}), or null when the caller has none to report
     * @param exitCode the sandbox process exit code, or null when none exists (no container ran)
     */
    public void fail(String executionId, String errorCode, String errorMessage, String kind, Integer exitCode) {
        String code = (errorCode == null || errorCode.isBlank())
                ? "SANDBOX_FAILED" : errorCode;
        Map<String, Object> vars = new HashMap<>(4);
        vars.put(EngineVariableNames.ERROR_CODE, code);
        if (errorMessage != null && !errorMessage.isBlank()) {
            vars.put(EngineVariableNames.ERROR_MESSAGE, errorMessage);
        }
        if (kind != null && !kind.isBlank()) {
            vars.put(EngineVariableNames.ERROR_KIND, kind);
        }
        if (exitCode != null) {
            vars.put(EngineVariableNames.ERROR_EXIT_CODE, String.valueOf(exitCode));
        }
        RefOnlyVariableGuard.assertRefOnly(vars);
        log.debug("engine.trigger.fail executionId={} errorCode={} kind={} exitCode={}",
                executionId, code, kind, exitCode);
        triggerSwallowingGone(executionId, vars);
    }

    /**
     * A gone {@code wait_<node>} (boundary already cancelled the execution) must not rethrow —
     * Activiti would mark the nested command failed and roll back the caller.
     */
    private void triggerSwallowingGone(String executionId, Map<String, Object> vars) {
        try {
            sqsEngine.trigger(executionId, vars);
        } catch (RuntimeException e) {
            if (isExecutionGone(e)) {
                log.debug("engine.trigger: execution gone executionId={}: {}", executionId, e.toString());
                return;
            }
            throw e;
        }
    }

    /** {@link ActivitiObjectNotFoundException} or a message/class that says the execution is gone. */
    static boolean isExecutionGone(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof ActivitiObjectNotFoundException) {
                return true;
            }
            String name = t.getClass().getName();
            if (name.contains("ActivitiObjectNotFound")) {
                return true;
            }
            String message = t.getMessage() == null ? "" : t.getMessage().toLowerCase();
            if ((message.contains("execution") || message.contains("process instance"))
                    && (message.contains("not found")
                    || message.contains("doesn't exist")
                    || message.contains("does not exist")
                    || message.contains("gone"))) {
                return true;
            }
        }
        return false;
    }
}
