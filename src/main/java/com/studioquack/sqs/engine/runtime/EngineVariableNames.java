package com.studioquack.sqs.engine.runtime;

import java.util.Set;

/**
 * Activiti variable names this daemon writes.
 * Engine variables carry only the encrypted context handle and small non-payload routing tokens.
 */
public final class EngineVariableNames {

    private EngineVariableNames() {
    }

    /**
     * Reserved-name prefix guarding every engine-internal control variable this daemon writes
     * ({@code __sq_error_code}, {@code __sq_error_message}, {@code __sq_be_epoch}, …). No
     * user-authored variable may start with it, so a control token can never be shadowed.
     */
    public static final String RESERVED_PREFIX = "__sq_";

    /** Process variable holding the encrypted execution-context handle. */
    public static final String STUDIOQUACK_REF = "studioquackRef";

    /**
     * Namespace prefix for execution-local, non-PHI gateway routing tokens. Written with
     * {@code setVariableLocal(executionId, routeTokenVariableName(nodeId), scalar)}; gateway
     * conditions read {@code ${route__<nodeId> == '…'}}. Never a shared global bag, never PHI.
     */
    public static final String ROUTE_PREFIX = "route__";

    /**
     * The routing-token variable name for {@code nodeId} — the SINGLE convention shared by the
     * BPMN generator (which emits {@code ${route__<id> == '…'}} conditions) and the runtime bridge
     * (which writes the token). The node id is mapped to a JUEL-legal identifier: FE ids are
     * hyphenated UUIDs, and a hyphen inside a {@code ${…}} identifier parses as subtraction (a
     * digit-leading segment after it is an outright parse error), so every non
     * {@code [A-Za-z0-9_]} character becomes {@code _}. The {@link #ROUTE_PREFIX} guarantees a
     * letter start, and the mapping is deterministic so writer and reader always agree.
     */
    public static String routeTokenVariableName(String nodeId) {
        return ROUTE_PREFIX + sanitizeToIdentifier(nodeId);
    }

    /**
     * Namespace prefix for execution-local <b>message-correlation pins</b>: when a
     * {@code message.intermediate.catch.event} parks with an authored {@code correlationKey},
     * the receiver listener pins the resolved key under {@code __sq_msgcorr_<sanitizedMessageName>}
     * on the parked execution so a sender can correlate by the SHARED authored key — not only by
     * the receiver run's businessKey (which a cross-flow sender cannot know at authoring time).
     * The value is a non-PHI routing token by contract (same class as a businessKey).
     */
    public static final String MSG_CORR_PREFIX = RESERVED_PREFIX + "msgcorr_";

    /**
     * Process-instance-scoped <b>start correlation key</b> of a message-started run: the key the
     * caller addressed (external webhook {@code correlationKey} / intra-engine bridge key) or the
     * receiver-derived key ({@code businessKeyExpression} / static receiver key) — correlation
     * DATA, never identity. The run's businessKey (= {@code workflowExecutionId}) is ALWAYS a
     * minted unique {@code wh_…} id, so two runs may carry the same start key (same key on
     * different message names, or a re-used key) without colliding in the run index. A catch
     * addressed by this key resolves through
     * {@code SqsEngine#findMessageEventExecutionByStartKey} — the third fallback after businessKey
     * and the {@link #MSG_CORR_PREFIX} alias pin; an ambiguous match (several runs share the key)
     * is a benign no-match, same rule as the alias. Non-PHI routing token by contract (same class
     * as a businessKey).
     */
    public static final String START_CORRELATION_KEY = RESERVED_PREFIX + "start_key";

    /**
     * Signal-name prefix for the <b>escalation bridge</b>: Activiti 9 has no escalation model at
     * all, so an escalation boundary event subscribes to a signal named
     * {@code __sq_esc_<escalationCode>} and the escalation throw/end listener fires it
     * instance-scoped (per parked execution — never the engine-wide broadcast). The
     * {@link #RESERVED_PREFIX} keeps a user-authored signal from colliding with the bridge;
     * {@code ActivitiSignalMessageRegistry} rejects such names at deploy time.
     */
    public static final String ESCALATION_SIGNAL_PREFIX = RESERVED_PREFIX + "esc_";

    /**
     * Namespace prefix for execution-local <b>call-activity businessKey pins</b>: the
     * {@code callActivityIoListener} start leg pins a DuckTape-mapped child businessKey under
     * {@code __sq_callbk_<nodeId>} on the call-activity execution, and the generated
     * {@code CallActivity}'s businessKey expression ({@code ${__sq_callbk_<nodeId>}}) resolves it
     * when the engine starts the child. The pin is set UNCONDITIONALLY (default a minted
     * {@code sub_<nanoId>} — a keyless child is invisible to the run index): the fail-closed
     * expression manager throws on an absent identifier and {@code CallActivityBehavior}
     * toString()s the value without a null check. A non-PHI routing token by contract (same
     * class as {@link #MSG_CORR_PREFIX}).
     */
    public static final String CALL_BK_PREFIX = RESERVED_PREFIX + "callbk_";

    /**
     * Execution-scoped pin prefix for the call-activity failure-propagation handoff:
     * {@code __sq_callfail_<superExecutionId>}. The writer is the child-end listener
     * ({@code callActivityChildFailureListener}), which pins the child's terminal error code on
     * the PARENT instance scope keyed by the super execution id; the reader is the
     * {@code callActivityFailurePropagationListener} on the call activity's end event, which
     * knows its own execution id and re-raises the code as a {@code BpmnError}. Keying by the
     * super execution id keeps concurrent call activities of one parent from cross-reading.
     * A non-PHI routing token by contract (same class as {@link #CALL_BK_PREFIX}).
     */
    public static final String CALL_FAIL_PREFIX = RESERVED_PREFIX + "callfail_";

    /** The failure-pin variable name for a call-activity super execution (writer/reader convention). */
    public static String callFailureVariableName(String superExecutionId) {
        return CALL_FAIL_PREFIX + sanitizeToIdentifier(superExecutionId);
    }

    /**
     * The businessKey-pin variable name for a {@code call.activity} node — the SINGLE convention
     * shared by the generator (which emits the {@code ${__sq_callbk_<id>}} expression) and the
     * listener (which writes the pin). Same identifier mapping as {@link #routeTokenVariableName}.
     */
    public static String callBusinessKeyVariableName(String nodeId) {
        return CALL_BK_PREFIX + sanitizeToIdentifier(nodeId);
    }

    /**
     * The correlation-pin variable name for {@code messageName} — the SINGLE convention shared by
     * the receiver listener (writer, at park time) and {@code SqsEngine}'s correlation fallback
     * (reader). Same identifier mapping as {@link #routeTokenVariableName} so writer and reader
     * always agree; the pin is execution-local, so two different messages sanitising alike never
     * collide (each parked execution carries only its own catch's pin).
     */
    public static String messageCorrelationVariableName(String messageName) {
        return MSG_CORR_PREFIX + sanitizeToIdentifier(messageName);
    }

    /** Maps an arbitrary authored string to a JUEL-legal identifier (see {@link #routeTokenVariableName}). */
    private static String sanitizeToIdentifier(String raw) {
        String value = raw == null ? "" : raw;
        StringBuilder safe = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            safe.append(c == '_' || Character.isLetterOrDigit(c) ? c : '_');
        }
        return safe.toString();
    }

    /**
     * Sentinel written right before a {@code ReceiveTask} is triggered on failure. The
     * generated post-wait {@code sqErrorGateDelegate} reads it and, if present, throws
     * {@code new BpmnError(code)} so the interrupting error boundary routes the token to
     * {@code Event_SandboxErrorEnd}.
     */
    public static final String ERROR_CODE = "__sq_error_code";

    /** Human-readable failure detail accompanying {@link #ERROR_CODE} (non-PHI). */
    public static final String ERROR_MESSAGE = "__sq_error_message";

    /**
     * The coarse failure classification accompanying {@link #ERROR_CODE}: one of {@code NONE},
     * {@code SCRIPT}, {@code CONTAINER}, {@code INFRA}, {@code ENGINE} (mirrors
     * {@code com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind}). Written alongside the error
     * sentinel so {@code SqBoundaryEventDelegate} can hand the boundary's ERROR handler a structured
     * payload instead of a bare code/message pair — non-PHI, a fixed small vocabulary.
     */
    public static final String ERROR_KIND = RESERVED_PREFIX + "error_kind";

    /**
     * The sandbox process exit code accompanying {@link #ERROR_CODE}, when one exists (a daemon-side
     * dispatch failure that never reached a container has none). Non-PHI (a small integer), stored as
     * a String on the engine like every other {@code __sq_} sentinel.
     */
    public static final String ERROR_EXIT_CODE = RESERVED_PREFIX + "error_exit_code";

    /** BpmnError code raised by the monitoring "skip step" action. */
    public static final String SKIP_ERROR_CODE = "SQ_SKIP";

    /**
     * Process-instance-scoped pin for the operator "Skip &amp; Continue" of a run HELD at
     * {@code hold_<nodeId>} (value: the flow-node id to skip, nothing else — non-PHI). The
     * {@code skip-step} command pins it before requeueing the hold's dead-letter job; the
     * re-dispatch of the pinned node reads and consumes it ({@code ScriptDispatchDelegate} /
     * {@code DoclingDelegate}) and short-circuits WITHOUT a sandbox job — an authored
     * {@link #SKIP_ERROR_CODE} boundary gets the typed failure, anything else continues with no
     * outputs. Deliberately NOT in {@link #ERROR_SENTINELS}: the hold requeue scrubs that family
     * off the scope chain and would wipe a pin it is meant to deliver.
     */
    public static final String SKIP_NODE = RESERVED_PREFIX + "skip_node";

    /**
     * Process-instance-scoped pin for an operator {@code retry-incident} repair on a held node
     * ({@code __sq_retry_<sanitizedNodeId>}). Set by {@code RetryIncidentCommandHandler} before
     * requeueing the hold job and consumed once when that node re-dispatches, so the resulting
     * sandbox-log row is explicitly stamped {@code retry=true} — distinguishing a genuine Quack Logs
     * Retry from a node invoked multiple times normally (loops, function-port sub-processes).
     */
    public static final String RETRY_PREFIX = RESERVED_PREFIX + "retry_";

    /** The retry-pin variable name for {@code nodeId}. */
    public static String retryNodeVariableName(String nodeId) {
        return RETRY_PREFIX + sanitizeToIdentifier(nodeId);
    }

    /**
     * The error-sentinel family, scrubbed from a failed step's scope chain before its dead-letter
     * job is requeued (and a subset of what {@code InheritedErrorSentinelScrubber} strips): a
     * {@code call.activity} child inherits every parent variable, and without the scrub a stale
     * sentinel from the parent's earlier caught failure would phantom-fire the child's first
     * error gate.
     */
    public static final Set<String> ERROR_SENTINELS = Set.of(
            ERROR_CODE, ERROR_MESSAGE, ERROR_KIND, ERROR_EXIT_CODE);

    /**
     * Everything {@code InheritedErrorSentinelScrubber} strips off a freshly started instance:
     * the {@link #ERROR_SENTINELS} family plus the {@link #SKIP_NODE} pin — a skip pinned for the
     * parent's failed step must never short-circuit whichever child node inherits it.
     */
    public static final Set<String> FRESH_INSTANCE_SCRUB = Set.of(
            ERROR_CODE, ERROR_MESSAGE, ERROR_KIND, ERROR_EXIT_CODE, SKIP_NODE);

    /**
     * Whether an {@link #ERROR_CODE} value marks the run as failed.
     *
     * <p>The single definition of "this run failed", shared by the sub-process completion listener and
     * the run-status listener so a run cannot be a failure to one and a success to the other.
     */
    public static boolean isFailureSentinel(Object errorCodeValue) {
        return errorCodeValue != null && !String.valueOf(errorCodeValue).isBlank();
    }

    /** Reserved engine variable name carrying the daemon's startup epoch (mirrors {@code BeRuntimeEpoch}). */
    public static final String BE_EPOCH = RESERVED_PREFIX + "be_epoch";

    /**
 * The <b>exact</b> set of non-PHI variable names permitted to persist to {@code ACT_*_VARIABLE}
 * / {@code ACT_HI_VARINST} (Mapping Invariant #15, REF-ONLY). Any variable NOT in this set and
 * not carrying the {@link #ROUTE_PREFIX} is a PHI-bearing payload leak and is rejected on the
 * trigger/complete path (see {@code RefOnlyVariableGuard}). Because the daemon runs at
 * {@code historyLevel=FULL}, this whitelist protects the history tables too.
 *
 * <p>This is the single source of truth for the audit the control plane documented on
 * {@code EngineTriggerService.complete}: it holds the encrypted {@code studioquackRef}
 * handle, the typed error sentinel + message, and the boot epoch. Execution-local gateway
 * routing tokens ({@code route__<gatewayId>}) and message-correlation pins
 * ({@code __sq_msgcorr_<messageName>}) are additionally allowed via the prefix rule so
 * this exact set stays small and closed.
 */
    public static final Set<String> WHITELIST_FOR_PERSIST = Set.of(
            STUDIOQUACK_REF,        // studioquackRef — the encrypted execution-context handle
            ERROR_CODE,             // __sq_error_code
            ERROR_MESSAGE,          // __sq_error_message
            ERROR_KIND,             // __sq_error_kind
            ERROR_EXIT_CODE,        // __sq_error_exit_code
            BE_EPOCH,               // __sq_be_epoch
            START_CORRELATION_KEY,  // __sq_start_key — a message-started run's correlation key
            SKIP_NODE               // __sq_skip_node — the operator Skip & Continue pin (node id only)
    );

    /**
     * @return true when {@code name} may persist to the engine variable tables: an exact
     * {@link #WHITELIST_FOR_PERSIST} member OR an execution-local {@link #ROUTE_PREFIX} routing
     * token OR an execution-local {@link #MSG_CORR_PREFIX} message-correlation pin OR an
     * execution-local {@link #CALL_BK_PREFIX} call-activity businessKey pin OR a
     * {@link #CALL_FAIL_PREFIX} call-activity failure-propagation pin. Every other
     * key is PHI payload that must stay sealed in {@code execution_context}.
     */
    public static boolean isPersistWhitelisted(String name) {
        if (name == null) {
            return false;
        }
        return WHITELIST_FOR_PERSIST.contains(name) || name.startsWith(ROUTE_PREFIX)
                || name.startsWith(MSG_CORR_PREFIX) || name.startsWith(CALL_BK_PREFIX)
                || name.startsWith(CALL_FAIL_PREFIX) || name.startsWith(RETRY_PREFIX);
    }
}
