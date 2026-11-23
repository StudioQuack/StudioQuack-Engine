package com.studioquack.sqs.engine.runtime;

import com.studioquack.common.dto.flow.BoundaryEventConfig;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a boundary event hands to the node it triggers, keyed by the same short codes the designer
 * declares on the boundary port's output schema ({@code boundaryEventSchemas.ts}). A mapping drawn
 * from a boundary port reads these keys, so the two lists must not drift.
 *
 * <p><b>Where the values go.</b> Not onto the engine: this is node payload, and only the encrypted
 * {@code studioquackRef} plus the small non-PHI tokens of {@link EngineVariableNames} may reach
 * {@code ACT_*_VARIABLE} (Mapping Invariant 15). The payload is sealed into the
 * {@code execution_context} store as an output addressed to the handler node — the same lane a
 * sandbox step's outputs travel — and the handler reads it through
 * {@code loadInputsTargetingNode}. The error tokens are the exception: the error gate already put
 * them on the engine as non-PHI sentinels, so this only copies them into the payload.
 *
 * <p><b>No free-form message/signal payload.</b> Every message/signal ingress funnels through
 * {@code MessageCorrelationService}, which rejects a non-whitelisted key before it can reach the
 * engine. There is therefore no authored payload for an inbound event to carry, and a field the
 * runtime cannot populate is not promised in the schema.
 */
public final class BoundaryEventVariables {

    private BoundaryEventVariables() {
    }

    /** Which boundary fired — the designer's boundary event id, unique across the workflow. */
    public static final String EVENT_ID = "eventId";
    /** Its authored label, so two boundaries feeding one handler stay tellable apart by a human. */
    public static final String EVENT_LABEL = "eventLabel";
    /** ISO-8601 instant the boundary fired. */
    public static final String FIRED_AT = "firedAt";

    public static final String TIMER_TYPE = "timerType";
    public static final String TIMER_VALUE = "timerValue";
    public static final String MESSAGE_NAME = "messageName";
    public static final String CORRELATION_KEY = "correlationKey";
    public static final String SIGNAL_NAME = "signalName";
    public static final String ESCALATION_CODE = "escalationCode";
    public static final String ERROR_CODE = "errorCode";
    public static final String ERROR_MESSAGE = "errorMessage";
    public static final String ERROR_SOURCE = "errorSource";
    /** Coarse classification mirroring {@link EngineVariableNames#ERROR_KIND} (SCRIPT/CONTAINER/INFRA/ENGINE). */
    public static final String ERROR_KIND = "errorKind";
    /** The sandbox process exit code mirroring {@link EngineVariableNames#ERROR_EXIT_CODE}, when one exists. */
    public static final String EXIT_CODE = "exitCode";

    /**
     * Builds the payload for one fired boundary.
     *
     * @param cfg            the boundary's authored config (its type decides the type-specific keys)
     * @param hostNodeId     the node the boundary is attached to — where an error came from
     * @param correlationKey the run's business key, which is what a message correlates on
     * @param errorCode      {@link EngineVariableNames#ERROR_CODE} as read off the execution, or null
     * @param errorMessage   {@link EngineVariableNames#ERROR_MESSAGE} as read off the execution, or null
     * @param errorKind      {@link EngineVariableNames#ERROR_KIND} as read off the execution, or null
     * @param exitCode       {@link EngineVariableNames#ERROR_EXIT_CODE} as read off the execution, or null
     */
    public static Map<String, Object> payload(BoundaryEventConfig cfg, String hostNodeId, String correlationKey,
                                              String errorCode, String errorMessage, String errorKind,
                                              String exitCode, Instant firedAt) {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (cfg == null) {
            return payload;
        }
        payload.put(EVENT_ID, StringUtils.trimToEmpty(cfg.getId()));
        payload.put(EVENT_LABEL, StringUtils.defaultIfBlank(cfg.getLabel(), StringUtils.trimToEmpty(cfg.getId())));
        payload.put(FIRED_AT, (firedAt == null ? Instant.now() : firedAt).toString());
        if (cfg.getType() == null) {
            return payload;
        }
        switch (cfg.getType()) {
            case TIMER -> {
                // Blank timerType is authored as a duration, matching what the generator deploys.
                payload.put(TIMER_TYPE, StringUtils.defaultIfBlank(cfg.getTimerType(), "duration"));
                payload.put(TIMER_VALUE, StringUtils.trimToEmpty(cfg.getTimerValue()));
            }
            case MESSAGE -> {
                payload.put(MESSAGE_NAME, StringUtils.trimToEmpty(cfg.getMessageName()));
                payload.put(CORRELATION_KEY, StringUtils.trimToEmpty(correlationKey));
            }
            case SIGNAL -> payload.put(SIGNAL_NAME, StringUtils.trimToEmpty(cfg.getSignalName()));
            // The boundary's own configured code: which escalation this boundary catches. The code
            // that was actually thrown selects the boundary (reserved-signal match) but is not
            // reported back — same contract as SIGNAL carrying the subscribed name.
            case ESCALATION -> payload.put(ESCALATION_CODE, StringUtils.trimToEmpty(cfg.getEscalationCode()));
            case ERROR -> {
                // The gate's sentinel is the code that actually fired; the config's code is what the
                // boundary was declared to match, and is the fallback when the sentinel is unreadable.
                payload.put(ERROR_CODE, StringUtils.defaultIfBlank(errorCode, StringUtils.trimToEmpty(cfg.getErrorCode())));
                payload.put(ERROR_MESSAGE, StringUtils.defaultIfBlank(errorMessage, StringUtils.trimToEmpty(cfg.getErrorMessage())));
                payload.put(ERROR_SOURCE, StringUtils.trimToEmpty(hostNodeId));
                // Structured classification: always present, defaulting to the
                // engine-side "unknown" reading rather than a null key silently missing from the map.
                payload.put(ERROR_KIND, StringUtils.trimToEmpty(errorKind));
                payload.put(EXIT_CODE, StringUtils.trimToEmpty(exitCode));
            }
            default -> {
                // A type this tier does not emit never fires, so it carries only the common keys.
            }
        }
        return payload;
    }
}
