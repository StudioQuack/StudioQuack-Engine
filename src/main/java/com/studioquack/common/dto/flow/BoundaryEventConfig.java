package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for a boundary event attached to a node.
 * Shared fields are stored directly; timer, message, signal, error, and escalation settings live in {@code config}.
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class BoundaryEventConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // =====================================================================
    // Identification
    // =====================================================================

    /** Unique id for the boundary event. Referenced from edge definitions. */
    private String id;

    /**
     * Optional display label for the boundary event.
     * Example: "30 minute timeout", "Cancel message"
     */
    private String label;

    // =====================================================================
    // Core Configuration
    // =====================================================================

    /**
     * Boundary event type.
     * @see BoundaryEventType
     */
    private BoundaryEventType type;

    /**
     * Interrupting behavior (default: true).
     * - true: when the event fires, the activity is cancelled
     * - false: when the event fires, the activity continues
     *
     * ERROR is always interrupting (BPMN constraint).
     */
    private boolean interrupting = true;

    // =====================================================================
    // Type-Specific Configuration (Dynamic Map)
    // =====================================================================

    /**
     * Type-specific configuration parameters.
     * Every type-dependent field sent by the frontend is stored in this map.
     *
     * Example TIMER config:
     * <pre>
     * {
     *   "timerType": "duration",
     *   "timerValue": "PT30M"
     * }
     * </pre>
     *
     * Example MESSAGE config:
     * <pre>
     * {
     *   "messageName": "CancelOrder",
     *   "correlationKey": "=orderId"
     * }
     * </pre>
     */
    private Map<String, Object> config = new HashMap<>();

    // =====================================================================
    // Runtime references (set by the builder; not sent by the frontend)
    // =====================================================================

    /**
     * Target node id taken when the event fires.
     * Resolved at runtime from the edge list.
     * Not sent by the frontend; set by the engine.
     */
    private transient String targetNodeId;

    /**
     * Id of the generated event-catch node.
     */
    private transient String eventCatchNodeId;

    /**
     * Id of the generated gateway node.
     */
    private transient String gatewayNodeId;

    // =====================================================================
    // Config Value Accessors (Type-safe convenience methods)
    // =====================================================================

    /**
     * Reads a String value from the config map.
     *
     * @param key config key
     * @return the value, or null
     */
    public String getConfigString(String key) {
        if (config == null) return null;
        Object value = config.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Reads a String value from the config map, or returns the default when absent.
     *
     * @param key config key
     * @param defaultValue value used when the key is missing
     * @return the value, or {@code defaultValue}
     */
    public String getConfigString(String key, String defaultValue) {
        String value = getConfigString(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Writes a value into the config map.
     *
     * @param key config key
     * @param value value to store
     * @return this (fluent)
     */
    public BoundaryEventConfig putConfig(String key, Object value) {
        if (this.config == null) {
            this.config = new HashMap<>();
        }
        this.config.put(key, value);
        return this;
    }

    /**
     * FE deploy/save JSON sends type-specific fields both flattened and under {@code config}.
     * Fold flattened keys into the config map so getters remain the source of truth.
     */
    @JsonAnySetter
    public void acceptFlattenedConfig(String key, Object value) {
        if (key == null || key.isBlank()) {
            return;
        }
        if (config != null && config.containsKey(key)) {
            return;
        }
        putConfig(key, value);
    }

    // =====================================================================
    // Timer Convenience Accessors
    // =====================================================================

    /**
     * Timer kind: "duration", "date", or "cycle".
     */
    @JsonIgnore
    public String getTimerType() {
        return getConfigString("timerType");
    }

    /**
     * Timer value (ISO 8601):
     * - duration: PT30M, PT1H, P1D
     * - date: 2026-12-31T23:59:59Z
     * - cycle: R/PT5M, R3/PT1H
     */
    @JsonIgnore
    public String getTimerValue() {
        return getConfigString("timerValue");
    }

    /**
     * Returns the timer duration, only when timerType is duration.
     * Kept for backward compatibility and builder convenience.
     */
    @JsonIgnore
    public String getTimerDuration() {
        if ("duration".equals(getTimerType())) {
            return getTimerValue();
        }
        return null;
    }

    /**
     * Returns the timer date, only when timerType is date.
     */
    @JsonIgnore
    public String getTimerDate() {
        if ("date".equals(getTimerType())) {
            return getTimerValue();
        }
        return null;
    }

    /**
     * Returns the timer cycle, only when timerType is cycle.
     */
    @JsonIgnore
    public String getTimerCycle() {
        if ("cycle".equals(getTimerType())) {
            return getTimerValue();
        }
        return null;
    }

    // =====================================================================
    // Message Convenience Accessors
    // =====================================================================

    @JsonIgnore
    public String getMessageName() {
        return getConfigString("messageName");
    }

    @JsonIgnore
    public String getMessageCorrelationKey() {
        return getConfigString("correlationKey");
    }

    // =====================================================================
    // Signal Convenience Accessors
    // =====================================================================

    @JsonIgnore
    public String getSignalName() {
        return getConfigString("signalName");
    }

    // =====================================================================
    // Error Convenience Accessors
    // =====================================================================

    @JsonIgnore
    public String getErrorCode() {
        return getConfigString("errorCode");
    }

    @JsonIgnore
    public String getErrorMessage() {
        return getConfigString("errorMessage");
    }

    // =====================================================================
    // Escalation Convenience Accessors
    // =====================================================================

    @JsonIgnore
    public String getEscalationCode() {
        return getConfigString("escalationCode");
    }

    // =====================================================================
    // Boundary Event Types
    // =====================================================================

    /**
     * Supported boundary event types.
     *
     * COMPENSATION/CONDITIONAL deliberately absent: the engine has no transaction/compensation
     * ecosystem (audit M2 fails compensate throw/end loudly), and REF-only engine variables give
     * a conditional nothing lawful to evaluate. Both use cases are covered by error boundaries
     * (compensating handler node) and message/signal boundaries.
     */
    public enum BoundaryEventType {
        /**
         * Timer boundary event
         * config: { timerType: "duration"|"date"|"cycle", timerValue: "ISO8601" }
         */
        TIMER,

        /**
         * Message boundary event
         * config: { messageName: "...", correlationKey: "..." } — correlationKey optional
         */
        MESSAGE,

        /**
         * Signal boundary event
         * config: { signalName: "..." }
         */
        SIGNAL,

        /**
         * Error boundary event (interrupting only)
         * config: { errorCode: "...", errorMessage: "..." }
         */
        ERROR,

        /**
         * Escalation boundary event
         * config: { escalationCode: "..." }
         */
        ESCALATION
    }

    // =====================================================================
    // Factory Methods (Fluent API)
    // =====================================================================

    /**
     * Builds a timer boundary event from a duration.
     */
    public static BoundaryEventConfig timer(String duration) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.TIMER);
        cfg.putConfig("timerType", "duration");
        cfg.putConfig("timerValue", duration);
        return cfg;
    }

    /**
     * Builds a timer boundary event from a specific date.
     */
    public static BoundaryEventConfig timerWithDate(String date) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.TIMER);
        cfg.putConfig("timerType", "date");
        cfg.putConfig("timerValue", date);
        return cfg;
    }

    /**
     * Builds a timer boundary event from a repeating cycle.
     * A cycle is usually non-interrupting.
     */
    public static BoundaryEventConfig timerWithCycle(String cycle) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.TIMER);
        cfg.putConfig("timerType", "cycle");
        cfg.putConfig("timerValue", cycle);
        cfg.setInterrupting(false); // A cycle is usually non-interrupting
        return cfg;
    }

    /**
     * Builds a message boundary event.
     */
    public static BoundaryEventConfig message(String messageName, String correlationKey) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.MESSAGE);
        cfg.putConfig("messageName", messageName);
        cfg.putConfig("correlationKey", correlationKey);
        return cfg;
    }

    /**
     * Builds a signal boundary event.
     */
    public static BoundaryEventConfig signal(String signalName) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.SIGNAL);
        cfg.putConfig("signalName", signalName);
        return cfg;
    }

    /**
     * Builds an error boundary event (always interrupting).
     */
    public static BoundaryEventConfig error(String errorCode) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.ERROR);
        cfg.putConfig("errorCode", errorCode);
        cfg.setInterrupting(true);
        return cfg;
    }

    /**
     * Builds an escalation boundary event.
     */
    public static BoundaryEventConfig escalation(String escalationCode) {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setType(BoundaryEventType.ESCALATION);
        cfg.putConfig("escalationCode", escalationCode);
        return cfg;
    }

    // =====================================================================
    // Fluent Configuration Methods
    // =====================================================================

    /**
     * Marks the boundary as non-interrupting.
     * Ignored for ERROR, which must stay interrupting.
     */
    public BoundaryEventConfig nonInterrupting() {
        if (this.type != BoundaryEventType.ERROR) {
            this.interrupting = false;
        }
        return this;
    }

    /**
     * Sets the boundary event id.
     */
    public BoundaryEventConfig withId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the boundary event label.
     */
    public BoundaryEventConfig withLabel(String label) {
        this.label = label;
        return this;
    }

    // =====================================================================
    // Validation
    // =====================================================================

    /**
     * Checks whether the configuration is valid.
     */
    public ValidationResult validate() {
        if (id == null || id.isBlank()) {
            return ValidationResult.error("id is required for boundary event");
        }

        if (type == null) {
            return ValidationResult.error("type is required");
        }

        switch (type) {
            case TIMER:
                String timerType = getTimerType();
                String timerValue = getTimerValue();
                if (timerType == null || timerType.isBlank()) {
                    return ValidationResult.error("Timer event requires config.timerType (duration, date, or cycle)");
                }
                if (!"duration".equals(timerType) && !"date".equals(timerType) && !"cycle".equals(timerType)) {
                    return ValidationResult.error("Timer timerType must be 'duration', 'date', or 'cycle'");
                }
                if (timerValue == null || timerValue.isBlank()) {
                    return ValidationResult.error("Timer event requires config.timerValue");
                }
                if ("cycle".equals(timerType) && interrupting) {
                    return ValidationResult.warning("Timer cycle is typically used with non-interrupting boundary");
                }
                break;
            case MESSAGE:
                if (getMessageName() == null || getMessageName().isBlank()) {
                    return ValidationResult.error("Message event requires config.messageName");
                }
                // correlationKey is optional: blank correlates on the run business key only;
                // an authored key is pinned at park time for shared-key (cross-flow) correlation
                break;
            case SIGNAL:
                if (getSignalName() == null || getSignalName().isBlank()) {
                    return ValidationResult.error("Signal event requires config.signalName");
                }
                break;
            case ERROR:
                // blank/missing errorCode is a catch-all; BPMN still requires interrupting
                if (!interrupting) {
                    return ValidationResult.error("Error boundary event must be interrupting (BPMN engine limitation)");
                }
                break;
            case ESCALATION:
                // escalationCode is optional
                break;
        }


        return ValidationResult.ok();
    }

    /**
     * Validation outcome.
     */
    @Data
    public static class ValidationResult {
        private final boolean valid;
        private final boolean warning;
        private final String message;

        private ValidationResult(boolean valid, boolean warning, String message) {
            this.valid = valid;
            this.warning = warning;
            this.message = message;
        }

        public static ValidationResult ok() {
            return new ValidationResult(true, false, null);
        }

        public static ValidationResult warning(String message) {
            return new ValidationResult(true, true, message);
        }

        public static ValidationResult error(String message) {
            return new ValidationResult(false, false, message);
        }

        public boolean isValid() {
            return valid;
        }

        public boolean hasWarning() {
            return warning;
        }
    }
}
