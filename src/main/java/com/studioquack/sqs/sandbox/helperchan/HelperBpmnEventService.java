package com.studioquack.sqs.sandbox.helperchan;

import com.studioquack.sqs.command.MessageCorrelationService;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.runtime.Execution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Shared BPMN throw seam for helper-channel {@code POST /escalate|/signal|/message} and for a
 * later stdout fold of {@code sqEvent:::{"kind":"escalate"|"signal"|"message",..}} in {@code
 * PodmanSandboxExecution.drainStdoutEvents}. Both callers resolve a {@link HelperJob} first; this
 * bean never looks up a bearer.
 */
@Slf4j
@Component
public class HelperBpmnEventService {

    private final SqsEngine engine;
    private final MessageCorrelationService messageCorrelation;
    private final Redactor redactor;

    @Autowired
    public HelperBpmnEventService(@Nullable SqsEngine engine,
                                  @Nullable MessageCorrelationService messageCorrelation,
                                  @Nullable Redactor redactor) {
        this.engine = engine;
        this.messageCorrelation = messageCorrelation;
        this.redactor = redactor;
    }

    /** Outcome the router maps to an HTTP-shaped helper response. */
    public record EventOutcome(boolean ok, int httpStatus, Map<String, Object> body) {

        public static EventOutcome success() {
            return new EventOutcome(true, 200, Map.of("ok", true));
        }

        public static EventOutcome delivered(int n) {
            return new EventOutcome(true, 200, Map.of("ok", true, "delivered", n));
        }

        public static EventOutcome notDelivered(String reason) {
            return new EventOutcome(false, 200, Map.of("ok", false, "reason", reason));
        }

        public static EventOutcome badRequest(String error) {
            return new EventOutcome(false, 400, Map.of("error", error));
        }

        public static EventOutcome unavailable() {
            return new EventOutcome(false, 503, Map.of("error", "unavailable"));
        }
    }

    /** {@code POST /escalate} and stdout {@code kind:escalate}. */
    public EventOutcome escalate(HelperJob job, String escalationCode, String message) {
        String code = trim(escalationCode);
        if (code.isEmpty()) {
            return EventOutcome.badRequest("escalationCode is required");
        }
        if (engine == null) {
            return EventOutcome.unavailable();
        }
        String reservedSignal = EngineVariableNames.ESCALATION_SIGNAL_PREFIX + code;
        try {
            fireInstanceScopedSignal(job, reservedSignal);
        } catch (RuntimeException e) {
            log.debug("helper-bpmn: escalate unavailable job={}: {}", job.id(), e.toString());
            return EventOutcome.unavailable();
        }
        if (message != null && !message.isBlank()) {
            String safe = redactor == null ? "" : redactor.redact(message);
            if (!safe.isBlank()) {
                log.info("helper-bpmn: escalate job={} code={} message={}", job.id(), code, safe);
            } else {
                log.info("helper-bpmn: escalate job={} code={}", job.id(), code);
            }
        } else {
            log.info("helper-bpmn: escalate job={} code={}", job.id(), code);
        }
        return EventOutcome.success();
    }

    public EventOutcome escalate(HelperJob job, HelperEventRequest req) {
        return escalate(job, req == null ? null : req.escalationCode(),
                req == null ? null : req.message());
    }

    /** {@code POST /signal} and stdout {@code kind:signal}. Reserved {@code __sq_*} names are refused. */
    public EventOutcome signal(HelperJob job, String signalName, String scope) {
        String name = trim(signalName);
        if (name.isEmpty()) {
            return EventOutcome.badRequest("signalName is required");
        }
        if (name.startsWith(EngineVariableNames.RESERVED_PREFIX)) {
            return EventOutcome.badRequest("reserved");
        }
        String resolvedScope = trim(scope);
        if (resolvedScope.isEmpty()) {
            resolvedScope = "global";
        }
        if (engine == null) {
            return EventOutcome.unavailable();
        }
        try {
            if ("global".equalsIgnoreCase(resolvedScope)) {
                engine.signalEventReceived(name, Map.of());
            } else if ("instance".equalsIgnoreCase(resolvedScope)) {
                fireInstanceScopedSignal(job, name);
            } else {
                return EventOutcome.badRequest("unknown scope");
            }
        } catch (RuntimeException e) {
            log.debug("helper-bpmn: signal unavailable job={}: {}", job.id(), e.toString());
            return EventOutcome.unavailable();
        }
        return EventOutcome.success();
    }

    public EventOutcome signal(HelperJob job, HelperEventRequest req) {
        return signal(job, req == null ? null : req.signalName(),
                req == null ? null : req.scope());
    }

    /** {@code POST /message} and stdout {@code kind:message}. Correlates by business key only. */
    public EventOutcome message(HelperJob job, String messageName, String correlationKey) {
        String name = trim(messageName);
        if (name.isEmpty()) {
            return EventOutcome.badRequest("messageName is required");
        }
        String key = trim(correlationKey);
        if (key.isEmpty()) {
            key = str(job.executionContext().get("workflowExecutionId"));
        }
        if (key.isEmpty()) {
            return EventOutcome.badRequest("correlationKey is required");
        }
        if (messageCorrelation == null) {
            return EventOutcome.unavailable();
        }
        try {
            MessageCorrelationService.MessageCorrelationOutcome outcome =
                    messageCorrelation.correlate(name, key, Map.of());
            if (outcome != null && outcome.correlated()) {
                return EventOutcome.delivered(1);
            }
            return EventOutcome.notDelivered("not_correlated");
        } catch (IllegalArgumentException bad) {
            return EventOutcome.notDelivered("bad_request");
        } catch (RuntimeException e) {
            log.debug("helper-bpmn: message unavailable job={}: {}", job.id(), e.toString());
            return EventOutcome.notDelivered("unavailable");
        }
    }

    public EventOutcome message(HelperJob job, HelperEventRequest req) {
        return message(job, req == null ? null : req.messageName(),
                req == null ? null : req.correlationKey());
    }

    /**
     * Instance-scoped fan-out: resolve the job's PI from {@code workflowExecutionId}, then
     * {@code signalEventReceived} each subscriber on that PI only (same as
     * {@code EscalationSenderExecutionListener}).
     */
    private void fireInstanceScopedSignal(HelperJob job, String signalName) {
        String processInstanceId = resolveJobProcessInstanceId(job);
        List<Execution> subscribers = engine.findSignalEventExecutions(signalName);
        if (subscribers == null || processInstanceId == null || processInstanceId.isBlank()) {
            return;
        }
        for (Execution subscriber : subscribers) {
            if (subscriber != null && processInstanceId.equals(subscriber.getProcessInstanceId())) {
                engine.signalEventReceived(signalName, subscriber.getId());
            }
        }
    }

    private String resolveJobProcessInstanceId(HelperJob job) {
        String wfeId = str(job.executionContext().get("workflowExecutionId"));
        if (wfeId.isEmpty()) {
            return null;
        }
        String resolved = engine.resolveProcessInstanceId(wfeId);
        return (resolved == null || resolved.isBlank()) ? wfeId : resolved;
    }

    private static String trim(String raw) {
        return raw == null ? "" : raw.trim();
    }

    private static String str(Object raw) {
        return raw == null ? "" : String.valueOf(raw).trim();
    }
}
