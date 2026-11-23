package com.studioquack.sqs.runtime;

import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.runtime.Job;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * surfaces Activiti-native <b>incidents</b> (dead-letter jobs, {@code ACT_RU_DEADLETTER_JOB})
 * as a read model for monitoring + the repair handlers, with every free-text field
 * <b>redacted through {@link Redactor} BEFORE it can be surfaced/persisted/logged</b> (the daemon must
 * never store a plaintext error payload).
 *
 * <p>An incident carries the failed job's {@code jobId} (the retry/repair selector), its
 * {@code processInstanceId} (the stable repair identity — never an incidentKey), the redacted
 * exception message + stacktrace, and a best-effort typed {@code errorCode} parsed from the message
 * (a leading {@code CODE:} / {@code CODE } token, as raised by {@code EngineTriggerService.fail} /
 * {@code studioquack.fail}). Reads go through {@link SqsEngine} only, so the single-tenant ArchUnit
 * rule (no raw Activiti service outside {@code engine}) stays green.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IncidentService {

    /** Leading uppercase token treated as the typed error code (e.g. {@code SANDBOX_FAILED: …}). */
    private static final Pattern LEADING_CODE = Pattern.compile("^\\s*([A-Z][A-Z0-9_]{2,})\\b");

    private final SqsEngine sqsEngine;
    private final Redactor redactor;

    /** Open incidents for a single run, addressed by process-instance identity (NOT incidentKey). */
    public List<IncidentView> listForProcessInstance(String processInstanceId) {
        if (processInstanceId == null || processInstanceId.isBlank()) {
            return List.of();
        }
        return toViews(sqsEngine.deadLetterJobsForProcessInstance(processInstanceId).list());
    }

    /** @return the number of open incidents for a run (cheap count, no redaction). */
    public long countForProcessInstance(String processInstanceId) {
        if (processInstanceId == null || processInstanceId.isBlank()) {
            return 0;
        }
        return sqsEngine.deadLetterJobsForProcessInstance(processInstanceId).count();
    }

    private List<IncidentView> toViews(List<Job> jobs) {
        List<IncidentView> views = new ArrayList<>(jobs.size());
        for (Job job : jobs) {
            views.add(toView(job));
        }
        return views;
    }

    private IncidentView toView(Job job) {
        // Redact BEFORE the message/stacktrace are ever returned to a caller (which may log/persist).
        String message = redactor.redact(job.getExceptionMessage());
        String stacktrace = redactor.redact(sqsEngine.deadLetterJobStacktrace(job.getId()));
        String errorCode = extractCode(message);
        return new IncidentView(
                job.getId(),
                job.getProcessInstanceId(),
                job.getExecutionId(),
                job.getProcessDefinitionId(),
                errorCode,
                message,
                stacktrace,
                job.getRetries());
    }

    /** Best-effort typed code from a redacted message; {@code null} when the message carries none. */
    private String extractCode(String redactedMessage) {
        if (redactedMessage == null) {
            return null;
        }
        Matcher m = LEADING_CODE.matcher(redactedMessage);
        return m.find() ? m.group(1) : null;
    }

    /**
 * A redacted incident read model. Every string here is already redacted; it is safe to log,
 * ship to the control plane, or persist. Repairs address the incident by {@link #processInstanceId} (and, for
 * retry, the {@link #jobId}).
 *
 * @param jobId the dead-letter job id (retry/delete selector)
 * @param processInstanceId the run identity used by every repair handler (never an incidentKey)
 * @param executionId the execution the failed job belonged to
 * @param processDefinitionId the definition of the failed run
 * @param errorCode best-effort typed code parsed from the message, or {@code null}
 * @param message redacted exception message
 * @param stacktrace redacted exception stacktrace (may be {@code null})
 * @param retries remaining retries (0 for a dead-letter)
 */
    public record IncidentView(String jobId,
                               String processInstanceId,
                               String executionId,
                               String processDefinitionId,
                               String errorCode,
                               String message,
                               String stacktrace,
                               int retries) {
    }
}
