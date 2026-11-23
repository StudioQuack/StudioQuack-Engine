package com.studioquack.sqs.sandbox.queue;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * One durable job record serialised into the WAL. Secrets are never WAL-persisted: only the user
 * {@code source} (the node's own code) + non-secret lifecycle fields are written.
 *
 * @param op The log op ({@code submit}/{@code lease}/{@code finish}/{@code cancel}).
 * @param jobId The durable job id.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public record WalRecord(
        String op,
        String jobId,
        String language,
        String source,
        JobState state,
        Integer exitCode,
        String stdout,
        String stderr,
        String errorMsg,
        Map<String, String> outputs,
        Long enqueuedAt,
        Long startedAt,
        Long finishedAt,
        String environmentId,
        String workflowExecutionId) {

    public static final String OP_SUBMIT = "submit";
    public static final String OP_LEASE = "lease";
    public static final String OP_FINISH = "finish";
    public static final String OP_CANCEL = "cancel";

    /** submit record — durable enqueue of a new job, stamped with the install's env id. */
    public static WalRecord submit(String jobId, String language, String source,
                                   String environmentId, long now) {
        return submit(jobId, language, source, environmentId, null, now);
    }

    /**
     * Submit record carrying the owning workflow run id so list-jobs rows can key historic sandbox
     * logs (rows are persisted under {@code workflowExecutionId}, not the per-dispatch job id).
     */
    public static WalRecord submit(String jobId, String language, String source,
                                   String environmentId, String workflowExecutionId, long now) {
        return new WalRecord(OP_SUBMIT, jobId, language, source, JobState.QUEUED,
                null, null, null, null, null, now, null, null, environmentId, workflowExecutionId);
    }

    /** lease record — job handed to a worker (queued → running). */
    public static WalRecord lease(String jobId, long now) {
        return new WalRecord(OP_LEASE, jobId, null, null, JobState.RUNNING,
                null, null, null, null, null, null, now, null, null, null);
    }

    /** finish record — terminal success/failure with captured (redacted) streams + outputs. */
    public static WalRecord finish(String jobId, JobState state, int exitCode, String stdout,
                                   String stderr, String errorMsg, Map<String, String> outputs, long now) {
        return new WalRecord(OP_FINISH, jobId, null, null, state,
                exitCode, stdout, stderr, errorMsg, outputs, null, null, now, null, null);
    }

    /** cancel record — queued/running → cancelled. */
    public static WalRecord cancel(String jobId, String reason, long now) {
        return new WalRecord(OP_CANCEL, jobId, null, null, JobState.CANCELLED,
                null, null, null, reason, null, null, null, now, null, null);
    }
}
