package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.queue.DurableJobQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Env-ops {@code list-jobs}: reports the most recent sandbox jobs that passed through the daemon's durable
 * queue, newest-first. This is the current-stack equivalent of the old Go dashboard's combined Queue /
 * Instances view (both read the daemon's {@code queue} array) — the FE Runtime "Instances" tab renders it as
 * an audit list.
 */
@Slf4j
@Component
public class ListJobsCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "list-jobs";

    /**
     * Fixed page size — the FE opens on the last 25 (parity with the old dashboard's capped view).
     */
    static final int DEFAULT_LIMIT = 25;

    /**
     * Bound on the error string carried per row so a chatty failure can't bloat the channel payload.
     */
    private static final int MAX_ERROR_CHARS = 500;

    private final ObjectMapper objectMapper;

    @Nullable
    private final DurableJobQueue jobQueue;

    @Autowired
    public ListJobsCommandHandler(ObjectMapper objectMapper, @Nullable DurableJobQueue jobQueue) {
        this.objectMapper = objectMapper;
        this.jobQueue = jobQueue;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            List<DurableJobQueue.Job> all = jobQueue == null ? List.of() : jobQueue.snapshot();
            List<Map<String, Object>> rows = all.stream()
                    // Newest-first by enqueue time (tie-break on finish so completed rows settle above equal-enqueue)
                    .sorted(Comparator
                            .comparingLong((DurableJobQueue.Job j) -> j.enqueuedAt).reversed()
                            .thenComparing(Comparator.comparingLong((DurableJobQueue.Job j) -> j.finishedAt).reversed()))
                    .limit(DEFAULT_LIMIT)
                    .map(this::toRow)
                    .toList();

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("reportedAt", Instant.now().toString());
            body.put("limit", DEFAULT_LIMIT);
            body.put("total", all.size());
            body.put("jobs", rows);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("list-jobs failed: " + message(e));
        }
    }

    /**
     * Maps one queue job to a lean audit row (no stdout/stderr; timestamps as ISO-8601, null when unset).
     */
    private Map<String, Object> toRow(DurableJobQueue.Job job) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("jobId", job.id);
        // Historic sandbox logs are persisted under the workflow run id, not the per-dispatch
        // job id — the Instances-tab log viewer keys off this field (null on legacy WAL rows).
        row.put("workflowExecutionId", job.workflowExecutionId);
        row.put("language", job.language);
        row.put("state", job.state == null ? null : job.state.wire());
        // exitCode is only meaningful once the job reached a terminal state.
        row.put("exitCode", job.state != null && job.state.terminal() ? job.exitCode : null);
        row.put("enqueuedAt", iso(job.enqueuedAt));
        row.put("startedAt", iso(job.startedAt));
        row.put("finishedAt", iso(job.finishedAt));
        row.put("runtimeMs", runtimeMs(job));
        row.put("error", truncate(job.errorMsg));
        return row;
    }

    /**
     * Wall time in ms: finished−started when both set; else elapsed since start for a live job; else 0.
     */
    private static long runtimeMs(DurableJobQueue.Job job) {
        if (job.startedAt <= 0) {
            return 0L;
        }
        long end = job.finishedAt > 0 ? job.finishedAt : System.currentTimeMillis();
        long delta = end - job.startedAt;
        return Math.max(0L, delta);
    }

    @Nullable
    private static String iso(long epochMillis) {
        return epochMillis > 0 ? Instant.ofEpochMilli(epochMillis).toString() : null;
    }

    @Nullable
    private static String truncate(@Nullable String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        return s.length() <= MAX_ERROR_CHARS ? s : s.substring(0, MAX_ERROR_CHARS) + "…";
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
