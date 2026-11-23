package com.studioquack.sqs.sandbox.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The durable job queue: an in-memory view backed by the {@link WriteAheadLog}. A job is derived
 * from its WAL records — it never lives only in memory, so the queue survives a daemon restart.
 */
@Slf4j
@Component
public class DurableJobQueue {

    /** Atomic queue counts consumed by host telemetry. */
    public record Counts(int running, int queued, int total) {
    }

    /** In-memory derived view of a job (the fold of its WAL records). */
    public static final class Job {
        public String id;
        public String language;
        public String source;
        public JobState state;
        public int exitCode;
        public String stdout;
        public String stderr;
        public String errorMsg;
        public Map<String, String> outputs;
        public long enqueuedAt;
        public long startedAt;
        public long finishedAt;
        /** Enrolled env id from submit; null/blank on legacy WAL rows (hidden from list/lease). */
        public String environmentId;
        /** Owning workflow run id from submit; null on legacy WAL rows. Keys historic sandbox logs. */
        public String workflowExecutionId;
    }

    private final ObjectMapper mapper;
    private final Path walPath;
    /** Normalized enrolled env id ({@code ""} when channel not configured). */
    private final String currentEnvironmentId;
    /** Keeps the "leasing without install scoping" warning to one line per boot. */
    private final AtomicBoolean unscopedLeaseWarned = new AtomicBoolean(false);
    private final Map<String, Job> all = new ConcurrentHashMap<>();
    private final Deque<String> readyOrder = new ArrayDeque<>();
    private final Object lock = new Object();
    private volatile WriteAheadLog wal;

    @Autowired
    public DurableJobQueue(ObjectMapper mapper,
                           @Value("${sqs.sandbox.wal-path:}") String walPathProp,
                           @Value("${sqs.channel.env-id:}") String environmentId) {
        this.mapper = mapper;
        this.walPath = resolvePath(walPathProp);
        this.currentEnvironmentId = normalizeEnvId(environmentId);
    }

    private DurableJobQueue(Path walPath, ObjectMapper mapper, String environmentId) {
        this.mapper = mapper;
        this.walPath = walPath;
        this.currentEnvironmentId = normalizeEnvId(environmentId);
    }

    /**
     * Test/embedded factory: open a queue at an explicit WAL path (replays it immediately).
     * Uses a blank env id (fail-closed scoping) — prefer {@link #openAt(Path, ObjectMapper, String)}.
     */
    public static DurableJobQueue openAt(Path walPath, ObjectMapper mapper) throws IOException {
        return openAt(walPath, mapper, "");
    }

    /** Test/embedded factory with an explicit install env id for scoping. */
    public static DurableJobQueue openAt(Path walPath, ObjectMapper mapper, String environmentId)
            throws IOException {
        DurableJobQueue q = new DurableJobQueue(walPath, mapper, environmentId);
        q.open();
        return q;
    }

    @PostConstruct
    void init() {
        try {
            open();
        } catch (IOException e) {
            log.warn("job-queue: could not open WAL at {} ({}); durable queue disabled this boot.",
                    walPath, e.getMessage());
        }
    }

    private void open() throws IOException {
        this.wal = new WriteAheadLog(walPath, mapper);
        List<WalRecord> records = wal.replay(msg -> log.warn("job-queue: {}", msg));
        int requeued = 0;
        synchronized (lock) {
            for (WalRecord r : records) {
                Job job = foldInto(all.computeIfAbsent(r.jobId(), k -> newJob(r)), r);
                all.put(r.jobId(), job);
            }
            // A job left RUNNING by a crash is re-queued for a fresh lease (sweeper owns dedup).
            for (Job job : all.values()) {
                if (job.state == JobState.QUEUED || job.state == JobState.RUNNING) {
                    job.state = JobState.QUEUED;
                    readyOrder.addLast(job.id);
                    requeued++;
                }
            }
        }
        log.info("job-queue: WAL replayed path={} jobs={} requeued={}", walPath, all.size(), requeued);
    }

    @PreDestroy
    void close() {
        WriteAheadLog w = this.wal;
        if (w != null) {
            try {
                w.close();
            } catch (IOException ignore) {
                // best-effort
            }
        }
    }

    /** Durably enqueues a new job (QUEUED). Secrets are never part of {@code source}/the WAL. */
    public String submit(String jobId, String language, String source) {
        return submit(jobId, language, source, null);
    }

    /**
     * As {@link #submit(String, String, String)} plus the owning workflow run id, so
     * list-jobs rows can key historic sandbox logs (persisted under the run id).
     */
    public String submit(String jobId, String language, String source, String workflowExecutionId) {
        long now = System.currentTimeMillis();
        appendSafe(WalRecord.submit(jobId, language, source, currentEnvironmentId,
                workflowExecutionId, now));
        synchronized (lock) {
            Job job = new Job();
            job.id = jobId;
            job.language = language;
            job.source = source;
            job.state = JobState.QUEUED;
            job.enqueuedAt = now;
            job.environmentId = currentEnvironmentId;
            job.workflowExecutionId = workflowExecutionId;
            all.put(jobId, job);
            readyOrder.addLast(jobId);
        }
        return jobId;
    }

    /** Leases the next QUEUED job (FIFO) into RUNNING, or empty when none are ready. */
    public Optional<Job> lease() {
        return lease(null);
    }

    /**
     * Leases the next QUEUED job for a language (any when {@code language} is null).
     * Skips jobs that do not belong to the current install (re-adds to readyOrder, like language
     * mismatch) so a preserved WAL cannot execute prior-install work.
     */
    public Optional<Job> lease(String language) {
        long now = System.currentTimeMillis();
        synchronized (lock) {
            int scanned = readyOrder.size();
            for (int i = 0; i < scanned; i++) {
                String id = readyOrder.pollFirst();
                if (id == null) {
                    break;
                }
                Job job = all.get(id);
                if (job == null || job.state != JobState.QUEUED) {
                    continue;
                }
                // Foreign / legacy rows stay in the deque but are never leased by this install.
                if (!isLeasableByCurrentInstall(job)) {
                    readyOrder.addLast(id);
                    continue;
                }
                if (language != null && !language.equalsIgnoreCase(job.language)) {
                    readyOrder.addLast(id); // not our language — keep it queued
                    continue;
                }
                job.state = JobState.RUNNING;
                job.startedAt = now;
                appendSafe(WalRecord.lease(id, now));
                return Optional.of(job);
            }
        }
        return Optional.empty();
    }

    /**
     * Leases one known job id. Dispatch delegates use this form so concurrent submissions cannot
     * accidentally mark a different FIFO job as RUNNING.
     */
    public Optional<Job> leaseById(String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return Optional.empty();
        }
        long now = System.currentTimeMillis();
        synchronized (lock) {
            Job job = all.get(jobId);
            if (job == null || job.state != JobState.QUEUED) {
                return Optional.empty();
            }
            job.state = JobState.RUNNING;
            job.startedAt = now;
            readyOrder.remove(jobId);
            appendSafe(WalRecord.lease(jobId, now));
            return Optional.of(job);
        }
    }

    /** Terminally finishes a job with its (already-redacted) streams + structured outputs. */
    public void finish(String jobId, JobState state, int exitCode, String stdout, String stderr,
                       String errorMsg, Map<String, String> outputs) {
        long now = System.currentTimeMillis();
        synchronized (lock) {
            Job job = all.get(jobId);
            if (job == null || job.state.terminal()) {
                return; // Idempotent finish.
            }
            job.state = state;
            job.exitCode = exitCode;
            job.stdout = stdout;
            job.stderr = stderr;
            job.errorMsg = errorMsg;
            job.outputs = outputs;
            job.finishedAt = now;
        }
        appendSafe(WalRecord.finish(jobId, state, exitCode, stdout, stderr, errorMsg, outputs, now));
    }

    /** Cancels a queued/running job (idempotent for terminal jobs). */
    public void cancel(String jobId, String reason) {
        long now = System.currentTimeMillis();
        synchronized (lock) {
            Job job = all.get(jobId);
            if (job == null || job.state.terminal()) {
                return;
            }
            job.state = JobState.CANCELLED;
            job.errorMsg = reason;
            job.finishedAt = now;
            readyOrder.remove(jobId);
        }
        appendSafe(WalRecord.cancel(jobId, reason, now));
    }

    /** @return the derived job by id. */
    public Optional<Job> get(String jobId) {
        return Optional.ofNullable(all.get(jobId));
    }

    /**
     * Snapshot of jobs belonging to the current install.
     * Filters the in-memory {@code all} map by env id (no secondary index — map is already
     * fully loaded from the WAL).
     */
    public List<Job> snapshot() {
        synchronized (lock) {
            List<Job> out = new ArrayList<>();
            for (Job j : all.values()) {
                if (belongsToCurrentInstall(j)) {
                    out.add(j);
                }
            }
            return out;
        }
    }

    /** @return number of current-install jobs currently QUEUED (dashboard/test). */
    public int queuedCount() {
        synchronized (lock) {
            return (int) all.values().stream()
                    .filter(this::belongsToCurrentInstall)
                    .filter(j -> j.state == JobState.QUEUED)
                    .count();
        }
    }

    /** @return one consistent running/queued/total snapshot for telemetry (current install only). */
    public Counts counts() {
        synchronized (lock) {
            int running = 0;
            int queued = 0;
            int total = 0;
            for (Job job : all.values()) {
                if (!belongsToCurrentInstall(job)) {
                    continue;
                }
                total++;
                if (job.state == JobState.RUNNING) {
                    running++;
                } else if (job.state == JobState.QUEUED) {
                    queued++;
                }
            }
            return new Counts(running, queued, total);
        }
    }

    /* ---- helpers ---- */

    /**
     * Current env must be non-blank and equal to the job's non-blank environmentId. Blank
     * currentEnvironmentId → false (fail closed; no false attribution).
     */
    private boolean belongsToCurrentInstall(Job j) {
        if (currentEnvironmentId.isEmpty()) {
            return false;
        }
        String jobEnv = normalizeEnvId(j.environmentId);
        return !jobEnv.isEmpty() && currentEnvironmentId.equals(jobEnv);
    }

    /**
     * Leasing must not share the read predicate's fail-closed stance. Hiding rows we cannot attribute
     * is free — a list is only a display.
     */
    private boolean isLeasableByCurrentInstall(Job j) {
        if (!currentEnvironmentId.isEmpty()) {
            return belongsToCurrentInstall(j);
        }
        if (unscopedLeaseWarned.compareAndSet(false, true)) {
            log.warn("sandbox queue: sqs.channel.env-id is not set — leasing only UNSTAMPED jobs "
                    + "so this daemon still executes its own work. Jobs stamped by another install "
                    + "stay queued; set the enrolled environment id to restore full scoping.");
        }
        // With no identity of our own, an unstamped job is one WE submitted this boot, so it is
        // safe to run; anything carrying another install's stamp is still refused.
        return normalizeEnvId(j.environmentId).isEmpty();
    }

    private static String normalizeEnvId(String environmentId) {
        return environmentId == null || environmentId.isBlank() ? "" : environmentId;
    }

    private void appendSafe(WalRecord record) {
        WriteAheadLog w = this.wal;
        if (w == null) {
            return; // WAL disabled this boot — in-memory only
        }
        try {
            w.append(record);
        } catch (IOException e) {
            log.warn("job-queue: WAL append failed op={} job={}: {}", record.op(), record.jobId(), e.getMessage());
        }
    }

    private static Job newJob(WalRecord r) {
        Job j = new Job();
        j.id = r.jobId();
        j.state = JobState.QUEUED;
        return j;
    }

    /** Applies one WAL record onto the derived job (fold). */
    private static Job foldInto(Job j, WalRecord r) {
        if (r.language() != null) {
            j.language = r.language();
        }
        if (r.source() != null) {
            j.source = r.source();
        }
        if (r.state() != null) {
            j.state = r.state();
        }
        if (r.exitCode() != null) {
            j.exitCode = r.exitCode();
        }
        if (r.stdout() != null) {
            j.stdout = r.stdout();
        }
        if (r.stderr() != null) {
            j.stderr = r.stderr();
        }
        if (r.errorMsg() != null) {
            j.errorMsg = r.errorMsg();
        }
        if (r.outputs() != null) {
            j.outputs = r.outputs();
        }
        if (r.enqueuedAt() != null) {
            j.enqueuedAt = r.enqueuedAt();
        }
        if (r.startedAt() != null) {
            j.startedAt = r.startedAt();
        }
        if (r.finishedAt() != null) {
            j.finishedAt = r.finishedAt();
        }
        // Fold env id from submit only; later ops leave it null so we keep the submit value.
        if (r.environmentId() != null) {
            j.environmentId = r.environmentId();
        }
        // Same submit-only rule for the owning run id.
        if (r.workflowExecutionId() != null) {
            j.workflowExecutionId = r.workflowExecutionId();
        }
        return j;
    }

    private static Path resolvePath(String prop) {
        if (prop != null && !prop.isBlank()) {
            return Path.of(prop);
        }
        String dataDir = System.getenv().getOrDefault("SQS_DATA_DIR",
                System.getProperty("java.io.tmpdir", "."));
        return Path.of(dataDir, "sqs-jobs.wal");
    }
}
