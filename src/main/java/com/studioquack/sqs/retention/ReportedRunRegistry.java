package com.studioquack.sqs.retention;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Records run ids whose logs have been confirmed stored, so retention deletes the local copy only
 * from that set. Rows live in the local database; the no-arg constructor is in-memory for tests.
 */
@Slf4j
@Component
public class ReportedRunRegistry {

    private static final String TABLE = "sqs_reported_run";

    /** In-memory (test) cap; eldest insertions evicted first. */
    private static final int MAX_TRACKED = 100_000;

    private final JdbcTemplate jdbc;
    private final Map<String, Instant> memory = new LinkedHashMap<>(1024, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Instant> eldest) {
            return size() > MAX_TRACKED;
        }
    };

    @Autowired
    public ReportedRunRegistry(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** In-memory form for focused unit tests (no persistence). */
    public ReportedRunRegistry() {
        this.jdbc = null;
    }

    @PostConstruct
    void init() {
        if (jdbc == null) {
            return;
        }
        jdbc.execute("create table if not exists " + TABLE
                + " (run_id varchar(128) primary key, reported_at timestamp not null)");
    }

    /** Records that BE confirmed this run's terminal status and logs. */
    public void markReported(String workflowExecutionId) {
        markReported(workflowExecutionId, Instant.now());
    }

    /** As {@link #markReported(String)} with an explicit time (tests age a run with this). */
    synchronized void markReported(String workflowExecutionId, Instant reportedAt) {
        if (workflowExecutionId == null || workflowExecutionId.isBlank()) {
            return;
        }
        if (jdbc == null) {
            memory.put(workflowExecutionId, reportedAt);
            return;
        }
        try {
            jdbc.update("merge into " + TABLE + " (run_id, reported_at) key (run_id) values (?, ?)",
                    workflowExecutionId, Timestamp.from(reportedAt));
        } catch (RuntimeException e) {
            // A lost mark only keeps the run's logs longer — never deletes anything early.
            log.warn("reported-runs: could not record run={}: {}", workflowExecutionId, e.toString());
        }
    }

    /**
     * Acked runs reported strictly before {@code cutoff}, oldest first, at most {@code limit}.
     * A run is reported at its terminal moment, so anything younger cannot be purge-eligible yet.
     */
    public synchronized List<String> reportedBefore(Instant cutoff, int limit) {
        if (jdbc == null) {
            List<String> out = new ArrayList<>();
            for (Map.Entry<String, Instant> e : memory.entrySet()) {
                if (e.getValue().isBefore(cutoff)) {
                    out.add(e.getKey());
                    if (out.size() >= limit) {
                        break;
                    }
                }
            }
            return out;
        }
        return jdbc.queryForList("select run_id from " + TABLE + " where reported_at < ? order by reported_at limit ?",
                String.class, Timestamp.from(cutoff), limit);
    }

    public synchronized boolean contains(String workflowExecutionId) {
        if (workflowExecutionId == null) {
            return false;
        }
        if (jdbc == null) {
            return memory.containsKey(workflowExecutionId);
        }
        Integer n = jdbc.queryForObject("select count(*) from " + TABLE + " where run_id = ?",
                Integer.class, workflowExecutionId);
        return n != null && n > 0;
    }

    /** Drop ids that have been purged. */
    public synchronized void forget(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        if (jdbc == null) {
            ids.forEach(memory::remove);
            return;
        }
        List<String> list = new ArrayList<>(ids);
        jdbc.update("delete from " + TABLE + " where run_id in (" + String.join(", ", Collections.nCopies(list.size(), "?")) + ")",
                list.toArray());
    }

    public synchronized boolean isEmpty() {
        if (jdbc == null) {
            return memory.isEmpty();
        }
        Integer n = jdbc.queryForObject("select count(*) from " + TABLE, Integer.class);
        return n == null || n == 0;
    }
}
