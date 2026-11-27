package com.studioquack.sqs.retention;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Keyset-paginated, DB-agnostic JDBC queries for selecting purge-eligible workflow runs before a
 * cutoff instant while guaranteeing live/running/waiting workflows are NEVER touched.
 *
 * <p>Covers all three categories of non-live log data that accumulate on an engine:
 * <ol>
 *   <li><b>Dead-letter-held failed runs</b> ({@link #findAgedDeadLetterHeldRuns}): root workflows
 *       that failed and parked at {@code hold_<nodeId>} on {@code ACT_RU_DEADLETTER_JOB} with no
 *       active executable job, timer, user task, or message catch and no activity at/after
 *       {@code olderThan}. Monitoring already treats these as {@code FAILED}.</li>
 *   <li><b>Terminated / zombie-history runs</b> ({@link #nextTerminatedRunBatch}): runs whose
 *       {@code ACT_HI_PROCINST} rows all finished strictly before {@code to} (or have
 *       {@code END_TIME_ IS NULL} with zero matching {@code ACT_RU_EXECUTION} runtime rows),
 *       excluding any child sub-PI whose parent still has live runtime or post-cutoff end.</li>
 *   <li><b>Orphaned / history-less log runs</b> ({@link #nextOrphanedLogRunBatch}): rows in
 *       {@code execution_context} or {@code sandbox_log} created before {@code olderThan} whose
 *       {@code workflow_execution_id} has no {@code ACT_HI_PROCINST} row and no live
 *       {@code ACT_RU_EXECUTION} row (e.g. startup failures or trial runs).</li>
 * </ol>
 */
@Slf4j
public final class TerminatedRunQuerySupport {

    /** One dead-letter-held failed process instance eligible to be cancelled and purged. */
    public record HeldFailedRun(String processInstanceId, String businessKey) {
    }

    private TerminatedRunQuerySupport() {
    }

    /**
     * Finds aged FAILED runs parked solely on {@code ACT_RU_DEADLETTER_JOB} before {@code olderThan}
     * (no active executable job, timer, user task, or event subscription, and no log row at/after
     * {@code olderThan}).
     */
    public static List<HeldFailedRun> findAgedDeadLetterHeldRuns(JdbcTemplate jdbc, Instant olderThan, int batchSize) {
        if (jdbc == null || olderThan == null) {
            return List.of();
        }
        int limit = Math.max(1, batchSize);
        Timestamp cutoff = Timestamp.from(olderThan);
        String sql = "SELECT p.PROC_INST_ID_, p.BUSINESS_KEY_ FROM ACT_HI_PROCINST p "
                + "WHERE p.END_TIME_ IS NULL "
                + "AND p.START_TIME_ < ? "
                + "AND EXISTS (SELECT 1 FROM ACT_RU_DEADLETTER_JOB d WHERE d.PROCESS_INSTANCE_ID_ = p.PROC_INST_ID_) "
                + "AND NOT EXISTS (SELECT 1 FROM ACT_RU_JOB j WHERE j.PROCESS_INSTANCE_ID_ = p.PROC_INST_ID_) "
                + "AND NOT EXISTS (SELECT 1 FROM ACT_RU_TIMER_JOB t WHERE t.PROCESS_INSTANCE_ID_ = p.PROC_INST_ID_) "
                + "AND NOT EXISTS (SELECT 1 FROM ACT_RU_TASK tk WHERE tk.PROC_INST_ID_ = p.PROC_INST_ID_) "
                + "AND NOT EXISTS (SELECT 1 FROM ACT_RU_EVENT_SUBSCR es WHERE es.PROC_INST_ID_ = p.PROC_INST_ID_) "
                + "AND (p.BUSINESS_KEY_ IS NULL OR NOT EXISTS ("
                + "  SELECT 1 FROM execution_context ec WHERE ec.workflow_execution_id = p.BUSINESS_KEY_ AND ec.cdate >= ?)) "
                + "AND (p.BUSINESS_KEY_ IS NULL OR NOT EXISTS ("
                + "  SELECT 1 FROM sandbox_log sl WHERE sl.workflow_execution_id = p.BUSINESS_KEY_ AND sl.cdate >= ?)) "
                + "ORDER BY p.PROC_INST_ID_ ASC LIMIT ?";
        try {
            return jdbc.query(sql, (rs, idx) -> new HeldFailedRun(rs.getString(1), rs.getString(2)),
                    cutoff, cutoff, cutoff, limit);
        } catch (DataAccessException ex) {
            log.debug("dead-letter held-run lookup skipped: {}", ex.getMessage());
            return List.of();
        }
    }

    /**
     * Returns the next keyset page of terminated (or runtime-less zombie) run ids
     * ({@code BUSINESS_KEY_}) ordered ascending.
     */
    public static List<String> nextTerminatedRunBatch(JdbcTemplate jdbc,
                                                      Instant from,
                                                      Instant to,
                                                      String afterBusinessKey,
                                                      int batchSize,
                                                      boolean onlyWithLogs) {
        if (jdbc == null || to == null) {
            return List.of();
        }
        int limit = Math.max(1, batchSize);
        try {
            return queryBatch(jdbc, from, to, afterBusinessKey, limit, onlyWithLogs, true);
        } catch (DataAccessException ex) {
            if (isMissingRelation(ex)) {
                log.debug("terminated-run query retrying without optional satellite tables: {}", ex.getMessage());
                return queryBatch(jdbc, from, to, afterBusinessKey, limit, false, false);
            }
            throw ex;
        }
    }

    /**
     * Returns the next keyset page of {@code workflow_execution_id}s present in
     * {@code execution_context} or {@code sandbox_log} strictly before {@code olderThan} that have
     * NO {@code ACT_HI_PROCINST} row and NO live {@code ACT_RU_EXECUTION} row.
     */
    public static List<String> nextOrphanedLogRunBatch(JdbcTemplate jdbc,
                                                       Instant olderThan,
                                                       String afterRunId,
                                                       int batchSize) {
        if (jdbc == null || olderThan == null) {
            return List.of();
        }
        int limit = Math.max(1, batchSize);
        Timestamp cutoff = Timestamp.from(olderThan);
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>(8);

        sql.append("SELECT run_id FROM ( ");
        sql.append("  SELECT DISTINCT workflow_execution_id AS run_id FROM execution_context ");
        sql.append("  WHERE workflow_execution_id IS NOT NULL AND cdate < ? ");
        params.add(cutoff);
        sql.append("  UNION ");
        sql.append("  SELECT DISTINCT workflow_execution_id AS run_id FROM sandbox_log ");
        sql.append("  WHERE workflow_execution_id IS NOT NULL AND cdate < ? ");
        params.add(cutoff);
        sql.append(") cand WHERE 1=1 ");
        if (afterRunId != null && !afterRunId.isEmpty()) {
            sql.append("AND cand.run_id > ? ");
            params.add(afterRunId);
        }
        sql.append("AND NOT EXISTS (SELECT 1 FROM execution_context ec2 WHERE ec2.workflow_execution_id = cand.run_id AND ec2.cdate >= ?) ");
        params.add(cutoff);
        sql.append("AND NOT EXISTS (SELECT 1 FROM sandbox_log sl2 WHERE sl2.workflow_execution_id = cand.run_id AND sl2.cdate >= ?) ");
        params.add(cutoff);
        sql.append("AND NOT EXISTS (SELECT 1 FROM ACT_HI_PROCINST p WHERE p.BUSINESS_KEY_ = cand.run_id) ");
        sql.append("AND NOT EXISTS (SELECT 1 FROM ACT_RU_EXECUTION ru WHERE ru.BUSINESS_KEY_ = cand.run_id) ");
        sql.append("ORDER BY cand.run_id ASC LIMIT ?");
        params.add(limit);

        try {
            return jdbc.queryForList(sql.toString(), String.class, params.toArray());
        } catch (DataAccessException ex) {
            log.debug("orphaned-log run lookup skipped: {}", ex.getMessage());
            return List.of();
        }
    }

    private static List<String> queryBatch(JdbcTemplate jdbc,
                                           Instant from,
                                           Instant to,
                                           String afterBusinessKey,
                                           int limit,
                                           boolean onlyWithLogs,
                                           boolean guardSubPiParent) {
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>(10);
        Timestamp toTs = Timestamp.from(to);

        sql.append("SELECT DISTINCT p.BUSINESS_KEY_ FROM ACT_HI_PROCINST p ");
        sql.append("WHERE p.BUSINESS_KEY_ IS NOT NULL ");
        if (from != null) {
            sql.append("AND p.END_TIME_ IS NOT NULL AND p.END_TIME_ >= ? AND p.END_TIME_ < ? ");
            params.add(Timestamp.from(from));
            params.add(toTs);
        } else {
            // Terminated before cutoff OR zombie history row with zero live ACT_RU_EXECUTION.
            sql.append("AND ((p.END_TIME_ IS NOT NULL AND p.END_TIME_ < ?) ");
            sql.append("  OR (p.END_TIME_ IS NULL AND p.START_TIME_ < ? ");
            sql.append("    AND NOT EXISTS (SELECT 1 FROM ACT_RU_EXECUTION ru WHERE ru.PROC_INST_ID_ = p.PROC_INST_ID_))) ");
            params.add(toTs);
            params.add(toTs);
        }
        if (afterBusinessKey != null && !afterBusinessKey.isEmpty()) {
            sql.append("AND p.BUSINESS_KEY_ > ? ");
            params.add(afterBusinessKey);
        }
        // Terminated-instance guard: zero live runtime executions and zero instances ending at/after cutoff.
        sql.append("AND NOT EXISTS (SELECT 1 FROM ACT_HI_PROCINST q ");
        sql.append("WHERE q.BUSINESS_KEY_ = p.BUSINESS_KEY_ ");
        sql.append("AND ((q.END_TIME_ IS NOT NULL AND q.END_TIME_ >= ?) ");
        sql.append("  OR (q.END_TIME_ IS NULL AND EXISTS (SELECT 1 FROM ACT_RU_EXECUTION ru2 WHERE ru2.PROC_INST_ID_ = q.PROC_INST_ID_)))) ");
        params.add(toTs);

        if (guardSubPiParent) {
            // Protect child sub-PI while its parent still has live runtime or ended at/after cutoff.
            sql.append("AND NOT EXISTS (SELECT 1 FROM sub_pi_invocation spi ");
            sql.append("JOIN ACT_HI_PROCINST parent ON parent.BUSINESS_KEY_ = spi.parent_execution_id ");
            sql.append("WHERE spi.child_execution_id = p.BUSINESS_KEY_ ");
            sql.append("AND ((parent.END_TIME_ IS NOT NULL AND parent.END_TIME_ >= ?) ");
            sql.append("  OR (parent.END_TIME_ IS NULL AND EXISTS (SELECT 1 FROM ACT_RU_EXECUTION pru WHERE pru.PROC_INST_ID_ = parent.PROC_INST_ID_)))) ");
            params.add(toTs);
        }
        if (onlyWithLogs) {
            sql.append("AND (EXISTS (SELECT 1 FROM execution_context ec WHERE ec.workflow_execution_id = p.BUSINESS_KEY_) ");
            sql.append("OR EXISTS (SELECT 1 FROM sandbox_log sl WHERE sl.workflow_execution_id = p.BUSINESS_KEY_)) ");
        }
        sql.append("ORDER BY p.BUSINESS_KEY_ ASC LIMIT ?");
        params.add(limit);

        return jdbc.queryForList(sql.toString(), String.class, params.toArray());
    }

    private static boolean isMissingRelation(DataAccessException ex) {
        String msg = ex.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("does not exist") || lower.contains("not found") || lower.contains("no such table");
    }
}
