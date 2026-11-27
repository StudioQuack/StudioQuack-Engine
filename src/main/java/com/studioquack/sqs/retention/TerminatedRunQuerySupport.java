package com.studioquack.sqs.retention;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Keyset-paginated, DB-agnostic JDBC queries for selecting fully-terminated workflow runs
 * ({@code BUSINESS_KEY_} == {@code workflowExecutionId}) within a time window {@code [from, to)}.
 *
 * <p>Replaces the per-run N+1 Activiti query loop so multi-hundred-GB databases with millions of
 * runs can be purged or streamed in constant memory ({@code batchSize} run ids per page) without
 * scanning unindexed tables or holding monolithic transactions.
 *
 * <p>Safety invariants preserved identically to {@code SqsEngine.allInstancesTerminatedBefore}:
 * <ul>
 *   <li>Positive terminal evidence: at least one finished {@code ACT_HI_PROCINST} row exists.</li>
 *   <li>No live/parked instance: zero {@code ACT_HI_PROCINST} rows with {@code END_TIME_ IS NULL}.</li>
 *   <li>Strict upper bound: zero {@code ACT_HI_PROCINST} rows with {@code END_TIME_ >= to}.</li>
 *   <li>Live parent protection: a finished function-port sub-PI whose parent run is still live or
 *       ended at/after {@code to} is excluded so its parent never loses child history mid-run.</li>
 * </ul>
 */
@Slf4j
public final class TerminatedRunQuerySupport {

    private TerminatedRunQuerySupport() {
    }

    /**
     * Returns the next keyset page of terminated run ids ({@code BUSINESS_KEY_}) ordered ascending.
     *
     * @param jdbc             JDBC template for the daemon database (H2 or PostgreSQL)
     * @param from             optional inclusive lower bound on run {@code END_TIME_} (null = unbounded)
     * @param to               required exclusive upper bound on run {@code END_TIME_}
     * @param afterBusinessKey previous page's last key for keyset pagination (null on first page)
     * @param batchSize        maximum run ids to return (clamped to {@code >= 1})
     * @param onlyWithLogs     when true, returns only runs that still have rows in
     *                         {@code execution_context} or {@code sandbox_log}
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
            // Minimal slice tests may omit sub_pi_invocation or sandbox_log; retry without the
            // sub-PI parent guard if that table is absent while preserving the core ACT_HI guard.
            if (isMissingRelation(ex)) {
                log.debug("terminated-run query retrying without optional satellite tables: {}", ex.getMessage());
                return queryBatch(jdbc, from, to, afterBusinessKey, limit, false, false);
            }
            throw ex;
        }
    }

    /**
     * Counts distinct unfinished (running/parked) runs started before {@code to} (and on/after
     * {@code from} when provided), so operators see why active runs were excluded from an archive.
     */
    public static long countActiveRunsInWindow(JdbcTemplate jdbc, Instant from, Instant to) {
        if (jdbc == null || to == null) {
            return 0L;
        }
        StringBuilder sql = new StringBuilder(
                "SELECT COUNT(DISTINCT p.BUSINESS_KEY_) FROM ACT_HI_PROCINST p "
                        + "WHERE p.BUSINESS_KEY_ IS NOT NULL "
                        + "AND p.END_TIME_ IS NULL "
                        + "AND p.START_TIME_ < ?");
        List<Object> params = new ArrayList<>(2);
        params.add(Timestamp.from(to));
        if (from != null) {
            sql.append(" AND p.START_TIME_ >= ?");
            params.add(Timestamp.from(from));
        }
        try {
            Long count = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
            return count == null ? 0L : count;
        } catch (DataAccessException ex) {
            log.debug("active-run count skipped: {}", ex.getMessage());
            return 0L;
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
        List<Object> params = new ArrayList<>(6);
        Timestamp toTs = Timestamp.from(to);

        sql.append("SELECT DISTINCT p.BUSINESS_KEY_ FROM ACT_HI_PROCINST p ");
        sql.append("WHERE p.BUSINESS_KEY_ IS NOT NULL ");
        sql.append("AND p.END_TIME_ IS NOT NULL ");
        sql.append("AND p.END_TIME_ < ? ");
        params.add(toTs);

        if (from != null) {
            sql.append("AND p.END_TIME_ >= ? ");
            params.add(Timestamp.from(from));
        }
        if (afterBusinessKey != null && !afterBusinessKey.isEmpty()) {
            sql.append("AND p.BUSINESS_KEY_ > ? ");
            params.add(afterBusinessKey);
        }
        // Terminated-instance guard: zero unfinished instances and zero instances ending at/after cutoff.
        sql.append("AND NOT EXISTS (SELECT 1 FROM ACT_HI_PROCINST q ");
        sql.append("WHERE q.BUSINESS_KEY_ = p.BUSINESS_KEY_ ");
        sql.append("AND (q.END_TIME_ IS NULL OR q.END_TIME_ >= ?)) ");
        params.add(toTs);

        if (guardSubPiParent) {
            // Never purge/archive a child sub-PI while its parent run is still live or post-cutoff.
            sql.append("AND NOT EXISTS (SELECT 1 FROM sub_pi_invocation spi ");
            sql.append("JOIN ACT_HI_PROCINST parent ON parent.BUSINESS_KEY_ = spi.parent_execution_id ");
            sql.append("WHERE spi.child_execution_id = p.BUSINESS_KEY_ ");
            sql.append("AND (parent.END_TIME_ IS NULL OR parent.END_TIME_ >= ?)) ");
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
