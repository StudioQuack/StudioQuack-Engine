package com.studioquack.sqs.retention;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Hard-deletes Activiti {@code ACT_HI_*} history for runs confirmed eligible for purge.
 *
 * <p><b>PostgreSQL transaction safety:</b> {@code ACT_HI_ENTITYLINK}, {@code ACT_HI_COMMENT}, and
 * {@code ACT_HI_ATTACHMENT} do not exist in every Activiti schema version. Issuing {@code DELETE}
 * against a missing table inside a PostgreSQL transaction aborts the ENTIRE transaction
 * ({@code 25P02: current transaction is aborted}) even if the exception is caught in Java, rolling
 * back every earlier delete in the batch. We therefore probe existing child tables once via
 * {@link DatabaseMetaData#getTables} (read-only metadata, never aborts a transaction) and only
 * delete from tables actually present in this schema.
 */
@Slf4j
@Component
public class ActivitiHistoryRetentionRepository {

    private static final List<String> CHILD_TABLES = List.of(
            "ACT_HI_DETAIL", "ACT_HI_VARINST", "ACT_HI_ACTINST", "ACT_HI_TASKINST",
            "ACT_HI_IDENTITYLINK", "ACT_HI_ENTITYLINK", "ACT_HI_COMMENT", "ACT_HI_ATTACHMENT");

    private final JdbcTemplate jdbc;

    /** Lazily cached subset of {@link #CHILD_TABLES} that actually exist in this database schema. */
    private volatile List<String> existingChildTables;

    public ActivitiHistoryRetentionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbc = jdbcTemplate;
    }

    /** Every process-instance id belonging to the given run business keys. */
    public List<String> findProcessInstanceIdsByBusinessKeys(Collection<String> businessKeys) {
        if (businessKeys == null || businessKeys.isEmpty()) {
            return List.of();
        }
        return jdbc.queryForList(
                "select PROC_INST_ID_ from ACT_HI_PROCINST where BUSINESS_KEY_ in (" + marks(businessKeys.size()) + ")",
                String.class, businessKeys.toArray());
    }

    /**
     * Run business keys whose engine history is all that is left: every instance ended before
     * {@code cutoff} (or is a runtime-less zombie started before {@code cutoff}), and the run has
     * no {@code execution_context} and no {@code sandbox_log} row left.
     */
    public List<String> findOrphanedHistoryBusinessKeys(java.sql.Timestamp cutoff, int limit) {
        return jdbc.queryForList(
                "select distinct p.BUSINESS_KEY_ from ACT_HI_PROCINST p"
                        + " where p.BUSINESS_KEY_ is not null"
                        + " and ((p.END_TIME_ is not null and p.END_TIME_ < ?)"
                        + "   or (p.END_TIME_ is null and p.START_TIME_ < ?"
                        + "     and not exists (select 1 from ACT_RU_EXECUTION ru where ru.PROC_INST_ID_ = p.PROC_INST_ID_)))"
                        + " and not exists (select 1 from ACT_HI_PROCINST q where q.BUSINESS_KEY_ = p.BUSINESS_KEY_"
                        + "   and ((q.END_TIME_ is not null and q.END_TIME_ >= ?)"
                        + "     or (q.END_TIME_ is null and exists (select 1 from ACT_RU_EXECUTION ru2 where ru2.PROC_INST_ID_ = q.PROC_INST_ID_))))"
                        + " and not exists (select 1 from execution_context e where e.workflow_execution_id = p.BUSINESS_KEY_)"
                        + " and not exists (select 1 from sandbox_log s where s.workflow_execution_id = p.BUSINESS_KEY_)"
                        + " limit ?",
                String.class, cutoff, cutoff, cutoff, limit);
    }

    /**
     * Hard-deletes history for the given process-instance ids, existing child tables first, then
     * {@code ACT_HI_PROCINST}. Transaction-safe on both PostgreSQL and H2.
     *
     * @return rows removed from {@code ACT_HI_PROCINST}
     */
    public int deleteByProcessInstanceIds(Collection<String> processInstanceIds) {
        if (processInstanceIds == null || processInstanceIds.isEmpty()) {
            return 0;
        }
        String marks = marks(processInstanceIds.size());
        Object[] args = processInstanceIds.toArray();
        for (String table : resolveExistingChildTables()) {
            jdbc.update("delete from " + table + " where PROC_INST_ID_ in (" + marks + ")", args);
        }
        return jdbc.update("delete from ACT_HI_PROCINST where PROC_INST_ID_ in (" + marks + ")", args);
    }

    private List<String> resolveExistingChildTables() {
        List<String> cached = this.existingChildTables;
        if (cached != null) {
            return cached;
        }
        List<String> detected = probeExistingChildTables();
        this.existingChildTables = detected;
        return detected;
    }

    private List<String> probeExistingChildTables() {
        try {
            Set<String> presentUpper = jdbc.execute((ConnectionCallback<Set<String>>) con -> {
                Set<String> names = new HashSet<>();
                DatabaseMetaData meta = con.getMetaData();
                try (ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"})) {
                    while (rs.next()) {
                        String name = rs.getString("TABLE_NAME");
                        if (name != null) {
                            names.add(name.toUpperCase(Locale.ROOT));
                        }
                    }
                }
                return names;
            });
            if (presentUpper == null || presentUpper.isEmpty()) {
                return CHILD_TABLES;
            }
            List<String> matched = new ArrayList<>();
            for (String candidate : CHILD_TABLES) {
                if (presentUpper.contains(candidate.toUpperCase(Locale.ROOT))) {
                    matched.add(candidate);
                } else {
                    log.debug("log-retention: ACT_HI child table {} absent from schema — will skip", candidate);
                }
            }
            return List.copyOf(matched);
        } catch (RuntimeException ex) {
            log.debug("log-retention: metadata probe fell back to core ACT_HI child tables: {}", ex.getMessage());
            return List.of("ACT_HI_DETAIL", "ACT_HI_VARINST", "ACT_HI_ACTINST", "ACT_HI_TASKINST",
                    "ACT_HI_IDENTITYLINK", "ACT_HI_COMMENT", "ACT_HI_ATTACHMENT");
        }
    }

    private static String marks(int count) {
        return String.join(", ", Collections.nCopies(count, "?"));
    }
}
