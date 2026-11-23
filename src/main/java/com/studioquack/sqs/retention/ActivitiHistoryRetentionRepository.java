package com.studioquack.sqs.retention;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Hard-deletes Activiti {@code ACT_HI_*} history for runs the {@link LogRetentionEnforcer} has
 * already confirmed terminated-and-aged and purged from {@code execution_context}. The daemon runs
 * at {@code historyLevel=FULL} — every activity and every variable update of every run lands in
 * {@code ACT_HI_*} and, until now, stayed there for the life of the install.
 */
@Slf4j
@Component
public class ActivitiHistoryRetentionRepository {

    /**
     * History tables carrying per-instance rows, children first. {@code ACT_HI_ENTITYLINK},
     * {@code ACT_HI_COMMENT} and {@code ACT_HI_ATTACHMENT} do not exist in every Activiti schema
     * version — a missing table is skipped, never fails the pass.
     */
    private static final List<String> CHILD_TABLES = List.of(
            "ACT_HI_DETAIL", "ACT_HI_VARINST", "ACT_HI_ACTINST", "ACT_HI_TASKINST",
            "ACT_HI_IDENTITYLINK", "ACT_HI_ENTITYLINK", "ACT_HI_COMMENT", "ACT_HI_ATTACHMENT");

    private final JdbcTemplate jdbc;

    public ActivitiHistoryRetentionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbc = jdbcTemplate;
    }

    /**
     * Every process-instance id (parents and sub-instances alike) belonging to the given run
     * business keys ({@code workflowExecutionId}s). Terminated runs only arrive here from the
     * enforcer's guarded batches.
     */
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
     * {@code cutoff}, and the run has no {@code execution_context} and no {@code sandbox_log} row on
     * this engine. These are runs finished under the earlier policy (whose local logs were already
     * deleted) or runs that never wrote a log row — their history protects no log content, and it was
     * the bulk of an engine database that only ever grew.
     */
    public List<String> findOrphanedHistoryBusinessKeys(java.sql.Timestamp cutoff, int limit) {
        return jdbc.queryForList(
                "select distinct p.BUSINESS_KEY_ from ACT_HI_PROCINST p"
                        + " where p.BUSINESS_KEY_ is not null"
                        + " and not exists (select 1 from ACT_HI_PROCINST q where q.BUSINESS_KEY_ = p.BUSINESS_KEY_"
                        + "   and (q.END_TIME_ is null or q.END_TIME_ >= ?))"
                        + " and not exists (select 1 from execution_context e where e.workflow_execution_id = p.BUSINESS_KEY_)"
                        + " and not exists (select 1 from sandbox_log s where s.workflow_execution_id = p.BUSINESS_KEY_)"
                        + " limit ?",
                String.class, cutoff, limit);
    }

    /**
     * Hard-deletes history for the given process-instance ids, children before {@code ACT_HI_PROCINST}
     * .
     *
     * @return Rows removed from {@code ACT_HI_PROCINST} (child-table counts are not tallied)
     */
    public int deleteByProcessInstanceIds(Collection<String> processInstanceIds) {
        if (processInstanceIds == null || processInstanceIds.isEmpty()) {
            return 0;
        }
        String marks = marks(processInstanceIds.size());
        Object[] args = processInstanceIds.toArray();
        for (String table : CHILD_TABLES) {
            try {
                jdbc.update("delete from " + table + " where PROC_INST_ID_ in (" + marks + ")", args);
            } catch (DataAccessException tableAbsent) {
                log.debug("log-retention: skipping {} (not in this schema): {}", table, tableAbsent.getMessage());
            }
        }
        return jdbc.update("delete from ACT_HI_PROCINST where PROC_INST_ID_ in (" + marks + ")", args);
    }

    private static String marks(int count) {
        return String.join(", ", Collections.nCopies(count, "?"));
    }
}
