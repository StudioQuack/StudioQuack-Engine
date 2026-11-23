package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Pointer stored on a process instance instead of the full execution context.
 * The engine keeps these ids; the sealed payload lives in the context store.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProcessContextRef implements Serializable {

    /** PK in {@code execution_history}; stable per execution attempt. */
    private String executionId;

    /**
     * Stable across retries / replays of the same logical execution; survives
     * even when the underlying process instance is replaced.
     */
    private String workflowExecutionId;

    /**
     * Per-node refId. Bumped on every fresh dispatch so a node that retries
     * 3× has 3 immutable rows in {@code execution_context}, preserving the
     * audit trail. The most recent row is the live one.
     */
    private String currentNodeRefId;

    /** Target environment id; matches {@code ExecutionContext.envId}. */
    private String envId;

    /**
     * Wire schema version. Bumped whenever the persisted shape of
     * {@code ExecutionContextDocumentEntity} changes in a non-backwards-
     * compatible way so deserialisers can pick the right migrator.
     */
    private int schemaVersion;

    /** Canonical engine variable name carrying this ref. */
    public static final String CONTEXT_REF_VARIABLE_NAME = "studioquackRef";
}
