package com.studioquack.sqs.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A Designer Run redeploys on every click while the user edits. Sharing the published BPMN keys
 * would make each unfinished draft the newest version of {@code Process_<workflowId>} — and
 * Activiti hands the newest version to the webhook, API and Duckbot triggers of the DEPLOYED
 * workflow.
 */
class DraftNamespaceIsolationTest {

    private static WorkflowGraph published() {
        return new WorkflowGraph("wf-1", "Test", "1", List.of(), List.of());
    }

    private static WorkflowGraph draft(String token) {
        return new WorkflowGraph("wf-1", "Test", "1", List.of(), List.of(), token);
    }

    @Test
    @DisplayName("a published graph keeps the bare process key")
    void publishedKeyUnchanged() {
        assertThat(published().processKey()).isEqualTo("Process_wf-1");
        assertThat(published().namespaceSuffix()).isEmpty();
        assertThat(published().isDraft()).isFalse();
    }

    @Test
    @DisplayName("a draft gets its own process key")
    void draftKeyIsolated() {
        WorkflowGraph draft = draft("abc123");

        assertThat(draft.processKey()).isEqualTo("Process_wf-1__draft_abc123");
        assertThat(draft.isDraft()).isTrue();
        assertThat(draft.processKey()).isNotEqualTo(published().processKey());
    }

    @Test
    @DisplayName("two Designer Runs of the same workflow do not collide with each other")
    void twoDraftsDoNotCollide() {
        assertThat(draft("aaa").processKey()).isNotEqualTo(draft("bbb").processKey());
    }

    @Test
    @DisplayName("a blank token is treated as published, not as a draft named ''")
    void blankTokenIsPublished() {
        assertThat(new WorkflowGraph("wf-1", "T", "1", List.of(), List.of(), "  ").processKey())
                .isEqualTo("Process_wf-1");
    }

    @Test
    @DisplayName("the namespace round-trips out of a process key so sub-process calls stay inside it")
    void namespaceRoundTrips() {
        assertThat(WorkflowGraph.namespaceSuffixOf("Process_wf-1__draft_abc123")).isEqualTo("__draft_abc123");
        assertThat(WorkflowGraph.namespaceSuffixOf("Process_wf-1")).isEmpty();
        assertThat(WorkflowGraph.namespaceSuffixOf(null)).isEmpty();
    }

}
