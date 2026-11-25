package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.MockBackend;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogFileHydrator;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import com.studioquack.sqs.status.log.SandboxLogRowMapper;
import com.studioquack.sqs.status.log.SandboxLogWireRow;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Sub-PI re-key regression gate: a function-port child runs under a fresh id the server never
 * registered, so flushing its buffered rows under the child id 404s ({@code Error_RunNotFound}) and
 * the provider node's logs never land. The flusher must POST to the top-level (reportable) run id
 * while still READING the rows buffered under the child id.
 */
class ChannelRunLogBatchFlusherTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String CHILD_RUN = "child-subpi";
    private static final String PARENT_RUN = "parent-top";
    private static final String ENV_BASE = "/be/v1/sqvm/environments/" + ENV_ID;

    private static SqsChannelProperties props() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId(ENV_ID);
        props.setBearer("sqvm_bearer_cb");
        return props;
    }

    private static SandboxLogEntity oneRow(String runId) {
        return SandboxLogEntity.builder()
                .id("slog_test1")
                .workflowExecutionId(runId)
                .nodeId("node-1")
                .nodeName("Provider")
                .status("COMPLETED")
                .language("python")
                .build();
    }

    private ChannelRunLogBatchFlusher newFlusher(SandboxLogRepository repo,
                                                 SubProcessCompletionRegistry registry,
                                                 String baseUrl) {
        SandboxLogRowMapper rowMapper = new SandboxLogRowMapper(MAPPER,
                new SandboxLogFileHydrator(MAPPER, new ObjectProvider<>() {
                    @Override
                    public com.studioquack.sqs.files.ClientFileStore getObject() {
                        return null;
                    }

                    @Override
                    public com.studioquack.sqs.files.ClientFileStore getIfAvailable() {
                        return null;
                    }
                }));
        return new ChannelRunLogBatchFlusher(props(), baseUrl, new ChannelHttpClients(null),
                MAPPER, new OutboundBaseHolder(), repo, rowMapper,
                new ObjectProvider<>() {
                    @Override
                    public SubProcessCompletionRegistry getObject() {
                        return registry;
                    }

                    @Override
                    public SubProcessCompletionRegistry getIfAvailable() {
                        return registry;
                    }
                });
    }

    @Test
    void subPiChild_flushPostsUnderTopLevelRun() throws Exception {
        SandboxLogRepository repo = mock(SandboxLogRepository.class);
        when(repo.findByWorkflowExecutionIdOrderByCdateAsc(CHILD_RUN))
                .thenReturn(List.of(oneRow(CHILD_RUN)));
        SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
        when(registry.resolveReportableRunId(anyString())).thenAnswer(inv ->
                CHILD_RUN.equals(inv.getArgument(0)) ? PARENT_RUN : inv.getArgument(0));

        try (MockBackend be = new MockBackend()) {
            be.stubJson(ENV_BASE + "/runs/" + PARENT_RUN + "/sandbox-logs/batch",
                    200, "{\"ok\":true,\"persisted\":1}");

            boolean durable = newFlusher(repo, registry, be.baseUrl()).flush(CHILD_RUN);

            assertThat(durable).isTrue();
            assertThat(be.requests())
                    .as("the child's rows must land on the run the BE actually registered")
                    .anySatisfy(r -> assertThat(r.path())
                            .isEqualTo(ENV_BASE + "/runs/" + PARENT_RUN + "/sandbox-logs/batch"));
            assertThat(be.requests())
                    .noneSatisfy(r -> assertThat(r.path()).contains(CHILD_RUN));
        }
    }

    @Test
    void topLevelRun_flushKeepsOwnId() throws Exception {
        SandboxLogRepository repo = mock(SandboxLogRepository.class);
        when(repo.findByWorkflowExecutionIdOrderByCdateAsc(PARENT_RUN))
                .thenReturn(List.of(oneRow(PARENT_RUN)));
        SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
        when(registry.resolveReportableRunId(anyString())).thenAnswer(inv -> inv.getArgument(0));

        try (MockBackend be = new MockBackend()) {
            be.stubJson(ENV_BASE + "/runs/" + PARENT_RUN + "/sandbox-logs/batch",
                    200, "{\"ok\":true,\"persisted\":1}");

            boolean durable = newFlusher(repo, registry, be.baseUrl()).flush(PARENT_RUN);

            assertThat(durable).isTrue();
            assertThat(be.requests()).singleElement().satisfies(r ->
                    assertThat(r.path()).isEqualTo(ENV_BASE + "/runs/" + PARENT_RUN + "/sandbox-logs/batch"));
        }
    }

    @Test
    void partitionByBudget_splitsWhenByteCapWouldBeExceeded() throws Exception {
        SandboxLogRepository repo = mock(SandboxLogRepository.class);
        SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
        ChannelRunLogBatchFlusher flusher = newFlusher(repo, registry, "http://127.0.0.1:1");

        String fat = "x".repeat(SandboxLogIngestLimits.MAX_CHUNK_BYTES / 2 + 1024);
        SandboxLogWireRow a = new SandboxLogWireRow(
                "n1", "A", "COMPLETED", 0, 1L, "python", null,
                fat, null, null, null, null, null, null, null, null, null, "slog_a", null);
        SandboxLogWireRow b = new SandboxLogWireRow(
                "n2", "B", "COMPLETED", 0, 1L, "python", null,
                fat, null, null, null, null, null, null, null, null, null, "slog_b", null);

        assertThat(flusher.partitionByBudget(List.of(a, b))).hasSize(2);
    }
}
