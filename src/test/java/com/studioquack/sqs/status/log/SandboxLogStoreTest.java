package com.studioquack.sqs.status.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The daemon-local sandbox-log write: mapping, full-fidelity storage, and the never-throws contract
 * that keeps a local custody failure off the dispatch path.
 */
class SandboxLogStoreTest {

    private final SandboxLogRepository repository = mock(SandboxLogRepository.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final SandboxLogStore store = new SandboxLogStore(repository, mapper);

    @Test
    void savesTerminalRecordWithAllFields() {
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        store.save(new SandboxLogCallback.SandboxLogRecord(
                "run-1", "node-1", "Step", "COMPLETED", 0, 42L, "python", "ctr-1",
                "hello-out", "hello-err", null,
                java.util.List.of(java.util.Map.of("k", "v")), null, null, null, "NONE", null));

        ArgumentCaptor<SandboxLogEntity> captor = ArgumentCaptor.forClass(SandboxLogEntity.class);
        verify(repository).save(captor.capture());
        SandboxLogEntity saved = captor.getValue();
        assertThat(saved.getId()).startsWith("slog_");
        assertThat(saved.getWorkflowExecutionId()).isEqualTo("run-1");
        assertThat(saved.getNodeId()).isEqualTo("node-1");
        assertThat(saved.getStatus()).isEqualTo("COMPLETED");
        assertThat(saved.getStdout()).isEqualTo("hello-out");
        assertThat(saved.getStderr()).isEqualTo("hello-err");
        assertThat(saved.getInputJson()).contains("\"k\":\"v\"");
        assertThat(saved.getCdate()).isNotNull();
    }

    @Test
    void storesFullStreamsWithoutTruncation() {
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        String huge = "x".repeat(200_000);

        store.save(new SandboxLogCallback.SandboxLogRecord(
                "run-1", "node-1", null, "COMPLETED", 0, 1L, "python", null,
                huge, null, null, null, null, null, null, "NONE", null));

        ArgumentCaptor<SandboxLogEntity> captor = ArgumentCaptor.forClass(SandboxLogEntity.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getStdout()).isEqualTo(huge);
    }

    @Test
    void storesFullIoJsonWithoutSpillMarker() throws Exception {
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        Map<String, String> payload = Map.of("blob", "y".repeat(300_000));

        store.save(new SandboxLogCallback.SandboxLogRecord(
                "run-1", "node-1", null, "COMPLETED", 0, 1L, "python", null,
                null, null, null, payload, null, null, null, "NONE", null));

        ArgumentCaptor<SandboxLogEntity> captor = ArgumentCaptor.forClass(SandboxLogEntity.class);
        verify(repository).save(captor.capture());
        Map<?, ?> parsed = mapper.readValue(captor.getValue().getInputJson(), Map.class);
        assertThat(parsed.get("blob")).isEqualTo(payload.get("blob"));
    }

    @Test
    void savesStartedMarker() {
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        store.saveStarted(new SandboxLogCallback.SandboxStartRecord("run-1", "node-1", "Step", "python"));

        ArgumentCaptor<SandboxLogEntity> captor = ArgumentCaptor.forClass(SandboxLogEntity.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo("STARTED");
    }

    @Test
    void dropsRecordWithoutIdsAndNeverThrowsOnRepositoryFailure() {
        store.save(new SandboxLogCallback.SandboxLogRecord(
                null, null, null, "COMPLETED", null, null, null, null,
                null, null, null, null, null, null, null, null, null));
        verify(repository, never()).save(any());

        when(repository.save(any())).thenThrow(new RuntimeException("db down"));
        store.save(new SandboxLogCallback.SandboxLogRecord(
                "run-1", "node-1", null, "COMPLETED", 0, 1L, "python", null,
                "out", null, null, null, null, null, null, "NONE", null));
    }
}
