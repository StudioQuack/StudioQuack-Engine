package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import com.studioquack.sqs.status.log.SandboxLogStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The fan-out routing of {@link CompositeSandboxLogCallback}: local store always; remote mirror for
 * every live (STREAM) run, never for a BATCH run.
 */
class CompositeSandboxLogCallbackTest {

    private static final SandboxLogCallback.SandboxLogRecord RECORD = new SandboxLogCallback.SandboxLogRecord(
            "run-1", "node-1", "Step", "COMPLETED", 0, 5L, "python", null,
            "out", null, null, null, null, null, null, "NONE", null);

    /** Records every call — stands in for the channel/logging remote mirror. */
    private static final class RecordingMirror implements SandboxLogCallback {
        private final List<SandboxLogRecord> reports = new CopyOnWriteArrayList<>();
        private final List<SandboxStartRecord> started = new CopyOnWriteArrayList<>();

        @Override
        public void report(SandboxLogRecord record) {
            reports.add(record);
        }

        @Override
        public void reportStarted(SandboxStartRecord record) {
            started.add(record);
        }
    }

    private static ObjectProvider<SandboxLogCallback> providerOf(SandboxLogCallback mirror) {
        return new ObjectProvider<>() {
            @Override
            public SandboxLogCallback getObject() {
                return mirror;
            }

            @Override
            public Stream<SandboxLogCallback> stream() {
                return Stream.of(mirror);
            }
        };
    }

    private SandboxLogStore realStore(SandboxLogRepository repository) {
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        return new SandboxLogStore(repository, new ObjectMapper());
    }

    @Test
    void liveRunFansOutToLocalStoreAndBackend() {
        RecordingMirror mirror = new RecordingMirror();
        SandboxLogRepository repository = mock(SandboxLogRepository.class);
        CompositeSandboxLogCallback composite = new CompositeSandboxLogCallback(
                providerOf(mirror), realStore(repository), new RunLogModeRegistry());

        composite.report(RECORD);
        composite.reportStarted(new SandboxLogCallback.SandboxStartRecord("run-1", "node-1", "Step", "python"));

        assertThat(mirror.reports).hasSize(1);
        assertThat(mirror.started).hasSize(1);
        org.mockito.Mockito.verify(repository, org.mockito.Mockito.times(2)).save(any());
    }

    @Test
    void batchRunBuffersLocallyAndSkipsBackendMirror() {
        RecordingMirror mirror = new RecordingMirror();
        SandboxLogRepository repository = mock(SandboxLogRepository.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("run-1", RunLogMode.BATCH);
        CompositeSandboxLogCallback composite = new CompositeSandboxLogCallback(
                providerOf(mirror), realStore(repository), registry);

        composite.report(RECORD);
        composite.reportStarted(new SandboxLogCallback.SandboxStartRecord("run-1", "node-1", "Step", "python"));

        assertThat(mirror.reports).isEmpty();
        assertThat(mirror.started).isEmpty();
        org.mockito.Mockito.verify(repository, org.mockito.Mockito.times(2)).save(any());
    }
}
