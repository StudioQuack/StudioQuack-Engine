package com.studioquack.sqs.engine.halt;

import com.studioquack.sqs.engine.quota.NodeExecutionMeter;
import com.studioquack.sqs.engine.update.WorkIntakeGate;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExecutionHaltGuardTest {

    private final MutableClock clock = new MutableClock();
    private final NodeExecutionMeter meter = new NodeExecutionMeter();
    private final WorkIntakeGate intake = new WorkIntakeGate();
    private final AsyncExecutor executor = mock(AsyncExecutor.class);
    private final AtomicBoolean executorActive = new AtomicBoolean();
    private ExecutionHaltGuard guard;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        when(executor.isActive()).thenAnswer(inv -> executorActive.get());
        doAnswer(inv -> { executorActive.set(true); return null; }).when(executor).start();
        doAnswer(inv -> { executorActive.set(false); return null; }).when(executor).shutdown();
        ProcessEngineConfigurationImpl cfg = mock(ProcessEngineConfigurationImpl.class);
        when(cfg.getAsyncExecutor()).thenReturn(executor);
        ProcessEngine engine = mock(ProcessEngine.class);
        when(engine.getProcessEngineConfiguration()).thenReturn(cfg);
        ObjectProvider<ProcessEngine> engineProvider = mock(ObjectProvider.class);
        when(engineProvider.getIfAvailable()).thenReturn(engine);
        ObjectProvider<WorkIntakeGate> intakeProvider = mock(ObjectProvider.class);
        when(intakeProvider.getIfAvailable()).thenReturn(intake);
        guard = new ExecutionHaltGuard(meter, engineProvider, intakeProvider, true, Duration.ofMinutes(5), clock);
    }

    @AfterEach
    void tearDown() {
        guard.shutdown();
    }

    @Test
    void coldBootIsOfflineUntilFirstContactThenStartsJobAcquisition() {
        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.OFFLINE);
        assertThat(guard.refusal()).startsWith(ExecutionHaltGuard.OFFLINE_ERROR_CODE);
        assertThatThrownBy(guard::requireActive).isInstanceOf(ExecutionHaltedException.class);

        guard.onControlPlaneContact();

        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.ACTIVE);
        assertThat(guard.refusal()).isNull();
        verify(executor, timeout(2_000)).start();
    }

    @Test
    void missedBeatsPastTheWindowGoOfflineAndPauseJobAcquisition() {
        guard.onControlPlaneContact();
        verify(executor, timeout(2_000)).start();

        clock.advance(Duration.ofMinutes(5).plusSeconds(1));
        guard.tick();

        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.OFFLINE);
        verify(executor, timeout(2_000)).shutdown();
    }

    @Test
    void overlayContactKeepsASilentDaemonOnline() {
        guard.onControlPlaneContact();
        verify(executor, timeout(2_000)).start();

        // OVERLAY_SILENT: no beat for longer than the window, but the backend keeps polling over the overlay.
        clock.advance(Duration.ofMinutes(4));
        guard.onOverlayContact();
        clock.advance(Duration.ofMinutes(4));
        guard.tick();

        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.ACTIVE);
        verify(executor, never()).shutdown();
    }

    @Test
    void overlayContactBeforeTheFirstBeatDoesNotAdmitWork() {
        guard.onOverlayContact();

        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.OFFLINE);
        verify(executor, timeout(500).times(0)).start();
    }

    @Test
    void spentAllowanceFromTheSnapshotIsLimitsExceeded() {
        meter.applySnapshot(Map.of("enforced", true, "granted", 100L, "consumed", 100L));
        guard.onControlPlaneContact();

        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.LIMITS_EXCEEDED);
        assertThat(guard.refusal()).startsWith(NodeExecutionMeter.ERROR_CODE);
        verify(executor, never()).start();
    }

    @Test
    void limitsExceededTakesPrecedenceOverOfflineAndSurvivesReinstallColdBoot() {
        // Fresh reinstall receives exhausted snapshot on beat #1: jumps straight to LIMITS_EXCEEDED, never ACTIVE.
        meter.applySnapshot(Map.of("enforced", true, "granted", 1500L, "consumed", 1500L));
        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.LIMITS_EXCEEDED);
        guard.onControlPlaneContact();
        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.LIMITS_EXCEEDED);
        verify(executor, never()).start();

        // Even after losing contact past the offline window, LIMITS_EXCEEDED still dominates OFFLINE.
        clock.advance(Duration.ofMinutes(10));
        guard.tick();
        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.LIMITS_EXCEEDED);
    }

    @Test
    void quotaRefusedLogEntersLimitsExceededUntilTheNextBeatSaysOtherwise() {
        guard.onControlPlaneContact();

        guard.observeLogRefusal(429, "{\"errorCode\":\"" + NodeExecutionMeter.ERROR_CODE + "\"}");
        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.LIMITS_EXCEEDED);

        guard.onControlPlaneContact();
        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.ACTIVE);
    }

    @Test
    void backfillThrottleAndOtherRefusalsDoNotHalt() {
        guard.onControlPlaneContact();

        guard.observeLogRefusal(429, "{\"errorCode\":\"Error_BackfillThrottled\"}");
        guard.observeLogRefusal(500, NodeExecutionMeter.ERROR_CODE);

        assertThat(guard.mode()).isEqualTo(ExecutionHaltGuard.Mode.ACTIVE);
    }

    @Test
    void doesNotRestartJobAcquisitionWhileAnUpdateOwnsIt() {
        intake.close(WorkIntakeGate.Reason.UPDATE);
        guard.onControlPlaneContact();
        guard.tick();

        verify(executor, timeout(500).times(0)).start();
    }

    @Test
    void disabledGuardNeverHalts() {
        @SuppressWarnings("unchecked")
        ExecutionHaltGuard disabled = new ExecutionHaltGuard(meter, mock(ObjectProvider.class),
                mock(ObjectProvider.class), false, Duration.ofMinutes(5), clock);
        meter.applySnapshot(Map.of("enforced", true, "granted", 1L, "consumed", 5L));

        assertThat(disabled.mode()).isEqualTo(ExecutionHaltGuard.Mode.ACTIVE);
        assertThat(disabled.refusal()).isNull();
        disabled.shutdown();
    }

    /** A clock the test moves forward explicitly. */
    private static final class MutableClock extends Clock {
        private final AtomicLong millis = new AtomicLong(1_700_000_000_000L);

        void advance(Duration by) {
            millis.addAndGet(by.toMillis());
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }

        @Override
        public long millis() {
            return millis.get();
        }
    }
}
