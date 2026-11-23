package com.studioquack.sqs.status;

import com.studioquack.sqs.command.ReplayPiCommandHandler;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.runtime.CancelledRunGuard;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.delegate.event.impl.ActivitiEntityEventImpl;
import org.activiti.engine.delegate.event.impl.ActivitiProcessCancelledEventImpl;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.activiti.engine.repository.ProcessDefinition;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Targeted unit tests for {@link RunStatusEventBridge.RunStatusEventListener}: the sentinel-mapping
 * fix (a {@code PROCESS_COMPLETED} carrying {@code ERROR_CODE} must report FAILED, not COMPLETED)
 * and the cancelled-run TTL guard suppressing late COMPLETED/FAILED for an already-cancelled run.
 */
class RunStatusEventBridgeTest {

    private DelegateExecution executionWithBusinessKey(String businessKey, String errorCode) {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceBusinessKey()).thenReturn(businessKey);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn(errorCode);
        return execution;
    }

    @Test
    void processCompleted_withoutErrorSentinel_reportsCompleted() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        DelegateExecution execution = executionWithBusinessKey("wf-1", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        verify(callback).report(eq("wf-1"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
    }

    @Test
    void processCompleted_withErrorSentinel_reportsFailed_notCompleted() {
        // This is the sentinel-mapping bug: the shared error-end event for this daemon's generated
        // BPMN is a PLAIN PROCESS_COMPLETED carrying __sq_error_code, not PROCESS_COMPLETED_WITH_ERROR_END_EVENT.
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        DelegateExecution execution = executionWithBusinessKey("wf-2", "SANDBOX_FAILED");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        verify(callback).report(eq("wf-2"), eq(RunStatusCallback.Status.FAILED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
    }

    @Test
    void processCancelled_alwaysReportsCanceled() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        DelegateExecution execution = executionWithBusinessKey("wf-3", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_CANCELLED));

        verify(callback).report(eq("wf-3"), eq(RunStatusCallback.Status.CANCELED),
                eq("engine event PROCESS_CANCELLED"), nullable(String.class), eq(true));
    }

    @Test
    void processCancelled_byReplayPiDelete_isSuppressed() {
        // Replay-pi cancels the old instance only to restart the same run/businessKey: a CANCELED
        // report would terminally close the run on the backend while the replayed instance is
        // already RUNNING under the same key. The delete reason rides the event's cause.
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        // The bridge reads the key through DelegateExecution (extractBusinessKey/executionOf), so
        // the entity must be both a ProcessInstance and a DelegateExecution — ExecutionEntity is.
        ExecutionEntity instance = mock(ExecutionEntity.class);
        when(instance.getProcessInstanceBusinessKey()).thenReturn("wf-replay");
        ActivitiProcessCancelledEventImpl event = new ActivitiProcessCancelledEventImpl(instance);
        event.setCause(ReplayPiCommandHandler.REPLAY_DELETE_REASON);

        listener.onEvent(event);

        verifyNoInteractions(callback);
    }

    @Test
    void processCancelled_withOtherCause_stillReportsCanceled() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        ExecutionEntity instance = mock(ExecutionEntity.class);
        when(instance.getProcessInstanceBusinessKey()).thenReturn("wf-cancel-op");
        ActivitiProcessCancelledEventImpl event = new ActivitiProcessCancelledEventImpl(instance);
        event.setCause("repair: cancel-instance");

        listener.onEvent(event);

        verify(callback).report(eq("wf-cancel-op"), eq(RunStatusCallback.Status.CANCELED),
                eq("engine event PROCESS_CANCELLED"), nullable(String.class), eq(true));
    }

    @Test
    void lateCompletedForAlreadyCancelledRun_isSuppressed() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        guard.markCancelled("wf-4");
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        DelegateExecution execution = executionWithBusinessKey("wf-4", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        verifyNoInteractions(callback);
    }

    @Test
    void lateFailedForAlreadyCancelledRun_isSuppressed() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        guard.markCancelled("wf-5");
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        DelegateExecution execution = executionWithBusinessKey("wf-5", "SANDBOX_FAILED");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED_WITH_ERROR_END_EVENT));

        verifyNoInteractions(callback);
    }

    @Test
    void cancelledEventItself_isNeverSuppressed_evenIfAlreadyMarked() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        guard.markCancelled("wf-6");
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, null);
        DelegateExecution execution = executionWithBusinessKey("wf-6", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_CANCELLED));

        verify(callback).report(eq("wf-6"), eq(RunStatusCallback.Status.CANCELED),
                eq("engine event PROCESS_CANCELLED"), nullable(String.class), eq(true));
    }

    @Test
    void nullCancelledRunGuard_doesNotSuppressAndDoesNotThrow() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, null, null);
        DelegateExecution execution = executionWithBusinessKey("wf-7", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        verify(callback).report(eq("wf-7"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
    }

    @Test
    void failedPath_drainsSandboxLogsThenReports() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        AsyncSandboxLogDispatcher dispatcher = mock(AsyncSandboxLogDispatcher.class);
        when(dispatcher.awaitRun(eq("wf-fail"), eq(Duration.ofSeconds(2)))).thenReturn(true);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, dispatcher);
        DelegateExecution execution = executionWithBusinessKey("wf-fail", "SANDBOX_FAILED");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        InOrder order = inOrder(dispatcher, callback);
        order.verify(dispatcher).awaitRun("wf-fail", Duration.ofSeconds(2));
        order.verify(callback).report(eq("wf-fail"), eq(RunStatusCallback.Status.FAILED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
    }

    @Test
    void failedPath_reportsAfterDelayedLogTask() throws Exception {
        AsyncSandboxLogDispatcher dispatcher = new AsyncSandboxLogDispatcher();
        try {
            CountDownLatch started = new CountDownLatch(1);
            AtomicBoolean logDone = new AtomicBoolean();
            dispatcher.enqueue("wf-fail-live", "node-1", () -> {
                started.countDown();
                try {
                    Thread.sleep(120);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                logDone.set(true);
            });
            assertThat(started.await(3, TimeUnit.SECONDS)).isTrue();

            AtomicBoolean logDoneWhenReported = new AtomicBoolean();
            RunStatusCallback callback = mock(RunStatusCallback.class);
            doAnswer(inv -> {
                logDoneWhenReported.set(logDone.get());
                return null;
            }).when(callback).report(any(), any(), any(), nullable(String.class), anyBoolean());

            CancelledRunGuard guard = new CancelledRunGuard(600_000L);
            RunStatusEventBridge.RunStatusEventListener listener =
                    new RunStatusEventBridge.RunStatusEventListener(callback, guard, dispatcher);
            DelegateExecution execution = executionWithBusinessKey("wf-fail-live", "SANDBOX_FAILED");

            listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

            verify(callback).report(eq("wf-fail-live"), eq(RunStatusCallback.Status.FAILED),
                    eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
            assertThat(logDoneWhenReported).isTrue();
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    void completedPath_drainsPendingLogsThenReports() throws Exception {
        // COMPLETED drains too: the terminal status must not overtake the last node's log post, or
        // the server refuses that row as Run_Already_Terminal and the node shows "no logs".
        RunStatusCallback callback = mock(RunStatusCallback.class);
        AsyncSandboxLogDispatcher dispatcher = mock(AsyncSandboxLogDispatcher.class);
        when(dispatcher.awaitRun(eq("wf-ok"), eq(Duration.ofSeconds(2)))).thenReturn(true);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, dispatcher);
        DelegateExecution execution = executionWithBusinessKey("wf-ok", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        InOrder order = inOrder(dispatcher, callback);
        order.verify(dispatcher).awaitRun("wf-ok", Duration.ofSeconds(2));
        order.verify(callback).report(eq("wf-ok"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));

        // Real dispatcher: COMPLETED must NOT report while this run's log task is still held —
        // the report lands only after the in-flight post finishes (drain), never before.
        AsyncSandboxLogDispatcher live = new AsyncSandboxLogDispatcher();
        try {
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            live.enqueue("wf-ok-live", "node-1", () -> {
                entered.countDown();
                try {
                    release.await(15, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();

            AtomicBoolean reportedWhileHeld = new AtomicBoolean();
            RunStatusCallback liveCallback = mock(RunStatusCallback.class);
            doAnswer(inv -> {
                reportedWhileHeld.set(release.getCount() > 0);
                return null;
            }).when(liveCallback).report(any(), any(), any(), nullable(String.class), anyBoolean());
            RunStatusEventBridge.RunStatusEventListener liveListener =
                    new RunStatusEventBridge.RunStatusEventListener(
                            liveCallback, new CancelledRunGuard(600_000L), live);
            CountDownLatch eventDone = new CountDownLatch(1);
            Thread eventThread = new Thread(() -> {
                liveListener.onEvent(new ActivitiEntityEventImpl(
                        executionWithBusinessKey("wf-ok-live", null), ActivitiEventType.PROCESS_COMPLETED));
                eventDone.countDown();
            });
            eventThread.start();

            // While the log post is held, the terminal report must not have happened yet.
            assertThat(eventDone.await(400, TimeUnit.MILLISECONDS)).isFalse();
            verify(liveCallback, never()).report(any(), any(), any(), any(), anyBoolean());

            release.countDown();
            assertThat(eventDone.await(5, TimeUnit.SECONDS)).isTrue();
            verify(liveCallback).report(eq("wf-ok-live"), eq(RunStatusCallback.Status.COMPLETED),
                    eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
            assertThat(reportedWhileHeld).isFalse();
        } finally {
            live.shutdown();
        }
    }

    @Test
    void canceledPath_drainTimeout_stillReports_butLogsAreNotDurable() {
        // The run index must still flip, but a timed-out drain means the server may not hold every
        // log row: logsDurable=false keeps the run from ever becoming purge-eligible on
        // self-hosted.
        RunStatusCallback callback = mock(RunStatusCallback.class);
        AsyncSandboxLogDispatcher dispatcher = mock(AsyncSandboxLogDispatcher.class);
        when(dispatcher.awaitRun(eq("wf-cancel"), eq(Duration.ofSeconds(2)))).thenReturn(false);
        CancelledRunGuard guard = new CancelledRunGuard(600_000L);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(callback, guard, dispatcher);
        DelegateExecution execution = executionWithBusinessKey("wf-cancel", null);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_CANCELLED));

        InOrder order = inOrder(dispatcher, callback);
        order.verify(dispatcher).awaitRun("wf-cancel", Duration.ofSeconds(2));
        order.verify(callback).report(eq("wf-cancel"), eq(RunStatusCallback.Status.CANCELED),
                eq("engine event PROCESS_CANCELLED"), nullable(String.class), eq(false));
    }

    @Test
    void streamRunThatLostALogPost_isReportedNotDurable() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        AsyncSandboxLogDispatcher dispatcher = mock(AsyncSandboxLogDispatcher.class);
        when(dispatcher.awaitRun(eq("wf-lossy"), eq(Duration.ofSeconds(2)))).thenReturn(true);
        when(dispatcher.consumeLossy("wf-lossy")).thenReturn(true);
        RunStatusEventBridge.RunStatusEventListener listener = new RunStatusEventBridge.RunStatusEventListener(
                callback, new CancelledRunGuard(600_000L), dispatcher);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("wf-lossy", null), ActivitiEventType.PROCESS_COMPLETED));

        verify(callback).report(eq("wf-lossy"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(false));
    }

    @Test
    void processStarted_unregistered_reportsRunningAndRegistersBatch() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, null);
        DelegateExecution execution = executionWithBusinessKey("wf-local", null);
        when(execution.getProcessDefinitionId()).thenReturn("Process_wfAbc:1:xyz");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_STARTED));

        assertThat(registry.isBatch("wf-local")).isTrue();
        verify(callback).report(eq("wf-local"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), eq("wfAbc"), eq(true), nullable(String.class));
    }

    @Test
    void processStarted_messageStartedRun_reportsItsStartCorrelationKey() {
        // A message-started run's businessKey is a minted id; the RUNNING report must carry the
        // seeded start correlation key so the remote run index row shows the caller-meaningful key.
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, null);
        DelegateExecution execution = executionWithBusinessKey("wh_minted1", null);
        when(execution.getProcessDefinitionId()).thenReturn("Process_wfAbc:1:xyz");
        when(execution.getVariable(
                com.studioquack.sqs.engine.runtime.EngineVariableNames.START_CORRELATION_KEY))
                .thenReturn("order-42");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_STARTED));

        verify(callback).report(eq("wh_minted1"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), eq("wfAbc"), eq(true), eq("order-42"));
    }

    @Test
    void processStarted_alreadyRegistered_doesNotDoubleReport() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("wf-designed", RunLogMode.STREAM);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, null);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("wf-designed", null), ActivitiEventType.PROCESS_STARTED));

        verifyNoInteractions(callback);
        assertThat(registry.modeOf("wf-designed")).isEqualTo(RunLogMode.STREAM);
    }

    @Test
    void batchTerminal_flushesThenReportsThenRemoves() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("wf-batch", RunLogMode.BATCH);
        RunLogBatchFlusher flusher = mock(RunLogBatchFlusher.class);
        when(flusher.flush("wf-batch")).thenReturn(true);
        AsyncSandboxLogDispatcher dispatcher = mock(AsyncSandboxLogDispatcher.class);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), dispatcher, registry, flusher);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("wf-batch", null), ActivitiEventType.PROCESS_COMPLETED));

        InOrder order = inOrder(flusher, callback);
        order.verify(flusher).flush("wf-batch");
        order.verify(callback).report(eq("wf-batch"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(true));
        verify(dispatcher, never()).awaitRun(any(), any());
        assertThat(registry.modeOf("wf-batch")).isEqualTo(RunLogMode.STREAM);
    }

    @Test
    void batchTerminal_refusedFlush_stillReportsButFlagsLogsNotDurable() {
        // The 401 regression: the server refused the batch, so the terminal status must carry
        // logsDurable=false — ChannelRunStatusCallback then withholds the reported-purge mark and
        // the local rows survive as the only copy.
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("wf-batch-401", RunLogMode.BATCH);
        RunLogBatchFlusher flusher = mock(RunLogBatchFlusher.class);
        when(flusher.flush("wf-batch-401")).thenReturn(false);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, flusher);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("wf-batch-401", null), ActivitiEventType.PROCESS_COMPLETED));

        InOrder order = inOrder(flusher, callback);
        order.verify(flusher).flush("wf-batch-401");
        order.verify(callback).report(eq("wf-batch-401"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), nullable(String.class), eq(false));
        assertThat(registry.modeOf("wf-batch-401")).isEqualTo(RunLogMode.STREAM);
    }

    @Test
    void processStarted_subPiChild_inheritsStreamFromParent() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("parent-run", RunLogMode.STREAM);
        SubProcessCompletionRegistry subPi = mock(SubProcessCompletionRegistry.class);
        when(subPi.isSubPiChild("child-run")).thenReturn(true);
        when(subPi.resolveReportableRunId("child-run")).thenReturn("parent-run");
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, null, subPi);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("child-run", null), ActivitiEventType.PROCESS_STARTED));

        assertThat(registry.modeOf("child-run")).isEqualTo(RunLogMode.STREAM);
        verify(callback).report(eq("child-run"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), nullable(String.class), eq(true), nullable(String.class));
    }

    @Test
    void processStarted_subPiChild_inheritsBatchFromParent() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        registry.registerIfAbsent("parent-run", RunLogMode.BATCH);
        SubProcessCompletionRegistry subPi = mock(SubProcessCompletionRegistry.class);
        when(subPi.isSubPiChild("child-run")).thenReturn(true);
        when(subPi.resolveReportableRunId("child-run")).thenReturn("parent-run");
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, null, subPi);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("child-run", null), ActivitiEventType.PROCESS_STARTED));

        assertThat(registry.isBatch("child-run")).isTrue();
        verify(callback).report(eq("child-run"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), nullable(String.class), eq(true), nullable(String.class));
    }

    @Test
    void processStarted_unknown_stillRegistersBatch() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        SubProcessCompletionRegistry subPi = mock(SubProcessCompletionRegistry.class);
        when(subPi.isSubPiChild("wf-local")).thenReturn(false);
        RunStatusEventBridge.RunStatusEventListener listener =
                new RunStatusEventBridge.RunStatusEventListener(
                        callback, new CancelledRunGuard(600_000L), null, registry, null, subPi);

        listener.onEvent(new ActivitiEntityEventImpl(
                executionWithBusinessKey("wf-local", null), ActivitiEventType.PROCESS_STARTED));

        assertThat(registry.isBatch("wf-local")).isTrue();
        verify(callback).report(eq("wf-local"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), nullable(String.class), eq(true), nullable(String.class));
    }

    @Test
    void processStarted_strongUuidDefinition_resolvesKeyBeforeReport() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        String definitionId = "11111111-1111-1111-1111-111111111111";
        RunStatusEventBridge.RunStatusEventListener listener =
                listenerWithEngine(callback, registry, definitionId, "Process_wfAbc__draft_deadbeef");
        DelegateExecution execution = executionWithBusinessKey("wh_minted1", null);
        when(execution.getProcessDefinitionId()).thenReturn(definitionId);
        when(execution.getVariable(EngineVariableNames.START_CORRELATION_KEY)).thenReturn("order-42");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_STARTED));

        verify(callback).report(eq("wh_minted1"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), eq("wfAbc"), eq(true), eq("order-42"));
    }

    @Test
    void processCompleted_strongUuidDefinition_resolvesKeyBeforeReport() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        String definitionId = "22222222-2222-2222-2222-222222222222";
        RunStatusEventBridge.RunStatusEventListener listener =
                listenerWithEngine(callback, null, definitionId, "Process_wfAbc");
        DelegateExecution execution = executionWithBusinessKey("wh_minted1", null);
        when(execution.getProcessDefinitionId()).thenReturn(definitionId);

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_COMPLETED));

        verify(callback).report(eq("wh_minted1"), eq(RunStatusCallback.Status.COMPLETED),
                eq("engine event PROCESS_COMPLETED"), eq("wfAbc"), eq(true));
    }

    @Test
    void processStarted_strongUuidLookupMiss_reportsWithoutWorkflowId() {
        RunStatusCallback callback = mock(RunStatusCallback.class);
        RunLogModeRegistry registry = new RunLogModeRegistry();
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.findProcessDefinition("33333333-3333-3333-3333-333333333333"))
                .thenThrow(new IllegalStateException("definition gone"));
        RunStatusEventBridge.RunStatusEventListener listener =
                listenerWithEngine(callback, registry, engine);
        DelegateExecution execution = executionWithBusinessKey("wh_minted1", null);
        when(execution.getProcessDefinitionId()).thenReturn("33333333-3333-3333-3333-333333333333");

        listener.onEvent(new ActivitiEntityEventImpl(execution, ActivitiEventType.PROCESS_STARTED));

        verify(callback).report(eq("wh_minted1"), eq(RunStatusCallback.Status.RUNNING),
                eq("engine event PROCESS_STARTED"), isNull(), eq(true), isNull());
    }

    @SuppressWarnings("unchecked")
    private RunStatusEventBridge.RunStatusEventListener listenerWithEngine(
            RunStatusCallback callback, RunLogModeRegistry registry,
            String definitionId, String processKey) {
        SqsEngine engine = mock(SqsEngine.class);
        ProcessDefinition def = mock(ProcessDefinition.class);
        when(def.getKey()).thenReturn(processKey);
        when(engine.findProcessDefinition(definitionId)).thenReturn(def);
        return listenerWithEngine(callback, registry, engine);
    }

    @SuppressWarnings("unchecked")
    private RunStatusEventBridge.RunStatusEventListener listenerWithEngine(
            RunStatusCallback callback, RunLogModeRegistry registry, SqsEngine engine) {
        ObjectProvider<SqsEngine> engines = mock(ObjectProvider.class);
        when(engines.getIfAvailable()).thenReturn(engine);
        return new RunStatusEventBridge.RunStatusEventListener(
                callback, new CancelledRunGuard(600_000L), null, registry, null, null,
                engines, Runnable::run);
    }

    @Test
    void workflowIdFromProcessDefinition_stripsProcessPrefixAndVersion() {
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition("Process_abc:3:def")).isEqualTo("abc");
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition("Process_abc")).isEqualTo("abc");
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition("other")).isNull();
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition(null)).isNull();
    }

    @Test
    void workflowIdFromProcessDefinition_stripsDraftNamespace() {
        // A Designer Run deploys as Process_<wf>__draft_<token> — the run-index row must key on
        // the real workflow id, or daemon-originated starts are invisible in the FE.
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition("Process_abc__draft_deadbeef:1:def")).isEqualTo("abc");
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition("Process_abc__draft_deadbeef")).isEqualTo("abc");
        assertThat(RunStatusEventBridge.RunStatusEventListener
                .workflowIdFromProcessDefinition("Process___draft_deadbeef")).isNull();
    }
}
