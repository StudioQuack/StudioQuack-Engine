package com.studioquack.sqs.status;

import com.studioquack.sqs.command.ReplayPiCommandHandler;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.runtime.CancelledRunGuard;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiCancelledEvent;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.spring.boot.ProcessEngineConfigurationConfigurer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Wires the terminal half of the {@link RunStatusCallback} seam: an Activiti event listener that
 * maps a process instance's lifecycle end to a run-index status (COMPLETED / FAILED / CANCELED)
 * and reports it vithe callback. It is registered as an additional engine event listener through
 * its own {@link ProcessEngineConfigurationConfigurer} using the same read-existing-then-append
 * pattern as {@code SqsActivitiEngineConfig}.
 */
@Slf4j
@Configuration
public class RunStatusEventBridge {

    /**
     * The listener bean; constructed with the resolved {@link RunStatusCallback}.
     * {@code SubProcessCompletionRegistry} is {@code @Lazy}: it depends on {@code EngineTriggerService}
     * → {@code SqsEngine} → this configurer, and an eager inject cycles the context.
     */
    @Bean
    public RunStatusEventListener runStatusEventListener(RunStatusCallback runStatusCallback,
                                                         CancelledRunGuard cancelledRunGuard,
                                                         AsyncSandboxLogDispatcher asyncSandboxLogDispatcher,
                                                         RunLogModeRegistry runLogModeRegistry,
                                                         RunLogBatchFlusher runLogBatchFlusher,
                                                         @Lazy SubProcessCompletionRegistry subProcessCompletionRegistry,
                                                         ObjectProvider<SqsEngine> engines) {
        return new RunStatusEventListener(runStatusCallback, cancelledRunGuard, asyncSandboxLogDispatcher,
                runLogModeRegistry, runLogBatchFlusher, subProcessCompletionRegistry,
                engines, Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Appends {@link #runStatusEventListener} to the engine's event listeners without dropping any
     * already configured ('s status listener / Activiti defaults). Append order is irrelevant: every
     * configurer reads the current list and adds its own, so all listeners survive.
     */
    @Bean
    public ProcessEngineConfigurationConfigurer runStatusListenerConfigurer(RunStatusEventListener listener) {
        return cfg -> {
            List<ActivitiEventListener> listeners = new ArrayList<>();
            if (cfg.getEventListeners() != null) {
                listeners.addAll(cfg.getEventListeners());
            }
            listeners.add(listener);
            cfg.setEventListeners(listeners);
            log.info("run-status event bridge registered (PROCESS_COMPLETED/…_ERROR_END/CANCELLED -> RunStatusCallback)");
        };
    }

    /**
     * Fully guarded and {@link #isFailOnException} is {@code false}: a callback hiccup must never roll
     * back or break an engine transaction (same contract as the status listener).
     */
    static final class RunStatusEventListener implements ActivitiEventListener, TerminalLogHandoff {

        private final RunStatusCallback callback;
        private final CancelledRunGuard cancelledRunGuard;
        /** Nullable so unit tests can keep the 2-arg constructor; null = skip drain. */
        private final AsyncSandboxLogDispatcher dispatcher;
        /** Nullable so the minimal unit-test constructors stay valid; null = STREAM everywhere. */
        private final RunLogModeRegistry runLogModeRegistry;
        /** Nullable so the minimal unit-test constructors stay valid; null = skip BATCH flush. */
        private final RunLogBatchFlusher runLogBatchFlusher;
        /** Nullable so the 2/3/5-arg test constructors stay valid; null = not a sub-PI child. */
        private final SubProcessCompletionRegistry subProcessRegistry;
        /**
         * Looked up only off the engine thread, and only when the definition id is not itself
         * {@code Process_<workflowId>}.
         */
        private final ObjectProvider<SqsEngine> engines;
        /** Runs the StrongUuid key lookup. Null keeps the report on the caller thread. */
        private final Executor workflowIdLookup;

        RunStatusEventListener(RunStatusCallback callback, CancelledRunGuard cancelledRunGuard) {
            this(callback, cancelledRunGuard, null, null, null);
        }

        RunStatusEventListener(RunStatusCallback callback,
                               CancelledRunGuard cancelledRunGuard,
                               AsyncSandboxLogDispatcher dispatcher) {
            this(callback, cancelledRunGuard, dispatcher, null, null);
        }

        RunStatusEventListener(RunStatusCallback callback,
                               CancelledRunGuard cancelledRunGuard,
                               AsyncSandboxLogDispatcher dispatcher,
                               RunLogModeRegistry runLogModeRegistry,
                               RunLogBatchFlusher runLogBatchFlusher) {
            this(callback, cancelledRunGuard, dispatcher, runLogModeRegistry, runLogBatchFlusher, null);
        }

        RunStatusEventListener(RunStatusCallback callback,
                               CancelledRunGuard cancelledRunGuard,
                               AsyncSandboxLogDispatcher dispatcher,
                               RunLogModeRegistry runLogModeRegistry,
                               RunLogBatchFlusher runLogBatchFlusher,
                               SubProcessCompletionRegistry subProcessRegistry) {
            this(callback, cancelledRunGuard, dispatcher, runLogModeRegistry, runLogBatchFlusher,
                    subProcessRegistry, null, null);
        }

        RunStatusEventListener(RunStatusCallback callback,
                               CancelledRunGuard cancelledRunGuard,
                               AsyncSandboxLogDispatcher dispatcher,
                               RunLogModeRegistry runLogModeRegistry,
                               RunLogBatchFlusher runLogBatchFlusher,
                               SubProcessCompletionRegistry subProcessRegistry,
                               ObjectProvider<SqsEngine> engines,
                               Executor workflowIdLookup) {
            this.callback = callback;
            this.cancelledRunGuard = cancelledRunGuard;
            this.dispatcher = dispatcher;
            this.runLogModeRegistry = runLogModeRegistry;
            this.runLogBatchFlusher = runLogBatchFlusher;
            this.subProcessRegistry = subProcessRegistry;
            this.engines = engines;
            this.workflowIdLookup = workflowIdLookup;
        }

        @Override
        public void onEvent(ActivitiEvent event) {
            // PROCESS_STARTED: classify + surface a daemon-local start. A run started via start-run
            // is already registered (and reported RUNNING) by its handler, so registerIfAbsent is a
            // no-op there and we emit nothing — only a genuinely local start reaches the report below.
            if (event.getType() == ActivitiEventType.PROCESS_STARTED) {
                handleProcessStarted(event);
                return;
            }
            RunStatusCallback.Status status = mapStatus(event);
            if (status == null) {
                return; // not a terminal lifecycle event we mirror
            }
            try {
                String businessKey = extractBusinessKey(event);
                if (businessKey == null || businessKey.isBlank()) {
                    log.debug("run-status bridge: no businessKey on {} — skipping", event.getType());
                    return;
                }
                // A cancel-instance command races any dispatch already in flight: a late COMPLETED/
                // FAILED for a run already marked cancelled would resurrect it in the BE run index. The
                // CANCELED event itself is exempt — it is the very report the guard exists to let through.
                if (status != RunStatusCallback.Status.CANCELED
                        && cancelledRunGuard != null && cancelledRunGuard.isCancelled(businessKey)) {
                    log.debug("run-status bridge: suppressing late {} for cancelled run businessKey={}",
                            status, businessKey);
                    return;
                }
                boolean logsDurable = handOffLogs(businessKey);
                reportRun(businessKey, status, "engine event " + event.getType(), logsDurable,
                        null, false, processDefinitionIdOf(event));
                // The run is over: drop its log-mode entry so the registry stays bounded.
                if (runLogModeRegistry != null) {
                    runLogModeRegistry.remove(businessKey);
                }
            } catch (Exception ex) {
                log.warn("run-status bridge failed for event {}: {}", event.getType(), ex.getMessage());
            }
        }

        /**
         * Registers a daemon-local start and reports RUNNING for it. A run that already went through
         * {@code start-run} is registered there, so {@code registerIfAbsent} returns false and this emits
         * nothing.
         */
        private void handleProcessStarted(ActivitiEvent event) {
            if (runLogModeRegistry == null) {
                return;
            }
            try {
                String businessKey = extractBusinessKey(event);
                if (businessKey == null || businessKey.isBlank()) {
                    return;
                }
                RunLogMode mode = RunLogMode.BATCH;
                if (subProcessRegistry != null && subProcessRegistry.isSubPiChild(businessKey)) {
                    String parent = subProcessRegistry.resolveReportableRunId(businessKey);
                    if (parent != null && !parent.equals(businessKey)) {
                        mode = runLogModeRegistry.modeOf(parent);
                    }
                }
                boolean created = runLogModeRegistry.registerIfAbsent(businessKey, mode);
                if (created) {
                    reportRun(businessKey, RunStatusCallback.Status.RUNNING,
                            "engine event " + event.getType(), true,
                            extractStartCorrelationKey(event), true, processDefinitionIdOf(event));
                }
            } catch (Exception ex) {
                log.warn("run-status bridge: PROCESS_STARTED handling failed: {}", ex.getMessage());
            }
        }

        /**
         * The correlation key a message-started run was launched with (the
         * {@link EngineVariableNames#START_CORRELATION_KEY} seed), so the control plane run-index row can
         * show the caller-meaningful key next to the minted businessKey. {@code null} for any other run.
         */
        private String extractStartCorrelationKey(ActivitiEvent event) {
            DelegateExecution execution = executionOf(event);
            if (execution == null) {
                return null;
            }
            try {
                Object value = execution.getVariable(EngineVariableNames.START_CORRELATION_KEY);
                return value == null || value.toString().isBlank() ? null : value.toString();
            } catch (RuntimeException ex) {
                log.debug("run-status bridge: could not read the start correlation key ({})", ex.toString());
                return null;
            }
        }

        /**
         * BATCH run: the per-node rows were buffered locally, never streamed — flush them all BEFORE the
         * status flips the run, so the control plane stores every row. STREAM run: drain the run's
         * in-flight posts so the last node's row is not refused against a terminal run; bounded (2 s) and
         * fail-open.
         */
        @Override
        public boolean handOffLogs(String businessKey) {
            if (runLogModeRegistry != null && runLogModeRegistry.isBatch(businessKey)) {
                return flushBatchLogs(businessKey);
            }
            boolean drained = drainPendingLogs(businessKey);
            boolean lossy = dispatcher != null && dispatcher.consumeLossy(businessKey);
            return drained && !lossy;
        }

        /**
         * Never throws; a failure still lets the caller report.
         *
         * @return {@code false} when the flush was refused; {@code true} when the rows are durable at the
         * control plane or no flusher is wired.
         */
        private boolean flushBatchLogs(String businessKey) {
            if (runLogBatchFlusher == null) {
                return true;
            }
            try {
                return runLogBatchFlusher.flush(businessKey);
            } catch (Throwable t) {
                log.warn("run-status bridge: BATCH flush failed (businessKey={})", businessKey);
                return false;
            }
        }

        /**
         * Bounded per-run drain. Never throws; timeout or fault logs WARN with ids only and lets the
         * caller still {@code report} the terminal status.
         *
         * @return {@code true} when every queued post for the run finished (or there is no dispatcher)
         */
        private boolean drainPendingLogs(String businessKey) {
            if (dispatcher == null) {
                return true;
            }
            try {
                boolean drained = dispatcher.awaitRun(businessKey, Duration.ofSeconds(2));
                if (!drained) {
                    log.warn("run-status bridge: sandbox-log drain timed out (businessKey={})",
                            businessKey);
                }
                return drained;
            } catch (Throwable t) {
                log.warn("run-status bridge: sandbox-log drain failed (businessKey={})",
                        businessKey);
                return false;
            }
        }

        @Override
        public boolean isFailOnException() {
            return false;
        }

        /**
         * Maps a lifecycle event to a run-index status. {@code PROCESS_COMPLETED_WITH_ERROR_END_EVENT}
         * never actually fires for this daemon's generated BPMN.
         */
        private RunStatusCallback.Status mapStatus(ActivitiEvent event) {
            return switch (event.getType()) {
                case PROCESS_COMPLETED ->
                        endedInFailure(event) ? RunStatusCallback.Status.FAILED : RunStatusCallback.Status.COMPLETED;
                case PROCESS_COMPLETED_WITH_ERROR_END_EVENT -> RunStatusCallback.Status.FAILED;
                case PROCESS_CANCELLED -> isReplayDelete(event) ? null : RunStatusCallback.Status.CANCELED;
                default -> null;
            };
        }

        /**
         * The replay restarts the same run/businessKey immediately, so reporting CANCELED would terminally
         * close the run on the backend — suppress it and let the replayed instance's own lifecycle events
         * speak.
         */
        private boolean isReplayDelete(ActivitiEvent event) {
            if (event instanceof ActivitiCancelledEvent cancelled
                    && ReplayPiCommandHandler.REPLAY_DELETE_REASON.equals(cancelled.getCause())) {
                log.debug("run-status bridge: suppressing CANCELED for replay-pi delete pi={}",
                        event.getProcessInstanceId());
                return true;
            }
            return false;
        }

        private boolean endedInFailure(ActivitiEvent event) {
            DelegateExecution execution = executionOf(event);
            if (execution == null) {
                return false;
            }
            try {
                return EngineVariableNames.isFailureSentinel(execution.getVariable(EngineVariableNames.ERROR_CODE));
            } catch (RuntimeException ex) {
                // The instance is being torn down; an unreadable variable is not evidence of failure.
                log.debug("run-status bridge: could not read the error sentinel ({})", ex.toString());
                return false;
            }
        }

        private DelegateExecution executionOf(ActivitiEvent event) {
            if (event instanceof ActivitiEntityEvent entityEvent
                    && entityEvent.getEntity() instanceof DelegateExecution execution) {
                return execution;
            }
            return null;
        }

        private String extractBusinessKey(ActivitiEvent event) {
            DelegateExecution execution = executionOf(event);
            return execution == null ? null : execution.getProcessInstanceBusinessKey();
        }

        /**
         * Reports a run status. A {@code Process_<workflowId>} id is parsed here, on the engine thread.
         */
        private void reportRun(String businessKey, RunStatusCallback.Status status, String detail,
                               boolean logsDurable, String correlationKey, boolean withCorrelation,
                               String processDefinitionId) {
            String cheap = workflowIdFromProcessDefinition(processDefinitionId);
            boolean defer = cheap == null
                    && engines != null
                    && workflowIdLookup != null
                    && processDefinitionId != null
                    && !processDefinitionId.isBlank();
            if (!defer) {
                deliver(businessKey, status, detail, logsDurable, correlationKey, withCorrelation, cheap);
                return;
            }
            String definitionId = processDefinitionId;
            workflowIdLookup.execute(() -> {
                String resolved = workflowIdFromEngine(definitionId);
                deliver(businessKey, status, detail, logsDurable, correlationKey, withCorrelation, resolved);
            });
        }

        private void deliver(String businessKey, RunStatusCallback.Status status, String detail,
                             boolean logsDurable, String correlationKey, boolean withCorrelation,
                             String workflowId) {
            try {
                if (withCorrelation) {
                    callback.report(businessKey, status, detail, workflowId, logsDurable, correlationKey);
                } else {
                    callback.report(businessKey, status, detail, workflowId, logsDurable);
                }
            } catch (RuntimeException ex) {
                log.warn("run-status bridge: report failed businessKey={} status={}: {}",
                        businessKey, status, ex.toString());
            }
        }

        private String processDefinitionIdOf(ActivitiEvent event) {
            DelegateExecution execution = executionOf(event);
            if (execution == null) {
                return null;
            }
            try {
                return execution.getProcessDefinitionId();
            } catch (RuntimeException ex) {
                log.debug("run-status bridge: could not read processDefinitionId ({})", ex.toString());
                return null;
            }
        }

        /** The engine's {@code getKey} is
{@code Process_<workflowId>} (draft suffix included). */
        private String workflowIdFromEngine(String processDefinitionId) {
            try {
                SqsEngine engine = engines == null ? null : engines.getIfAvailable();
                if (engine == null) {
                    return null;
                }
                ProcessDefinition def = engine.findProcessDefinition(processDefinitionId);
                if (def == null || def.getKey() == null || def.getKey().isBlank()) {
                    return null;
                }
                return workflowIdFromProcessDefinition(def.getKey());
            } catch (RuntimeException ex) {
                log.debug("run-status bridge: process-definition key resolve skipped ({})", ex.toString());
                return null;
            }
        }

        /**
         * A Designer Run deploys under {@code Process_wf__draft_<token>} — the draft namespace is stripped
         * too, or a daemon-originated start (message/signal start fan-out on a draft deployment) lands in
         * the run index under a workflow id that matches.
         */
        static String workflowIdFromProcessDefinition(String processDefinitionId) {
            if (processDefinitionId == null || processDefinitionId.isBlank()) {
                return null;
            }
            String key = processDefinitionId;
            int colon = key.indexOf(':');
            if (colon > 0) {
                key = key.substring(0, colon);
            }
            if (key.startsWith("Process_")) {
                String rest = key.substring("Process_".length());
                int draft = rest.indexOf(WorkflowGraph.DRAFT_MARKER);
                if (draft >= 0) {
                    rest = rest.substring(0, draft);
                }
                return rest.isBlank() ? null : rest;
            }
            return null;
        }
    }
}
