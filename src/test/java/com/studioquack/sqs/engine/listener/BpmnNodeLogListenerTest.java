package com.studioquack.sqs.engine.listener;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.command.store.DeployedArtifactStore.DeployedGraph;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder;
import com.studioquack.sqs.status.AsyncSandboxLogDispatcher;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.event.ActivitiActivityEvent;
import org.activiti.engine.delegate.event.ActivitiEntityEvent;
import org.activiti.engine.delegate.event.ActivitiErrorEvent;
import org.activiti.engine.delegate.event.ActivitiEvent;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.repository.ProcessDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Engine-native BPMN node-log listener: enqueue-only on the engine thread, allowlist after
 * graph load, synthetic activity ids never reported.
 */
class BpmnNodeLogListenerTest {

    private final RecordingCallback callback = new RecordingCallback();
    private final AsyncSandboxLogDispatcher dispatcher = mock(AsyncSandboxLogDispatcher.class);
    private final BpmnNodeLogPayloads payloads = mock(BpmnNodeLogPayloads.class);
    private final ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
    private final DeployedArtifactStore artifactStore = mock(DeployedArtifactStore.class);
    private final GatewayDecisionRecorder recorder = new GatewayDecisionRecorder() {
        @Override
        public void record(GatewayDecision decision) {
            // tests do not exercise record()
        }

        @Override
        public GatewayDecision latest(String workflowExecutionId, String gatewayNodeId) {
            return null;
        }
    };

    private BpmnNodeLogListener listener;

    @BeforeEach
    void setUp() {
        doAnswer(inv -> {
            inv.getArgument(2, Runnable.class).run();
            return null;
        }).when(dispatcher).enqueue(any(), any(), any());

        when(payloads.buildTerminal(any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenAnswer(inv -> new SandboxLogCallback.SandboxLogRecord(
                        inv.getArgument(0),
                        inv.getArgument(1),
                        inv.getArgument(2),
                        inv.getArgument(4),
                        null,
                        inv.getArgument(5),
                        BpmnNodeLogPayloads.LANGUAGE,
                        null,
                        "bpmn-stdout",
                        null,
                        inv.getArgument(9),
                        inv.getArgument(6),
                        null,
                        null,
                        null,
                        "NONE",
                        null));

        listener = new BpmnNodeLogListener(
                callback, dispatcher, payloads, contextStore, artifactStore,
                providerOf(recorder), providerOf(null));
    }

    @Test
    @DisplayName("isFailOnException is false")
    void failOnExceptionIsFalse() {
        assertThat(listener.isFailOnException()).isFalse();
    }

    @Test
    @DisplayName("ACTIVITY_STARTED enqueues reportStarted")
    void startedEnqueuesReportStarted() {
        stubGraph("gw-1", "exclusive.gateway", null);

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_STARTED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.started).hasSize(1);
        SandboxLogCallback.SandboxStartRecord started = callback.started.get(0);
        assertThat(started.workflowExecutionId()).isEqualTo("run-1");
        assertThat(started.nodeId()).isEqualTo("gw-1");
        assertThat(started.language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
        assertThat(callback.reports).isEmpty();
    }

    @Test
    @DisplayName("ACTIVITY_COMPLETED enqueues report with language=bpmn")
    void completedEnqueuesReportWithBpmnLanguage() {
        stubGraph("gw-1", "exclusive.gateway", null);

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.reports).hasSize(1);
        SandboxLogCallback.SandboxLogRecord record = callback.reports.get(0);
        assertThat(record.language()).isEqualTo("bpmn");
        assertThat(record.language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
        assertThat(record.nodeId()).isEqualTo("gw-1");
        assertThat(record.status()).isEqualTo("COMPLETED");
        assertThat(callback.started).isEmpty();
    }

    @Test
    @DisplayName("wait_ activity does not call report")
    void waitActivityDoesNotReport() {
        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "wait_n1", "wait", "run-1", "Process_wf-1:1:uuid"));

        verify(dispatcher, never()).enqueue(any(), any(), any());
        assertThat(callback.reports).isEmpty();
        assertThat(callback.started).isEmpty();
    }

    @Test
    @DisplayName("wait_ ACTIVITY_CANCELLED reports CANCELLED for the authored node")
    void waitCancelledReportsCancelled() {
        stubGraph("n1", null, "function-script-executor");

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_CANCELLED, "wait_n1", "wait", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.reports).hasSize(1);
        SandboxLogCallback.SandboxLogRecord record = callback.reports.get(0);
        assertThat(record.nodeId()).isEqualTo("n1");
        assertThat(record.status()).isEqualTo("CANCELED");
        assertThat(record.errorMessage()).startsWith("interrupted by boundary");
        assertThat(callback.started).isEmpty();
    }

    @Test
    @DisplayName("errgate_ activity does not call report")
    void errgateActivityDoesNotReport() {
        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "errgate_n1", "gate", "run-1", "Process_wf-1:1:uuid"));

        verify(dispatcher, never()).enqueue(any(), any(), any());
        assertThat(callback.reports).isEmpty();
        assertThat(callback.started).isEmpty();
    }

    @Test
    @DisplayName("python script node (null bpmnType / function-script-executor) does not call report")
    void pythonScriptNodeDoesNotReport() {
        stubGraph("script-1", null, "function-script-executor");

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_script-1", "Py", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.reports).isEmpty();
        assertThat(callback.started).isEmpty();
        verify(payloads, never()).buildTerminal(any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("exclusive.gateway node DOES call report")
    void exclusiveGatewayDoesReport() {
        stubGraph("gw-1", "exclusive.gateway", null);

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).nodeId()).isEqualTo("gw-1");
        verify(payloads).buildTerminal(
                anyString(), anyString(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("gwfork_ STARTED+COMPLETED enqueue without an authored graph node")
    void generatorForkEnqueuesWithoutAuthoredNode() {
        when(artifactStore.loadDeployedGraph("wf-1"))
                .thenReturn(Optional.of(new DeployedGraph(List.of(), new ArrayList<EdgeDto>())));

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_STARTED, "gwfork_n1", "Fork n1", "run-1", "Process_wf-1:1:uuid"));
        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "gwfork_n1", "Fork n1", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.started).hasSize(1);
        assertThat(callback.started.get(0).nodeId()).isEqualTo("gwfork_n1");
        assertThat(callback.started.get(0).language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).nodeId()).isEqualTo("gwfork_n1");
        assertThat(callback.reports.get(0).status()).isEqualTo("COMPLETED");
        assertThat(callback.reports.get(0).language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
        verify(payloads).buildTerminal(
                anyString(), anyString(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("gwjoin_ STARTED+COMPLETED enqueue without an authored graph node")
    void generatorJoinEnqueuesWithoutAuthoredNode() {
        when(artifactStore.loadDeployedGraph("wf-1"))
                .thenReturn(Optional.of(new DeployedGraph(List.of(), new ArrayList<EdgeDto>())));

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_STARTED, "gwjoin_n1", "Join n1", "run-1", "Process_wf-1:1:uuid"));
        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "gwjoin_n1", "Join n1", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.started).hasSize(1);
        assertThat(callback.started.get(0).nodeId()).isEqualTo("gwjoin_n1");
        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).status()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("COMPLETED exclusive gateway with no selected branch remaps to FAILED")
    void conditionMissCompletedRemapsToFailed() {
        stubGraph("gw-1", "exclusive.gateway", null);
        GatewayDecisionRecorder missRecorder = new GatewayDecisionRecorder() {
            @Override
            public void record(GatewayDecision decision) {
                // unused
            }

            @Override
            public GatewayDecision latest(String workflowExecutionId, String gatewayNodeId) {
                return new GatewayDecision(
                        workflowExecutionId, gatewayNodeId, "XOR",
                        List.of(new BranchEvaluation("endN", "@x == \" \"", false, null)),
                        com.studioquack.sqs.runtime.GatewayRouteResolver.DEAD_END_SENTINEL,
                        false, true, System.currentTimeMillis());
            }
        };
        listener = new BpmnNodeLogListener(
                callback, dispatcher, payloads, contextStore, artifactStore,
                providerOf(missRecorder), providerOf(null));

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).status()).isEqualTo("FAILED");
        assertThat(callback.reports.get(0).errorMessage()).isEqualTo("no outgoing flow matched — condition miss");
    }

    @Test
    @DisplayName("COMPLETED exclusive with intended default stays COMPLETED")
    void intendedDefaultCompletedStaysCompleted() {
        stubGraph("gw-1", "exclusive.gateway", null);
        GatewayDecisionRecorder defaultRecorder = new GatewayDecisionRecorder() {
            @Override
            public void record(GatewayDecision decision) {
                // unused
            }

            @Override
            public GatewayDecision latest(String workflowExecutionId, String gatewayNodeId) {
                return new GatewayDecision(
                        workflowExecutionId, gatewayNodeId, "XOR",
                        List.of(
                                new BranchEvaluation("emptyTarget", null, false, null),
                                new BranchEvaluation("condTarget",
                                        "@ai_agent.main_output.final_answer == \" \"", false, null)),
                        "emptyTarget", true, false, System.currentTimeMillis());
            }
        };
        listener = new BpmnNodeLogListener(
                callback, dispatcher, payloads, contextStore, artifactStore,
                providerOf(defaultRecorder), providerOf(null));

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid"));

        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).status()).isEqualTo("COMPLETED");
        assertThat(callback.reports.get(0).errorMessage()).isNull();
    }

    @Test
    @DisplayName("ACTIVITY_ERROR_RECEIVED enqueues a FAILED row")
    void activityErrorReceivedEnqueuesFailed() {
        stubGraph("gw-1", "exclusive.gateway", null);

        listener.onEvent(errorActivityEvent(
                "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid", "NO_OUTGOING_FLOW"));

        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).nodeId()).isEqualTo("gw-1");
        assertThat(callback.reports.get(0).status()).isEqualTo("FAILED");
        assertThat(callback.reports.get(0).language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
        assertThat(callback.reports.get(0).errorMessage()).isEqualTo("NO_OUTGOING_FLOW");
    }

    @Test
    @DisplayName("StrongUuid processDefinitionId still resolves the authored graph")
    void strongUuidProcessDefinitionIdResolvesAuthoredGraph() {
        stubGraph("gw-1", "exclusive.gateway", null);
        ProcessDefinition def = mock(ProcessDefinition.class);
        when(def.getKey()).thenReturn("Process_wf-1");
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.findProcessDefinition("1929a181-raw")).thenReturn(def);
        listener = new BpmnNodeLogListener(
                callback, dispatcher, payloads, contextStore, artifactStore,
                providerOf(recorder), providerOf(engine));

        listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_gw-1", "XOR", "run-1", "1929a181-raw"));

        assertThat(callback.reports).hasSize(1);
        assertThat(callback.reports.get(0).nodeId()).isEqualTo("gw-1");
        assertThat(callback.reports.get(0).language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
    }

    @Test
    @DisplayName("ActivitiActivityEvent that is not an entity event still enqueues")
    void activityOnlyEventResolvesBusinessKeyAsync() {
        stubGraph("gw-1", "exclusive.gateway", null);
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.findBusinessKey("pi-1")).thenReturn("run-1");
        listener = new BpmnNodeLogListener(
                callback, dispatcher, payloads, contextStore, artifactStore,
                providerOf(recorder), providerOf(engine));

        ActivitiActivityEvent event = mock(ActivitiActivityEvent.class);
        when(event.getType()).thenReturn(ActivitiEventType.ACTIVITY_STARTED);
        when(event.getActivityId()).thenReturn("node_gw-1");
        when(event.getActivityName()).thenReturn("XOR");
        when(event.getProcessDefinitionId()).thenReturn("Process_wf-1:1:uuid");
        when(event.getProcessInstanceId()).thenReturn("pi-1");

        listener.onEvent(event);

        assertThat(callback.started).hasSize(1);
        assertThat(callback.started.get(0).workflowExecutionId()).isEqualTo("run-1");
        assertThat(callback.started.get(0).language()).isEqualTo(BpmnNodeLogPayloads.LANGUAGE);
    }

    @Test
    @DisplayName("true sentinels stay skipped (wait_/errgate_/flow_)")
    void trueSentinelsStaySkipped() {
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("wait_n1")).isTrue();
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("errgate_n1")).isTrue();
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("flow_n1")).isTrue();
        // Failure-hold plumbing is generator-sentinel too: never double-logged as authored nodes.
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("failroute_n1")).isTrue();
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("hold_n1")).isTrue();
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("gwfork_n1")).isFalse();
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("gwjoin_n1")).isFalse();
        assertThat(BpmnNodeLogListener.shouldSkipActivityId("node_gw-1")).isFalse();
    }

    @Test
    @DisplayName("onEvent never throws when stores throw")
    void onEventNeverThrowsWhenStoresThrow() {
        when(artifactStore.loadDeployedGraph(any())).thenThrow(new RuntimeException("store down"));

        assertThatCode(() -> listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_COMPLETED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid")))
                .doesNotThrowAnyException();
        assertThat(callback.reports).isEmpty();
    }

    @Test
    @DisplayName("onEvent never throws when the dispatcher itself throws")
    void onEventNeverThrowsWhenDispatcherThrows() {
        doThrow(new RuntimeException("queue full")).when(dispatcher).enqueue(any(), any(), any());

        assertThatCode(() -> listener.onEvent(activityEvent(
                ActivitiEventType.ACTIVITY_STARTED, "node_gw-1", "XOR", "run-1", "Process_wf-1:1:uuid")))
                .doesNotThrowAnyException();
    }

    private void stubGraph(String nodeId, String bpmnType, String jobType) {
        NodeDto node = new NodeDto();
        node.setId(nodeId);
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setJobType(jobType);
        template.setLabel("node-" + nodeId);
        node.setData(template);
        when(artifactStore.loadDeployedGraph("wf-1"))
                .thenReturn(Optional.of(new DeployedGraph(List.of(node), new ArrayList<EdgeDto>())));
    }

    private static ActivitiEvent activityEvent(ActivitiEventType type, String activityId, String activityName,
                                              String businessKey, String processDefinitionId) {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceBusinessKey()).thenReturn(businessKey);
        ActivitiActivityEvent event = mock(ActivitiActivityEvent.class,
                withSettings().extraInterfaces(ActivitiEntityEvent.class));
        when(event.getType()).thenReturn(type);
        when(event.getActivityId()).thenReturn(activityId);
        when(event.getActivityName()).thenReturn(activityName);
        when(event.getProcessDefinitionId()).thenReturn(processDefinitionId);
        when(((ActivitiEntityEvent) event).getEntity()).thenReturn(execution);
        return event;
    }

    /** ACTIVITY_ERROR_RECEIVED is an activity + error event (and entity in this mock so businessKey works). */
    private static ActivitiEvent errorActivityEvent(String activityId, String activityName,
                                                    String businessKey, String processDefinitionId,
                                                    String errorCode) {
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getProcessInstanceBusinessKey()).thenReturn(businessKey);
        ActivitiErrorEvent event = mock(ActivitiErrorEvent.class,
                withSettings().extraInterfaces(ActivitiEntityEvent.class));
        when(event.getType()).thenReturn(ActivitiEventType.ACTIVITY_ERROR_RECEIVED);
        when(event.getActivityId()).thenReturn(activityId);
        when(event.getActivityName()).thenReturn(activityName);
        when(event.getProcessDefinitionId()).thenReturn(processDefinitionId);
        when(event.getErrorCode()).thenReturn(errorCode);
        when(((ActivitiEntityEvent) event).getEntity()).thenReturn(execution);
        return event;
    }

    private static <T> ObjectProvider<T> providerOf(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }

            @Override
            public Stream<T> stream() {
                return value == null ? Stream.empty() : Stream.of(value);
            }
        };
    }

    /** Records every callback — stands in for the {@code @Primary} composite. */
    private static final class RecordingCallback implements SandboxLogCallback {
        private final List<SandboxLogRecord> reports = new ArrayList<>();
        private final List<SandboxStartRecord> started = new ArrayList<>();

        @Override
        public void report(SandboxLogRecord record) {
            reports.add(record);
        }

        @Override
        public void reportStarted(SandboxStartRecord record) {
            started.add(record);
        }
    }
}
