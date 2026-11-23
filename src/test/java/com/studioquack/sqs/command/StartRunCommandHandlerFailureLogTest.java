package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.runtime.BeRuntimeEpoch;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.files.StartRunFileIngest;
import com.studioquack.sqs.status.RunLogModeRegistry;
import com.studioquack.sqs.status.RunStatusCallback;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class StartRunCommandHandlerFailureLogTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void nodeLibraryFailure_emitsFailedSandboxLogAndStatus() {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.latestDefinitionExists("Process_wf-1__draft_abc")).thenReturn(true);
        DeployedArtifactStore store = mock(DeployedArtifactStore.class);
        NodeDto scriptNode = new NodeDto();
        scriptNode.setId("n1");
        TemplateDto t = new TemplateDto();
        t.setBpmnType("service.task");
        t.setJobType("custom-script-executor");
        scriptNode.setData(t);
        when(store.loadDeployedGraph("wf-1__draft_abc"))
                .thenReturn(Optional.of(new DeployedArtifactStore.DeployedGraph(List.of(scriptNode), List.of())));

        BuiltinNodeLibraryManager lib = mock(BuiltinNodeLibraryManager.class);
        when(lib.isUsable()).thenReturn(false);
        when(lib.startRunBlockReason()).thenReturn("Podman is not running");

        RunStatusCallback runStatus = mock(RunStatusCallback.class);
        SandboxLogCallback sandboxLog = mock(SandboxLogCallback.class);
        StartRunCommandHandler handler = new StartRunCommandHandler(
                mapper, engine, mock(ExecutionContextStore.class), new BeRuntimeEpoch(),
                runStatus, mock(WorkflowDeploymentService.class), store,
                mock(StartRunFileIngest.class), new RunLogModeRegistry(), lib);
        handler.setSandboxLogCallback(sandboxLog);
        handler.deployWait = Duration.ofMillis(100);

        String payload = """
                {"workflowExecutionId":"run-err-1","processKey":"Process_wf-1__draft_abc",
                 "seedContext":{"workflowExecutionId":"run-err-1","workflowId":"wf-1","inDesign":true},
                 "logMode":"STREAM"}
                """;
        DaemonCommandOutcome outcome = handler.handle(new DaemonCommand("c1", "start-run", "2026-11-22T10:00:00Z", payload));

        assertFalse(outcome.succeeded());
        ArgumentCaptor<SandboxLogCallback.SandboxLogRecord> captor =
                ArgumentCaptor.forClass(SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLog).report(captor.capture());
        assertEquals("run-err-1", captor.getValue().workflowExecutionId());
        assertEquals("FAILED", captor.getValue().status());
        assertEquals("INFRA", captor.getValue().failureKind());
        assertTrue(captor.getValue().errorMessage().contains("Podman is not running"));
        verify(runStatus).report(eq("run-err-1"), eq(RunStatusCallback.Status.FAILED), any());
    }

    @Test
    void pureBpmnNoneStartEnd_startsWithoutRequiringNodeLibrary() {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.latestDefinitionExists("Process_wf-2__draft_xyz")).thenReturn(true);
        ProcessInstance pi = mock(ProcessInstance.class);
        when(pi.getId()).thenReturn("pi-1");
        when(pi.getProcessDefinitionId()).thenReturn("Process_wf-2__draft_xyz:1:abc");
        when(engine.startLatest(eq("Process_wf-2__draft_xyz"), eq("run-pure-1"), any())).thenReturn(pi);

        DeployedArtifactStore store = mock(DeployedArtifactStore.class);
        NodeDto start = new NodeDto();
        start.setId("s1");
        TemplateDto st = new TemplateDto();
        st.setBpmnType("start.event");
        start.setData(st);
        NodeDto end = new NodeDto();
        end.setId("e1");
        TemplateDto et = new TemplateDto();
        et.setBpmnType("end.event");
        end.setData(et);
        when(store.loadDeployedGraph("wf-2__draft_xyz"))
                .thenReturn(Optional.of(new DeployedArtifactStore.DeployedGraph(List.of(start, end), List.of())));
        when(store.loadDeployVariables(any())).thenReturn(Optional.empty());

        ExecutionContextStore ctxStore = mock(ExecutionContextStore.class);
        when(ctxStore.snapshot(any(), any())).thenReturn(new com.studioquack.common.dto.flow.ProcessContextRef("ref-1", "run-pure-1", "wf-2", "sha", 1));

        BuiltinNodeLibraryManager lib = mock(BuiltinNodeLibraryManager.class);
        when(lib.isUsable()).thenReturn(false);
        when(lib.startRunBlockReason()).thenReturn("Podman is not running");

        RunStatusCallback runStatus = mock(RunStatusCallback.class);
        StartRunCommandHandler handler = new StartRunCommandHandler(
                mapper, engine, ctxStore, new BeRuntimeEpoch(),
                runStatus, mock(WorkflowDeploymentService.class), store,
                mock(StartRunFileIngest.class), new RunLogModeRegistry(), lib);
        handler.deployWait = Duration.ofMillis(100);

        String payload = """
                {"workflowExecutionId":"run-pure-1","processKey":"Process_wf-2__draft_xyz",
                 "seedContext":{"workflowExecutionId":"run-pure-1","workflowId":"wf-2","inDesign":true},
                 "logMode":"STREAM"}
                """;
        DaemonCommandOutcome outcome = handler.handle(new DaemonCommand("c2", "start-run", "2026-11-22T10:00:00Z", payload));

        assertTrue(outcome.succeeded());
        verify(engine).startLatest(eq("Process_wf-2__draft_xyz"), eq("run-pure-1"), any());
        verify(lib, never()).awaitUsable(any());
    }
}
