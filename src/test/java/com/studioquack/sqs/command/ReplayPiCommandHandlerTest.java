package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.WorkflowReplayService;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code replay-pi} across both run states: a RUNNING instance is read → deleted (with the
 * replay reason the status bridge recognises) → replayed; an ENDED run is replayed straight from
 * history (business key + definition key + historic seed ref) with NO delete; a run unknown to
 * both runtime and history fails.
 */
class ReplayPiCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SqsEngine engine = mock(SqsEngine.class);
    private final WorkflowReplayService replayService = mock(WorkflowReplayService.class);
    // Unstubbed → Optional.empty(): no seed row identifiable, so the existing tests exercise the
    // WARN fallback (engine-variable read) — the pre-change behaviour.
    private final ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
    private final ReplayPiCommandHandler handler =
            new ReplayPiCommandHandler(mapper, engine, replayService, contextStore);

    private static ProcessInstance runningInstance(String pid, String businessKey, String definitionId) {
        ProcessInstance instance = mock(ProcessInstance.class);
        when(instance.getId()).thenReturn(pid);
        when(instance.getBusinessKey()).thenReturn(businessKey);
        when(instance.getProcessDefinitionId()).thenReturn(definitionId);
        return instance;
    }

    private void definitionKey(String definitionId, String key) {
        ProcessDefinition def = mock(ProcessDefinition.class);
        when(def.getKey()).thenReturn(key);
        when(engine.findProcessDefinition(definitionId)).thenReturn(def);
    }

    @Test
    void runningInstance_isDeletedWithReplayReasonThenReplayed() throws Exception {
        ProcessContextRef seedRef = new ProcessContextRef();
        seedRef.setCurrentNodeRefId("ref-seed");
        when(engine.resolveProcessInstanceId("pi-run")).thenReturn("pi-run");
        ProcessInstance running = runningInstance("pi-run", "wfe-1", "def-1");
        when(engine.findProcessInstance("pi-run")).thenReturn(running);
        definitionKey("def-1", "Process_wf1");
        when(engine.getProcessVariable("pi-run", EngineVariableNames.STUDIOQUACK_REF)).thenReturn(seedRef);
        ProcessInstance replayed = runningInstance("pi-new", "wfe-1", "def-2");
        when(replayService.replay("Process_wf1", "wfe-1", seedRef)).thenReturn(replayed);

        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-run\"}"));

        assertTrue(out.succeeded(), () -> "expected ok, got " + out.error());
        verify(engine).deleteProcessInstance("pi-run", ReplayPiCommandHandler.REPLAY_DELETE_REASON);
        verify(replayService).replay("Process_wf1", "wfe-1", seedRef);
    }

    @Test
    void endedRun_isReplayedFromHistory_withoutDelete() throws Exception {
        ProcessContextRef seedRef = new ProcessContextRef();
        seedRef.setCurrentNodeRefId("ref-seed-hist");
        when(engine.resolveProcessInstanceId("wfe-ended")).thenReturn("wfe-ended");
        when(engine.findProcessInstance("wfe-ended")).thenReturn(null); // no ACTIVE instance
        HistoricProcessInstance historic = mock(HistoricProcessInstance.class);
        when(historic.getId()).thenReturn("pi-hist");
        when(historic.getBusinessKey()).thenReturn("wfe-ended");
        when(historic.getProcessDefinitionId()).thenReturn("def-old");
        when(engine.findLatestHistoricProcessInstance("wfe-ended")).thenReturn(historic);
        definitionKey("def-old", "Process_wf2");
        // Historic variables may come back as the JSON map form — the same normalisation the
        // runtime read applies must coerce it.
        when(engine.getHistoricProcessVariable("pi-hist", EngineVariableNames.STUDIOQUACK_REF))
                .thenReturn(Map.of("currentNodeRefId", "ref-seed-hist", "executionId", "exec-1"));
        ProcessInstance replayed = runningInstance("pi-new", "wfe-ended", "def-2");
        when(replayService.replay(eq("Process_wf2"), eq("wfe-ended"),
                org.mockito.ArgumentMatchers.argThat(r -> r != null
                        && "ref-seed-hist".equals(r.getCurrentNodeRefId())))).thenReturn(replayed);

        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"wfe-ended\"}"));

        assertTrue(out.succeeded(), () -> "expected ok, got " + out.error());
        verify(engine, never()).deleteProcessInstance(anyString(), anyString());
    }

    @Test
    void runningInstance_replaysFromTheStoreSeedRef_notTheCurrentInstanceRef() throws Exception {
        // A run held at hold_<X> carries X's failed-dispatch ref as its current studioquackRef —
        // the replay must restart from the run's SEED row (first execution_context row of the
        // business key), never from that mid-run ref.
        ProcessContextRef seedRef = new ProcessContextRef();
        seedRef.setCurrentNodeRefId("node_seed");
        when(contextStore.seedRefOf("wfe-1")).thenReturn(Optional.of(seedRef));
        ProcessContextRef failedNodeRef = new ProcessContextRef();
        failedNodeRef.setCurrentNodeRefId("node_X_failed_attempt");
        when(engine.resolveProcessInstanceId("pi-run")).thenReturn("pi-run");
        ProcessInstance running = runningInstance("pi-run", "wfe-1", "def-1");
        when(engine.findProcessInstance("pi-run")).thenReturn(running);
        definitionKey("def-1", "Process_wf1");
        when(engine.getProcessVariable("pi-run", EngineVariableNames.STUDIOQUACK_REF))
                .thenReturn(failedNodeRef);
        ProcessInstance replayed = runningInstance("pi-new", "wfe-1", "def-2");
        when(replayService.replay("Process_wf1", "wfe-1", seedRef)).thenReturn(replayed);

        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-run\"}"));

        assertTrue(out.succeeded(), () -> "expected ok, got " + out.error());
        verify(replayService).replay("Process_wf1", "wfe-1", seedRef);
    }

    @Test
    void endedRun_replaysFromTheStoreSeedRef_notTheHistoricRef() throws Exception {
        // ENDED runs: the seed row outlives the run, so the historic studioquackRef variable
        // (equally the failed node's ref on a held-then-ended run) is never consulted.
        ProcessContextRef seedRef = new ProcessContextRef();
        seedRef.setCurrentNodeRefId("node_seed");
        when(contextStore.seedRefOf("wfe-ended")).thenReturn(Optional.of(seedRef));
        when(engine.resolveProcessInstanceId("wfe-ended")).thenReturn("wfe-ended");
        when(engine.findProcessInstance("wfe-ended")).thenReturn(null);
        HistoricProcessInstance historic = mock(HistoricProcessInstance.class);
        when(historic.getId()).thenReturn("pi-hist");
        when(historic.getBusinessKey()).thenReturn("wfe-ended");
        when(historic.getProcessDefinitionId()).thenReturn("def-old");
        when(engine.findLatestHistoricProcessInstance("wfe-ended")).thenReturn(historic);
        definitionKey("def-old", "Process_wf2");
        ProcessInstance replayed = runningInstance("pi-new", "wfe-ended", "def-2");
        when(replayService.replay("Process_wf2", "wfe-ended", seedRef)).thenReturn(replayed);

        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"wfe-ended\"}"));

        assertTrue(out.succeeded(), () -> "expected ok, got " + out.error());
        verify(replayService).replay("Process_wf2", "wfe-ended", seedRef);
        verify(engine, never()).getHistoricProcessVariable(anyString(), anyString());
    }

    @Test
    void unknownToRuntimeAndHistory_fails() {
        when(engine.resolveProcessInstanceId("pi-gone")).thenReturn("pi-gone");
        when(engine.findProcessInstance("pi-gone")).thenReturn(null);
        when(engine.findLatestHistoricProcessInstance("pi-gone")).thenReturn(null);

        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-gone\"}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("running or ended"),
                "the failure must say neither runtime nor history knows the run");
        verify(engine, never()).deleteProcessInstance(anyString(), anyString());
    }

    private static DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-replay", ReplayPiCommandHandler.TYPE, null, payload);
    }
}
