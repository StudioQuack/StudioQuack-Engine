package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.store.OperatorInputOverrideStore;
import org.activiti.engine.runtime.DeadLetterJobQuery;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code apply-inputs} on the operator-override store: the edited inbound list is sealed under
 * {@code (workflowExecutionId, nodeId)} so it survives Retry AND Replay. The failed node X is
 * resolved off the instance's dead-letter job parked at {@code hold_<X>}; with no held node the
 * single {@code targetNodeId} shared by every input row is the fallback; anything else refuses.
 */
class ApplyInputsCommandHandlerTest {

    private static final String WFE = "wfe-1";

    private final ObjectMapper mapper = new ObjectMapper();

    /** Engine mock whose addressed instance is ACTIVE (apply-inputs refuses ended runs). */
    private static SqsEngine liveEngine(String pid) {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.resolveProcessInstanceId(pid)).thenReturn(pid);
        ProcessInstance instance = mock(ProcessInstance.class);
        when(instance.getBusinessKey()).thenReturn(WFE);
        when(engine.findProcessInstance(pid)).thenReturn(instance);
        // Default: no dead-letter jobs (no held failed step).
        DeadLetterJobQuery noJobs = mock(DeadLetterJobQuery.class);
        when(noJobs.list()).thenReturn(List.of());
        when(engine.deadLetterJobsForProcessInstance(pid)).thenReturn(noJobs);
        return engine;
    }

    /** Pins one dead-letter job whose execution parks at {@code hold_<nodeId>}. */
    private static void holdAt(SqsEngine engine, String pid, String nodeId) {
        Job job = mock(Job.class);
        when(job.getExecutionId()).thenReturn("exec-held");
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        when(query.list()).thenReturn(List.of(job));
        when(engine.deadLetterJobsForProcessInstance(pid)).thenReturn(query);
        when(engine.findExecutionActivityId("exec-held")).thenReturn("hold_" + nodeId);
    }

    private static InOutValueDto input(String targetNodeId, String portValue) {
        InOutValueDto dto = new InOutValueDto();
        dto.setTargetNodeId(targetNodeId);
        dto.setPortValue(portValue);
        return dto;
    }

    @Test
    void applyInputs_storesOverrideForTheHeldNode() {
        SqsEngine engine = liveEngine("pi-a");
        holdAt(engine, "pi-a", "nodeX");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"inputs\":[{\"portValue\":\"x\"}]}"));

        assertTrue(out.succeeded());
        verify(store).saveOverride(eq(WFE), eq("nodeX"), org.mockito.ArgumentMatchers.argThat(
                list -> list != null && list.size() == 1 && "x".equals(list.get(0).getPortValue())));
    }

    @Test
    void applyInputs_emptyListIsAValidOverride() {
        // The operator really cleared every input — the empty list must reach the store untouched.
        SqsEngine engine = liveEngine("pi-a");
        holdAt(engine, "pi-a", "nodeX");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-a\",\"inputs\":[]}"));

        assertTrue(out.succeeded());
        verify(store).saveOverride(WFE, "nodeX", List.of());
    }

    @Test
    void applyInputs_fallsBackToTheSingleTargetNodeId_whenNothingIsHeld() {
        SqsEngine engine = liveEngine("pi-a");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"inputs\":["
                        + "{\"targetNodeId\":\"nodeY\",\"portValue\":\"a\"},"
                        + "{\"targetNodeId\":\"nodeY\",\"portValue\":\"b\"}]}"));

        assertTrue(out.succeeded());
        verify(store).saveOverride(eq(WFE), eq("nodeY"), anyList());
    }

    @Test
    void applyInputs_refusesWhenNoHeldStepAndNoSingleTarget() {
        SqsEngine engine = liveEngine("pi-a");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"inputs\":["
                        + "{\"targetNodeId\":\"nodeY\",\"portValue\":\"a\"},"
                        + "{\"targetNodeId\":\"nodeZ\",\"portValue\":\"b\"}]}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("no failed step is held on run pi-a"), out.error());
        verify(store, never()).saveOverride(anyString(), anyString(), anyList());
    }

    @Test
    void applyInputs_refusesWhenNoHeldStepAndInputsAreUnaddressed() {
        SqsEngine engine = liveEngine("pi-a");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"inputs\":[{\"portValue\":\"x\"}]}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("no failed step is held on run pi-a"), out.error());
        verify(store, never()).saveOverride(anyString(), anyString(), anyList());
    }

    @Test
    void applyInputs_refusesEndedRun_beforeTouchingJobsOrStore() {
        SqsEngine engine = mock(SqsEngine.class);
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);
        when(engine.resolveProcessInstanceId("pi-ended")).thenReturn("pi-ended");
        when(engine.findProcessInstance("pi-ended")).thenReturn(null);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-ended\",\"inputs\":[{\"portValue\":\"x\"}]}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("pi-ended has ended")
                        && out.error().contains("held at a failed step"),
                "ended-run refusal must say parameters only change on a held run");
        verify(engine, never()).deadLetterJobsForProcessInstance(anyString());
        verify(store, never()).saveOverride(anyString(), anyString(), anyList());
    }

    @Test
    void applyInputs_resultPayloadNamesRunNodeAndCount() {
        SqsEngine engine = liveEngine("pi-a");
        holdAt(engine, "pi-a", "nodeX");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"inputs\":[{\"portValue\":\"x\"}]}"));

        assertTrue(out.succeeded());
        assertEquals("{\"processInstanceId\":\"pi-a\",\"nodeId\":\"nodeX\",\"inputCount\":1}",
                out.output());
    }

    @Test
    void applyInputs_outputsOnly_storesTheOutputOverride() {
        SqsEngine engine = liveEngine("pi-a");
        holdAt(engine, "pi-a", "nodeX");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"outputs\":[{\"portValue\":\"result\",\"value\":42}]}"));

        assertTrue(out.succeeded());
        verify(store).saveOutputsOverride(eq(WFE), eq("nodeX"), org.mockito.ArgumentMatchers.argThat(
                list -> list != null && list.size() == 1 && "result".equals(list.get(0).getPortValue())));
        // The inputs lane is untouched — an absent lane keeps its previously stored value.
        verify(store, never()).saveOverride(anyString(), anyString(), anyList());
        assertEquals("{\"processInstanceId\":\"pi-a\",\"nodeId\":\"nodeX\",\"outputCount\":1}",
                out.output());
    }

    @Test
    void applyInputs_inputsAndOutputs_storesBothLanesOnTheSameRow() {
        SqsEngine engine = liveEngine("pi-a");
        holdAt(engine, "pi-a", "nodeX");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\","
                        + "\"inputs\":[{\"portValue\":\"prompt\",\"value\":\"v\"}],"
                        + "\"outputs\":[{\"portValue\":\"result\",\"value\":1},"
                        + "{\"portValue\":\"extra\",\"value\":2}]}"));

        assertTrue(out.succeeded());
        verify(store).saveOverride(eq(WFE), eq("nodeX"), anyList());
        verify(store).saveOutputsOverride(eq(WFE), eq("nodeX"), org.mockito.ArgumentMatchers.argThat(
                list -> list != null && list.size() == 2));
        assertEquals("{\"processInstanceId\":\"pi-a\",\"nodeId\":\"nodeX\",\"inputCount\":1,"
                + "\"outputCount\":2}", out.output());
    }

    @Test
    void applyInputs_refusesWhenBothLanesAreAbsent() {
        SqsEngine engine = liveEngine("pi-a");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-a\"}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("requires inputs and/or outputs"), out.error());
        verify(store, never()).saveOverride(anyString(), anyString(), anyList());
        verify(store, never()).saveOutputsOverride(anyString(), anyString(), anyList());
    }

    @Test
    void applyInputs_outputsOnly_fallsBackToTheSingleTargetNodeId_whenNothingIsHeld() {
        SqsEngine engine = liveEngine("pi-a");
        OperatorInputOverrideStore store = mock(OperatorInputOverrideStore.class);

        ApplyInputsCommandHandler handler = new ApplyInputsCommandHandler(mapper, engine, store);
        DaemonCommandOutcome out = handler.handle(cmd(
                "{\"processInstanceId\":\"pi-a\",\"outputs\":["
                        + "{\"targetNodeId\":\"nodeY\",\"portValue\":\"result\",\"value\":1}]}"));

        assertTrue(out.succeeded());
        verify(store).saveOutputsOverride(eq(WFE), eq("nodeY"), anyList());
    }

    private static DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-apply", ApplyInputsCommandHandler.TYPE, null, payload);
    }
}
