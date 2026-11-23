package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.runtime.HeldJobRequeue;
import com.studioquack.sqs.runtime.EngineTriggerService;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.CallActivity;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.ServiceTask;
import org.activiti.engine.runtime.DeadLetterJobQuery;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Repair — {@code skip-step}. Pins the unchanged parked-at-{@code wait_X} path (a direct {@code
 * SQ_SKIP} BpmnError through the error gate) and the held-at-{@code hold_X} Skip &amp; Continue:
 * pin {@code __sq_skip_node} on the instance, requeue the hold's dead-letter job, and let the
 * re-dispatch consume the pin.
 */
class SkipStepCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final EngineTriggerService triggerService = mock(EngineTriggerService.class);

    private static SqsEngine liveEngine(String pid) {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.resolveProcessInstanceId(pid)).thenReturn(pid);
        ProcessInstance instance = mock(ProcessInstance.class);
        when(instance.getProcessDefinitionId()).thenReturn("def-1");
        when(engine.findProcessInstance(pid)).thenReturn(instance);
        return engine;
    }

    private static Job deadLetter(String jobId, String executionId) {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.getExecutionId()).thenReturn(executionId);
        return job;
    }

    /** A deployed model whose {@code node_<id>} element is the given type (ServiceTask / CallActivity). */
    private static BpmnModel modelWithNode(String nodeId, boolean serviceTask) {
        BpmnModel model = new BpmnModel();
        Process process = new Process();
        process.setId("Process_test");
        model.addProcess(process);
        if (serviceTask) {
            ServiceTask task = new ServiceTask();
            task.setId("node_" + nodeId);
            process.addFlowElement(task);
        } else {
            CallActivity call = new CallActivity();
            call.setId("node_" + nodeId);
            process.addFlowElement(call);
        }
        return model;
    }

    /** A run held at {@code hold_<nodeId>}: one dead-letter job whose execution parks there. */
    private static void stubHold(SqsEngine engine, String pid, String nodeId,
                                 Job job, BpmnModel model) {
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        when(engine.deadLetterJobsForProcessInstance(pid)).thenReturn(query);
        when(query.list()).thenReturn(List.of(job));
        when(engine.findExecutionActivityId(job.getExecutionId())).thenReturn("hold_" + nodeId);
        when(engine.readDeployedModel("def-1")).thenReturn(model);
    }

    @Test
    void parkedWait_still_raises_skip_error_through_the_gate() {
        SqsEngine engine = liveEngine("pi-a");
        when(engine.findUnfinishedWaitExecution("pi-a", "wait_n1")).thenReturn("exec-w");

        SkipStepCommandHandler handler =
                new SkipStepCommandHandler(mapper, engine, triggerService, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(
                cmd("{\"processInstanceId\":\"pi-a\",\"targetFlowNodeId\":\"n1\"}"));

        assertTrue(out.succeeded());
        verify(triggerService).fail(eq("exec-w"), eq(EngineVariableNames.SKIP_ERROR_CODE), anyString());
        // The parked path never touches the hold machinery.
        verify(engine, never()).deadLetterJobsForProcessInstance(anyString());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
        verify(engine, never()).setProcessVariables(anyString(), any());
    }

    @Test
    void heldRun_pins_skip_node_and_requeues_the_hold() {
        SqsEngine engine = liveEngine("pi-a");
        when(engine.findUnfinishedWaitExecution("pi-a", "wait_n1")).thenReturn(null);
        Job holdJob = deadLetter("job-h", "exec-h");
        stubHold(engine, "pi-a", "n1", holdJob, modelWithNode("n1", true));

        SkipStepCommandHandler handler =
                new SkipStepCommandHandler(mapper, engine, triggerService, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(
                cmd("{\"processInstanceId\":\"pi-a\",\"targetFlowNodeId\":\"n1\"}"));

        assertTrue(out.succeeded(), () -> "expected success, got: " + out.error());
        assertTrue(out.output().contains("\"resumedFromHold\":true"),
                () -> "the result must say the run resumed via the hold: " + out.output());
        // The pin is set BEFORE the requeue: the async executor must never see the hold pass
        // without the skip pin in place.
        var order = inOrder(engine);
        order.verify(engine).setProcessVariables("pi-a",
                Map.of(EngineVariableNames.SKIP_NODE, "n1"));
        // The shared requeue scrubs the failure sentinels, then moves the job back to executable.
        order.verify(engine).removeVariables("exec-h", EngineVariableNames.ERROR_SENTINELS);
        order.verify(engine).retryDeadLetterJob("job-h", 1);
        verify(triggerService, never()).fail(anyString(), anyString(), anyString());
    }

    @Test
    void heldRun_without_skip_boundary_target_call_activity_is_refused() {
        // A call activity's node_<id> element is a CallActivity, not a ServiceTask: the hold's
        // retry target is the sub-workflow CALL, which Skip & Continue does not support.
        SqsEngine engine = liveEngine("pi-a");
        when(engine.findUnfinishedWaitExecution("pi-a", "wait_sub1")).thenReturn(null);
        Job holdJob = deadLetter("job-h", "exec-h");
        stubHold(engine, "pi-a", "sub1", holdJob, modelWithNode("sub1", false));

        SkipStepCommandHandler handler =
                new SkipStepCommandHandler(mapper, engine, triggerService, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(
                cmd("{\"processInstanceId\":\"pi-a\",\"targetFlowNodeId\":\"sub1\"}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("not supported for sub-workflow calls")
                        && out.error().contains("use Retry or Replay"),
                () -> "call-activity refusal must point at Retry/Replay: " + out.error());
        verify(engine, never()).setProcessVariables(anyString(), any());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
    }

    @Test
    void endedRun_is_refused_with_replay_hint() {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.resolveProcessInstanceId("pi-ended")).thenReturn("pi-ended");
        when(engine.findUnfinishedWaitExecution("pi-ended", "wait_n1")).thenReturn(null);
        when(engine.findProcessInstance("pi-ended")).thenReturn(null); // no ACTIVE instance

        SkipStepCommandHandler handler =
                new SkipStepCommandHandler(mapper, engine, triggerService, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(
                cmd("{\"processInstanceId\":\"pi-ended\",\"targetFlowNodeId\":\"n1\"}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("pi-ended has ended")
                        && out.error().contains("use Replay"),
                () -> "ended-run refusal must point at Replay: " + out.error());
        verify(engine, never()).deadLetterJobsForProcessInstance(anyString());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
    }

    @Test
    void no_parked_wait_and_no_matching_hold_is_a_clear_failure() {
        SqsEngine engine = liveEngine("pi-a");
        when(engine.findUnfinishedWaitExecution("pi-a", "wait_n1")).thenReturn(null);
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        when(engine.deadLetterJobsForProcessInstance("pi-a")).thenReturn(query);
        when(query.list()).thenReturn(List.of()); // no dead-letter jobs on the instance

        SkipStepCommandHandler handler =
                new SkipStepCommandHandler(mapper, engine, triggerService, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(
                cmd("{\"processInstanceId\":\"pi-a\",\"targetFlowNodeId\":\"n1\"}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("wait_n1") && out.error().contains("hold_n1"),
                () -> "the failure must name both places it looked: " + out.error());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
    }

    private static DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-skip", SkipStepCommandHandler.TYPE, null, payload);
    }
}
