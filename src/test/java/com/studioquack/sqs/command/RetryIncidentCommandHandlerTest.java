package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.runtime.HeldJobRequeue;
import org.activiti.engine.runtime.DeadLetterJobQuery;
import org.activiti.engine.runtime.Job;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deny path: {@code retry-incident} with an explicit {@code jobId} that matches no dead-letter job
 * on the addressed process instance is refused and moves nothing. Also pins the failure-sentinel
 * scrub before the move and the ended-run refusal.
 */
class RetryIncidentCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private static SqsEngine liveEngine(String pid) {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.resolveProcessInstanceId(pid)).thenReturn(pid);
        when(engine.findProcessInstance(pid)).thenReturn(mock(ProcessInstance.class));
        return engine;
    }

    private static Job deadLetter(String jobId, String executionId) {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.getExecutionId()).thenReturn(executionId);
        return job;
    }

    @Test
    void retryOne_refuses_jobId_from_another_instance() {
        SqsEngine engine = liveEngine("pi-a");
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        when(engine.deadLetterJobsForProcessInstance("pi-a")).thenReturn(query);
        when(query.jobId("job-b")).thenReturn(query);
        when(query.singleResult()).thenReturn(null);
        when(query.list()).thenReturn(List.of()); // no dead-letter jobs of its own either

        RetryIncidentCommandHandler handler =
                new RetryIncidentCommandHandler(mapper, engine, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-a\",\"jobId\":\"job-b\"}"));

        assertFalse(out.succeeded());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
    }

    @Test
    void retryOne_unmatchedJobId_is_refused_and_moves_nothing() {
        // An id matching no dead-letter job on the addressed instance (missing or foreign) fails
        // the command and never moves any job — no fallback to the instance's own jobs.
        SqsEngine engine = liveEngine("pi-a");
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        Job own = deadLetter("job-own", "exec-own");
        when(engine.deadLetterJobsForProcessInstance("pi-a")).thenReturn(query);
        when(query.jobId("incident-xyz")).thenReturn(query);
        when(query.singleResult()).thenReturn(null);
        when(query.list()).thenReturn(List.of(own));

        RetryIncidentCommandHandler handler =
                new RetryIncidentCommandHandler(mapper, engine, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(
                cmd("{\"processInstanceId\":\"pi-a\",\"jobId\":\"incident-xyz\"}"));

        assertFalse(out.succeeded());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
    }

    @Test
    void retryOne_moves_job_that_belongs_to_the_instance_after_scrubbing_sentinels() {
        SqsEngine engine = liveEngine("pi-a");
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        Job job = deadLetter("job-a", "exec-a");
        when(engine.deadLetterJobsForProcessInstance("pi-a")).thenReturn(query);
        when(query.jobId("job-a")).thenReturn(query);
        when(query.singleResult()).thenReturn(job);

        RetryIncidentCommandHandler handler =
                new RetryIncidentCommandHandler(mapper, engine, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-a\",\"jobId\":\"job-a\"}"));

        assertTrue(out.succeeded());
        // The four failure sentinels must be scrubbed BEFORE the job moves back, or the held run
        // re-holds instead of re-dispatching the failed node.
        var order = inOrder(engine);
        order.verify(engine).removeVariables("exec-a", EngineVariableNames.ERROR_SENTINELS);
        order.verify(engine).retryDeadLetterJob("job-a", 1);
    }

    @Test
    void retryAll_scrubs_sentinels_on_every_job() {
        SqsEngine engine = liveEngine("pi-a");
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        Job first = deadLetter("job-1", "exec-1");
        Job second = deadLetter("job-2", "exec-2");
        when(engine.deadLetterJobsForProcessInstance("pi-a")).thenReturn(query);
        when(query.list()).thenReturn(List.of(first, second));

        RetryIncidentCommandHandler handler =
                new RetryIncidentCommandHandler(mapper, engine, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-a\"}"));

        assertTrue(out.succeeded());
        verify(engine).removeVariables("exec-1", EngineVariableNames.ERROR_SENTINELS);
        verify(engine).removeVariables("exec-2", EngineVariableNames.ERROR_SENTINELS);
        verify(engine).retryDeadLetterJob("job-1", 1);
        verify(engine).retryDeadLetterJob("job-2", 1);
    }

    @Test
    void retryOne_pinsRetryFlagForHeldNodeBeforeRequeue() {
        SqsEngine engine = liveEngine("pi-a");
        DeadLetterJobQuery query = mock(DeadLetterJobQuery.class);
        Job job = deadLetter("job-a", "exec-a");
        when(engine.deadLetterJobsForProcessInstance("pi-a")).thenReturn(query);
        when(query.jobId("job-a")).thenReturn(query);
        when(query.singleResult()).thenReturn(job);
        when(engine.findExecutionActivityId("exec-a")).thenReturn("hold_node-42");

        RetryIncidentCommandHandler handler =
                new RetryIncidentCommandHandler(mapper, engine, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-a\",\"jobId\":\"job-a\"}"));

        assertTrue(out.succeeded());
        var order = inOrder(engine);
        order.verify(engine).setProcessVariables("pi-a",
                Map.of(EngineVariableNames.retryNodeVariableName("node-42"), Boolean.TRUE));
        order.verify(engine).removeVariables("exec-a", EngineVariableNames.ERROR_SENTINELS);
        order.verify(engine).retryDeadLetterJob("job-a", 1);
    }

    @Test
    void retry_refuses_ended_run_with_replay_hint() {
        SqsEngine engine = mock(SqsEngine.class);
        when(engine.resolveProcessInstanceId("pi-ended")).thenReturn("pi-ended");
        when(engine.findProcessInstance("pi-ended")).thenReturn(null); // no ACTIVE instance

        RetryIncidentCommandHandler handler =
                new RetryIncidentCommandHandler(mapper, engine, new HeldJobRequeue(engine));
        DaemonCommandOutcome out = handler.handle(cmd("{\"processInstanceId\":\"pi-ended\"}"));

        assertFalse(out.succeeded());
        assertTrue(out.error().contains("pi-ended has ended")
                && out.error().contains("use Replay"), "ended-run refusal must point at Replay");
        verify(engine, never()).deadLetterJobsForProcessInstance(anyString());
        verify(engine, never()).retryDeadLetterJob(anyString(), anyInt());
    }

    private static DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-retry", RetryIncidentCommandHandler.TYPE, null, payload);
    }
}
