package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Proves a stateful dispatch routes to the LIVE container (no new container): the launcher is
 * exec'd IN the instance's container, the per-request helper channel is opened for the request and
 * revoked after, and the app's response is mapped to output variables. Also pins the FIFO overload
 * policy (reject-loud, never silent drop).
 */
class StatefulSandboxExecutionTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private HelperRequestRouter helper;
    private StatefulInstanceManager instances;
    private StatefulRuntimeProperties props;
    private com.studioquack.sqs.runtime.SubProcessCompletionRegistry subProcessRegistry;
    private StatefulSandboxExecution exec;

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        helper = new HelperRequestRouter(mapper,
                new com.studioquack.sqs.aimodel.AiModelProviderProxy(
                        new com.studioquack.sqs.aimodel.AiModelProviderHolder(), mapper), null);
        instances = mock(StatefulInstanceManager.class);
        when(instances.brokerAlive(any())).thenReturn(false);
        when(instances.beginDispatch(any())).thenReturn(true);
        props = new StatefulRuntimeProperties();
        subProcessRegistry = mock(com.studioquack.sqs.runtime.SubProcessCompletionRegistry.class);
        StatefulLauncher launcher = new StatefulLauncher(cli, props);
        exec = new StatefulSandboxExecution(helper, instances, launcher, new Redactor(), mapper, props,
                subProcessRegistry);
    }

    private StatefulInstance runningInstance() {
        StatefulInstance inst = new StatefulInstance("inst1", "env-1", "myenv/pg:1",
                "sq-stateful-inst1", "sq-stateful-vol-inst1", StatefulCaps.defaults(), true);
        inst.setContainerId("cid-live-1");
        inst.setStatus(StatefulStatus.RUNNING);
        return inst;
    }

    private SandboxJobRequest statefulJob() {
        return SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1")
                .currentNodeId("n1")
                .workflowExecutionId("wfe-1")
                .ociImageRef("myenv/pg:1")
                .stateful(true)
                .build();
    }

    @Test
    void dead_broker_restart_that_stopped_the_instance_fails_clearly_instead_of_dispatching() {
        StatefulInstance inst = runningInstance();
        when(instances.recycleForImage("env-1", "myenv/pg:1", "cid-live-1")).thenAnswer(inv -> {
            inst.setContainerId(null);
            inst.setStatus(StatefulStatus.STOPPED);
            return new StatefulInstanceManager.RecycleResult(false, "inst1", StatefulStatus.FAILED,
                    "container exited during boot");
        });

        SandboxJobResponse resp = exec.execute(statefulJob(), inst);

        assertFalse(resp.ok());
        assertTrue(resp.errorMessage().contains("could not be restarted: container exited during boot"),
                resp.errorMessage());
        assertFalse(cli.sawCallContaining("exec"), "must not launch into the stopped container");
    }

    private StatefulInstance successorInstance() {
        StatefulInstance inst = new StatefulInstance("inst1", "env-1", "myenv/pg:1",
                "sq-stateful-inst1-b", "sq-stateful-vol-inst1", StatefulCaps.defaults(), true);
        inst.setContainerId("cid-new-1");
        inst.setStatus(StatefulStatus.RUNNING);
        return inst;
    }

    /**
     * A job resolved the old library container, then waited in the FIFO while an update switched
     * routing and drained it. It must run on the successor — never bounce the successor through
     * the dead-broker recovery (that killed every request running in it).
     */
    @Test
    void job_queued_across_a_library_switch_runs_on_the_successor_without_any_restart() {
        StatefulInstance stale = runningInstance();
        stale.setStatus(StatefulStatus.STOPPED);
        stale.setContainerId(null);
        StatefulInstance successor = successorInstance();
        when(instances.resolveRunning("env-1", "myenv/pg:1")).thenReturn(java.util.Optional.of(successor));
        when(instances.brokerAlive(successor)).thenReturn(true);
        cli.responder = argv -> new OciCli.Result(0, "{\"answer\":\"42\"}", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), stale);

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        assertEquals("cid-new-1", resp.containerId());
        assertTrue(cli.sawCallContaining("exec", "cid-new-1"));
        verify(instances, org.mockito.Mockito.never()).recycleForImage(any(), any(), any());
        verify(instances, org.mockito.Mockito.never()).recycleForImage(any(), any());
        verify(instances).endDispatch("cid-new-1");
    }

    /** The drain closed the old container between the routing check and admission: re-route. */
    @Test
    void admission_refused_by_a_closing_drain_reroutes_to_the_successor() {
        StatefulInstance old = runningInstance();
        StatefulInstance successor = successorInstance();
        when(instances.beginDispatch("cid-live-1")).thenReturn(false);
        when(instances.resolveRunning("env-1", "myenv/pg:1")).thenReturn(java.util.Optional.of(successor));
        when(instances.brokerAlive(successor)).thenReturn(true);
        cli.responder = argv -> new OciCli.Result(0, "{\"answer\":\"42\"}", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), old);

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        assertFalse(cli.sawCallContaining("exec", "cid-live-1"), "never launched into the closed container");
        assertTrue(cli.sawCallContaining("exec", "cid-new-1"));
        verify(instances, org.mockito.Mockito.never()).endDispatch("cid-live-1");
    }

    private StatefulInstance temporaryInstance() {
        StatefulInstance container = new StatefulInstance("tmp_abc", "env-1", "myenv/pg:1",
                "sq-tmp-abc", "sq-stateful-vol-tmp_abc", StatefulCaps.defaults(), true, true);
        container.setContainerId("cid-tmp-1");
        container.setStatus(StatefulStatus.RUNNING);
        return container;
    }

    /** A trial job whose temporary container went away fails; it never moves onto the operator's instance. */
    @Test
    void a_stopped_temporary_container_never_falls_back_to_the_instance_of_its_image() {
        StatefulInstance container = temporaryInstance();
        container.setStatus(StatefulStatus.STOPPED);
        container.setContainerId(null);
        when(instances.resolveRunning("env-1", "myenv/pg:1")).thenReturn(java.util.Optional.of(successorInstance()));

        SandboxJobResponse resp = exec.execute(statefulJob(), container);

        assertFalse(resp.ok());
        verify(instances, org.mockito.Mockito.never()).resolveRunning(any(), any());
        assertFalse(cli.sawCallContaining("exec"));
    }

    /** No bounce and no re-route for a temporary container: both would key on the operator's instance. */
    @Test
    void a_temporary_container_is_never_recycled_or_rerouted() {
        StatefulInstance container = temporaryInstance();
        when(instances.beginDispatch("cid-tmp-1")).thenReturn(false);
        when(instances.resolveRunning("env-1", "myenv/pg:1")).thenReturn(java.util.Optional.of(successorInstance()));

        SandboxJobResponse resp = exec.execute(statefulJob(), container);

        assertFalse(resp.ok());
        verify(instances, org.mockito.Mockito.never()).recycleForImage(any(), any(), any());
        verify(instances, org.mockito.Mockito.never()).resolveRunning(any(), any());
        assertFalse(cli.sawCallContaining("exec", "cid-new-1"));
    }

    @Test
    void nothing_routed_after_the_queue_fails_clearly_and_launches_nothing() {
        StatefulInstance stale = runningInstance();
        stale.setStatus(StatefulStatus.STOPPED);
        stale.setContainerId(null);
        when(instances.resolveRunning("env-1", "myenv/pg:1")).thenReturn(java.util.Optional.empty());

        SandboxJobResponse resp = exec.execute(statefulJob(), stale);

        assertFalse(resp.ok());
        assertTrue(resp.errorMessage().contains("replaced or stopped while this dispatch waited"),
                resp.errorMessage());
        assertFalse(cli.sawCallContaining("exec"));
    }

    @Test
    void routes_to_live_container_maps_response_and_closes_channel() {
        cli.responder = argv -> new OciCli.Result(0, "{\"answer\":\"42\",\"note\":\"ok\"}", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        assertEquals("cid-live-1", resp.containerId(), "response carries the LIVE container id");
        assertEquals("42", resp.outputVariables().get("answer"), "app response mapped to outputs");
        assertEquals("ok", resp.outputVariables().get("note"));

        assertTrue(cli.sawCallContaining("exec", "cid-live-1"), "launcher exec targeted the live container");
        List<String> exec1 = cli.firstCallWith("exec");
        assertFalse(exec1.contains("run"), "no new container is started for a stateful dispatch");

        assertEquals(0, helper.activeJobs(), "per-request helper token revoked after the request");
    }

    @Test
    void llm_request_summaries_fold_into_displayed_stdout() {
        cli.responder = argv -> {
            // Mid-request: the router recorded what the node sent to the model.
            try {
                JsonNode req = mapper.readTree(new String(cli.lastStdin, StandardCharsets.UTF_8));
                String token = req.path("helperToken").asText(null);
                if (token != null) {
                    helper.lookup(token).ifPresent(job -> job.logs().add(java.util.Map.of(
                            "kind", "llm-request", "level", "info",
                            "message", "model=gpt-x stream=true messages=1 [user:text(chars=4)]")));
                }
            } catch (Exception ignored) {
                // responder must not fail the launch
            }
            return new OciCli.Result(0, "{\"answer\":\"42\"}", "", false);
        };

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        assertTrue(resp.stdout().contains("[llm] model=gpt-x stream=true"),
                "llm-request summary rides the node log's stdout: " + resp.stdout());
        assertEquals("42", resp.outputVariables().get("answer"),
                "fold happens after output parsing — app response still maps");
    }

    @Test
    void no_llm_request_entries_leave_stdout_untouched() {
        cli.responder = argv -> new OciCli.Result(0, "{\"answer\":\"42\"}", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        assertEquals("{\"answer\":\"42\"}", resp.stdout(), "no llm-request entries → verbatim stdout");
    }

    @Test
    void dispatcher_fail_envelope_is_a_typed_failure_never_bound_to_ports() {
        // The in-instance dispatcher could not hand the request to a node (here: it did not know the
        // templateId). It answers 200 {"__sq_fail":{...}} — that must end the step with the code,
        // not flow downstream as an output named "__sq_fail" or "error".
        cli.responder = argv -> new OciCli.Result(0,
                "{\"__sq_fail\":{\"errorCode\":\"DISPATCH_NO_HANDLER\",\"message\":\"no app registered for templateId 'tpl_x'\"}}",
                "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertFalse(resp.ok(), "dispatcher envelope → failed response");
        assertEquals("DISPATCH_NO_HANDLER", resp.errorCode());
        assertEquals("no app registered for templateId 'tpl_x'", resp.errorMessage());
        assertEquals(SandboxJobResponse.FailureKind.ENGINE, resp.failureKind(), "protocol fault, not user code");
        assertTrue(resp.outputVariables() == null || resp.outputVariables().isEmpty(),
                "nothing from the envelope is bound to output ports");
        assertEquals(0, helper.activeJobs(), "token still revoked on failure");
    }

    @Test
    void dispatcher_fail_envelope_without_code_still_fails_as_dispatch_error() {
        cli.responder = argv -> new OciCli.Result(0, "{\"__sq_fail\":{\"message\":\"boom\"}}", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertFalse(resp.ok());
        assertEquals("DISPATCH_ERROR", resp.errorCode());
    }

    @Test
    void nonzero_launcher_exit_is_a_failure_response() {
        cli.responder = argv -> new OciCli.Result(3, "", "app crashed", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertFalse(resp.ok(), "non-zero launcher exit → failed response");
        assertEquals(SandboxJobResponse.ERROR_SANDBOX_FAILED, resp.errorCode());
        assertEquals("cid-live-1", resp.containerId());
        assertEquals(0, helper.activeJobs(), "token still revoked on failure");
    }

    @Test
    void broker_down_request_json_carries_helperBrokerAbsent_and_helperToken() throws Exception {
        cli.responder = argv -> new OciCli.Result(0, "{\"ok\":true}", "", false);
        when(instances.brokerAlive(any())).thenReturn(false);
        StatefulInstance inst = runningInstance();
        inst.setBrokerNote("image has no usable broker runtime (python3 is required for the "
                + "in-container helper broker); helper callbacks unavailable");

        SandboxJobResponse resp = exec.execute(statefulJob(), inst);

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        JsonNode body = mapper.readTree(new String(cli.lastStdin, StandardCharsets.UTF_8));
        assertTrue(body.hasNonNull("helperToken"), "helperToken still issued when broker is down");
        assertTrue(body.hasNonNull("helperBrokerAbsent"), "cause must travel to the in-instance app");
        assertTrue(body.get("helperBrokerAbsent").asText().contains("python3"),
                body.get("helperBrokerAbsent").asText());
    }

    @Test
    void broker_up_request_json_omits_helperBrokerAbsent() throws Exception {
        cli.responder = argv -> new OciCli.Result(0, "{\"ok\":true}", "", false);
        when(instances.brokerAlive(any())).thenReturn(true);
        StatefulInstance inst = runningInstance();
        inst.setBrokerReady(true);

        SandboxJobResponse resp = exec.execute(statefulJob(), inst);

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        JsonNode body = mapper.readTree(new String(cli.lastStdin, StandardCharsets.UTF_8));
        assertTrue(body.hasNonNull("helperToken"));
        assertFalse(body.has("helperBrokerAbsent"),
                "helperBrokerAbsent must be absent when the broker is up");
    }

    @Test
    void saturated_queue_rejects_immediately_with_sandbox_overloaded() throws Exception {
        props.setStatefulQueueMaxDepth(1);
        props.setStatefulQueueWaitTimeoutMs(5_000L);
        StatefulInstance inst = runningInstance();

        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        cli.responder = argv -> {
            holding.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread holder = new Thread(() -> exec.execute(statefulJob(), inst), "fifo-holder");
        holder.start();
        assertTrue(holding.await(5, TimeUnit.SECONDS), "first dispatch must be in-flight");

        // One waiter saturates maxDepth=1; the next must reject without parking on acquire.
        CountDownLatch waiterStarted = new CountDownLatch(1);
        AtomicReference<SandboxJobResponse> waiterResp = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterStarted.countDown();
            // Block in tryAcquire — occupies the single queue slot.
            waiterResp.set(exec.execute(
                    SandboxJobRequest.builder("job-waiter", SandboxLanguage.PYTHON, "print(1)")
                            .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                    inst));
        }, "fifo-waiter");
        waiter.start();
        assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));
        // Give the waiter a moment to park on the permit.
        Thread.sleep(50);

        long t0 = System.nanoTime();
        SandboxJobResponse rejected = exec.execute(
                SandboxJobRequest.builder("job-reject", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                inst);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        assertFalse(rejected.ok());
        assertEquals("SANDBOX_OVERLOADED", rejected.errorCode());
        assertEquals(SandboxJobResponse.FailureKind.INFRA, rejected.failureKind());
        assertTrue(elapsedMs < 500, "must reject immediately without parking; took " + elapsedMs + "ms");

        release.countDown();
        holder.join(5_000);
        waiter.join(5_000);
        assertTrue(waiterResp.get() == null || waiterResp.get().ok() || "SANDBOX_OVERLOADED".equals(waiterResp.get().errorCode())
                || SandboxJobResponse.ERROR_SANDBOX_FAILED.equals(waiterResp.get().errorCode()));
    }

    @Test
    void wait_timeout_rejects_with_sandbox_overloaded() throws Exception {
        props.setStatefulQueueMaxDepth(64);
        props.setStatefulQueueWaitTimeoutMs(50L);
        StatefulInstance inst = runningInstance();

        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        cli.responder = argv -> {
            holding.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread holder = new Thread(() -> exec.execute(statefulJob(), inst), "fifo-holder-timeout");
        holder.start();
        assertTrue(holding.await(5, TimeUnit.SECONDS));

        SandboxJobResponse timedOut = exec.execute(
                SandboxJobRequest.builder("job-timeout", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                inst);

        assertFalse(timedOut.ok());
        assertEquals("SANDBOX_OVERLOADED", timedOut.errorCode());
        assertEquals(SandboxJobResponse.FailureKind.INFRA, timedOut.failureKind());
        assertTrue(timedOut.errorMessage().contains("timed out waiting"),
                timedOut.errorMessage());

        release.countDown();
        holder.join(5_000);
    }

    @Test
    void streaming_job_waits_past_the_queue_cap() throws Exception {
        // SSE-streaming node (e.g. local-llm): the permit holder may legitimately stream for
        // minutes, so a streaming waiter must NOT die at the 30 s queue cap — it waits
        // (interruptibly) until the holder releases.
        props.setStatefulQueueMaxDepth(64);
        props.setStatefulQueueWaitTimeoutMs(50L);
        StatefulInstance inst = runningInstance();

        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        cli.responder = argv -> {
            holding.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread holder = new Thread(() -> exec.execute(statefulJob(), inst), "fifo-holder-stream");
        holder.start();
        assertTrue(holding.await(5, TimeUnit.SECONDS));

        AtomicReference<SandboxJobResponse> waiterResp = new AtomicReference<>();
        Thread waiter = new Thread(() -> waiterResp.set(exec.execute(
                SandboxJobRequest.builder("job-stream-waiter", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true)
                        .streaming(true).build(),
                inst)), "fifo-stream-waiter");
        waiter.start();

        // Well past the 50 ms cap: a non-streaming waiter would already be SANDBOX_OVERLOADED.
        Thread.sleep(300);
        assertTrue(waiter.isAlive(), "streaming waiter must still be parked, not timed out");
        assertNull(waiterResp.get());

        release.countDown();
        holder.join(5_000);
        waiter.join(5_000);
        assertNotNull(waiterResp.get());
        assertTrue(waiterResp.get().ok(), () -> "expected success after the holder released, got: "
                + waiterResp.get().errorMessage());
    }

    @Test
    void reentrant_sub_pi_child_bypasses_the_fifo_while_ancestor_holds_it() throws Exception {
        // Single-permit (unlabeled) instance: the parent dispatch parks inside the container
        // waiting on its function-port sub-PI. The child's dispatch targets the SAME instance —
        // queuing it on the FIFO deadlocks (parent's wall-clock is the only way out).
        props.setStatefulQueueMaxDepth(64);
        props.setStatefulQueueWaitTimeoutMs(50L);
        StatefulInstance inst = runningInstance();

        CountDownLatch holding = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        // Call #1 is the holder (we await `holding` before starting the child); any later call
        // is the re-entrant child, whose invoke returns at once.
        AtomicInteger calls = new AtomicInteger();
        cli.responder = argv -> {
            if (calls.incrementAndGet() > 1) {
                return new OciCli.Result(0, "{\"child\":true}", "", false);
            }
            holding.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread holder = new Thread(() -> exec.execute(statefulJob(), inst), "fifo-holder-reentrant");
        holder.start();
        assertTrue(holding.await(5, TimeUnit.SECONDS), "parent dispatch must be in-flight");

        // The sub-PI registry links the child run to its parent (wfe-1, the in-flight holder).
        org.mockito.Mockito.when(subProcessRegistry.hasAncestorIn(
                org.mockito.ArgumentMatchers.eq("wfe-child-1"), any())).thenReturn(true);
        SandboxJobRequest childJob = SandboxJobRequest
                .builder("job-child-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1").currentNodeId("n-child")
                .workflowExecutionId("wfe-child-1")
                .ociImageRef("myenv/pg:1").stateful(true).streaming(true).build();

        AtomicReference<SandboxJobResponse> childResp = new AtomicReference<>();
        Thread child = new Thread(() -> childResp.set(exec.execute(childJob, inst)), "fifo-child-reentrant");
        child.start();
        child.join(5_000);

        assertFalse(child.isAlive(), "re-entrant child must not park on the FIFO");
        assertNotNull(childResp.get());
        assertTrue(childResp.get().ok(), () -> "child must complete while the parent holds the "
                + "permit, got: " + childResp.get().errorMessage());
        assertEquals(true, childResp.get().outputVariables().get("child"));

        release.countDown();
        holder.join(5_000);
    }

    @Test
    void dispatch_capable_instance_permits_overlapping_acquires() throws Exception {
        props.setStatefulDispatchParallelism(8);
        StatefulInstance inst = runningInstance();
        inst.setDispatchCapable(true);

        CountDownLatch bothInside = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        cli.responder = argv -> {
            int n = inFlight.incrementAndGet();
            maxInFlight.updateAndGet(prev -> Math.max(prev, n));
            bothInside.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            inFlight.decrementAndGet();
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread t1 = new Thread(() -> exec.execute(statefulJob(), inst), "dispatch-a");
        Thread t2 = new Thread(() -> exec.execute(
                SandboxJobRequest.builder("job-2", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                inst), "dispatch-b");
        t1.start();
        t2.start();
        assertTrue(bothInside.await(5, TimeUnit.SECONDS),
                "dispatch-capable instance must admit two overlapping acquires");
        assertTrue(maxInFlight.get() >= 2, "peak in-flight must be >= 2, was " + maxInFlight.get());

        release.countDown();
        t1.join(5_000);
        t2.join(5_000);
    }

    @Test
    void unlabeled_image_stays_fifo() throws Exception {
        StatefulInstance inst = runningInstance();
        assertFalse(inst.dispatchCapable(), "default is unlabeled / FIFO");

        CountDownLatch firstInside = new CountDownLatch(1);
        CountDownLatch secondInside = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();
        cli.responder = argv -> {
            int n = entered.incrementAndGet();
            if (n == 1) {
                firstInside.countDown();
                try {
                    assertTrue(releaseFirst.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                secondInside.countDown();
            }
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread first = new Thread(() -> exec.execute(statefulJob(), inst), "fifo-first");
        Thread second = new Thread(() -> exec.execute(
                SandboxJobRequest.builder("job-2", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                inst), "fifo-second");
        first.start();
        assertTrue(firstInside.await(5, TimeUnit.SECONDS), "first dispatch must be in-flight");
        second.start();
        Thread.sleep(100);
        assertEquals(1, entered.get(),
                "unlabeled instance must serialize — second dispatch entered while first held");

        releaseFirst.countDown();
        assertTrue(secondInside.await(5, TimeUnit.SECONDS), "second dispatch runs after first releases");
        first.join(5_000);
        second.join(5_000);
    }

    @Test
    void dispatch_capable_saturation_at_n_permits_rejects_with_sandbox_overloaded() throws Exception {
        props.setStatefulDispatchParallelism(2);
        props.setStatefulQueueMaxDepth(1);
        props.setStatefulQueueWaitTimeoutMs(5_000L);
        StatefulInstance inst = runningInstance();
        inst.setDispatchCapable(true);

        CountDownLatch bothHolding = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        cli.responder = argv -> {
            bothHolding.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new OciCli.Result(0, "{\"ok\":true}", "", false);
        };

        Thread h1 = new Thread(() -> exec.execute(statefulJob(), inst), "cap-holder-1");
        Thread h2 = new Thread(() -> exec.execute(
                SandboxJobRequest.builder("job-h2", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                inst), "cap-holder-2");
        h1.start();
        h2.start();
        assertTrue(bothHolding.await(5, TimeUnit.SECONDS), "both permits must be in-flight");

        CountDownLatch waiterStarted = new CountDownLatch(1);
        AtomicReference<SandboxJobResponse> waiterResp = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterStarted.countDown();
            waiterResp.set(exec.execute(
                    SandboxJobRequest.builder("job-waiter", SandboxLanguage.PYTHON, "print(1)")
                            .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                    inst));
        }, "cap-waiter");
        waiter.start();
        assertTrue(waiterStarted.await(1, TimeUnit.SECONDS));
        Thread.sleep(50);

        long t0 = System.nanoTime();
        SandboxJobResponse rejected = exec.execute(
                SandboxJobRequest.builder("job-reject", SandboxLanguage.PYTHON, "print(1)")
                        .envId("env-1").ociImageRef("myenv/pg:1").stateful(true).build(),
                inst);
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        assertFalse(rejected.ok());
        assertEquals("SANDBOX_OVERLOADED", rejected.errorCode());
        assertEquals(SandboxJobResponse.FailureKind.INFRA, rejected.failureKind());
        assertTrue(elapsedMs < 500, "must reject immediately without parking; took " + elapsedMs + "ms");

        release.countDown();
        h1.join(5_000);
        h2.join(5_000);
        waiter.join(5_000);
    }

    @Test
    void response_at_transport_cap_and_unparseable_fails_loud_not_silent_empty() {
        // The launcher stdout IS the result transport: a file-bearing envelope truncated at the
        // cap never parses, and succeeding with empty outputs is the silent "agent saw nothing"
        // defect. It must surface as a named CONTAINER failure instead.
        props.setStatefulResponseMaxBytes(1024); // the clamped floor
        String truncated = "{\"llm_output\":{\"result\":{\"content\":[{\"type\":\"text\",\"text\":\""
                + "x".repeat(2048); // > cap bytes, no closing brace — the truncated envelope
        cli.responder = argv -> new OciCli.Result(0, truncated, "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertFalse(resp.ok(), "a truncated transport must not succeed with empty outputs");
        assertEquals("SANDBOX_RESPONSE_OVERFLOW", resp.errorCode());
        assertEquals(SandboxJobResponse.FailureKind.CONTAINER, resp.failureKind());
        assertTrue(resp.errorMessage().contains("stateful-response-max-bytes"),
                "the operator-facing message names the knob: " + resp.errorMessage());
    }

    @Test
    void large_parseable_response_flows_through_the_raised_transport_cap() {
        // A multi-hundred-KiB image envelope is over the old 256 KiB log cap but under the
        // transport default — it must reach the caller's outputs intact.
        String bigText = "y".repeat(512 * 1024);
        cli.responder = argv -> new OciCli.Result(0,
                "{\"llm_output\":{\"result\":{\"text\":\"" + bigText + "\"}}}", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        Object llmOutput = resp.outputVariables().get("llm_output");
        assertNotNull(llmOutput, "the large envelope survived the transport");
        assertTrue(llmOutput instanceof java.util.Map<?, ?>,
                "envelope keeps its structure: " + llmOutput.getClass());
    }

    @Test
    void non_json_success_response_warns_but_stays_soft_for_byo_compat() {
        // A BYO app answering plain text with exit 0 violates the JSON port-map contract but must
        // not hard-fail — outputs stay empty, exactly the pre-fix behaviour, now with a WARN.
        props.setStatefulResponseMaxBytes(1024 * 1024);
        cli.responder = argv -> new OciCli.Result(0, "plain text, not json", "", false);

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertTrue(resp.ok(), () -> "non-JSON BYO response stays a soft empty-output success, got: "
                + resp.errorMessage());
        assertTrue(resp.outputVariables().containsKey("$stdout"),
                "std streams still fold into outputs");
    }

    @Test
    void abortThisRequest_postsBoundaryInterruptToInvokeCancel() {
        cli.calls.clear();
        cli.lastStdin = new byte[0];
        exec.abortThisRequest(runningInstance(), "job-1");

        assertTrue(cli.sawCallContaining("exec", "cid-live-1"),
                "cancel rides podman exec into the live instance");
        String body = new String(cli.lastStdin, StandardCharsets.UTF_8);
        assertEquals(StatefulLauncher.cancelJson("job-1"), body);
        assertTrue(body.contains("\"reason\":\"BOUNDARY_INTERRUPT\""));

        java.util.List<String> argv = cli.firstCallWith("exec");
        assertNotNull(argv);
        String wrapper = argv.get(argv.size() - 1);
        int s = wrapper.indexOf('\'');
        int e = wrapper.indexOf('\'', s + 1);
        String planted = new String(java.util.Base64.getDecoder().decode(wrapper.substring(s + 1, e)),
                StandardCharsets.UTF_8);
        assertTrue(planted.contains("${SQ_APP_PATH:-/invoke}/cancel"),
                "POST target is SQ_APP_PATH/cancel (typically /invoke/cancel)");
    }

    /**
     * Mirrors {@link StatefulInstanceManager}'s per-container map so {@code inFlight} is readable
     * from the FakeOciCli responder while {@code invoke} is inside the begin/end pair.
     */
    private void stubInFlightAccounting() {
        ConcurrentHashMap<String, AtomicInteger> inflight = new ConcurrentHashMap<>();
        doAnswer(inv -> {
            String cid = inv.getArgument(0);
            if (cid != null && !cid.isBlank()) {
                inflight.compute(cid, (k, n) -> {
                    if (n == null) {
                        return new AtomicInteger(1);
                    }
                    n.incrementAndGet();
                    return n;
                });
            }
            return true;
        }).when(instances).beginDispatch(any());
        doAnswer(inv -> {
            String cid = inv.getArgument(0);
            if (cid != null && !cid.isBlank()) {
                inflight.compute(cid, (k, n) -> {
                    if (n == null) {
                        return null;
                    }
                    return n.decrementAndGet() <= 0 ? null : n;
                });
            }
            return null;
        }).when(instances).endDispatch(any());
        when(instances.inFlight(any())).thenAnswer(inv -> {
            String cid = inv.getArgument(0);
            if (cid == null || cid.isBlank()) {
                return 0;
            }
            AtomicInteger n = inflight.get(cid);
            return n == null ? 0 : Math.max(0, n.get());
        });
    }

    @Test
    void execute_counts_in_flight_during_invoke_and_clears_after() {
        stubInFlightAccounting();
        AtomicInteger duringInvoke = new AtomicInteger(-1);
        cli.responder = argv -> {
            duringInvoke.set(instances.inFlight("cid-live-1"));
            return new OciCli.Result(0, "{\"answer\":\"42\"}", "", false);
        };

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertTrue(resp.ok(), () -> "expected success, got: " + resp.errorMessage());
        assertEquals(1, duringInvoke.get(), "beginDispatch must be live for the duration of invoke");
        assertEquals(0, instances.inFlight("cid-live-1"), "endDispatch must clear after execute returns");
        verify(instances).beginDispatch("cid-live-1");
        verify(instances).endDispatch("cid-live-1");
    }

    @Test
    void execute_nonzero_launcher_exit_still_ends_in_flight() {
        stubInFlightAccounting();
        AtomicInteger duringInvoke = new AtomicInteger(-1);
        cli.responder = argv -> {
            duringInvoke.set(instances.inFlight("cid-live-1"));
            return new OciCli.Result(3, "", "app crashed", false);
        };

        SandboxJobResponse resp = exec.execute(statefulJob(), runningInstance());

        assertFalse(resp.ok(), "non-zero launcher exit → failed response");
        assertEquals(1, duringInvoke.get(), "beginDispatch still wraps a failing invoke");
        assertEquals(0, instances.inFlight("cid-live-1"), "endDispatch must run on the failure path");
        verify(instances).beginDispatch("cid-live-1");
        verify(instances).endDispatch("cid-live-1");
    }
}
