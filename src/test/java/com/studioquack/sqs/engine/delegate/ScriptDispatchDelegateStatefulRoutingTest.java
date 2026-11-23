package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulSandboxExecution;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Pins dispatch-time image-based stateful selection independently from
 * Activiti and the background executor.
 */
class ScriptDispatchDelegateStatefulRoutingTest {

    private SandboxExecutionPort ephemeralSandbox;
    private StatefulInstanceManager statefulInstances;
    private StatefulSandboxExecution statefulSandbox;
    private ScriptDispatchDelegate delegate;
    private org.springframework.mock.env.MockEnvironment environment;

    @BeforeEach
    void setUp() {
        environment = new org.springframework.mock.env.MockEnvironment();
        ephemeralSandbox = mock(SandboxExecutionPort.class);
        statefulInstances = mock(StatefulInstanceManager.class);
        statefulSandbox = mock(StatefulSandboxExecution.class);
        delegate = new ScriptDispatchDelegate(
                null, // transformPipeline
                null, // generateScriptService
                null, // edgeTransformPreStep
                ephemeralSandbox,
                statefulInstances,
                statefulSandbox,
                null, // refOnlyVarBuilder
                null, // contextStore
                null, // dispatchRegistry
                null, // resultCoordinator
                null, // routeResolver
                null, // jobQueue
                null, // sandboxLogCallback
                null, // duckbotBridge
                null, // secretResolver
                null, // expressionResolver
                null, // secretBindingGuard
                null, // tamperEventReporter
                null, // sandboxFileMaterializer
                null, // contentBlockHydrator
                null, // objectMapper
                null, // dispatchCancellationRegistry
                null, // cancelledRunGuard
                environment,
                null); // sandboxEgressManager (no posture note under test)
    }

    @Test
    void builtin_library_cold_start_reuses_the_system_instance_identity_and_daemon_env() {
        // The library is ONE platform instance under the daemon's env id; a job env that drifted
        // from it used to miss, cold-start a second random-id copy, and leave two running.
        environment.setProperty("sqs.channel.env-id", "daemon-env");
        String lib = BuiltinNodeLibraryManager.IMAGE_REF;
        SandboxJobRequest job = SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1")
                .currentNodeId("node-1")
                .ociImageRef(lib)
                .build();
        StatefulInstance started = runningInstance();
        when(statefulInstances.resolveRunning("daemon-env", lib))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(started));
        when(statefulInstances.start(eq("daemon-env"), eq(lib), eq(true), any(),
                eq(BuiltinNodeLibraryManager.INSTANCE_ID), isNull(), isNull(), isNull()))
                .thenReturn(new StatefulInstanceManager.StartResult(
                        BuiltinNodeLibraryManager.INSTANCE_ID, "c1", StatefulStatus.RUNNING, null));
        SandboxJobResponse expected = success("c1");
        when(statefulSandbox.execute(job, started)).thenReturn(expected);

        assertSame(expected, delegate.dispatch(job));
        verify(statefulInstances, never()).start(any(), any(), anyBoolean(), any());
        verify(statefulInstances, never()).resolveRunning("env-1", lib);
    }

    @Test
    void automatically_routes_matching_image_without_legacy_stateful_flag() {
        SandboxJobRequest job = job(false);
        StatefulInstance instance = runningInstance();
        SandboxJobResponse expected = success("stateful-container");
        when(statefulInstances.resolveRunning("env-1", "registry/app:1"))
                .thenReturn(Optional.of(instance));
        when(statefulSandbox.execute(job, instance)).thenReturn(expected);

        SandboxJobResponse actual = delegate.dispatch(job);

        assertSame(expected, actual);
        verify(statefulInstances).resolveRunning("env-1", "registry/app:1");
        verify(statefulSandbox).execute(job, instance);
        verifyNoInteractions(ephemeralSandbox);
    }

    @Test
    void no_live_instance_cold_starts_a_stateful_container() {
        // A custom image's entrypoint is a serve-forever listener — the ephemeral
        // one-shot path cannot run it (exec parks until the wall-clock SIGKILL, exit
        // 137). With no live instance the dispatch cold-starts one and serves through it.
        SandboxJobRequest job = job(false);
        StatefulInstance started = runningInstance();
        SandboxJobResponse expected = success("stateful-container");
        when(statefulInstances.resolveRunning("env-1", "registry/app:1"))
                .thenReturn(Optional.empty())   // first lookup: nothing live
                .thenReturn(Optional.of(started)); // re-resolve after the start
        when(statefulInstances.start(eq("env-1"), eq("registry/app:1"), eq(true), any()))
                .thenReturn(new StatefulInstanceManager.StartResult(
                        started.instanceId(), started.containerId(), StatefulStatus.RUNNING, null));
        when(statefulSandbox.execute(job, started)).thenReturn(expected);

        SandboxJobResponse actual = delegate.dispatch(job);

        assertSame(expected, actual);
        verify(statefulInstances).start(eq("env-1"), eq("registry/app:1"), eq(true), any());
        verify(statefulSandbox).execute(job, started);
        verifyNoInteractions(ephemeralSandbox);
    }

    @Test
    void cold_start_failure_returns_container_failure_without_touching_ephemeral() {
        SandboxJobRequest job = job(false);
        when(statefulInstances.resolveRunning("env-1", "registry/app:1"))
                .thenReturn(Optional.empty());
        when(statefulInstances.start(eq("env-1"), eq("registry/app:1"), eq(true), any()))
                .thenReturn(new StatefulInstanceManager.StartResult(
                        null, null, StatefulStatus.FAILED, null, "no runtime"));

        SandboxJobResponse actual = delegate.dispatch(job);

        org.junit.jupiter.api.Assertions.assertFalse(actual.ok());
        org.junit.jupiter.api.Assertions.assertEquals(
                SandboxJobResponse.FailureKind.CONTAINER, actual.failureKind());
        verifyNoInteractions(ephemeralSandbox);
        verifyNoInteractions(statefulSandbox);
    }

    @Test
    void legacy_stateful_true_still_routes_to_a_live_instance() {
        SandboxJobRequest job = job(true);
        StatefulInstance instance = runningInstance();
        SandboxJobResponse expected = success("stateful-container");
        when(statefulInstances.resolveRunning("env-1", "registry/app:1"))
                .thenReturn(Optional.of(instance));
        when(statefulSandbox.execute(job, instance)).thenReturn(expected);

        SandboxJobResponse actual = delegate.dispatch(job);

        assertSame(expected, actual);
        verify(statefulSandbox).execute(job, instance);
        verifyNoInteractions(ephemeralSandbox);
    }

    @Test
    void stateful_true_without_live_instance_also_cold_starts() {
        SandboxJobRequest job = job(true);
        StatefulInstance started = runningInstance();
        SandboxJobResponse expected = success("stateful-container");
        when(statefulInstances.resolveRunning("env-1", "registry/app:1"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(started));
        when(statefulInstances.start(eq("env-1"), eq("registry/app:1"), eq(true), any()))
                .thenReturn(new StatefulInstanceManager.StartResult(
                        started.instanceId(), started.containerId(), StatefulStatus.RUNNING, null));
        when(statefulSandbox.execute(job, started)).thenReturn(expected);

        SandboxJobResponse actual = delegate.dispatch(job);

        assertSame(expected, actual);
        verify(statefulInstances).start(eq("env-1"), eq("registry/app:1"), eq(true), any());
        verify(statefulSandbox).execute(job, started);
        verifyNoInteractions(ephemeralSandbox);
    }

    @Test
    void a_create_node_trial_job_runs_in_a_temporary_container_and_never_looks_up_an_instance() {
        SandboxJobRequest job = temporaryJob();
        StatefulInstance container = runningTemporary();
        SandboxJobResponse expected = success("tmp-container");
        when(statefulInstances.startTemporary("env-1", "registry/app:1"))
                .thenReturn(new StatefulInstanceManager.StartResult("tmp_abc", "tmp-container",
                        StatefulStatus.RUNNING, null));
        when(statefulInstances.temporary("tmp_abc")).thenReturn(Optional.of(container));
        when(statefulSandbox.execute(job, container)).thenReturn(expected);

        assertSame(expected, delegate.dispatch(job));
        verify(statefulInstances).removeTemporary("tmp_abc");
        verify(statefulInstances, never()).resolveRunning(any(), any());
        verify(statefulInstances, never()).start(any(), any(), anyBoolean(), any());
        verifyNoInteractions(ephemeralSandbox);
    }

    @Test
    void a_temporary_container_that_did_not_start_reports_why_and_is_still_removed() {
        SandboxJobRequest job = temporaryJob();
        when(statefulInstances.startTemporary("env-1", "registry/app:1"))
                .thenReturn(new StatefulInstanceManager.StartResult("tmp_abc", null,
                        StatefulStatus.FAILED, null, "app never listened on 8080"));
        when(statefulInstances.temporary("tmp_abc")).thenReturn(Optional.empty());

        SandboxJobResponse response = delegate.dispatch(job);

        assertTrue(response.errorMessage().contains("app never listened on 8080"), response.errorMessage());
        verify(statefulInstances).removeTemporary("tmp_abc");
        verify(statefulInstances, never()).resolveRunning(any(), any());
        verifyNoInteractions(ephemeralSandbox, statefulSandbox);
    }

    @Test
    void the_temporary_container_is_removed_even_when_the_job_blows_up() {
        SandboxJobRequest job = temporaryJob();
        StatefulInstance container = runningTemporary();
        when(statefulInstances.startTemporary("env-1", "registry/app:1"))
                .thenReturn(new StatefulInstanceManager.StartResult("tmp_abc", "tmp-container",
                        StatefulStatus.RUNNING, null));
        when(statefulInstances.temporary("tmp_abc")).thenReturn(Optional.of(container));
        when(statefulSandbox.execute(job, container)).thenThrow(new IllegalStateException("boom"));

        assertFalse(delegate.dispatch(job).ok());
        verify(statefulInstances).removeTemporary("tmp_abc");
    }

    @Test
    void an_inject_mode_trial_gets_its_staged_workspace_first_and_the_staging_is_deleted_after() throws Exception {
        OciBuildContextStore staging = stagingHolding(true);
        SandboxJobRequest job = injectTrialJob();
        StatefulInstance container = runningTemporary();
        SandboxJobResponse expected = success("tmp-container");
        when(statefulInstances.startTemporary("env-1", "registry/app:1")).thenReturn(runningStart());
        when(statefulInstances.temporary("tmp_abc")).thenReturn(Optional.of(container));
        when(statefulSandbox.execute(job, container)).thenReturn(expected);

        assertSame(expected, delegate.dispatch(job));
        InOrder order = inOrder(statefulInstances, statefulSandbox, staging);
        order.verify(statefulInstances).installTemporaryFiles("tmp_abc", "tmpl-1", STAGED_DIR);
        order.verify(statefulSandbox).execute(job, container);
        order.verify(statefulInstances).removeTemporary("tmp_abc");
        order.verify(staging).deleteContext(STAGED);
    }

    @Test
    void an_inject_mode_trial_whose_staged_workspace_is_gone_fails_without_running() throws Exception {
        stagingHolding(false);
        when(statefulInstances.startTemporary("env-1", "registry/app:1")).thenReturn(runningStart());
        when(statefulInstances.temporary("tmp_abc")).thenReturn(Optional.of(runningTemporary()));

        SandboxJobResponse response = delegate.dispatch(injectTrialJob());

        assertTrue(response.errorMessage().contains("is not on the engine"), response.errorMessage());
        verify(statefulInstances, never()).installTemporaryFiles(any(), any(), any());
        verifyNoInteractions(statefulSandbox);
        verify(statefulInstances).removeTemporary("tmp_abc");
    }

    @Test
    void a_workspace_that_cannot_be_copied_fails_the_trial_and_everything_is_still_cleaned_up() throws Exception {
        OciBuildContextStore staging = stagingHolding(true);
        when(statefulInstances.startTemporary("env-1", "registry/app:1")).thenReturn(runningStart());
        when(statefulInstances.temporary("tmp_abc")).thenReturn(Optional.of(runningTemporary()));
        doThrow(new IOException("writing workspace.zip failed: sh: base64: not found"))
                .when(statefulInstances).installTemporaryFiles(eq("tmp_abc"), eq("tmpl-1"), any());

        SandboxJobResponse response = delegate.dispatch(injectTrialJob());

        assertTrue(response.errorMessage().contains("base64: not found"), response.errorMessage());
        verifyNoInteractions(statefulSandbox);
        verify(statefulInstances).removeTemporary("tmp_abc");
        verify(staging).deleteContext(STAGED);
    }

    private OciBuildContextStore stagingHolding(boolean present) {
        OciBuildContextStore staging = mock(OciBuildContextStore.class);
        when(staging.contextExists(STAGED)).thenReturn(present);
        when(staging.contextDir(STAGED)).thenReturn(STAGED_DIR);
        delegate.setBuildContextStore(staging);
        return staging;
    }

    @Test
    void blank_oci_image_ref_skips_stateful_lookup() {
        SandboxJobRequest job = SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1")
                .currentNodeId("node-1")
                .stateful(true)
                .build();
        SandboxJobResponse expected = success("ephemeral-container");
        when(ephemeralSandbox.execute(job)).thenReturn(expected);

        SandboxJobResponse actual = delegate.dispatch(job);

        assertSame(expected, actual);
        verifyNoInteractions(statefulInstances);
        verifyNoInteractions(statefulSandbox);
        verify(ephemeralSandbox).execute(job);
    }

    private static SandboxJobRequest job(boolean legacyStateful) {
        return SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1")
                .currentNodeId("node-1")
                .ociImageRef("registry/app:1")
                .stateful(legacyStateful)
                .build();
    }

    private static SandboxJobRequest temporaryJob() {
        return SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1")
                .currentNodeId("node-1")
                .ociImageRef("registry/app:1")
                .temporaryContainer(true)
                .build();
    }

    private static final String STAGED = "0b6f9a8e-6c1d-4f7e-9a51-3c2d1e0f4a7b";
    private static final Path STAGED_DIR = Path.of("/data/oci-build", STAGED);

    private static SandboxJobRequest injectTrialJob() {
        return SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .envId("env-1")
                .currentNodeId("node-1")
                .templateId("tmpl-1")
                .ociImageRef("registry/app:1")
                .temporaryContainer(true)
                .stagedWorkspaceId(STAGED)
                .build();
    }

    private static StatefulInstanceManager.StartResult runningStart() {
        return new StatefulInstanceManager.StartResult("tmp_abc", "tmp-container", StatefulStatus.RUNNING, null);
    }

    private static StatefulInstance runningTemporary() {
        StatefulInstance container = new StatefulInstance("tmp_abc", "env-1", "registry/app:1",
                "sq-tmp-abc", "sq-stateful-vol-tmp_abc", StatefulCaps.defaults(), true, true);
        container.setContainerId("tmp-container");
        container.setStatus(StatefulStatus.RUNNING);
        return container;
    }

    private static StatefulInstance runningInstance() {
        StatefulInstance instance = new StatefulInstance(
                "instance-1",
                "env-1",
                "registry/app:1",
                "stateful-instance-1",
                "stateful-volume-1",
                StatefulCaps.defaults(),
                true);
        instance.setContainerId("stateful-container");
        instance.setStatus(StatefulStatus.RUNNING);
        return instance;
    }

    private static SandboxJobResponse success(String containerId) {
        return SandboxJobResponse.success(0, "", "", Map.of(), 1L, containerId);
    }
}

