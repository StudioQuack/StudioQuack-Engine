package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Pins graceful cancel: SIGTERM is attempted before forceKill, and forceKill is skipped when
 * the container is already gone. Overrides the CLI hooks so this does not need a live podman.
 */
class PodmanSandboxExecutionKillTest {

    @Test
    void killContainer_sendsTermBeforeForceKill_skipsForceKillWhenGone() {
        WarmPool pool = mock(WarmPool.class);
        RecordingKill sandbox = recording(pool);
        sandbox.gone = true;

        sandbox.killContainer("cid-1");

        assertEquals(List.of("cid-1"), sandbox.terms, "TERM is sent first");
        verify(pool, never()).forceKill("cid-1");
    }

    @Test
    void killContainer_forceKillsWhenStillRunningAfterTerm() {
        WarmPool pool = mock(WarmPool.class);
        RecordingKill sandbox = recording(pool);
        sandbox.gone = false;

        sandbox.killContainer("cid-1");

        assertEquals(List.of("cid-1"), sandbox.terms, "TERM is attempted before SIGKILL");
        verify(pool).forceKill("cid-1");
        assertTrue(sandbox.termBeforeForce, "TERM is recorded before forceKill");
    }

    @Test
    void killContainer_blankIdIsNoop() {
        WarmPool pool = mock(WarmPool.class);
        RecordingKill sandbox = recording(pool);

        sandbox.killContainer("  ");
        sandbox.killContainer(null);

        assertTrue(sandbox.terms.isEmpty());
        verify(pool, never()).forceKill(org.mockito.ArgumentMatchers.any());
    }

    private static RecordingKill recording(WarmPool pool) {
        ObjectMapper mapper = new ObjectMapper();
        return new RecordingKill(
                new ContainerRuntime(mapper, "auto", false),
                mock(SecretResolver.class),
                mock(Redactor.class),
                mock(HelperRequestRouter.class),
                pool,
                new MockEnvironment(),
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new DispatchCancellationRegistry());
    }

    /** Overrides TERM + wait so the unit test never shells out to podman. */
    static final class RecordingKill extends PodmanSandboxExecution {
        final List<String> terms = new ArrayList<>();
        volatile boolean gone;
        volatile boolean termBeforeForce;

        RecordingKill(ContainerRuntime runtime, SecretResolver secrets, Redactor redactor,
                      HelperRequestRouter helper, WarmPool pool, MockEnvironment env,
                      DependencyManifestHolder deps, SandboxOwnership ownership,
                      DispatchCancellationRegistry registry) {
            super(runtime, secrets, redactor, helper, pool, env, deps, ownership, registry);
        }

        @Override
        void sendSigterm(String containerId) {
            terms.add(containerId);
        }

        @Override
        boolean waitUntilGone(String containerId) {
            termBeforeForce = !terms.isEmpty();
            return gone;
        }
    }
}
