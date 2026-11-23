package com.studioquack.sqs.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Targeted unit tests for {@link DispatchCancellationRegistry}: bounded self-cleaning tracking,
 * cancel reaping (future interrupt + container teardown callback), idempotent/no-throw cancel for
 * unknown or already-gone dispatches, and "missing container" counting as a clean success.
 */
class DispatchCancellationRegistryTest {

    @Test
    void cancelRun_interruptsFutureAndKillsBoundContainer() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-1", "dispatch-1");

        Future<?> future = new CompletableFuture<>();
        registry.attachFuture("dispatch-1", future);

        AtomicReference<String> killed = new AtomicReference<>();
        registry.bindContainer("dispatch-1", "container-abc", killed::set);

        int reaped = registry.cancelRun("pi-1");

        assertThat(reaped).isEqualTo(1);
        assertThat(future.isCancelled()).isTrue();
        assertThat(killed.get()).isEqualTo("container-abc");
        // Self-cleaning: the reaped dispatch is no longer tracked.
        assertThat(registry.trackedCount()).isZero();
        // Late helper /result must still see the run-cancel mark.
        assertThat(registry.isCancelled("dispatch-1")).isTrue();
        assertThat(registry.cancelReason("dispatch-1"))
                .isEqualTo(DispatchCancellationRegistry.REASON_RUN_CANCEL);
    }

    @Test
    void cancelRun_missingContainerCountsAsCleanSuccess() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-2", "dispatch-2");
        registry.attachFuture("dispatch-2", new CompletableFuture<>());
        // No bindContainer call — container never came up before the cancel landed.

        int reaped = registry.cancelRun("pi-2");

        assertThat(reaped).isEqualTo(1);
        assertThat(registry.trackedCount()).isZero();
    }

    @Test
    void cancelRun_unknownProcessInstanceIsIdempotentNoThrow() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();

        int reaped = registry.cancelRun("no-such-pi");

        assertThat(reaped).isZero();
    }

    @Test
    void cancelRun_containerKillerThrowing_neverPropagates() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-3", "dispatch-3");
        registry.attachFuture("dispatch-3", new CompletableFuture<>());
        AtomicBoolean invoked = new AtomicBoolean(false);
        registry.bindContainer("dispatch-3", "container-boom", id -> {
            invoked.set(true);
            throw new RuntimeException("podman rm failed");
        });

        int reaped = registry.cancelRun("pi-3");

        assertThat(reaped).isEqualTo(1);
        assertThat(invoked.get()).isTrue();
    }

    @Test
    void endTracking_selfCleansBeforeAnyCancel() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-4", "dispatch-4");
        registry.attachFuture("dispatch-4", new CompletableFuture<>());
        registry.endTracking("dispatch-4");

        assertThat(registry.trackedCount()).isZero();
        // A cancel landing after the dispatch already finished naturally reaps nothing.
        assertThat(registry.cancelRun("pi-4")).isZero();
    }

    @Test
    void unbindContainer_clearsBindingSoLaterCancelSkipsKiller() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-5", "dispatch-5");
        Future<?> future = new CompletableFuture<>();
        registry.attachFuture("dispatch-5", future);
        AtomicBoolean invoked = new AtomicBoolean(false);
        registry.bindContainer("dispatch-5", "container-x", id -> invoked.set(true));
        registry.unbindContainer("dispatch-5");

        int reaped = registry.cancelRun("pi-5");

        assertThat(reaped).isEqualTo(1);
        assertThat(invoked.get()).isFalse();
        // No live container → do not interrupt; the thread may already be in embedded database/JPA.
        assertThat(future.isCancelled()).isFalse();
    }

    @Test
    void cancelRun_afterEnginePhase_doesNotInterruptFuture() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-7", "dispatch-7");
        Future<?> future = new CompletableFuture<>();
        registry.attachFuture("dispatch-7", future);
        registry.bindContainer("dispatch-7", "container-late", id -> { });
        registry.markEnginePhase("dispatch-7");

        int reaped = registry.cancelRun("pi-7");

        assertThat(reaped).isEqualTo(1);
        assertThat(future.isCancelled()).isFalse();
        assertThat(registry.trackedCount()).isZero();
    }

    @Test
    void cancelRun_withoutBoundContainer_doesNotInterruptFuture() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-8", "dispatch-8");
        Future<?> future = new CompletableFuture<>();
        registry.attachFuture("dispatch-8", future);

        int reaped = registry.cancelRun("pi-8");

        assertThat(reaped).isEqualTo(1);
        assertThat(future.isCancelled()).isFalse();
    }

    @Test
    void beginTracking_withBlankDispatchId_isNoop() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-6", "");
        registry.beginTracking("pi-6", null);

        assertThat(registry.trackedCount()).isZero();
        assertThat(registry.cancelRun("pi-6")).isZero();
    }

    @Test
    void cancelStep_killsContainer_interruptsFuture_runsRevokeAndAbort_staysCancelledAfterEndTracking() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-step", "dispatch-step", "nodeA");

        Future<?> future = new CompletableFuture<>();
        registry.attachFuture("dispatch-step", future);

        AtomicReference<String> killed = new AtomicReference<>();
        registry.bindContainer("dispatch-step", "container-step", killed::set);
        AtomicBoolean revoked = new AtomicBoolean(false);
        AtomicBoolean aborted = new AtomicBoolean(false);
        registry.bindHelperRevoke("dispatch-step", () -> revoked.set(true));
        registry.bindStatefulAbort("dispatch-step", () -> aborted.set(true));

        int reaped = registry.cancelStep("pi-step", "nodeA");

        assertThat(reaped).isEqualTo(1);
        assertThat(future.isCancelled()).isTrue();
        assertThat(killed.get()).isEqualTo("container-step");
        assertThat(revoked.get()).isTrue();
        assertThat(aborted.get()).isTrue();
        assertThat(registry.isCancelled("dispatch-step")).isTrue();
        assertThat(registry.isStepCancelled("pi-step", "nodeA")).isTrue();
        assertThat(registry.cancelReason("dispatch-step"))
                .isEqualTo(DispatchCancellationRegistry.REASON_BOUNDARY_INTERRUPT);

        registry.endTracking("dispatch-step");
        assertThat(registry.trackedCount()).isZero();
        assertThat(registry.isCancelled("dispatch-step")).isTrue();
        assertThat(registry.isStepCancelled("pi-step", "nodeA")).isTrue();
        assertThat(registry.cancelReason("dispatch-step"))
                .isEqualTo(DispatchCancellationRegistry.REASON_BOUNDARY_INTERRUPT);
    }

    @Test
    void cancelStep_unknownProcessOrNode_isZeroNoThrow() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-known", "dispatch-known", "nodeA");

        assertThat(registry.cancelStep("no-such-pi", "nodeA")).isZero();
        assertThat(registry.cancelStep("pi-known", "no-such-node")).isZero();
        assertThat(registry.cancelStep("pi-known", "")).isZero();
        assertThat(registry.cancelStep("pi-known", null)).isZero();
        assertThat(registry.isCancelled("dispatch-known")).isFalse();
        assertThat(registry.trackedCount()).isEqualTo(1);
    }

    @Test
    void cancelStep_doesNotCancelSiblingNodeOnSameProcessInstance() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-sib", "dispatch-a", "nodeA");
        registry.beginTracking("pi-sib", "dispatch-b", "nodeB");
        Future<?> futureA = new CompletableFuture<>();
        Future<?> futureB = new CompletableFuture<>();
        registry.attachFuture("dispatch-a", futureA);
        registry.attachFuture("dispatch-b", futureB);
        AtomicBoolean killedA = new AtomicBoolean(false);
        AtomicBoolean killedB = new AtomicBoolean(false);
        registry.bindContainer("dispatch-a", "c-a", id -> killedA.set(true));
        registry.bindContainer("dispatch-b", "c-b", id -> killedB.set(true));

        int reaped = registry.cancelStep("pi-sib", "nodeA");

        assertThat(reaped).isEqualTo(1);
        assertThat(registry.isCancelled("dispatch-a")).isTrue();
        assertThat(registry.isCancelled("dispatch-b")).isFalse();
        assertThat(killedA.get()).isTrue();
        assertThat(killedB.get()).isFalse();
        assertThat(futureA.isCancelled()).isTrue();
        assertThat(futureB.isCancelled()).isFalse();
    }

    @Test
    void cancelStep_nonMatchingNodeId_leavesLateResultPathUncancelled() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-late", "dispatch-late", "nodeKeep");

        assertThat(registry.cancelStep("pi-late", "otherNode")).isZero();
        assertThat(registry.isCancelled("dispatch-late")).isFalse();
        assertThat(registry.cancelReason("dispatch-late")).isNull();
    }

    @Test
    void cancelStep_withoutBoundContainer_stillInterruptsAndRunsAbort() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        registry.beginTracking("pi-stateful", "dispatch-stateful", "nodeS");
        Future<?> future = new CompletableFuture<>();
        registry.attachFuture("dispatch-stateful", future);
        AtomicBoolean aborted = new AtomicBoolean(false);
        registry.bindStatefulAbort("dispatch-stateful", () -> aborted.set(true));

        int reaped = registry.cancelStep("pi-stateful", "nodeS");

        assertThat(reaped).isEqualTo(1);
        assertThat(future.isCancelled()).isTrue();
        assertThat(aborted.get()).isTrue();
        assertThat(registry.isCancelled("dispatch-stateful")).isTrue();
        assertThat(registry.isStepCancelled("pi-stateful", "nodeS")).isTrue();
    }

    @Test
    void bindHelperRevokeAndStatefulAbort_areNoopWhenEntryGone() {
        DispatchCancellationRegistry registry = new DispatchCancellationRegistry();
        AtomicBoolean revoked = new AtomicBoolean(false);
        AtomicBoolean aborted = new AtomicBoolean(false);
        registry.bindHelperRevoke("gone", () -> revoked.set(true));
        registry.bindStatefulAbort("gone", () -> aborted.set(true));
        assertThat(revoked.get()).isFalse();
        assertThat(aborted.get()).isFalse();
    }
}
