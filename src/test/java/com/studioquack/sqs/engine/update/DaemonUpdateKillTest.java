package com.studioquack.sqs.engine.update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.RunTerminationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * An in-place update kills the runs that still have a node in flight instead of waiting for them —
 * one long or stuck node used to hold every update for hours. Runs without a node in flight
 * (parked between nodes, waiting on a message or timer) are never touched.
 */
class DaemonUpdateKillTest {

    private final DispatchExecutionRegistry registry = new DispatchExecutionRegistry();
    private final RunTerminationService termination = mock(RunTerminationService.class);
    private final DaemonUpdateService service = new DaemonUpdateService(null, registry, termination, null,
            null, new ObjectMapper(), null, null, "");

    @AfterEach
    void resetBuildState() {
        DaemonBuild.updateIdle();
    }

    private static Object updateDetail() {
        return ((Map<?, ?>) DaemonBuild.snapshot().get("update")).get("detail");
    }

    @Test
    @DisplayName("every run with a node in flight is killed once, Designer Runs included, and leaves no wait-state")
    void killsEveryRunWithANodeInFlight() {
        List<Object> detailsWhileKilling = new ArrayList<>();
        when(termination.terminate(anyString(), eq(DaemonUpdateService.KILL_REASON))).thenAnswer(inv -> {
            detailsWhileKilling.add(updateDetail());
            return new RunTerminationService.Outcome(inv.getArgument(0), true, 1, 0);
        });
        registry.register("d1", "pi-deployed", "bk-deployed", "node1", "wait_node1");
        registry.register("d2", "pi-deployed", "bk-deployed", "node2", "wait_node2");
        registry.register("d3", "pi-designer", "bk-designer", "node1", "wait_node1");
        assertThat(service.runningInstances()).as("the preview counts runs, not nodes").isEqualTo(2);
        DaemonBuild.updateStarted();

        service.killRunningInstances();

        verify(termination, times(1)).terminate("pi-deployed", DaemonUpdateService.KILL_REASON);
        verify(termination, times(1)).terminate("pi-designer", DaemonUpdateService.KILL_REASON);
        assertThat(detailsWhileKilling).as("Engine Settings shows why the update is busy")
                .containsOnly("stopping 2 running instance(s)");
        assertThat(updateDetail()).as("cleared once the kill is done").isNull();
        assertThat(registry.pending()).as("nothing left for the next boot to hydrate").isZero();
        assertThat(service.runningInstances()).isZero();
    }

    @Test
    @DisplayName("a run without a node in flight is never killed")
    void aRunWithoutANodeInFlightIsNotKilled() {
        service.killRunningInstances();

        verify(termination, never()).terminate(anyString(), anyString());
    }

    @Test
    @DisplayName("a failed kill is tried once, keeps its wait-state, and does not stop the update")
    void aFailedKillDoesNotHoldTheUpdate() {
        when(termination.terminate("pi-stuck", DaemonUpdateService.KILL_REASON))
                .thenThrow(new IllegalStateException("optimistic lock"));
        when(termination.terminate("pi-ok", DaemonUpdateService.KILL_REASON))
                .thenReturn(new RunTerminationService.Outcome("pi-ok", true, 1, 0));
        registry.register("d1", "pi-stuck", "bk1", "node1", "wait_node1");
        registry.register("d2", "pi-ok", "bk2", "node1", "wait_node1");

        service.killRunningInstances();

        verify(termination, times(1)).terminate("pi-stuck", DaemonUpdateService.KILL_REASON);
        assertThat(registry.processInstancesInFlight())
                .as("the next boot discards it as an orphan")
                .containsExactly("pi-stuck");
    }

    @Test
    @DisplayName("a run whose node registers while a pass is killing is caught by the next pass")
    void aLateRunIsCaughtByTheNextPass() {
        List<String> killed = new ArrayList<>();
        when(termination.terminate(anyString(), eq(DaemonUpdateService.KILL_REASON))).thenAnswer(inv -> {
            String processInstanceId = inv.getArgument(0);
            killed.add(processInstanceId);
            if ("pi-parent".equals(processInstanceId)) {
                // A sub-process its node started just before the reap.
                registry.register("d-child", "pi-child", "bk-child", "node1", "wait_node1");
            }
            return new RunTerminationService.Outcome(processInstanceId, true, 1, 0);
        });
        registry.register("d-parent", "pi-parent", "bk-parent", "node1", "wait_node1");

        service.killRunningInstances();

        assertThat(killed).containsExactly("pi-parent", "pi-child");
        assertThat(registry.pending()).isZero();
    }
}
