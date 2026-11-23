package com.studioquack.sqs.engine;

import org.activiti.engine.repository.Deployment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Reaping of throwaway Designer-Run deployments. The two guards are the whole point of these tests.
 * Getting either wrong is worse than not reaping at all: deleting a deployment whose run has not
 * started yet, or whose run is still going, breaks a live execution.
 */
class EphemeralDeploymentReaperTest {

    private static final long GRACE_MINUTES = 15;

    private SqsEngine engine;
    private EphemeralDeploymentReaper reaper;

    @BeforeEach
    void setUp() {
        engine = mock(SqsEngine.class);
        when(engine.deleteDeployment(anyString())).thenReturn(true);
        reaper = new EphemeralDeploymentReaper(engine, true, GRACE_MINUTES);
    }

    /** A deployment that is {@code minutesAgo} old with {@code runningInstances} live process instances. */
    private Deployment deployment(String id, long minutesAgo, long runningInstances) {
        return deployment(id, minutesAgo, runningInstances, runningInstances);
    }

    /**
     * @param runningInstances instances live right now
     * @param historicInstances instances ever started (finished or not); {@code 0} means the run has
     *                          not started yet, which is what the grace window protects
     */
    private Deployment deployment(String id, long minutesAgo, long runningInstances, long historicInstances) {
        Deployment d = mock(Deployment.class);
        when(d.getId()).thenReturn(id);
        when(d.getDeploymentTime()).thenReturn(Date.from(Instant.now().minus(Duration.ofMinutes(minutesAgo))));
        when(engine.runningInstanceCount(id)).thenReturn(runningInstances);
        when(engine.historicInstanceCount(id)).thenReturn(historicInstances);
        return d;
    }

    private void given(Deployment... deployments) {
        when(engine.findDeploymentsByCategory(SqsEngine.EPHEMERAL_CATEGORY)).thenReturn(List.of(deployments));
    }

    @Test
    @DisplayName("a deployment whose run has FINISHED is deleted immediately, without waiting out the grace")
    void finishedRun_isReapedImmediately() {
        // The whole point of the historic-instance check. start-run resolves by process key and takes
        // the LATEST deployed version, so a spent draft left lying around is the version a published
        // trigger would execute. It has to go as soon as the run ends, not 15 minutes later.
        given(deployment("dep-just-finished", 0, 0, 1));

        assertEquals(1, reaper.reap("test"));
        verify(engine).deleteDeployment("dep-just-finished");
    }

    @Test
    @DisplayName("an old, idle ephemeral deployment is deleted")
    void oldAndIdle_isReaped() {
        given(deployment("dep-old", GRACE_MINUTES + 5, 0, 1));

        assertEquals(1, reaper.reap("test"));
        verify(engine).deleteDeployment("dep-old");
    }

    @Test
    @DisplayName("a deployment that has not run yet is kept inside the grace window")
    void deployedButNotStarted_isKept() {
        // This is the deploy -> start-run window. The backend sends those as two separate commands, so
        // for a moment a perfectly live deployment has no instances -- and no history either. Reaping
        // here kills the run that is about to start with "no processes deployed with key ...".
        given(deployment("dep-just-created", 1, 0, 0));

        assertEquals(0, reaper.reap("test"));
        verify(engine, never()).deleteDeployment(anyString());
    }

    @Test
    @DisplayName("a deployment that never ran is dropped once it is past the grace window")
    void neverStarted_isReapedAfterGrace() {
        // Abandoned: the run was cancelled before start, or the daemon restarted between the deploy and
        // start-run commands. Nothing will ever start it, so it must not be kept forever.
        given(deployment("dep-abandoned", GRACE_MINUTES + 5, 0, 0));

        assertEquals(1, reaper.reap("test"));
        verify(engine).deleteDeployment("dep-abandoned");
    }

    @Test
    @DisplayName("an old deployment with a still-running instance is kept")
    void runningInstance_vetoesDeletion() {
        // A slow designer run: past the grace window but still executing. Cascading its deployment
        // would strand the run mid-flight.
        given(deployment("dep-busy", GRACE_MINUTES + 60, 1));

        assertEquals(0, reaper.reap("test"));
        verify(engine, never()).deleteDeployment(anyString());
    }

    @Test
    @DisplayName("each deployment is judged on its own; one survivor does not block the others")
    void mixedBatch_reapsOnlyTheEligible() {
        given(deployment("dep-finished", 0, 0, 1),
                deployment("dep-pending-start", 2, 0, 0),
                deployment("dep-old-busy", GRACE_MINUTES + 5, 3, 3),
                deployment("dep-older-idle", GRACE_MINUTES + 90, 0, 2));

        assertEquals(2, reaper.reap("test"));
        verify(engine).deleteDeployment("dep-finished");
        verify(engine).deleteDeployment("dep-older-idle");
        verify(engine, never()).deleteDeployment("dep-pending-start");
        verify(engine, never()).deleteDeployment("dep-old-busy");
    }

    @Test
    @DisplayName("only deployments carrying the ephemeral category are ever considered")
    void onlyEphemeralCategoryIsQueried() {
        // Published deployments must be untouchable: the query is the sole entry point, so asserting
        // the category pins the blast radius.
        given();

        reaper.reap("test");

        verify(engine).findDeploymentsByCategory(SqsEngine.EPHEMERAL_CATEGORY);
    }

    @Test
    @DisplayName("the reaper can be switched off")
    void disabled_doesNothing() {
        EphemeralDeploymentReaper off = new EphemeralDeploymentReaper(engine, false, GRACE_MINUTES);
        given(deployment("dep-old", GRACE_MINUTES + 5, 0));

        assertEquals(0, off.reap("test"));
        verify(engine, never()).findDeploymentsByCategory(anyString());
    }

    @Test
    @DisplayName("an engine failure is swallowed so housekeeping never fails a deploy")
    void engineFailure_isContained() {
        when(engine.findDeploymentsByCategory(anyString())).thenThrow(new IllegalStateException("db gone"));

        assertEquals(0, reaper.reap("test"));
    }

    @Test
    @DisplayName("a grace period below one minute is clamped, never zero")
    void graceIsClamped() {
        // A zero grace would reopen the deploy -> start-run race, so the floor is enforced.
        EphemeralDeploymentReaper clamped = new EphemeralDeploymentReaper(engine, true, 0);
        given(deployment("dep-30s", 0, 0, 0));

        assertEquals(0, clamped.reap("test"));
        verify(engine, never()).deleteDeployment(anyString());
    }
}
