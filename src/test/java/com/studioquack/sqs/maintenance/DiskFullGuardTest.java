package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.engine.update.WorkIntakeGate;
import com.studioquack.sqs.retention.DiskSpaceProbe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A full disk stops intake and is reported — nothing is deleted, the stop does not flap, and it
 * never collides with an update's own intake reason. The floor is absolute free space, on every
 * engine: a percentage on a big laptop disk stops far too early.
 */
class DiskFullGuardTest {

    private static final long GB = 1024L * 1024L * 1024L;
    private static final long TOTAL = 253L * GB;

    private DiskSpaceProbe probe;
    private WorkIntakeGate intake;

    @BeforeEach
    void setUp() {
        DiskFullGuard.reset();
        probe = mock(DiskSpaceProbe.class);
        intake = new WorkIntakeGate();
        when(probe.path()).thenReturn("/data");
        when(probe.totalBytes()).thenReturn(TOTAL);
    }

    @AfterEach
    void tearDown() {
        DiskFullGuard.reset();
    }

    /** The self-hosted default: 2 GB stop floor, 4 GB resume. */
    private DiskFullGuard guard(boolean enabled) {
        return new DiskFullGuard(probe, intake, enabled, 2048, 4096);
    }

    private void free(double gb) {
        long free = (long) (gb * GB);
        when(probe.usableBytes()).thenReturn(free);
        when(probe.usagePercent()).thenReturn((int) Math.round(100.0 * (TOTAL - free) / TOTAL));
    }

    @Test
    void stopsBelowTheFreeFloor_resumesOnlyAboveTheResumeMark() {
        DiskFullGuard guard = guard(true);

        free(1.5);
        guard.check();
        assertThat(intake.isAccepting()).isFalse();
        assertThat(DiskFullGuard.snapshot()).containsEntry("stopped", true).containsEntry("freeMb", 1536L);

        free(3.0); // between the marks: stays stopped (no flapping)
        guard.check();
        assertThat(intake.isAccepting()).isFalse();

        free(5.0);
        guard.check();
        assertThat(intake.isAccepting()).isTrue();
        assertThat(DiskFullGuard.snapshot()).containsEntry("stopped", false);
    }

    /** 2026-11-11: a laptop at 95.5 % used still had 11.3 GB free — that is not a full disk. */
    @Test
    void aBigDiskAtNinetyFivePercentWithGigabytesFree_keepsWorking() {
        free(11.3);
        guard(true).check();

        assertThat(intake.isAccepting()).isTrue();
        assertThat(DiskFullGuard.snapshot()).containsEntry("stopped", false).containsEntry("usagePercent", 96);
    }

    @Test
    void aDiskFullStop_neverParksTokens_andSurvivesAnUpdateReopeningItsOwnReason() {
        DiskFullGuard guard = guard(true);
        free(0.5);
        guard.check();

        assertThat(intake.isUpdateInProgress()).as("only an update parks tokens").isFalse();
        intake.close(WorkIntakeGate.Reason.UPDATE);
        intake.open(WorkIntakeGate.Reason.UPDATE); // an aborted update must not lift the disk stop
        assertThat(intake.isAccepting()).isFalse();
    }

    @Test
    void disabledOrUnknownVolume_neverStops() {
        free(0.1);
        guard(false).check();
        assertThat(intake.isAccepting()).isTrue();

        when(probe.totalBytes()).thenReturn(0L);
        guard(true).check();
        assertThat(intake.isAccepting()).isTrue();
    }
}
