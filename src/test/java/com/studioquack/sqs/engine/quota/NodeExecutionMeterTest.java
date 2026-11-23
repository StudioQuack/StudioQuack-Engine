package com.studioquack.sqs.engine.quota;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeExecutionMeterTest {

    @Test
    void admitsUntilASnapshotSaysTheAllowanceIsSpent() {
        NodeExecutionMeter meter = new NodeExecutionMeter();
        assertTrue(meter.admitNewRun());
        meter.recordActivation();
        meter.applySnapshot(Map.of("enforced", true, "granted", 2L, "consumed", 0L));
        assertTrue(meter.admitNewRun());
        meter.recordActivation();
        assertFalse(meter.admitNewRun());
    }

    @Test
    void proSnapshotStopsCountingAndAlwaysAdmits() {
        NodeExecutionMeter meter = new NodeExecutionMeter();
        meter.applySnapshot(Map.of("enforced", false));
        meter.recordActivation();
        assertTrue(meter.admitNewRun());
        assertEquals0(meter.drainDelta());
    }

    @Test
    void aFailedBeatRestoresTheDrainedDelta() {
        NodeExecutionMeter meter = new NodeExecutionMeter();
        meter.applySnapshot(Map.of("enforced", true, "granted", 10L, "consumed", 0L));
        meter.recordActivation();
        long drained = meter.drainDelta();
        assertTrue(meter.admitNewRun());
        meter.restoreDelta(drained);
        meter.applySnapshot(Map.of("enforced", true, "granted", 1L, "consumed", 1L));
        assertFalse(meter.admitNewRun());
    }

    @Test
    void aRenewedPeriodAdmitsAgainAndDropsTheOldPeriodsUnflushedCount() {
        NodeExecutionMeter meter = new NodeExecutionMeter();
        meter.applySnapshot(Map.of("enforced", true, "granted", 2L, "consumed", 1L,
                "periodStart", "2026-10-20T00:00:00Z"));
        meter.recordActivation();
        assertFalse(meter.admitNewRun());

        meter.applySnapshot(Map.of("enforced", true, "granted", 2L, "consumed", 0L,
                "periodStart", "2026-11-20T00:00:00Z"));

        assertTrue(meter.admitNewRun());
        assertEquals0(meter.drainDelta());
    }

    @Test
    void tracksPendingDeltaAcrossRecordDrainAndRestore() {
        NodeExecutionMeter meter = new NodeExecutionMeter();
        assertFalse(meter.hasPendingDelta());
        meter.recordActivation();
        assertTrue(meter.hasPendingDelta());
        long drained = meter.drainDelta();
        assertFalse(meter.hasPendingDelta());
        meter.restoreDelta(drained);
        assertTrue(meter.hasPendingDelta());
    }

    private static void assertEquals0(long value) {
        org.junit.jupiter.api.Assertions.assertEquals(0L, value);
    }
}
