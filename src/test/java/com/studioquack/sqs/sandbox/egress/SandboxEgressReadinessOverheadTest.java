package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.ExecResult;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.Platform;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Warm-pool / dispatch hot-path micro-benchmark for {@link SandboxEgressManager#readiness}. The
 * dispatch path gains exactly one volatile read; all rule programming happens at overlay-up, off
 * the dispatch path.
 */
class SandboxEgressReadinessOverheadTest {

    private static final int ITERATIONS = 1_000_000;

    @Test
    void readiness_avg_under_1ms_per_call() {
        SandboxEgressManager m = SandboxEgressManager.forTest(
                "auto", true, true, "sqnet0", Platform.supported(),
                "/usr/bin/podman",
                (argv, timeout) -> new ExecResult(!argv.contains("-C"),
                        argv.contains("-C") ? 1 : 0, ""),
                cli -> List.of(new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1")));
        m.overlayUp("100.64.1.7/32");

        for (int i = 0; i < 10_000; i++) {
            m.readiness();
        }

        long start = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            m.readiness();
        }
        long elapsed = System.nanoTime() - start;
        double nsPerOp = (double) elapsed / ITERATIONS;
        System.out.println("SandboxEgressReadinessOverheadTest readiness() ns/op="
                + String.format("%.2f", nsPerOp)
                + " (iterations=" + ITERATIONS + ")");

        assertTrue(nsPerOp < 1_000_000.0,
                "readiness() avg must stay under 1ms/call; measured ns/op=" + nsPerOp);
    }
}
