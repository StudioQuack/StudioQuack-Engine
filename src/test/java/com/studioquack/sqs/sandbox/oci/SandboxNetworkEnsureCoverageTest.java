package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sandbox network takes containers only once the host firewall filters it: {@code ensure} runs
 * the coverage hook before it returns, and no spawn may skip past a network still being covered.
 */
class SandboxNetworkEnsureCoverageTest {

    private static final SandboxNetworkEnsure.Exec CREATES = (argv, timeout) -> argv.contains("exists")
            ? new SandboxNetworkEnsure.ExecResult(false, 1, "missing")
            : new SandboxNetworkEnsure.ExecResult(true, 0, "");

    @AfterEach
    void removeHook() {
        SandboxNetworkEnsure.setCoverageHook(null);
    }

    @Test
    void aNewNetworkIsCoveredBeforeEnsureReturns() {
        List<String> order = new ArrayList<>();
        SandboxNetworkEnsure.setCoverageHook(name -> order.add("cover " + name));

        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-cover-new", (argv, timeout) -> {
            order.add(argv.get(1) + " " + argv.get(2));
            return CREATES.run(argv, timeout);
        });

        assertEquals(List.of("network exists", "network create", "cover sqs-cover-new"), order);
    }

    @Test
    void aNetworkTheHookCannotCover_isRefused_andTheNextEnsureCoversIt() {
        AtomicInteger covers = new AtomicInteger();
        AtomicBoolean refuse = new AtomicBoolean(true);
        SandboxNetworkEnsure.setCoverageHook(name -> {
            covers.incrementAndGet();
            if (refuse.get()) {
                throw new IllegalStateException("not filtered by the host firewall");
            }
        });

        assertThrows(IllegalStateException.class,
                () -> SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-cover-retry", CREATES));
        refuse.set(false);
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-cover-retry", CREATES);
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-cover-retry", CREATES);

        assertEquals(2, covers.get(), "covered again after the refusal, then ready without another cover");
    }

    @Test
    void aSecondSpawnWaitsWhileTheNetworkIsStillBeingCovered() throws Exception {
        CountDownLatch covering = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SandboxNetworkEnsure.setCoverageHook(name -> {
            covering.countDown();
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        Thread first = new Thread(() -> SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-cover-race", CREATES));
        first.start();
        assertTrue(covering.await(5, TimeUnit.SECONDS));

        AtomicBoolean secondReturned = new AtomicBoolean();
        Thread second = new Thread(() -> {
            SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-cover-race", CREATES);
            secondReturned.set(true);
        });
        second.start();
        Thread.sleep(200);
        assertFalse(secondReturned.get(), "the network is created but not yet filtered");

        release.countDown();
        first.join(5_000);
        second.join(5_000);
        assertTrue(secondReturned.get());
    }

    @Test
    void ensureWithoutCoverage_createsNetworkWithoutInvokingHookAndEvictsStaleCache() {
        AtomicInteger covers = new AtomicInteger();
        SandboxNetworkEnsure.setCoverageHook(name -> covers.incrementAndGet());

        // Seed cache via normal ensure, then simulate runtime reset where network disappeared.
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-no-cover", CREATES);
        assertEquals(1, covers.get());

        List<String> calls = new ArrayList<>();
        SandboxNetworkEnsure.ensureWithoutCoverage("/usr/bin/podman", "sqs-no-cover", (argv, timeout) -> {
            calls.add(argv.get(1) + " " + argv.get(2));
            return CREATES.run(argv, timeout);
        });

        assertEquals(List.of("network exists", "network create"), calls);
        assertEquals(1, covers.get(), "ensureWithoutCoverage must not call the coverage hook");

        // Subsequent normal ensure must run the coverage hook again because READY was evicted.
        SandboxNetworkEnsure.ensure("/usr/bin/podman", "sqs-no-cover", (argv, timeout) ->
                new SandboxNetworkEnsure.ExecResult(true, 0, ""));
        assertEquals(2, covers.get());
    }
}
