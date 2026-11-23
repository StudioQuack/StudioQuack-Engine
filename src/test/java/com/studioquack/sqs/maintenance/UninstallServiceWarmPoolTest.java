package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import com.studioquack.sqs.sandbox.warmpool.Warmer;
import com.studioquack.sqs.secrets.inventory.ClientSecretInventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Uninstall must leave no container behind, including the ones a run still had checked out. It must
 * also latch the pool shut.
 */
class UninstallServiceWarmPoolTest {

    private static final SandboxOwnership OWNERSHIP = SandboxOwnership.of("owner-1", "boot-1");

    @TempDir
    Path temp;

    @Test
    void purge_stopsEverythingTheDaemonHeldOpen_andTheWarmPoolCannotReopenAny() throws Exception {
        HostWarmer warmer = new HostWarmer();
        WarmPool pool = new WarmPool(warmer, Map.of("python:3.11-slim", 4), Runnable::run, OWNERSHIP);
        pool.reconcile();
        WarmSlot inFlight = pool.acquire("python:3.11-slim").orElseThrow();
        // A cold, single-job container: created outside the reservoir, so only ownership finds it.
        warmer.plant("cold-job", HardeningArgs.ROLE_JOB);
        assertEquals(6, warmer.host.size(), "reservoir + the slot out with a run + a cold job container");

        UninstallService.UninstallSummary summary = service(pool).purge(false);

        assertTrue(warmer.host.isEmpty(), "uninstall left containers open: " + warmer.host.keySet());
        assertEquals(0, pool.leasedCount(), "the slot that was out with a run was stopped too");
        assertTrue(warmer.host.get(inFlight.containerId()) == null);
        assertTrue(summary.reapedContainers() >= 6, "every stopped container is reported back to the BE");
        assertTrue(summary.notes().stream().anyMatch(n -> n.startsWith("warm pool closed:")));

        // The reconcile loop is still alive at this point in a real uninstall; it must find nothing to do.
        pool.setTarget("python:3.11-slim", 4);
        pool.reconcile();
        assertTrue(warmer.host.isEmpty(), "the warm pool re-opened containers after the uninstall purge");
    }

    /** The purge must still complete when there is no pool wired (ops endpoint on a degraded daemon). */
    @Test
    void purge_survivesWithoutAWarmPool() throws Exception {
        UninstallService.UninstallSummary summary = service(null).purge(false);

        assertEquals(0, summary.reapedContainers());
        assertTrue(summary.notes().stream().noneMatch(n -> n.startsWith("warm pool closed:")));
    }

    private UninstallService service(WarmPool pool) throws Exception {
        ClientSecretInventoryService inventory = mock(ClientSecretInventoryService.class);
        when(inventory.purgeAllOwned()).thenReturn(0);
        ContainerRuntime runtime = mock(ContainerRuntime.class);
        when(runtime.available()).thenReturn(false);
        Path data = temp.resolve("sqs").resolve("data");
        Files.createDirectories(data);
        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());
        return new UninstallService(inventory, runtime, env, pool);
    }

    /** In-memory stand-in for the host's container list, independent of the pool's bookkeeping. */
    private static final class HostWarmer implements Warmer {
        final Map<String, OwnedContainer> host = new LinkedHashMap<>();
        private final AtomicInteger spawns = new AtomicInteger();

        @Override
        public String kind() {
            return "fake";
        }

        @Override
        public WarmSlot warm(WarmSpec spec) {
            String cid = "c" + spawns.incrementAndGet();
            plant(cid, HardeningArgs.ROLE_WARM);
            return new WarmSlot(spec.language(), "w_" + cid, cid, kind(), spec.imageRef(), Instant.now());
        }

        @Override
        public void cool(WarmSlot slot) {
            if (slot != null) {
                remove(slot.containerId());
            }
        }

        @Override
        public void remove(String containerId) {
            host.remove(containerId);
        }

        @Override
        public List<OwnedContainer> ownedContainers() {
            return List.copyOf(host.values());
        }

        @Override
        public boolean alive(WarmSlot slot) {
            return slot != null && host.containsKey(slot.containerId());
        }

        void plant(String containerId, String role) {
            host.put(containerId, new OwnedContainer(containerId, role, OWNERSHIP.bootId()));
        }
    }
}
