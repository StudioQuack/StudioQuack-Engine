package com.studioquack.sqs.runtime;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The in-memory dispatch→ReceiveTask wait-state registry (register/find/complete). */
class DispatchExecutionRegistryTest {

    private final DispatchExecutionRegistry registry = new DispatchExecutionRegistry();

    @Test
    void register_find_and_complete_track_pending_dispatches() {
        assertEquals(0, registry.pending());

        DispatchExecutionRegistry.WaitState state =
                registry.register("d1", "pi1", "bk1", "nodeA", "wait_nodeA");
        assertEquals("wait_nodeA", state.waitActivityId());
        assertEquals(1, registry.pending());

        Optional<DispatchExecutionRegistry.WaitState> found = registry.find("d1");
        assertTrue(found.isPresent());
        assertEquals("nodeA", found.get().nodeId());

        Optional<DispatchExecutionRegistry.WaitState> completed = registry.complete("d1");
        assertTrue(completed.isPresent());
        assertEquals(0, registry.pending(), "completed dispatch removed");
        assertTrue(registry.find("d1").isEmpty());
        assertTrue(registry.complete("d1").isEmpty(), "complete is idempotent");
    }
}
