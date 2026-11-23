package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.LifecycleResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link RestartCommandHandler}: exact wire {@code type()}, mapping accepted/refused
 * outcomes, and never throwing out of {@code handle}.
 */
class MaintenanceCommandHandlersTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private DaemonLifecycleService lifecycle;

    @BeforeEach
    void setUp() {
        lifecycle = mock(DaemonLifecycleService.class);
    }

    private static DaemonCommand cmd(String id, String type) {
        return new DaemonCommand(id, type, "2026-07-13T00:00:00Z", null);
    }

    @Test
    void handlerTypes_matchBeWireTypes() {
        assertEquals("restart", new RestartCommandHandler(mapper, lifecycle).type());
    }

    @Test
    void restart_accepted_isOk_refused_isFailed() {
        when(lifecycle.requestRestart())
                .thenReturn(new LifecycleResult(true, "restart", "SYSTEMD", "scheduled"));
        assertTrue(new RestartCommandHandler(mapper, lifecycle).handle(cmd("r1", "restart")).succeeded());

        when(lifecycle.requestRestart())
                .thenReturn(new LifecycleResult(false, "restart", "NONE", "not running under a service manager"));
        DaemonCommandOutcome refused = new RestartCommandHandler(mapper, lifecycle).handle(cmd("r2", "restart"));
        assertFalse(refused.succeeded());
        assertTrue(refused.error().contains("service manager"));
    }

    @Test
    void lifecycleHandlers_neverThrow_whenServiceThrows() {
        when(lifecycle.requestRestart()).thenThrow(new RuntimeException("kaboom"));

        assertFalse(new RestartCommandHandler(mapper, lifecycle).handle(cmd("r", "restart")).succeeded());
    }
}
