package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import com.studioquack.sqs.maintenance.DaemonLifecycleService.LifecycleResult;
import com.studioquack.sqs.maintenance.UninstallService;
import com.studioquack.sqs.maintenance.UninstallService.UninstallSummary;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The {@code uninstall} command handler purges (with the requested deps flag), schedules a graceful
 * exit, and ACKs a summary. Never actually exits — the {@link DaemonLifecycleService} exit seam is
 * mocked.
 */
class UninstallCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void handle_purgesWithDepsFlag_thenSchedulesExit_andAcksSummary() throws Exception {
        UninstallService uninstall = mock(UninstallService.class);
        DaemonLifecycleService lifecycle = mock(DaemonLifecycleService.class);
        when(uninstall.purge(true)).thenReturn(new UninstallSummary(2, true, 1, List.of()));
        when(lifecycle.requestUninstall(true, false))
                .thenReturn(new LifecycleResult(true, "uninstall", "side-car", "side-car teardown spawned"));

        UninstallCommandHandler handler = new UninstallCommandHandler(mapper, uninstall, lifecycle);
        DaemonCommand cmd = new DaemonCommand("cmd-9", "uninstall", null, "{\"removeDependencies\":true}");

        DaemonCommandOutcome out = handler.handle(cmd);

        assertEquals("ok", out.status());
        JsonNode body = mapper.readTree(out.output());
        assertEquals(2, body.get("purgedSecrets").asInt());
        assertTrue(body.get("dependenciesRemoved").asBoolean());
        assertEquals(1, body.get("reapedContainers").asInt());
        assertTrue(body.get("exitScheduled").asBoolean());
        verify(uninstall).purge(true);
        verify(lifecycle).requestUninstall(true, false);
    }

    @Test
    void handle_passesRemovePodmanThrough_whenOperatorOptedIn() throws Exception {
        UninstallService uninstall = mock(UninstallService.class);
        DaemonLifecycleService lifecycle = mock(DaemonLifecycleService.class);
        when(uninstall.purge(false)).thenReturn(new UninstallSummary(0, false, 3, List.of()));
        when(lifecycle.requestUninstall(false, true))
                .thenReturn(new LifecycleResult(true, "uninstall", "side-car", "side-car teardown spawned"));

        UninstallCommandHandler handler = new UninstallCommandHandler(mapper, uninstall, lifecycle);
        DaemonCommand cmd = new DaemonCommand("cmd-11", "uninstall", null, "{\"removePodman\":true}");

        assertTrue(handler.handle(cmd).succeeded());
        // The two opt-ins are independent: removing Podman must not drag the dependency cache with it.
        verify(uninstall).purge(false);
        verify(lifecycle).requestUninstall(false, true);
    }

    @Test
    void handle_defaultsRemovePodmanFalse_onMalformedPayload() throws Exception {
        UninstallService uninstall = mock(UninstallService.class);
        DaemonLifecycleService lifecycle = mock(DaemonLifecycleService.class);
        when(uninstall.purge(false)).thenReturn(new UninstallSummary(0, false, 0, List.of()));
        when(lifecycle.requestUninstall(false, false))
                .thenReturn(new LifecycleResult(true, "uninstall", "none", "exit scheduled"));

        UninstallCommandHandler handler = new UninstallCommandHandler(mapper, uninstall, lifecycle);
        // Garbage payload must never be read as consent to delete something off the operator's machine.
        DaemonCommand cmd = new DaemonCommand("cmd-12", "uninstall", null, "{not-json");

        assertTrue(handler.handle(cmd).succeeded());
        verify(lifecycle).requestUninstall(false, false);
    }

    @Test
    void handle_defaultsRemoveDependenciesFalse_whenPayloadAbsent() throws Exception {
        UninstallService uninstall = mock(UninstallService.class);
        DaemonLifecycleService lifecycle = mock(DaemonLifecycleService.class);
        when(uninstall.purge(false)).thenReturn(new UninstallSummary(0, false, 0, List.of()));
        when(lifecycle.requestUninstall(false, false))
                .thenReturn(new LifecycleResult(true, "uninstall", "none", "exit scheduled"));

        UninstallCommandHandler handler = new UninstallCommandHandler(mapper, uninstall, lifecycle);
        DaemonCommand cmd = new DaemonCommand("cmd-10", "uninstall", null, null);

        DaemonCommandOutcome out = handler.handle(cmd);

        assertEquals("ok", out.status());
        verify(uninstall).purge(false);
    }
}
