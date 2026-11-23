package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager.OpenResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for shell open/input/close command handlers. Mocks {@link StatefulShellSessionManager}
 * so no real {@code podman exec} is started.
 */
class StatefulShellCommandHandlersTest {

    private static final String ENV = "env-1";
    private static final String INSTANCE_ID = "inst_abc";
    private static final String SESSION_ID = "sess-be-minted-uuid";

    private final ObjectMapper mapper = new ObjectMapper();
    private StatefulInstanceManager instanceManager;
    private StatefulShellSessionManager shellSessions;
    private OciCli cli;
    private StatefulShellOpenCommandHandler open;
    private StatefulShellInputCommandHandler input;
    private StatefulShellCloseCommandHandler close;

    @BeforeEach
    void setUp() {
        instanceManager = mock(StatefulInstanceManager.class);
        shellSessions = mock(StatefulShellSessionManager.class);
        cli = mock(OciCli.class);
        open = new StatefulShellOpenCommandHandler(mapper, instanceManager, shellSessions, cli);
        input = new StatefulShellInputCommandHandler(mapper, shellSessions);
        close = new StatefulShellCloseCommandHandler(mapper, shellSessions);
    }

    @Test
    void input_unknown_session_fails() throws Exception {
        doThrow(new IllegalStateException("stateful-shell-input: unknown sessionId"))
                .when(shellSessions).writeInput(eq(SESSION_ID), any());

        DaemonCommandOutcome out = input.handle(cmd(StatefulShellInputCommandHandler.TYPE,
                "{\"envId\":\"" + ENV + "\",\"sessionId\":\"" + SESSION_ID + "\",\"data\":\"ls\\n\"}"));

        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status());
        assertTrue(out.error() != null && out.error().contains("unknown sessionId"),
                "expected unknown-session error, got: " + out.error());
    }

    @Test
    void open_refuses_when_manager_refuses_cap() throws Exception {
        when(instanceManager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningInstance()));
        when(shellSessions.open(eq(ENV), eq(INSTANCE_ID), eq(SESSION_ID), anyInt(), anyInt(),
                any(StatefulInstance.class), eq(cli)))
                .thenReturn(OpenResult.fail("stateful-shell-open: concurrent session cap (5) reached"));

        DaemonCommandOutcome out = open.handle(cmd(StatefulShellOpenCommandHandler.TYPE,
                payloadOpen()));

        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status());
        assertTrue(out.error() != null && out.error().contains("concurrent session cap"),
                "expected cap refusal, got: " + out.error());
    }

    @Test
    void open_refuses_when_instance_not_running() throws Exception {
        when(instanceManager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(stoppedInstance()));
        when(shellSessions.open(eq(ENV), eq(INSTANCE_ID), eq(SESSION_ID), anyInt(), anyInt(),
                any(StatefulInstance.class), eq(cli)))
                .thenReturn(OpenResult.fail("stateful-shell-open: instance is not RUNNING"));

        DaemonCommandOutcome out = open.handle(cmd(StatefulShellOpenCommandHandler.TYPE,
                payloadOpen()));

        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status());
        assertTrue(out.error() != null && out.error().contains("not RUNNING"),
                "expected not-running refusal, got: " + out.error());
    }

    @Test
    void open_success_echoes_sessionId_verbatim() throws Exception {
        when(instanceManager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningInstance()));
        when(shellSessions.open(eq(ENV), eq(INSTANCE_ID), eq(SESSION_ID), anyInt(), anyInt(),
                any(StatefulInstance.class), eq(cli)))
                .thenReturn(OpenResult.success(SESSION_ID));

        DaemonCommandOutcome out = open.handle(cmd(StatefulShellOpenCommandHandler.TYPE,
                payloadOpen()));

        assertEquals(DaemonCommandOutcome.STATUS_OK, out.status());
        JsonNode body = mapper.readTree(out.output());
        assertEquals(SESSION_ID, body.path("sessionId").asText(),
                "sessionId must be echoed verbatim for BE acceptance");
        assertEquals(StatefulShellSessionManager.SHELL, body.path("shell").asText());
    }

    @Test
    void input_success_acks_empty_body() throws Exception {
        DaemonCommandOutcome out = input.handle(cmd(StatefulShellInputCommandHandler.TYPE,
                "{\"envId\":\"" + ENV + "\",\"sessionId\":\"" + SESSION_ID + "\",\"data\":\"x\"}"));
        assertEquals(DaemonCommandOutcome.STATUS_OK, out.status());
        assertEquals("{}", out.output());
        verify(shellSessions).writeInput(SESSION_ID, "x");
    }

    @Test
    void close_unknown_session_is_idempotent_ok() throws Exception {
        doThrow(new IllegalStateException("stateful-shell-close: unknown sessionId"))
                .when(shellSessions).close(eq(SESSION_ID), any());

        DaemonCommandOutcome out = close.handle(cmd(StatefulShellCloseCommandHandler.TYPE,
                "{\"envId\":\"" + ENV + "\",\"sessionId\":\"" + SESSION_ID + "\"}"));

        assertEquals(DaemonCommandOutcome.STATUS_OK, out.status());
        assertEquals("{}", out.output());
    }

    private static String payloadOpen() {
        return "{\"envId\":\"" + ENV + "\",\"instanceId\":\"" + INSTANCE_ID
                + "\",\"sessionId\":\"" + SESSION_ID + "\",\"cols\":120,\"rows\":40}";
    }

    private static DaemonCommand cmd(String type, String payload) {
        return new DaemonCommand("cmd-1", type, "2026-08-06T00:00:00Z", payload);
    }

    private static StatefulInstance runningInstance() {
        StatefulInstance inst = new StatefulInstance(
                INSTANCE_ID, ENV, "redis:7",
                "sq-stateful-" + INSTANCE_ID, "sq-vol-" + INSTANCE_ID,
                StatefulCaps.defaults(), true);
        inst.setContainerId("cid-1");
        inst.setStatus(StatefulStatus.RUNNING);
        return inst;
    }

    private static StatefulInstance stoppedInstance() {
        StatefulInstance inst = new StatefulInstance(
                INSTANCE_ID, ENV, "redis:7",
                "sq-stateful-" + INSTANCE_ID, "sq-vol-" + INSTANCE_ID,
                StatefulCaps.defaults(), true);
        inst.setStatus(StatefulStatus.STOPPED);
        return inst;
    }
}
