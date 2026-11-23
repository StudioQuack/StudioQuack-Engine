package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code update-node-library} handler contract: payload validation, enrolled-env mismatch
 * refusal, immediate ack shape (accepted / refused-with-reason), and the guarantee that a
 * refused command never touches the pipeline. The pipeline itself is covered by
 * {@code BuiltinNodeLibraryManagerTest}.
 */
class UpdateNodeLibraryCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final BuiltinNodeLibraryManager manager = mock(BuiltinNodeLibraryManager.class);

    private UpdateNodeLibraryCommandHandler handler(String enrolledEnvId) {
        return new UpdateNodeLibraryCommandHandler(mapper, manager, enrolledEnvId);
    }

    private DaemonCommand command(String payload) {
        return new DaemonCommand("cmd-1", UpdateNodeLibraryCommandHandler.TYPE, null, payload);
    }

    @Test
    void malformedPayloadFailsWithoutTouchingThePipeline() {
        DaemonCommandOutcome outcome = handler("env-1").handle(command("{not json"));
        assertEquals("failed", outcome.status());
        verify(manager, never()).updateNow();
    }

    @Test
    void missingEnvIdFails() {
        DaemonCommandOutcome outcome = handler("env-1").handle(command("{}"));
        assertEquals("failed", outcome.status());
        assertTrue(outcome.error().contains("envId"));
        verify(manager, never()).updateNow();
    }

    @Test
    void payloadNamingAnotherEnvironmentIsRefused() {
        // The command channel is authenticated, but this host owns exactly ONE system
        // executor — a payload for a different env must never recycle it.
        DaemonCommandOutcome outcome = handler("env-1").handle(command("{\"envId\":\"env-2\"}"));
        assertEquals("failed", outcome.status());
        verify(manager, never()).updateNow();
    }

    @Test
    void acceptedUpdateAcksImmediately() throws Exception {
        when(manager.updateNow()).thenReturn(new BuiltinNodeLibraryManager.UpdateAck(true, null));
        DaemonCommandOutcome outcome = handler("env-1").handle(command("{\"envId\":\"env-1\"}"));
        assertEquals("ok", outcome.status());
        JsonNode body = mapper.readTree(outcome.output());
        assertTrue(body.get("accepted").asBoolean());
        assertFalse(body.has("reason"), "an accepted ack carries no reason");
        verify(manager).updateNow();
    }

    @Test
    void rejectedUpdateSurfacesTheReasonAsAnOkOutcome() throws Exception {
        // "Already in progress" is a state, not a command failure — the caller must see
        // accepted:false rather than a failed command it might retry into a second build.
        when(manager.updateNow()).thenReturn(
                new BuiltinNodeLibraryManager.UpdateAck(false, "an update is already in progress"));
        DaemonCommandOutcome outcome = handler("env-1").handle(command("{\"envId\":\"env-1\"}"));
        assertEquals("ok", outcome.status());
        JsonNode body = mapper.readTree(outcome.output());
        assertFalse(body.get("accepted").asBoolean());
        assertNotNull(body.get("reason"));
    }

    @Test
    void blankEnrolledEnvTrustsThePayloadEnv() throws Exception {
        // Pre-enrolment / dev posture: no local env to cross-check, the manager's own
        // precondition (unenrolled → rejected ack) is the guard.
        when(manager.updateNow()).thenReturn(new BuiltinNodeLibraryManager.UpdateAck(true, null));
        DaemonCommandOutcome outcome = handler("").handle(command("{\"envId\":\"env-9\"}"));
        assertEquals("ok", outcome.status());
        verify(manager).updateNow();
    }
}
