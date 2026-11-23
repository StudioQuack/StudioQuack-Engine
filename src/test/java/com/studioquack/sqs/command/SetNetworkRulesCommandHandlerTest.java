package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * The set-network-rules command replaces the daemon's projected rule set wholesale and acks the
 * count; a malformed payload fails the command without touching the live projection.
 */
class SetNetworkRulesCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final NetworkRuleStore store = NetworkRuleStore.inMemory();
    /** No stateful manager on this lane by default — the republish hook is a no-op. */
    private final SetNetworkRulesCommandHandler handler =
            new SetNetworkRulesCommandHandler(mapper, store, emptyProvider());

    private static DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-1", SetNetworkRulesCommandHandler.TYPE, null, payload);
    }

    @Test
    void fullSetReplacesTheProjection() {
        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":2222}]}"));

        assertTrue(outcome.succeeded(), () -> outcome.error());
        assertEquals(Set.of("engine-2"), store.allowedSourceEngineIds(2222));
    }

    @Test
    void outboundMirrorRidesTheSameCommand() {
        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":2222}],"
                        + "\"outbound\":[{\"targetEnvId\":\"engine-4\",\"port\":9090}]}"));

        assertTrue(outcome.succeeded(), () -> outcome.error());
        assertEquals(Set.of(new NetworkRuleStore.OutboundRule("engine-4", 9090)),
                store.outboundSnapshot());
        assertTrue(outcome.output().contains("\"outbound\":1"), "the ack counts both halves");
    }

    @Test
    void emptySetWipesTheProjection_soDeletedRulesNeverLinger() {
        handler.handle(cmd("{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":2222}],"
                + "\"outbound\":[{\"targetEnvId\":\"engine-4\",\"port\":9090}]}"));

        DaemonCommandOutcome outcome = handler.handle(cmd("{\"rules\":[],\"outbound\":[]}"));

        assertTrue(outcome.succeeded(), () -> outcome.error());
        assertTrue(store.allowedSourceEngineIds(2222).isEmpty());
        assertTrue(store.outboundSnapshot().isEmpty(),
                "the outbound view wipes wholesale as well");
    }

    @Test
    void malformedPayloadFailsWithoutTouchingTheLiveSet() {
        handler.handle(cmd("{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":2222}]}"));

        DaemonCommandOutcome outcome = handler.handle(cmd("not-json"));

        assertFalse(outcome.succeeded());
        assertEquals(Set.of("engine-2"), store.allowedSourceEngineIds(2222),
                "a failed command must not clobber the projection it could not parse");
    }

    @Test
    void missingPayloadMeansEmptySet() {
        handler.handle(cmd("{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":2222}]}"));

        DaemonCommandOutcome outcome = handler.handle(cmd(null));

        assertTrue(outcome.succeeded(), () -> outcome.error());
        assertTrue(store.snapshot().isEmpty());
        assertTrue(store.outboundSnapshot().isEmpty());
    }

    /**
     * A rule ADDED while a reverse-labelled container is already running must not wait for the
     * next handshake: the handler re-asserts surviving reverse ports against the fresh projection.
     * A republish failure must never fail the ack — the rules ARE applied.
     */
    @Test
    @SuppressWarnings("unchecked")
    void rulesPushTriggersReversePortRepublish_butNeverFailsTheAck() {
        StatefulInstanceManager manager = mock(StatefulInstanceManager.class);
        ObjectProvider<StatefulInstanceManager> provider = mock(ObjectProvider.class);
        doAnswer(inv -> {
            ((Consumer<StatefulInstanceManager>) inv.getArgument(0)).accept(manager);
            return null;
        }).when(provider).ifAvailable(any());
        SetNetworkRulesCommandHandler hooked = new SetNetworkRulesCommandHandler(mapper, store, provider);

        DaemonCommandOutcome outcome = hooked.handle(cmd(
                "{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":5001}]}"));

        assertTrue(outcome.succeeded(), () -> outcome.error());
        verify(manager).republishSurvivingReversePorts();

        // A throwing republish still acks — the projection landed; the relay's accept-time ACL
        // enforces it regardless of when a listener rebinds.
        org.mockito.Mockito.doThrow(new IllegalStateException("podman down"))
                .when(manager).republishSurvivingReversePorts();
        DaemonCommandOutcome second = hooked.handle(cmd(
                "{\"rules\":[{\"sourceEnvId\":\"engine-2\",\"port\":5001}]}"));
        assertTrue(second.succeeded(), () -> second.error());
    }

    /** An {@link ObjectProvider} that has no bean — {@code ifAvailable} runs nothing. */
    @SuppressWarnings("unchecked")
    private static ObjectProvider<StatefulInstanceManager> emptyProvider() {
        return mock(ObjectProvider.class);
    }
}
