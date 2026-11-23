package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import com.studioquack.sqs.channel.overlay.PeerCallAuthenticator;
import com.studioquack.sqs.channel.overlay.PeerCallRegistry;
import com.studioquack.sqs.sandbox.helperchan.HelperExecuteService;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A caller whose run ends mid-call cancels the work it placed on this daemon: the cancel route
 * revokes the stand-in for the caller's request, which is what ends the child run here. Only the
 * environment that placed the call can cancel it.
 */
class OverlayPeerListenerCancelTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newHttpClient();
    private static final String EXECUTE_BODY = "{\"executionContext\":{\"envId\":\"env-b\",\"workflowExecutionId\":\"wfe-1\"},"
            + "\"targetNodeId\":\"node-7\",\"portValue\":\"model\",\"payload\":{}}";

    /** Caller env = the bearer value, so tests can speak as two environments. */
    private static final PeerCallAuthenticator BY_BEARER =
            (authorization, scope) -> PeerCallAuthenticator.AuthResult.allow(authorization);

    private final CountDownLatch entered = new CountDownLatch(1);
    private final AtomicReference<HelperJob> seenCaller = new AtomicReference<>();

    /** An execute that runs until its caller stand-in is revoked. */
    private HelperExecuteService blockingExecute() throws Exception {
        HelperExecuteService svc = mock(HelperExecuteService.class);
        when(svc.executeForPeer(any(), any(), any(), any(), any())).thenAnswer(inv -> {
            HelperJob caller = inv.getArgument(4);
            seenCaller.set(caller);
            CountDownLatch revoked = new CountDownLatch(1);
            caller.onRevoke(revoked::countDown);
            entered.countDown();
            revoked.await(10, TimeUnit.SECONDS);
            return Map.of("cancelled", caller.revoked());
        });
        return svc;
    }

    @Test
    void theCallerCancelsItsOwnInFlightExecute() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0, blockingExecute(), BY_BEARER, MAPPER);
        listener.start();
        try {
            CompletableFuture<HttpResponse<String>> execute = send(listener, "/sqs/v1/overlay/execute", "env-a", "call-1", EXECUTE_BODY);
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            HttpResponse<String> cancel = send(listener, PeerCallRegistry.CANCEL_PATH, "env-a", "call-1", null).get(5, TimeUnit.SECONDS);
            assertEquals(204, cancel.statusCode());
            assertTrue(seenCaller.get().revoked(), "the cancel revokes the caller's stand-in");
            assertEquals(200, execute.get(5, TimeUnit.SECONDS).statusCode());
            assertEquals(404, send(listener, PeerCallRegistry.CANCEL_PATH, "env-a", "call-1", null)
                    .get(5, TimeUnit.SECONDS).statusCode(), "a finished call is no longer cancellable");
        } finally {
            listener.stop();
        }
    }

    @Test
    void anotherEnvironmentCannotCancelTheCall() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0, blockingExecute(), BY_BEARER, MAPPER);
        listener.start();
        try {
            CompletableFuture<HttpResponse<String>> execute = send(listener, "/sqs/v1/overlay/execute", "env-a", "call-2", EXECUTE_BODY);
            assertTrue(entered.await(5, TimeUnit.SECONDS));

            assertEquals(404, send(listener, PeerCallRegistry.CANCEL_PATH, "env-x", "call-2", null)
                    .get(5, TimeUnit.SECONDS).statusCode());
            assertFalse(seenCaller.get().revoked(), "a foreign cancel leaves the call running");

            assertEquals(204, send(listener, PeerCallRegistry.CANCEL_PATH, "env-a", "call-2", null)
                    .get(5, TimeUnit.SECONDS).statusCode());
            assertEquals(200, execute.get(5, TimeUnit.SECONDS).statusCode());
        } finally {
            listener.stop();
        }
    }

    @Test
    void cancelOfAnUnknownCallIs404() throws Exception {
        OverlayPeerListener listener = new OverlayPeerListener("127.0.0.1", 0, mock(HelperExecuteService.class),
                BY_BEARER, MAPPER);
        listener.start();
        try {
            assertEquals(404, send(listener, PeerCallRegistry.CANCEL_PATH, "env-a", "no-such-call", null)
                    .get(5, TimeUnit.SECONDS).statusCode());
        } finally {
            listener.stop();
        }
    }

    private static CompletableFuture<HttpResponse<String>> send(OverlayPeerListener listener, String path,
                                                                String bearer, String callId, String body) {
        HttpRequest.Builder req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + listener.boundPort() + path))
                .header("Authorization", bearer)
                .header(PeerCallRegistry.CALL_ID_HEADER, callId);
        req = body == null ? req.POST(HttpRequest.BodyPublishers.noBody())
                : req.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        return CLIENT.sendAsync(req.build(), HttpResponse.BodyHandlers.ofString());
    }
}
