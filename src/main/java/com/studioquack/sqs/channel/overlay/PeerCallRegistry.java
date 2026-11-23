package com.studioquack.sqs.channel.overlay;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cross-env calls (execute, llm) this daemon is serving for a peer, keyed by the call id the CALLER chose, so
 * the caller can cancel its side the moment its own run ends. Without it the callee keeps working — a
 * sub-process, a model generation — for a caller that is gone, because a peer request carries no time limit
 * and the HTTP server cannot see the caller hang up.
 */
public final class PeerCallRegistry {

    /**
     * Request header carrying the caller-chosen call id.
     */
    public static final String CALL_ID_HEADER = "X-SQ-Peer-Call";

    /**
     * {@code POST} with the call id in {@link #CALL_ID_HEADER} and no body — cancels the caller's own in-flight
     * call. Authenticated with the cancelled call's own grant scope.
     */
    public static final String CANCEL_PATH = "/sqs/v1/overlay/cancel";

    private record Call(String callerEnvId, String scope, Runnable abort) {
    }

    private final Map<String, Call> calls = new ConcurrentHashMap<>();

    /**
     * A fresh call id for a caller to send with one cross-env call.
     */
    public static String newCallId() {
        return UUID.randomUUID().toString();
    }

    /**
     * The caller's cancel for {@code callId}, addressed to the peer at {@code baseUrl}.
     */
    public static HttpRequest cancelRequest(String baseUrl, String meshToken, String callId) {
        return HttpRequest.newBuilder(URI.create(baseUrl + CANCEL_PATH))
                .header("Authorization", "Bearer " + meshToken)
                .header(CALL_ID_HEADER, callId)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
    }

    /**
     * Registers an in-flight call placed by {@code callerEnvId} under grant {@code scope}; {@code false} when the
     * id is blank or already in use (the call is then served without cancel support rather than letting one caller
     * replace another's entry).
     */
    public boolean open(String callId, String callerEnvId, String scope, Runnable abort) {
        if (callId == null || callId.isBlank() || abort == null) {
            return false;
        }
        return calls.putIfAbsent(callId, new Call(callerEnvId, scope, abort)) == null;
    }

    /**
     * The grant scope the call was placed under, {@code null} when no such call is in flight.
     */
    public String scopeOf(String callId) {
        Call call = callId == null ? null : calls.get(callId);
        return call == null ? null : call.scope();
    }

    /**
     * Drops the call once it has answered (or failed).
     */
    public void close(String callId) {
        if (callId != null) {
            calls.remove(callId);
        }
    }

    /**
     * Aborts the call when {@code callerEnvId} is the environment that placed it.
     *
     * @return {@code false} for an unknown call or another environment's call
     */
    public boolean cancel(String callId, String callerEnvId) {
        if (callId == null) {
            return false;
        }
        Call call = calls.get(callId);
        if (call == null || !Objects.equals(call.callerEnvId(), callerEnvId) || !calls.remove(callId, call)) {
            return false;
        }
        call.abort().run();
        return true;
    }

    /**
     * In-flight call count (diagnostics/tests).
     */
    public int size() {
        return calls.size();
    }
}
