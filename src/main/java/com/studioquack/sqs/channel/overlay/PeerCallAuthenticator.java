package com.studioquack.sqs.channel.overlay;

/**
 * Authentication/authorization seam for the daemon→daemon peer listener ({@link OverlayPeerListener}). wires
 * {@link #allowAll()}: reachability over the WireGuard overlay is the only check (the gateway drops
 * cross-owner traffic before it arrives).
 */
public interface PeerCallAuthenticator {

    /**
     * Grant scope required by {@code POST /sqs/v1/overlay/execute}. Matches the backend's grant model.
     */
    String SCOPE_EXECUTE = "execute";
    /**
     * Grant scope required by {@code POST /sqs/v1/overlay/llm/*} (route lands in a later slice).
     */
    String SCOPE_LLM = "llm";

    /**
     * Authenticates the peer and authorizes it for the route's scope.
     *
     * @param authorizationHeader the raw {@code Authorization} header value, {@code null} when absent
     * @param scope the grant scope the requested route requires ({@link #SCOPE_EXECUTE} / {@link #SCOPE_LLM})
     * @return the decision; never {@code null}
     */
    AuthResult authenticate(String authorizationHeader, String scope);

    /**
     * Slice-1 no-op: every caller that reached the listener over the overlay is allowed. The gateway is the
     * enforcement point at this stage; the per-pair check replaces this impl in.
     */
    static PeerCallAuthenticator allowAll() {
        return (authorizationHeader, scope) -> AuthResult.allow(null);
    }

    /**
     * The decision the listener acts on. {@code httpStatus} / {@code errorCode} / {@code message} are only
     * meaningful on a denial and are written verbatim as the response, so the server never needs to know WHY a
     * call was refused.
     *
     * @param callerEnvId the authenticated caller environment id (from the mesh token {@code sub}); {@code null}
     * under the slice-1 allow-all posture
     */
    record AuthResult(boolean allowed, int httpStatus, String errorCode, String callerEnvId,
                      String message) {

        public static AuthResult allow(String callerEnvId) {
            return new AuthResult(true, 200, null, callerEnvId, null);
        }

        public static AuthResult deny(int httpStatus, String errorCode, String message) {
            return new AuthResult(false, httpStatus, errorCode, null, message);
        }
    }
}
