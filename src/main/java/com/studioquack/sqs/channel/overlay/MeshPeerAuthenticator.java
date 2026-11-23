package com.studioquack.sqs.channel.overlay;

import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore;
import com.studioquack.sqs.channel.wg.mesh.MeshTokenVerifier;

/**
 * Authenticates an inbound overlay peer call: bearer mesh token, then a local grant, then one
 * control-plane check on a grant miss. A miss the control plane cannot answer is a deny.
 */
public class MeshPeerAuthenticator implements PeerCallAuthenticator {

    /**
     * Inbound denial: the presented mesh token is missing, malformed, forged or expired.
     */
    public static final String ERROR_REJECTED_IDENTITY = "Error_PeerRejectedIdentity";
    /**
     * Inbound denial: the caller holds no grant for the route's scope on this environment.
     */
    public static final String ERROR_NOT_GRANTED = "Error_CrossEnvNotGranted";
    /**
     * Inbound denial: grant miss and the control plane could not answer — a DENY, not a pass.
     */
    public static final String ERROR_UNAVAILABLE = "Error_AuthorizationUnavailable";

    private static final String BEARER_PREFIX = "Bearer ";

    private final MeshTokenVerifier tokenVerifier;
    private final CrossEnvGrantStore grantStore;
    private final CrossEnvAuthorizationClient authorizationClient;
    private final String selfEnvId;
    private com.studioquack.sqs.channel.net.NetTelemetry telemetry;

    public MeshPeerAuthenticator(MeshTokenVerifier tokenVerifier, CrossEnvGrantStore grantStore,
                                 CrossEnvAuthorizationClient authorizationClient, String selfEnvId) {
        this.tokenVerifier = tokenVerifier;
        this.grantStore = grantStore;
        this.authorizationClient = authorizationClient;
        this.selfEnvId = selfEnvId;
    }

    public void setTelemetry(com.studioquack.sqs.channel.net.NetTelemetry telemetry) {
        this.telemetry = telemetry;
    }

    @Override
    public AuthResult authenticate(String authorizationHeader, String scope) {

        if (authorizationHeader == null
                || !authorizationHeader.regionMatches(true, 0, BEARER_PREFIX, 0, BEARER_PREFIX.length())) {
            return rejectIdentity("missing or malformed Authorization header — a cross-env peer call requires the "
                    + "backend-signed mesh token as a Bearer credential");
        }
        String token = authorizationHeader.substring(BEARER_PREFIX.length()).trim();
        if (token.isEmpty()) {
            return rejectIdentity("empty Bearer credential — a cross-env peer call requires the backend-signed mesh token");
        }

        // The token proves WHO calls, never WHAT it may do.
        MeshTokenVerifier.Verification verification = tokenVerifier.verifyDetailed(token);
        if (!verification.ok()) {
            return switch (verification.failure()) {
                case EXPIRED -> rejectIdentity("peer mesh token expired (token exp="
                        + verification.expiresAtEpochSeconds()
                        + "s, daemon clock=" + (System.currentTimeMillis() / 1000L)
                        + "s) — the caller's daemon must re-handshake to refresh its identity");
                case NO_VERIFICATION_KEY -> rejectIdentity("this daemon holds no mesh verification key (the backend issued none at "
                        + "handshake) — cross-env identity is unavailable here");
                default -> rejectIdentity("peer mesh token rejected (" + verification.failure() + ")");
            };
        }
        String callerEnvId = verification.callerEnvId();


        if (grantStore.check(callerEnvId, scope)) {
            return AuthResult.allow(callerEnvId);
        }


        CrossEnvAuthorizationClient.Outcome outcome =
                authorizationClient.authorize(selfEnvId, callerEnvId, scope);
        return switch (outcome) {
            case ALLOWED -> AuthResult.allow(callerEnvId);
            case DENIED -> AuthResult.deny(403, ERROR_NOT_GRANTED,
                    "cross-env call not granted: caller env '" + callerEnvId + "' has no '" + scope
                            + "' grant on target env '" + selfEnvId + "' — grant it on the target "
                            + "env's Settings → Cross-env access");
            case UNAVAILABLE -> AuthResult.deny(403, ERROR_UNAVAILABLE,
                    "cross-env authorization unavailable: no local grant for caller env '" + callerEnvId
                            + "' scope '" + scope + "' on target env '" + selfEnvId + "', and sq-backend "
                            + "could not be reached to authorize — this is a DENY, not a pass");
        };
    }

    private AuthResult rejectIdentity(String message) {
        if (telemetry != null) {
            telemetry.noteMeshTokenReject();
        }
        return AuthResult.deny(401, ERROR_REJECTED_IDENTITY, message);
    }
}
