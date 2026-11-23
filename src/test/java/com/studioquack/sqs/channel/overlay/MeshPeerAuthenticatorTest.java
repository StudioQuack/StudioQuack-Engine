package com.studioquack.sqs.channel.overlay;

import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.studioquack.sqs.channel.wg.mesh.MeshTokenVerifier;
import com.studioquack.sqs.entitlement.CrossEnvInboundGrant;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import org.bouncycastle.crypto.signers.Ed25519Signer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * {@link MeshPeerAuthenticator}: the full inbound decision chain against a real token verifier
 * (throwaway Ed25519 keypair, signed per the backend contract) and a stubbed on-miss authorize
 * client. Pins the distinguishable denial surface of spec: {@code 401 Error_PeerRejectedIdentity}
 * (missing/malformed/forged/expired token), {@code 403 Error_CrossEnvNotGranted} (verified caller
 * without a grant) and {@code 403 Error_AuthorizationUnavailable} (grant miss + backend unreachable
 * — a DENY).
 */
class MeshPeerAuthenticatorTest {

    private static final String SELF_ENV = "env-self-1";
    private static final String CALLER_ENV = "env-caller-9";
    private static final String HEADER = "{\"alg\":\"EdDSA\",\"typ\":\"SQMT\"}";

    private final Ed25519PrivateKeyParameters signingKey =
            new Ed25519PrivateKeyParameters(new SecureRandom());

    private MeshIdentity identity;
    private CrossEnvGrantStore grantStore;
    private CrossEnvAuthorizationClient authorizeClient;
    private MeshPeerAuthenticator authenticator;

    @BeforeEach
    void setUp() {
        Ed25519PublicKeyParameters verificationKey = signingKey.generatePublicKey();
        identity = new MeshIdentity();
        identity.update(null, Base64.getEncoder().encodeToString(verificationKey.getEncoded()), "self");
        grantStore = new CrossEnvGrantStore();
        authorizeClient = mock(CrossEnvAuthorizationClient.class);
        authenticator = new MeshPeerAuthenticator(
                new MeshTokenVerifier(identity), grantStore, authorizeClient, SELF_ENV);
    }

    /** Signs the token:. */
    private String tokenFor(String sub, long exp) {
        String payload = "{\"iss\":\"sq-backend\",\"sub\":\"" + sub + "\",\"own\":\"abc\","
                + "\"iat\":" + (exp - 2100) + ",\"exp\":" + exp + ","
                + "\"jti\":\"3f6b2a90-1234-4bcd-9def-0123456789ab\"}";
        String h = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(HEADER.getBytes(StandardCharsets.UTF_8));
        String p = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        Ed25519Signer signer = new Ed25519Signer();
        signer.init(true, signingKey);
        byte[] message = (h + "." + p).getBytes(StandardCharsets.US_ASCII);
        signer.update(message, 0, message.length);
        return h + "." + p + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(signer.generateSignature());
    }

    private static String bearer(String token) {
        return "Bearer " + token;
    }

    // ---- identity layer (401 Error_PeerRejectedIdentity) ---------------------------------------

    @Test
    void missingAuthorizationHeader_is401RejectedIdentity() {
        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(null, PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(401, r.httpStatus());
        assertEquals("Error_PeerRejectedIdentity", r.errorCode());
        verifyNoInteractions(authorizeClient);
    }

    @Test
    void malformedAuthorizationHeader_is401() {
        PeerCallAuthenticator.AuthResult r =
                authenticator.authenticate("Basic dXNlcjpwYXNz", PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(401, r.httpStatus());
        assertEquals("Error_PeerRejectedIdentity", r.errorCode());
    }

    @Test
    void emptyBearerCredential_is401() {
        PeerCallAuthenticator.AuthResult r = authenticator.authenticate("Bearer   ", PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(401, r.httpStatus());
        assertEquals("Error_PeerRejectedIdentity", r.errorCode());
    }

    @Test
    void forgedToken_is401() {
        // Sign with a different key than the held verification key.
        Ed25519PrivateKeyParameters forger = new Ed25519PrivateKeyParameters(new SecureRandom());
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String payload = "{\"iss\":\"sq-backend\",\"sub\":\"" + CALLER_ENV + "\",\"exp\":" + exp + "}";
        String h = Base64.getUrlEncoder().withoutPadding().encodeToString(HEADER.getBytes(StandardCharsets.UTF_8));
        String p = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        Ed25519Signer signer = new Ed25519Signer();
        signer.init(true, forger);
        byte[] message = (h + "." + p).getBytes(StandardCharsets.US_ASCII);
        signer.update(message, 0, message.length);
        String forged = h + "." + p + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(signer.generateSignature());

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(forged), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(401, r.httpStatus());
        assertEquals("Error_PeerRejectedIdentity", r.errorCode());
        verifyNoInteractions(authorizeClient);
    }

    @Test
    void expiredToken_is401_andNamesExpiryAgainstTheClock() {
        long exp = System.currentTimeMillis() / 1000L - 3600;
        String expired = tokenFor(CALLER_ENV, exp);

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(expired), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(401, r.httpStatus());
        assertEquals("Error_PeerRejectedIdentity", r.errorCode());
        assertTrue(r.message().contains("expired"), "the message names expiry: " + r.message());
        assertTrue(r.message().contains("exp=" + exp),
                "the message states the token exp against the daemon clock (spec §8): " + r.message());
    }

    @Test
    void noVerificationKeyHeld_is401_crossEnvIdentityUnavailable() {
        identity.clear(); // a handshake on a no-signing backend leaves the identity empty
        long exp = System.currentTimeMillis() / 1000L + 2100;

        PeerCallAuthenticator.AuthResult r =
                authenticator.authenticate(bearer(tokenFor(CALLER_ENV, exp)), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed(), "no verification key → refuse peer calls (fail closed)");
        assertEquals(401, r.httpStatus());
        assertEquals("Error_PeerRejectedIdentity", r.errorCode());
        assertTrue(r.message().contains("verification key"), r.message());
    }

    // ---- grant layer ----------------------------------------------------------------------------

    @Test
    void verifiedCaller_withInboundGrant_isAllowed_withoutBackendCall() {
        grantStore.replace(List.of(new CrossEnvInboundGrant(CALLER_ENV, List.of("execute"))));
        String token = tokenFor(CALLER_ENV, System.currentTimeMillis() / 1000L + 2100);

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertTrue(r.allowed());
        assertEquals(CALLER_ENV, r.callerEnvId(), "the listener learns WHO is calling");
        // A grant hit never pays the control-plane hop.
        verifyNoInteractions(authorizeClient);
    }

    @Test
    void verifiedCaller_withGrantForAnotherScope_isDenied() {
        // Scope honoured: an llm grant must not permit an execute call, and the on-miss authorize
        // is what decides — here it says no.
        grantStore.replace(List.of(new CrossEnvInboundGrant(CALLER_ENV, List.of("llm"))));
        when(authorizeClient.authorize(SELF_ENV, CALLER_ENV, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.DENIED);
        String token = tokenFor(CALLER_ENV, System.currentTimeMillis() / 1000L + 2100);

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(403, r.httpStatus());
        assertEquals("Error_CrossEnvNotGranted", r.errorCode());
    }

    // ---- on-miss authorize (403 Error_CrossEnvNotGranted / Error_AuthorizationUnavailable) ------

    @Test
    void grantMiss_authorizeAllowed_isAllowed_andAskedForThisEnv() {
        when(authorizeClient.authorize(SELF_ENV, CALLER_ENV, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.ALLOWED);
        String token = tokenFor(CALLER_ENV, System.currentTimeMillis() / 1000L + 2100);

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertTrue(r.allowed(), "a grant created seconds ago works via the on-miss check (§9)");
        assertEquals(CALLER_ENV, r.callerEnvId());
        // The CALLEE asks "may callerEnvId call ME": the path env id is THIS environment's.
        verify(authorizeClient).authorize(eq(SELF_ENV), eq(CALLER_ENV), eq("execute"));
    }

    @Test
    void grantMiss_authorizeDenied_is403NotGranted_namingCallerTargetScope() {
        when(authorizeClient.authorize(SELF_ENV, CALLER_ENV, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.DENIED);
        String token = tokenFor(CALLER_ENV, System.currentTimeMillis() / 1000L + 2100);

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(403, r.httpStatus());
        assertEquals("Error_CrossEnvNotGranted", r.errorCode());
        assertTrue(r.message().contains(CALLER_ENV), "names the caller: " + r.message());
        assertTrue(r.message().contains(SELF_ENV), "names the target (self): " + r.message());
        assertTrue(r.message().contains("execute"), "names the scope: " + r.message());
        assertTrue(r.message().contains("Settings → Cross-env access"),
                "names where to grant it: " + r.message());
    }

    @Test
    void grantMiss_backendUnavailable_is403Unavailable_explicitlyADeny() {
        when(authorizeClient.authorize(SELF_ENV, CALLER_ENV, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE);
        String token = tokenFor(CALLER_ENV, System.currentTimeMillis() / 1000L + 2100);

        PeerCallAuthenticator.AuthResult r = authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);

        assertFalse(r.allowed());
        assertEquals(403, r.httpStatus());
        assertEquals("Error_AuthorizationUnavailable", r.errorCode());
        assertTrue(r.message().contains("DENY, not a pass"),
                "the message states explicitly it is a deny, not a pass (spec §8): " + r.message());
    }

    @Test
    void denialCarriesNoCallerIdentity_andAllowCarriesIt() {
        when(authorizeClient.authorize(SELF_ENV, CALLER_ENV, "execute"))
                .thenReturn(CrossEnvAuthorizationClient.Outcome.DENIED);
        String token = tokenFor(CALLER_ENV, System.currentTimeMillis() / 1000L + 2100);

        PeerCallAuthenticator.AuthResult denied =
                authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);
        assertNull(denied.callerEnvId(), "a denial does not vouch for an identity");

        grantStore.replace(List.of(new CrossEnvInboundGrant(CALLER_ENV, List.of("execute"))));
        PeerCallAuthenticator.AuthResult allowed =
                authenticator.authenticate(bearer(token), PeerCallAuthenticator.SCOPE_EXECUTE);
        assertEquals(CALLER_ENV, allowed.callerEnvId());
    }
}
