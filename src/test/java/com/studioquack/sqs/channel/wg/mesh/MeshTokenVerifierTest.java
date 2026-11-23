package com.studioquack.sqs.channel.wg.mesh;

import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import org.bouncycastle.crypto.signers.Ed25519Signer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MeshTokenVerifier} round-trip against throwaway Ed25519 keypairs, signing EXACTLY per the
 * backend's contract ({@code base64url(header).base64url(payload).base64url(sig)}, Ed25519 over the
 * ASCII bytes {@code "<headerB64>.<payloadB64>"}). This is the "forged/expired mesh token rejected"
 * acceptance on the daemon side: a valid token verifies and yields {@code sub}; a forged key, an
 * expired {@code exp}, a tampered payload, a wrong {@code iss}, a wrong {@code alg} and a missing
 * verification key are all rejected (fail closed).
 */
class MeshTokenVerifierTest {

    private static final String HEADER = "{\"alg\":\"EdDSA\",\"typ\":\"SQMT\"}";

    private final Ed25519PrivateKeyParameters signingKey =
            new Ed25519PrivateKeyParameters(new SecureRandom());
    private final Ed25519PublicKeyParameters verificationKey = signingKey.generatePublicKey();

    private MeshIdentity identityHolding(Ed25519PublicKeyParameters key) {
        MeshIdentity identity = new MeshIdentity();
        identity.update(null, Base64.getEncoder().encodeToString(key.getEncoded()), "self-env");
        return identity;
    }

    /** Signs the token: Ed25519 over the ASCII bytes "<headerB64>.<payloadB64>". */
    private static String sign(Ed25519PrivateKeyParameters key, String headerJson, String payloadJson) {
        String h = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));
        String p = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));
        Ed25519Signer signer = new Ed25519Signer();
        signer.init(true, key);
        byte[] message = (h + "." + p).getBytes(StandardCharsets.US_ASCII);
        signer.update(message, 0, message.length);
        return h + "." + p + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(signer.generateSignature());
    }

    private static String payload(String iss, String sub, long exp) {
        return "{\"iss\":\"" + iss + "\",\"sub\":\"" + sub + "\","
                + "\"own\":\"0123456789abcdef\",\"iat\":" + (exp - 2100) + ",\"exp\":" + exp + ","
                + "\"jti\":\"3f6b2a90-1234-4bcd-9def-0123456789ab\"}";
    }

    @Test
    void validToken_verifies_andReturnsCallerEnvId() {
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String token = sign(signingKey, HEADER, payload("sq-backend", "env-caller-1", exp));
        MeshTokenVerifier verifier = new MeshTokenVerifier(identityHolding(verificationKey));

        Optional<String> caller = verifier.verify(token);

        assertTrue(caller.isPresent(), "a correctly signed, unexpired token must verify");
        assertEquals("env-caller-1", caller.get(), "the caller identity is the sub claim");
        MeshTokenVerifier.Verification detailed = verifier.verifyDetailed(token);
        assertEquals(MeshTokenVerifier.Failure.NONE, detailed.failure());
        assertEquals(exp, detailed.expiresAtEpochSeconds());
    }

    @Test
    void tokenWithinExpiryLeeway_stillVerifies() {
        // exp 30s in the past is inside the 60s leeway — clock skew must not reject a live token.
        long exp = System.currentTimeMillis() / 1000L - 30;
        String token = sign(signingKey, HEADER, payload("sq-backend", "env-caller-1", exp));
        assertTrue(new MeshTokenVerifier(identityHolding(verificationKey)).verify(token).isPresent());
    }

    @Test
    void forgedToken_signedByAWrongKey_isRejected() {
        Ed25519PrivateKeyParameters forger = new Ed25519PrivateKeyParameters(new SecureRandom());
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String forged = sign(forger, HEADER, payload("sq-backend", "env-caller-1", exp));

        MeshTokenVerifier.Verification v =
                new MeshTokenVerifier(identityHolding(verificationKey)).verifyDetailed(forged);

        assertFalse(v.ok());
        assertEquals(MeshTokenVerifier.Failure.BAD_SIGNATURE, v.failure());
    }

    @Test
    void expiredToken_isRejected_andReportsExpiryAgainstTheClock() {
        long exp = System.currentTimeMillis() / 1000L - 3600; // an hour past, beyond the leeway
        String expired = sign(signingKey, HEADER, payload("sq-backend", "env-caller-1", exp));

        MeshTokenVerifier.Verification v =
                new MeshTokenVerifier(identityHolding(verificationKey)).verifyDetailed(expired);

        assertFalse(v.ok());
        assertEquals(MeshTokenVerifier.Failure.EXPIRED, v.failure());
        assertEquals(exp, v.expiresAtEpochSeconds(),
                "the denial message needs the token's exp to state expiry vs the daemon clock");
    }

    @Test
    void tamperedPayload_isRejected() {
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String genuine = sign(signingKey, HEADER, payload("sq-backend", "env-caller-1", exp));
        // Swap the payload segment for a DIFFERENT (well-formed) one after signing: the signature
        // no longer covers the content — the classic privilege-escalation attempt (sub rewrite).
        String[] seg = genuine.split("\\.");
        String swappedPayload = Base64.getUrlEncoder().withoutPadding().encodeToString(
                payload("sq-backend", "env-admin", exp).getBytes(StandardCharsets.UTF_8));
        String tampered = seg[0] + "." + swappedPayload + "." + seg[2];

        MeshTokenVerifier.Verification v =
                new MeshTokenVerifier(identityHolding(verificationKey)).verifyDetailed(tampered);

        assertFalse(v.ok(), "a tampered payload must not verify");
        assertEquals(MeshTokenVerifier.Failure.BAD_SIGNATURE, v.failure());
    }

    @Test
    void wrongIssuer_isRejected() {
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String token = sign(signingKey, HEADER, payload("not-the-backend", "env-caller-1", exp));

        MeshTokenVerifier.Verification v =
                new MeshTokenVerifier(identityHolding(verificationKey)).verifyDetailed(token);

        assertFalse(v.ok());
        assertEquals(MeshTokenVerifier.Failure.WRONG_ISSUER, v.failure());
    }

    @Test
    void wrongAlgorithm_isRejected() {
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String token = sign(signingKey, "{\"alg\":\"none\",\"typ\":\"SQMT\"}",
                payload("sq-backend", "env-caller-1", exp));

        MeshTokenVerifier.Verification v =
                new MeshTokenVerifier(identityHolding(verificationKey)).verifyDetailed(token);

        assertFalse(v.ok());
        assertEquals(MeshTokenVerifier.Failure.WRONG_ALGORITHM, v.failure());
    }

    @Test
    void missingVerificationKey_isRejected() {
        long exp = System.currentTimeMillis() / 1000L + 2100;
        String token = sign(signingKey, HEADER, payload("sq-backend", "env-caller-1", exp));

        // An identity the (no-signing) handshake left empty: cross-env identity is unavailable.
        MeshTokenVerifier.Verification v = new MeshTokenVerifier(new MeshIdentity()).verifyDetailed(token);

        assertFalse(v.ok());
        assertEquals(MeshTokenVerifier.Failure.NO_VERIFICATION_KEY, v.failure());
    }

    @Test
    void malformedTokens_areRejected() {
        MeshTokenVerifier verifier = new MeshTokenVerifier(identityHolding(verificationKey));

        assertEquals(MeshTokenVerifier.Failure.MALFORMED, verifier.verifyDetailed(null).failure());
        assertEquals(MeshTokenVerifier.Failure.MALFORMED, verifier.verifyDetailed("").failure());
        assertEquals(MeshTokenVerifier.Failure.MALFORMED, verifier.verifyDetailed("two.segments").failure());
        assertEquals(MeshTokenVerifier.Failure.MALFORMED, verifier.verifyDetailed("a.b.c.d").failure());
        assertEquals(MeshTokenVerifier.Failure.MALFORMED,
                verifier.verifyDetailed("!!!.!!!.!!!").failure());
    }
}
