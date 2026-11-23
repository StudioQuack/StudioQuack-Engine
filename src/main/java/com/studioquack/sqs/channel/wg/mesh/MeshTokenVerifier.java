package com.studioquack.sqs.channel.wg.mesh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import org.bouncycastle.crypto.signers.Ed25519Signer;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Optional;

/**
 * Verifies a backend-signed MESH TOKEN presented by a peer daemon and returns the caller's identity. Checks,
 * in order: exactly three segments; {@code alg=EdDSA}; the expected issuer; a non-blank {@code sub}; a held
 * verification key; a valid Ed25519 signature under that key; and {@code exp} not more than
 * {@value #EXPIRY_LEEWAY_SECONDS}s in the past.
 */
public class MeshTokenVerifier {

    /**
     * The only issuer whose tokens are accepted (the control plane's mesh signing identity).
     */
    public static final String ISSUER = "sq-backend";
    /**
     * The only JOSE algorithm accepted.
     */
    public static final String ALGORITHM = "EdDSA";
    /**
     * Clock-skew leeway applied to {@code exp} (seconds).
     */
    public static final long EXPIRY_LEEWAY_SECONDS = 60L;

    /**
     * Why a token was rejected — the only detail that may leave this class.
     */
    public enum Failure {
        NONE,
        /**
         * Not three non-empty base64url segments, or unparseable header/payload JSON.
         */
        MALFORMED,
        /**
         * {@code alg} is not {@code EdDSA}.
         */
        WRONG_ALGORITHM,
        /**
         * This daemon holds no mesh verification key (a no-signing backend).
         */
        NO_VERIFICATION_KEY,
        /**
         * {@code iss} is not the control plane.
         */
        WRONG_ISSUER,
        /**
         * The Ed25519 signature does not verify under the held key (forged or tampered).
         */
        BAD_SIGNATURE,
        /**
         * {@code exp} (plus leeway) is in the past.
         */
        EXPIRED
    }

    /**
     * The verification outcome. {@link #callerEnvId()} is set only when {@link #ok()}.
     */
    public record Verification(String callerEnvId, Failure failure, Long expiresAtEpochSeconds) {
        public boolean ok() {
            return failure == Failure.NONE && callerEnvId != null;
        }

        static Verification rejected(Failure failure, Long exp) {
            return new Verification(null, failure, exp);
        }
    }

    private final MeshIdentity identity;
    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    public MeshTokenVerifier(MeshIdentity identity) {
        this(identity, Clock.systemUTC());
    }

    public MeshTokenVerifier(MeshIdentity identity, Clock clock) {
        this.identity = identity;
        this.clock = clock;
    }

    /**
     * Verifies {@code bearerToken} and returns the caller's environment id (the {@code sub} claim). Empty on ANY
     * failure — absent key, malformed, forged, wrong issuer, expired.
     */
    public Optional<String> verify(String bearerToken) {
        Verification v = verifyDetailed(bearerToken);
        return v.ok() ? Optional.of(v.callerEnvId()) : Optional.empty();
    }

    /**
     * As {@link #verify(String)} but with the failure category, for distinguishable denials.
     */
    public Verification verifyDetailed(String bearerToken) {
        if (bearerToken == null || bearerToken.isBlank()) {
            return Verification.rejected(Failure.MALFORMED, null);
        }
        String[] seg = bearerToken.trim().split("\\.", -1);
        if (seg.length != 3 || seg[0].isEmpty() || seg[1].isEmpty() || seg[2].isEmpty()) {
            return Verification.rejected(Failure.MALFORMED, null);
        }
        JsonNode header = parseJsonSegment(seg[0]);
        if (header == null) {
            return Verification.rejected(Failure.MALFORMED, null);
        }
        if (!ALGORITHM.equals(header.path("alg").asText(null))) {
            return Verification.rejected(Failure.WRONG_ALGORITHM, null);
        }
        JsonNode payload = parseJsonSegment(seg[1]);
        if (payload == null) {
            return Verification.rejected(Failure.MALFORMED, null);
        }
        Long exp = payload.hasNonNull("exp") && payload.get("exp").isIntegralNumber()
                ? payload.get("exp").longValue() : null;
        if (!ISSUER.equals(payload.path("iss").asText(null))) {
            return Verification.rejected(Failure.WRONG_ISSUER, exp);
        }
        String sub = payload.path("sub").asText(null);
        if (sub == null || sub.isBlank() || exp == null) {
            return Verification.rejected(Failure.MALFORMED, exp);
        }
        Ed25519PublicKeyParameters key = identity.verificationKey().orElse(null);
        if (key == null) {
            return Verification.rejected(Failure.NO_VERIFICATION_KEY, exp);
        }
        if (!signatureValid(key, seg[0] + "." + seg[1], seg[2])) {
            return Verification.rejected(Failure.BAD_SIGNATURE, exp);
        }
        long now = clock.instant().getEpochSecond();
        if (now > exp + EXPIRY_LEEWAY_SECONDS) {
            return Verification.rejected(Failure.EXPIRED, exp);
        }
        return new Verification(sub, Failure.NONE, exp);
    }

    /**
     * Base64url-decodes and JSON-parses one segment; {@code null} on any defect.
     */
    private JsonNode parseJsonSegment(String segment) {
        try {
            return mapper.readTree(Base64.getUrlDecoder().decode(segment));
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Ed25519 over the ASCII bytes of {@code <headerB64>.<payloadB64>} — the signed content.
     */
    private static boolean signatureValid(Ed25519PublicKeyParameters key, String signedContent,
                                          String signatureSegment) {
        try {
            byte[] signature = Base64.getUrlDecoder().decode(signatureSegment);
            Ed25519Signer verifier = new Ed25519Signer();
            verifier.init(false, key);
            byte[] message = signedContent.getBytes(StandardCharsets.US_ASCII);
            verifier.update(message, 0, message.length);
            return verifier.verifySignature(signature);
        } catch (Exception e) {
            return false;
        }
    }
}
