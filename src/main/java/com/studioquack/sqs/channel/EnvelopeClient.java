package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.SecureEnvelope;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;

/**
 * Direction-aware convenience wrapper over the vendored {@link EnvelopeCrypto} for the daemon's half of the
 * application-layer envelope. The daemon always seals with {@link Direction#AGENT_TO_BE} and opens with
 * {@link Direction#BE_TO_AGENT}; this class pins those directions + the env id so callers only supply the
 * {@code jobKey} and payload.
 */
public final class EnvelopeClient {

    private final String envId;
    private final byte[] ikm;
    private final ObjectMapper mapper;

    private EnvelopeClient(String envId, byte[] ikm, ObjectMapper mapper) {
        this.envId = Objects.requireNonNull(envId, "envId");
        this.ikm = Objects.requireNonNull(ikm, "ikm");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        if (ikm.length == 0) {
            throw new IllegalArgumentException("EnvelopeClient: empty IKM");
        }
    }

    /**
     * Builds a client whose IKM is {@code SHA-256(bearer)} (v1 PRESHARED_HASH).
     */
    public static EnvelopeClient fromBearer(String envId, String bearer, ObjectMapper mapper) {
        byte[] ikm = EnvelopeCrypto.ikmFromBearer(bearer);
        if (ikm == null) {
            throw new IllegalArgumentException("EnvelopeClient.fromBearer: blank bearer");
        }
        return new EnvelopeClient(envId, ikm, mapper);
    }

    /**
     * Builds a client from an explicit 32-byte IKM (e.g. the hybrid-KEM transcript output).
     */
    public static EnvelopeClient withIkm(String envId, byte[] ikm, ObjectMapper mapper) {
        return new EnvelopeClient(envId, ikm.clone(), mapper);
    }

    /**
     * Prefer the hybrid {@code kemIkm} when present; otherwise fall back to {@link #fromBearer}. Callers that
     * still hold only the installer bearer after burn cannot open kemIkm-sealed envelopes.
     *
     * @param kemIkmOrNull hybrid IKM bytes, or {@code null}/empty to use the bearer path
     * @param bearer installer/bootstrap bearer for the legacy IKM fallback (never the {@code sqikm1.} channel
     * credential — that is not envelope IKM)
     */
    public static EnvelopeClient preferKemIkm(String envId, byte[] kemIkmOrNull, String bearer,
                                              ObjectMapper mapper) {
        if (kemIkmOrNull != null && kemIkmOrNull.length > 0) {
            return withIkm(envId, kemIkmOrNull, mapper);
        }
        return fromBearer(envId, bearer, mapper);
    }

    /**
     * Seals raw plaintext agent→BE, binding {@code jobKey} into the AAD.
     */
    public SecureEnvelope seal(String jobKey, byte[] plaintext) throws GeneralSecurityException {
        return EnvelopeCrypto.seal(ikm, Direction.AGENT_TO_BE, envId, jobKey, plaintext);
    }

    /**
     * Seals a JSON-serialisable object agent→BE.
     */
    public SecureEnvelope sealJson(String jobKey, Object payload)
            throws GeneralSecurityException, IOException {
        return EnvelopeCrypto.sealJson(ikm, Direction.AGENT_TO_BE, envId, jobKey, payload, mapper);
    }

    /**
     * Opens a BE→agent envelope, verifying version/alg/kemSchema/direction and that the envId + jobKey bind to
     * this channel. Returns the plaintext.
     */
    public byte[] open(SecureEnvelope envelope, String jobKey) throws GeneralSecurityException {
        return EnvelopeCrypto.open(ikm, envelope, Direction.BE_TO_AGENT, envId, jobKey);
    }

    /**
     * Opens a BE→agent envelope and deserialises the plaintext to {@code type}.
     */
    public <T> T openJson(SecureEnvelope envelope, String jobKey, Class<T> type)
            throws GeneralSecurityException, IOException {
        byte[] plaintext = open(envelope, jobKey);
        return mapper.readValue(plaintext, type);
    }

    /**
     * The env id this client binds every envelope to.
     */
    public String envId() {
        return envId;
    }
}
