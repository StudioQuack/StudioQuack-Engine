package com.studioquack.common.security.envelope;

/**
 * One primitive in the hybrid KEM transcript.
 * Implementations generate a keypair, encapsulate against a peer public key, and decapsulate.
 */
public interface HybridKemPrimitive {

    /**
     * Stable wire identifier used in the canonical transcript and
     * in JSON wire payloads. Matches the {@code KemSchema} enum
     * names so the receiver can pick the matching primitive
     * without an extra lookup table.
     */
    String name();

    /**
     * Length of the public key in bytes. Used by the wire-format
     * sanity checks to reject malformed enrolment payloads
     * BEFORE calling into the underlying provider (where a wrong
     * length surfaces as a confusing internal error).
     */
    int publicKeyLength();

    /** Length of the wire ciphertext (encapsulation output). */
    int ciphertextLength();

    /** Generate a fresh keypair. The implementation seeds from {@link java.security.SecureRandom}. */
    KemKeypair generateKeypair() throws KemException;

    /**
     * Encapsulate a fresh shared secret against the peer's
     * {@code peerPublicKey}. The returned {@link KemEncapsulation}
     * carries the on-the-wire ciphertext + the locally-derived
     * shared secret; callers MUST send the ciphertext to the
     * peer and feed the shared secret into HKDF locally.
     */
    KemEncapsulation encapsulate(byte[] peerPublicKey) throws KemException;

    /**
     * Decapsulate a peer's ciphertext using the local private
     * key. Returns the shared-secret bytes — symmetric to the
     * encapsulator's {@link KemEncapsulation#getSharedSecret()}.
     */
    byte[] decapsulate(byte[] ciphertext, byte[] privateKey) throws KemException;

    /**
     * Domain-specific runtime exception for KEM failures. Wraps
     * the underlying {@link java.security.GeneralSecurityException}
     * with a primitive-name tag so operators can identify which
     * half of the hybrid failed without parsing stack traces.
     */
    class KemException extends RuntimeException {
        public KemException(String primitiveName, String message, Throwable cause) {
            super("KEM[" + primitiveName + "]: " + message, cause);
        }
    }
}
