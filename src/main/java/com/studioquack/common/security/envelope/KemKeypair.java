package com.studioquack.common.security.envelope;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Freshly generated KEM keypair, as raw bytes.
 * The private half stays local and must be zeroed after enrolment.
 */
@Data
@AllArgsConstructor
public class KemKeypair {

    /**
     * Public-key bytes. Sent over the wire (always plaintext —
     * the public half is, by definition, public). Length matches
     * {@link HybridKemPrimitive#publicKeyLength()} for the
     * primitive that produced it.
     */
    private byte[] publicKey;

    /**
     * Private-key bytes. Held locally only. Callers zero this array after enrolment.
     */
    private byte[] privateKey;
}
