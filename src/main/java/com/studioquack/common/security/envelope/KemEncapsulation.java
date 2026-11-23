package com.studioquack.common.security.envelope;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Single-shot KEM ciphertext + handle blob produced by a KEM
 * {@code encapsulate} call. The {@code ciphertext} half travels
 * to the peer; the {@code sharedSecret} half stays local and is
 * fed into the HKDF transcript by
 * {@link HybridKemEnrolment#combineTranscript}.
 *
 * <p>Two-shape policy: ML-KEM emits a structured ciphertext +
 * shared secret, while a Diffie-Hellman primitive (X25519)
 * emits the encapsulator's ephemeral public key as the
 * "ciphertext" and the DH shared point as the secret. We carry
 * both flavours through this struct so the hybrid combiner does
 * not need to discriminate by primitive type — every step is
 * "feed both halves of every encapsulation into HKDF in
 * canonical order".
 *
 * <p>Both byte arrays are caller-owned and MUST NOT be mutated
 * after construction. Callers that need to zeroise sensitive
 * material should produce a defensive copy.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KemEncapsulation {

    /**
     * Wire bytes the encapsulator hands to the decapsulator. For
     * ML-KEM this is the FIPS-203 ciphertext; for X25519 this is
     * the encapsulator's ephemeral public key (32 bytes).
     */
    private byte[] ciphertext;

    /**
     * Shared secret derived locally by the encapsulator. The
     * decapsulator recomputes the same bytes via the matching
     * {@link HybridKemPrimitive#decapsulate} call and the two
     * sides feed identical bytes into HKDF — confirming the
     * transcript without ever transmitting the secret.
     */
    private byte[] sharedSecret;
}
