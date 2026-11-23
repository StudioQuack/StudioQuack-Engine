package com.studioquack.common.security.envelope;

/**
 * How the envelope input keying material is derived.
 * Declaration order is part of the AAD, so new values append at the end.
 */
public enum KemSchema {

    /** IKM is the SHA-256 of the enrolment bearer, recomputed on each side. */
    PRESHARED_HASH,

    /** IKM from an X25519 and ML-KEM-768 transcript established at enrolment. */
    HYBRID_X25519_MLKEM768,

    /**
     * v2 contract — same shape as {@link #HYBRID_X25519_MLKEM768}
     * but with the larger ML-KEM-1024 parameter set for higher
     * post-quantum security margin. Reserved.
     */
    HYBRID_X25519_MLKEM1024;
}
