package com.studioquack.common.security.envelope;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Body the control plane returns from hybrid-KEM enrolment.
 * Ciphertexts are keyed by primitive name. The IKM is not on the wire; both sides derive it.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HybridEnrolmentResponse {

    private String kemSchema;

    /**
     * Per-primitive ciphertext bytes, base64-encoded, keyed by
     * {@link HybridKemPrimitive#name()}. Receivers MUST tolerate
     * extra entries and MUST require the canonical primitive
     * order from
     * {@link HybridKemEnrolment#SCHEMA_X25519_MLKEM768_ORDER}.
     */
    private Map<String, String> ciphertexts;

    /**
     * PEM leaf certificate signed at enrolment. Null when the request omitted
     * {@link HybridEnrolmentRequest#getCsrPublicKey()}.
     */
    private String leafCertPem;

    /**
     * PEM CA that signed {@link #leafCertPem}. The daemon uses it as the trust store
     * for calls to the control plane. Null when the request omitted a public key.
     */
    private String caCertPem;
}
