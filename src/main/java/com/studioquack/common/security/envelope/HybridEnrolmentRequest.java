package com.studioquack.common.security.envelope;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Body the daemon sends to the control plane's hybrid-KEM enrolment endpoint.
 * Carries the enrolment bearer, one public key per primitive, and the chosen schema.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class HybridEnrolmentRequest {

    @NotNull
    private String envId;

    @NotNull
    private String kemSchema;

    @NotNull
    private String presentedBearer;

    /**
     * Per-primitive public-key bytes, base64-encoded, keyed by
     * {@link HybridKemPrimitive#name()}. Receivers MUST tolerate
     * extra entries (forward-compatibility with v2 hybrid schemas)
     * but MUST require the entries listed in
     * {@link HybridKemEnrolment#SCHEMA_X25519_MLKEM768_ORDER} to
     * be present and well-formed.
     */
    @NotEmpty
    private Map<String, String> publicKeys;

    /**
     * Optional PEM public key. When set, enrolment returns a leaf certificate
     * and CA for the daemon to install and use for TLS.
     */
    private String csrPublicKey;
}
