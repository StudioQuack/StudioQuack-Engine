package com.studioquack.common.security.envelope;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wire DTO for the security envelope between the daemon and the control plane.
 * Null fields are omitted. Cryptography lives in {@link EnvelopeCrypto}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SecureEnvelope {

    /**
     * Wire version. v1 is the only currently-supported shape; new
     * versions append fields and bump this byte. Receivers MUST
     * reject envelopes whose {@code v} they do not understand.
     */
    private int v;

    /**
     * Algorithm identifier. v1 only ever emits
     * {@link EnvelopeCrypto#ALG_AES256GCM_HKDF_SHA256}. Receivers
     * compare against the constant; mismatches are loud errors.
     */
    private String alg;

    /** KEM schema name ({@link KemSchema#name()}). */
    private String kemSchema;

    /**
     * Direction binding (matches {@link Direction#name()}). The
     * receiver passes its expected direction to
     * {@link EnvelopeCrypto#open(byte[], SecureEnvelope, Direction, String, String)}
     * and any mismatch fails the open BEFORE attempting AEAD
     * decryption.
     */
    private String dir;

    /**
     * Target environment id this envelope is bound to. Woven into
     * AAD so a stolen envelope cannot be replayed against a
     * different env id.
     */
    private String envId;

    /**
     * Job-key correlation tag this envelope is bound to. May be
     * {@code "0"} for non-job-bound payloads (e.g. a workflow-level
     * studioquack.execute callback) — the receiver still verifies
     * the literal value matches what its caller passed in.
     */
    private String jobKey;

    /** HKDF salt — base64-encoded 16 random bytes. */
    private String salt;

    /** AES-GCM nonce — base64-encoded 12 random bytes. */
    private String nonce;

    /**
     * Canonical AAD blob — base64-encoded. The receiver recomputes
     * the layout from
     * {@link EnvelopeCrypto#buildAad(int, KemSchema, Direction, String, String)}
     * and refuses to open if the recomputed bytes differ from the
     * sender-supplied value (defence against AAD field tampering).
     */
    private String aad;

    /**
     * AES-256-GCM ciphertext + 16-byte authentication tag,
     * base64-encoded. Plaintext is recovered via
     * {@link EnvelopeCrypto#open}.
     */
    private String ciphertext;
}
