package com.studioquack.sqs.channel.wg.mesh;

import lombok.extern.slf4j.Slf4j;
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.Optional;

/**
 * This daemon's cross-environment MESH IDENTITY, delivered by the control plane on every successful WireGuard
 * handshake (plain AND drain — {@code WireGuardClient.handshake()} is the single parse point):
 * {@code meshToken} — the backend-signed Ed25519 assertion of THIS environment's identity, presented as
 * {@code Authorization: Bearer <token>} on outbound daemon→daemon peer calls (
 * {@code HelperExecuteService.forwardToPeer}). {@code meshVerificationKey} — the raw 32-byte Ed25519 public
 * key (STANDARD Base64 on the wire) with which this daemon verifies tokens presented by INBOUND peers.
 */
@Slf4j
@Component
public class MeshIdentity {

    /**
     * The backend-signed assertion of this env's own identity. Never logged.
     */
    private volatile String meshToken;
    /**
     * The mesh-wide verification key for tokens presented by peers. Never logged.
     */
    private volatile Ed25519PublicKeyParameters verificationKey;
    /**
     * This environment's own name as the backend knows it.
     */
    private volatile String ownEnvName;

    /**
     * Wholesale replace from a successful handshake. Any absent/blank piece CLEARS what was held — fail closed, so
     * a backend that stopped signing immediately withdraws this daemon's ability to make (and its willingness to
     * serve) cross-env calls.
     */
    public synchronized void update(String token, String verificationKeyBase64, String envName) {
        this.meshToken = blank(token) ? null : token.trim();
        this.verificationKey = decodeKey(verificationKeyBase64);
        this.ownEnvName = blank(envName) ? null : envName.trim();
        // Presence booleans only — the token is a bearer credential and must never hit the logs.
        log.info("channel/mesh: identity refreshed (tokenHeld={} verificationKeyHeld={} ownEnvName='{}')",
                this.meshToken != null, this.verificationKey != null, this.ownEnvName);
    }

    /**
     * Drops everything held (channel teardown).
     */
    public synchronized void clear() {
        this.meshToken = null;
        this.verificationKey = null;
        this.ownEnvName = null;
    }

    /**
     * The token to present on outbound peer calls; empty while no signing backend is enrolled.
     */
    public Optional<String> currentToken() {
        return Optional.ofNullable(meshToken);
    }

    /**
     * The key inbound peer tokens are verified against; empty on a no-signing backend.
     */
    public Optional<Ed25519PublicKeyParameters> verificationKey() {
        return Optional.ofNullable(verificationKey);
    }

    /**
     * This environment's own backend-side name, when the handshake delivered it.
     */
    public Optional<String> ownEnvName() {
        return Optional.ofNullable(ownEnvName);
    }

    /**
     * STANDARD Base64 of the raw 32-byte Ed25519 public key; any defect → absent (fail closed).
     */
    private static Ed25519PublicKeyParameters decodeKey(String base64) {
        if (blank(base64)) {
            return null;
        }
        try {
            byte[] raw = Base64.getDecoder().decode(base64.trim());
            if (raw.length != 32) {
                log.warn("channel/mesh: mesh verification key has {} bytes, expected 32 — ignored "
                        + "(cross-env identity unavailable)", raw.length);
                return null;
            }
            return new Ed25519PublicKeyParameters(raw, 0);
        } catch (IllegalArgumentException badBase64) {
            log.warn("channel/mesh: mesh verification key is not valid Base64 — ignored "
                    + "(cross-env identity unavailable)");
            return null;
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }
}
