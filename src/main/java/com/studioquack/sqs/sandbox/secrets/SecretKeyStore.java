package com.studioquack.sqs.sandbox.secrets;

import java.util.Optional;

/**
 * SPI over the secret keystore. Consumes this seam; the real keystore backed by the OS secret
 * manager (macOS Keychain / Windows DPAPI / Linux Secret Service) is delivered in.
 */
public interface SecretKeyStore {

    /**
     * The resolved secret: the env-var {@code key} the SDK contract refers to and the plaintext
     * {@code value}. The caller is responsible for zeroising sensitive buffers when done.
     */
    record ResolvedSecret(String key, String value) {
    }

    /**
     * Resolves a reference to its (key, plaintext) pair, or empty when the keystore has no entry
     * for the id + version. Never returns a placeholder / sentinel string.
     */
    Optional<ResolvedSecret> resolve(SecretRef ref);
}
