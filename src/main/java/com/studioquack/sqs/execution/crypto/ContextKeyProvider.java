package com.studioquack.sqs.execution.crypto;

/**
 * SPI that supplies the AES key used to seal the {@code execution_context} store rows
 * at rest (see {@link ExecutionContextCrypto}).
 *
 * <p>The daemon loads context keys from
 * the <b>OS secret manager</b> (Keychain on macOS, DPAPI on Windows, Secret Service / machine-file
 * / keyring on Linux), never from a config file or environment variable. When no key is available,
 * rows stay plaintext and boot logs one warning.
 */
public interface ContextKeyProvider {

    /**
 * @return the Base64-encoded AES key (decoding to 16, 24, or 32 bytes for
 * AES-128/192/256), or {@code null}/blank when at-rest encryption is disabled for this
 * deployment. A blank result puts {@link ExecutionContextCrypto} into transparent
 * pass-through mode (plaintext rows), matching the control plane rollout posture.
 */
    String contextEncryptionKeyBase64();

    /**
     * @return a short, non-secret label identifying where the key came from, for the
     * boot banner / audit log (e.g. {@code "dev-property"}, {@code "os-keychain"},
     * {@code "disabled"}). Never returns the key material itself.
     */
    default String source() {
        return "unknown";
    }
}
