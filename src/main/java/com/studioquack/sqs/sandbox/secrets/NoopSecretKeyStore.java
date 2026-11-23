package com.studioquack.sqs.sandbox.secrets;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Default {@link SecretKeyStore} for resolves nothing. The production keystore backed by the OS
 * secret manager arrives in and replaces this.
 */
@Slf4j
public class NoopSecretKeyStore implements SecretKeyStore {

    public NoopSecretKeyStore() {
        log.info("SecretKeyStore: no-op (WP-E3). $secretRef resolution is a no-op until the "
                + "OS-secret-manager-backed keystore lands in WP-E4/E5.");
    }

    @Override
    public Optional<ResolvedSecret> resolve(SecretRef ref) {
        return Optional.empty();
    }
}
