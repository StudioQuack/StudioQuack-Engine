package com.studioquack.sqs.sandbox.secrets;

import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/** Production {@link SecretKeyStore} backed by the host {@link OsSecretManager}. */
@Slf4j
public class OsSecretKeyStore implements SecretKeyStore {

    private final OsSecretManager osSecretManager;
    private final SecretStoreNamespace namespace;

    public OsSecretKeyStore(OsSecretManager osSecretManager, SecretStoreNamespace namespace) {
        this.osSecretManager = osSecretManager;
        this.namespace = namespace;
        log.info("SecretKeyStore: OS-secret-manager-backed ('{}'). $secretRef resolves from the "
                + "platform store; plaintext stays off the WAL/logs and lives only in the per-job "
                + "tmpfs .env (unlinked on exit).", osSecretManager.name());
    }

    private String secretService() {
        return namespace.secretService();
    }

    @Override
    public Optional<ResolvedSecret> resolve(SecretRef ref) {
        if (ref == null) {
            return Optional.empty();
        }
        try {
            // Value only — the env-var key is derived by the resolver from the control plane-provided hint
            // (shortCode / port), matching the SDK contract.
            return osSecretManager.get(secretService(), ref.wire())
                    .filter(v -> !v.isEmpty())
                    .map(v -> new ResolvedSecret(null, v));
        } catch (RuntimeException e) {
            // Fail-safe: log the SAFE ref (never the value) and report a miss.
            log.warn("secret resolve via OS secret manager failed ref={} detail={}", ref.wire(), e.getMessage());
            return Optional.empty();
        }
    }
}
