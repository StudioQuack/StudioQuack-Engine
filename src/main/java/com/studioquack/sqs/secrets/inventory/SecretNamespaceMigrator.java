package com.studioquack.sqs.secrets.inventory;

import com.studioquack.sqs.sandbox.secrets.SecretRef;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * One-shot boot migration into the engine-scoped OS secret-store namespace (
 * {@link SecretStoreNamespace}). Left there, two engines on the same host could resolve each
 * other's refs, and a reinstall could inherit its predecessor's leftovers.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SecretNamespaceMigrator {

    private final ClientOwnedSecretRepository repository;
    private final OsSecretManager osSecrets;
    private final SecretStoreNamespace namespace;

    @PostConstruct
    void migrateLegacyNamespace() {
        String legacy = namespace.legacySecretService();
        String scoped = namespace.secretService();
        if (legacy.equals(scoped)) {
            // Unenrolled (dev) daemon — there is no scoped namespace to migrate into.
            return;
        }
        List<ClientOwnedSecretEntity> owned;
        try {
            owned = repository.findAll();
        } catch (Exception e) {
            log.warn("secret-migration: inventory unreadable ({}); retrying on next boot", e.getMessage());
            return;
        }
        int moved = 0;
        for (ClientOwnedSecretEntity row : owned) {
            int max = Math.max(1, row.getMaxVersion());
            for (int v = 1; v <= max; v++) {
                String account = new SecretRef(row.getSecretId(), v).wire();
                try {
                    Optional<String> value = osSecrets.get(legacy, account);
                    if (value.isEmpty()) {
                        continue;
                    }
                    osSecrets.put(scoped, account, value.get());
                    osSecrets.delete(legacy, account);
                    moved++;
                } catch (Exception e) {

                    log.warn("secret-migration: move failed secretId={} version={}: {}",
                            row.getSecretId(), v, e.getMessage());
                }
            }
        }
        if (moved > 0) {
            log.info("secret-migration: moved {} secret version(s) into the engine-scoped namespace {}", moved, scoped);
        }
    }
}
