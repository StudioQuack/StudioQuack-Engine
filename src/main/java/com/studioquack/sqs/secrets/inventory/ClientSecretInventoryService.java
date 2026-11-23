package com.studioquack.sqs.secrets.inventory;

import com.studioquack.sqs.sandbox.secrets.SecretRef;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Tracks — and can purge — the OS-store secrets this daemon owns. {@code SecretCommandHandlers}
 * calls {@link #recordCommit(String, int)} after every successful {@code osSecrets.put}
 * (commit/rotate) and {@link #remove(String)} on delete, so the {@link ClientOwnedSecretEntity}
 * inventory always mirrors what.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClientSecretInventoryService {

    private final ClientOwnedSecretRepository repository;
    private final OsSecretManager osSecrets;
    private final SecretStoreNamespace namespace;

    /** The engine-scoped OS-store service this daemon's entries live under. */
    public String secretServiceName() {
        return namespace.secretService();
    }

    /**
     * Upsert the ownership row for {@code secretId}, raising the recorded max version if this commit
     * is newer. Idempotent; safe to call on every commit/rotate.
     */
    @Transactional
    public void recordCommit(String secretId, int version) {
        if (secretId == null || secretId.isBlank()) {
            return;
        }
        int v = Math.max(1, version);
        ClientOwnedSecretEntity row = repository.findById(secretId).orElse(null);
        if (row == null) {
            repository.save(new ClientOwnedSecretEntity(secretId, v, OffsetDateTime.now()));
        } else {
            row.setMaxVersion(Math.max(row.getMaxVersion(), v));
            row.setCommittedAt(OffsetDateTime.now());
            repository.save(row);
        }
        log.debug("secret-inventory: recorded ownership secretId={} version={}", secretId, v);
    }

    /**
     * Drop the ownership row for {@code secretId} (called on delete-secret).
     * Idempotent; safe to call if the secret is already deleted (e.g. on command retry).
     */
    @Transactional
    public void remove(String secretId) {
        if (secretId == null || secretId.isBlank()) {
            return;
        }
        if (repository.existsById(secretId)) {
            repository.deleteById(secretId);
            log.debug("secret-inventory: removed ownership secretId={}", secretId);
        }
    }

    /**
     * Delete every stored version of {@code secretId} from the OS store, then drop its ownership row.
     * Every OS-store call is a subprocess (Keychain CLI, a fresh PowerShell for DPAPI), so a blind
     * fixed version range is unaffordable — it held delete-secret for minutes and past the control
     * plane's command budget.
     *
     * @return The number of OS-store entries actually removed
     */
    public int deleteAllVersions(String secretId) {
        if (secretId == null || secretId.isBlank()) {
            return 0;
        }
        String service = secretServiceName();
        int tracked = repository.findById(secretId).map(r -> Math.max(1, r.getMaxVersion())).orElse(0);
        int removed = 0;
        if (tracked > 0) {
            for (int v = 1; v <= tracked; v++) {
                if (osSecrets.delete(service, new SecretRef(secretId, v).wire())) {
                    removed++;
                }
            }
        } else {
            for (int v = 1; v <= UNTRACKED_VERSION_CEILING
                    && osSecrets.delete(service, new SecretRef(secretId, v).wire()); v++) {
                removed++;
            }
        }
        remove(secretId);
        return removed;
    }

    private static final int UNTRACKED_VERSION_CEILING = 999;

    /**
     * Delete every inventory-listed secret from the OS store (versions {@code 1.maxVersion}) and clear
     * the inventory. Only entries THIS daemon committed are touched — never other apps'.
     */
    @Transactional
    public int purgeAllOwned() {
        String service = secretServiceName();
        String legacy = namespace.legacySecretService();
        boolean sweepLegacy = !legacy.equals(service);
        List<ClientOwnedSecretEntity> owned = repository.findAll();
        int purged = 0;
        for (ClientOwnedSecretEntity row : owned) {
            int max = Math.max(1, row.getMaxVersion());
            for (int v = 1; v <= max; v++) {
                String account = new SecretRef(row.getSecretId(), v).wire();
                deleteQuietly(service, account, row.getSecretId(), v);
                if (sweepLegacy) {
                    deleteQuietly(legacy, account, row.getSecretId(), v);
                }
            }
            purged++;
        }
        repository.deleteAll();
        log.info("secret-inventory: purged {} owned secret id(s) from OS store service={} backend={}",
                purged, service, safeName());
        return purged;
    }

    private void deleteQuietly(String service, String account, String secretId, int version) {
        try {
            osSecrets.delete(service, account);
        } catch (Exception e) {
            // Best-effort per version: a missing/failed delete must not abort the purge of the rest.
            log.warn("secret-inventory: purge failed secretId={} version={} backend={}: {}",
                    secretId, version, safeName(), e.getMessage());
        }
    }

    private String safeName() {
        try {
            return osSecrets.name();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
