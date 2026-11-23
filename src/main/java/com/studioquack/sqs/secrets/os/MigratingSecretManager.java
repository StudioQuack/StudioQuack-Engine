package com.studioquack.sqs.secrets.os;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

/**
 * Read-through migration wrapper: serves reads from the {@code primary} manager. Writes always go
 * to the primary.
 */
@Slf4j
public class MigratingSecretManager implements OsSecretManager {

    private final OsSecretManager primary;
    private final OsSecretManager legacy;

    public MigratingSecretManager(OsSecretManager primary, OsSecretManager legacy) {
        this.primary = primary;
        this.legacy = legacy;
    }

    @Override
    public boolean available() {
        return primary.available();
    }

    @Override
    public String name() {
        return primary.name();
    }

    @Override
    public Optional<String> get(String service, String account) {
        Optional<String> current = primary.get(service, account);
        if (current.isPresent()) {
            return current;
        }
        Optional<String> stale = legacy.get(service, account);
        if (stale.isEmpty()) {
            return Optional.empty();
        }
        // Migrate best-effort: a failed primary write still returns the value (retried next read).
        try {
            primary.put(service, account, stale.get());
            try {
                legacy.delete(service, account);
            } catch (Exception e) {
                log.debug("secret migration: legacy delete failed for {}/{} ({}) — harmless",
                        service, account, e.getMessage());
            }
            log.info("secret migration: {}/{} moved from '{}' to the persistent store",
                    service, account, legacy.name());
        } catch (Exception e) {
            log.warn("secret migration: could not persist {}/{} into '{}' ({}) — serving the "
                    + "legacy value, will retry on next read", service, account, primary.name(), e.getMessage());
        }
        return stale;
    }

    @Override
    public void put(String service, String account, String secret) {
        primary.put(service, account, secret);

        try {
            legacy.delete(service, account);
        } catch (Exception e) {
            log.debug("secret migration: legacy shadow-delete failed for {}/{} ({}) — harmless",
                    service, account, e.getMessage());
        }
    }

    @Override
    public boolean delete(String service, String account) {
        boolean removed = primary.delete(service, account);
        try {
            removed |= legacy.delete(service, account);
        } catch (Exception e) {
            log.debug("secret migration: legacy delete failed for {}/{} ({}) — harmless",
                    service, account, e.getMessage());
        }
        return removed;
    }
}
