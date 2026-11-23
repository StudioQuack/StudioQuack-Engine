package com.studioquack.sqs.secrets.os;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MigratingSecretManager} — the keyctl → machine-file cut-over semantics: reads prefer the
 * primary, a legacy-only entry is served, copied into the primary, and removed from the legacy;
 * writes land on the primary and shadow the legacy copy.
 */
class MigratingSecretManagerTest {

    private final InMemorySecretManager primary = new InMemorySecretManager();
    private final InMemorySecretManager legacy = new InMemorySecretManager();
    private final MigratingSecretManager mgr = new MigratingSecretManager(primary, legacy);

    @Test
    void primary_hit_never_consults_the_legacy() {
        primary.put("svc", "a", "new");
        legacy.put("svc", "a", "stale");
        assertEquals(Optional.of("new"), mgr.get("svc", "a"), "primary wins");
        assertEquals(Optional.of("stale"), legacy.get("svc", "a"), "legacy untouched on a primary hit");
    }

    @Test
    void legacy_only_entry_is_served_then_migrated() {
        legacy.put("svc", "a", "old-key");

        assertEquals(Optional.of("old-key"), mgr.get("svc", "a"), "legacy value served on the miss");
        assertEquals(Optional.of("old-key"), primary.get("svc", "a"), "copied into the primary");
        assertTrue(legacy.get("svc", "a").isEmpty(), "legacy copy removed after migration");

        // Second read is served purely from the primary.
        assertEquals(Optional.of("old-key"), mgr.get("svc", "a"));
    }

    @Test
    void absent_everywhere_is_empty() {
        assertTrue(mgr.get("svc", "missing").isEmpty());
    }

    @Test
    void put_writes_primary_and_shadows_legacy() {
        legacy.put("svc", "a", "stale");
        mgr.put("svc", "a", "fresh");
        assertEquals(Optional.of("fresh"), primary.get("svc", "a"));
        assertTrue(legacy.get("svc", "a").isEmpty(), "stale legacy value must be shadowed");
    }

    @Test
    void delete_removes_from_both_stores() {
        primary.put("svc", "a", "x");
        legacy.put("svc", "a", "x");
        assertTrue(mgr.delete("svc", "a"));
        assertTrue(primary.get("svc", "a").isEmpty());
        assertTrue(legacy.get("svc", "a").isEmpty());
        assertFalse(mgr.delete("svc", "a"), "idempotent when nothing remains");
    }

    @Test
    void availability_and_name_delegate_to_the_primary() {
        assertTrue(mgr.available());
        assertEquals(primary.name(), mgr.name());

        MigratingSecretManager down = new MigratingSecretManager(
                new UnavailableSecretManager(), legacy);
        assertFalse(down.available(), "an unavailable primary means unavailable custody");
    }
}
