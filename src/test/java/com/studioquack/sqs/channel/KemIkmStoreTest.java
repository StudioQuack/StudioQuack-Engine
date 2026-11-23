package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link KemIkmStore} round-trip under {@code <dataDir>/kem/ikm.b64}. */
class KemIkmStoreTest {

    @TempDir
    Path dataDir;

    @Test
    void save_load_roundTrip_preserves_bytes() throws Exception {
        byte[] ikm = new byte[32];
        for (int i = 0; i < ikm.length; i++) {
            ikm[i] = (byte) (i + 1);
        }
        KemIkmStore store = KemIkmStore.under(dataDir);
        store.save(ikm);

        assertTrue(Files.isRegularFile(store.path()));
        Optional<byte[]> loaded = store.load();
        assertTrue(loaded.isPresent());
        assertArrayEquals(ikm, loaded.get());
    }

    @Test
    void load_missing_file_is_empty() {
        assertTrue(KemIkmStore.under(dataDir).load().isEmpty());
        assertTrue(KemIkmStore.loadOrNull(dataDir) == null);
    }

    @Test
    void clear_removes_persisted_ikm() throws Exception {
        KemIkmStore store = KemIkmStore.under(dataDir);
        store.save(new byte[]{1, 2, 3, 4});
        store.clear();
        assertFalse(Files.exists(store.path()));
        assertTrue(store.load().isEmpty());
    }
}
