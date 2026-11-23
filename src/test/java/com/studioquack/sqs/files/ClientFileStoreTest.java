package com.studioquack.sqs.files;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The single secure path every client-side file travels. Runs against a real schema rather than
 * mocks because the sealing, the size guard and the scoped cleanup are the reasons this class
 * exists — all three are storage behaviour.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-clientfile-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.files.max-bytes=4096"
})
@ActiveProfiles("base")
class ClientFileStoreTest {

    @Autowired
    private ClientFileStore store;

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("content round-trips through the sealed envelope")
    void store_andResolve() {
        String ref = store.store(ClientFileStore.SCOPE_RUN, "run-1", "report.pdf",
                "application/pdf", bytes("hello-file"));

        assertTrue(ref.startsWith(ClientFileStore.REF_PREFIX));
        Optional<ClientFileStore.StoredFile> loaded = store.resolve(ref);
        assertTrue(loaded.isPresent());
        assertArrayEquals(bytes("hello-file"), loaded.get().content());
        assertEquals("report.pdf", loaded.get().fileName());
        assertEquals("application/pdf", loaded.get().mimeType());
        assertEquals(10L, loaded.get().size());
    }

    @Test
    @DisplayName("binary content survives intact")
    void binaryContent_isNotCorrupted() {
        byte[] binary = new byte[256];
        for (int i = 0; i < 256; i++) {
            binary[i] = (byte) i;
        }

        String ref = store.store(ClientFileStore.SCOPE_RUN, "run-bin", "b.bin", null, binary);

        assertArrayEquals(binary, store.resolve(ref).orElseThrow().content());
    }

    @Test
    @DisplayName("a path traversal in the supplied name cannot survive into the sandbox")
    void fileName_isSanitised() {
        // The stored name becomes a filename inside /sandbox/files, so a surviving traversal
        // sequence here would be a traversal executed there.
        String ref = store.store(ClientFileStore.SCOPE_RUN, "run-1",
                "../../../etc/passwd", "text/plain", bytes("x"));

        String name = store.resolve(ref).orElseThrow().fileName();
        assertEquals("passwd", name);
        assertFalse(name.contains("/"), name);
        assertFalse(name.contains(".."), name);
    }

    @Test
    @DisplayName("names are reduced to a safe basename in every shape")
    void safeName_cases() {
        assertEquals("passwd", ClientFileStore.safeName("../../etc/passwd"));
        assertEquals("evil.sh", ClientFileStore.safeName("C:\\windows\\evil.sh"));
        assertEquals("file", ClientFileStore.safeName(null));
        assertEquals("file", ClientFileStore.safeName("   "));
        assertEquals("file", ClientFileStore.safeName("../"), "a name that is only traversal");
        assertEquals("file", ClientFileStore.safeName("..."), "a name of dots only");
        // Shell-significant characters cannot reach the bootstrap that decodes the name.
        assertEquals("a_b_c_.txt", ClientFileStore.safeName("a;b$c`.txt"));
        assertEquals("sp_ace.txt", ClientFileStore.safeName("sp ace.txt"));
        assertEquals(128, ClientFileStore.safeName("x".repeat(400)).length(), "long names are bounded");
    }

    @Test
    @DisplayName("an oversized file is refused at ingest, not at exec time")
    void oversizedFile_isRefused() {
        // The sandbox scratch is a 64 MiB tmpfs; failing here gives the caller an actionable error
        // instead of a container that dies mid-run.
        byte[] tooBig = new byte[5000];

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> store.store(ClientFileStore.SCOPE_RUN, "run-1", "big.bin", null, tooBig));
        assertTrue(ex.getMessage().contains("above the"), ex.getMessage());
    }

    @Test
    @DisplayName("deleting a run's scope leaves other runs untouched")
    void deleteScope_isScoped() {
        String keep = store.store(ClientFileStore.SCOPE_RUN, "run-keep", "a.txt", null, bytes("a"));
        store.store(ClientFileStore.SCOPE_RUN, "run-doomed", "b.txt", null, bytes("b"));
        store.store(ClientFileStore.SCOPE_RUN, "run-doomed", "c.txt", null, bytes("c"));

        assertEquals(2, store.deleteScope(ClientFileStore.SCOPE_RUN, "run-doomed"));

        assertTrue(store.resolve(keep).isPresent(), "another run's files must survive");
        assertTrue(store.listForScope(ClientFileStore.SCOPE_RUN, "run-doomed").isEmpty());
    }

    @Test
    @DisplayName("run scope and deploy scope with the same key do not collide")
    void scopes_areIndependent() {
        store.store(ClientFileStore.SCOPE_RUN, "same-key", "run.txt", null, bytes("run"));
        String deployRef = store.store(ClientFileStore.SCOPE_DEPLOY, "same-key", "dep.txt", null, bytes("dep"));

        store.deleteScope(ClientFileStore.SCOPE_RUN, "same-key");

        assertTrue(store.resolve(deployRef).isPresent(),
                "clearing a run must not reclaim the workflow's deployed files");
    }

    @Test
    @DisplayName("an unknown or non-ref value resolves to empty rather than throwing")
    void unknownRef_isEmpty() {
        assertTrue(store.resolve(ClientFileStore.REF_PREFIX + "does-not-exist").isEmpty());
        assertTrue(store.resolve("just a string").isEmpty());
        assertTrue(store.resolve(null).isEmpty());
        assertTrue(store.resolve(42).isEmpty());
    }

    @Test
    @DisplayName("isRef only matches the prefixed handle")
    void isRef_discriminates() {
        assertTrue(ClientFileStore.isRef(ClientFileStore.REF_PREFIX + "abc"));
        assertFalse(ClientFileStore.isRef("sq-ref:gridfs:abc"), "the backend's GridFS ref is a different thing");
        assertFalse(ClientFileStore.isRef("0_contract.pdf"));
        assertFalse(ClientFileStore.isRef(null));
    }

    @Test
    @DisplayName("the age backstop reclaims rows whose run never reported terminal")
    void deleteOlderThan_sweeps() {
        // Asserted by identity, not by row count: the sweep is global by design, so a count would
        // also see whatever the sibling tests left in the shared schema.
        String ref = store.store(ClientFileStore.SCOPE_RUN, "run-old", "a.txt", null, bytes("a"));

        store.deleteOlderThan(OffsetDateTime.now().minusHours(1));
        assertTrue(store.resolve(ref).isPresent(), "a row younger than the cutoff must survive");

        store.deleteOlderThan(OffsetDateTime.now().plusHours(1));
        assertTrue(store.resolve(ref).isEmpty(), "a row older than the cutoff is reclaimed");
    }

    @Test
    @DisplayName("an empty file is storable")
    void emptyFile_isAllowed() {
        String ref = store.store(ClientFileStore.SCOPE_RUN, "run-1", "empty.txt", null, new byte[0]);

        assertEquals(0, store.resolve(ref).orElseThrow().content().length);
    }

    @Test
    @DisplayName("null content is rejected outright")
    void nullContent_isRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> store.store(ClientFileStore.SCOPE_RUN, "run-1", "x.txt", null, null));
    }
}
