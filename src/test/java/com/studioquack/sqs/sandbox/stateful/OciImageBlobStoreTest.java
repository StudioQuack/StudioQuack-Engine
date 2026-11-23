package com.studioquack.sqs.sandbox.stateful;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OciImageBlobStoreTest {

    @TempDir
    Path tmp;

    private OciImageBlobStore store() {
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(tmp.resolve("blobs").toString());
        return new OciImageBlobStore(props);
    }

    @Test
    void store_acceptsBodyAtLimit() throws Exception {
        byte[] body = "abcd".getBytes(StandardCharsets.UTF_8);
        OciImageBlobStore.StoredBlob stored =
                store().store("env/img:1", new ByteArrayInputStream(body), 4);
        assertEquals(4, stored.bytes());
        assertTrue(Files.exists(stored.path()));
        assertFalse(Files.exists(stored.path().resolveSibling("env_img_1.tar.part")));
    }

    @Test
    void store_rejectsOversizedAndLeavesNoPartial() throws Exception {
        byte[] body = "abcde".getBytes(StandardCharsets.UTF_8);
        OciImageBlobStore s = store();
        assertThrows(OciImageBlobStore.TooLarge.class,
                () -> s.store("env/img:1", new ByteArrayInputStream(body), 4));
        Path target = s.pathFor("env/img:1");
        assertFalse(Files.exists(target));
        assertFalse(Files.exists(target.resolveSibling(target.getFileName() + ".part")));
    }
}

class OciImageBlobIngestServiceTest {

    @TempDir
    Path tmp;

    @Test
    void ingest_oversize_returns413_andDoesNotLoad() throws Exception {
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(tmp.resolve("blobs").toString());
        props.setUploadToken("tok");
        OciImageBlobStore blobStore = new OciImageBlobStore(props) {
            @Override
            public StoredBlob store(String ref, java.io.InputStream in) throws java.io.IOException {
                return store(ref, in, 4);
            }
        };
        StatefulInstanceManager manager = mock(StatefulInstanceManager.class);
        OciImageDigestReporter reporter = mock(OciImageDigestReporter.class);
        OciImageBlobIngestService ingest =
                new OciImageBlobIngestService(props, blobStore, manager, reporter);

        OciImageBlobIngestService.Result r = ingest.ingest(
                "env/img:1", "env", "n",
                new ByteArrayInputStream("abcde".getBytes(StandardCharsets.UTF_8)),
                "tok");

        assertEquals(413, r.httpStatus());
        assertEquals("Error_RequestBodyTooLarge", r.body().get("errorCode"));
        verify(manager, never()).loadImage(any(), any(), any());
        assertFalse(Files.exists(blobStore.pathFor("env/img:1")));
    }
}
