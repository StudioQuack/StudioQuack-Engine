package com.studioquack.sqs.sandbox.stateful;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * The daemon-local OCI image blob store. The upload ingress streams an image tarball straight to
 * disk here (SIZE-STREAMED, no base64 cap — the large-artifact path the heartbeat channel
 * deliberately avoids, mirroring the log-download rationale) and {@link StatefulInstanceManager}
 * reads the same path for {@code podman load}.
 */
@Slf4j
@Component
public class OciImageBlobStore {

    private final StatefulRuntimeProperties props;

    public OciImageBlobStore(StatefulRuntimeProperties props) {
        this.props = props;
    }

    /** The (sanitised, traversal-safe) tarball path for a ref — {@code <blobStore>/<safeRef>.tar}. */
    public Path pathFor(String ref) {
        String safe = ref == null ? "" : ref.replaceAll("[^A-Za-z0-9._-]", "_");
        return Path.of(props.getBlobStore(), safe + ".tar");
    }

    /**
     * Streams {@code in} to the ref's tarball path (create dirs, replace existing), returning the
     * bytes written. The caller owns the stream lifecycle; we do not close it.
     */
    public StoredBlob store(String ref, InputStream in) throws IOException {
        return store(ref, in, MAX_BLOB_BYTES);
    }

    StoredBlob store(String ref, InputStream in, long maxBytes) throws IOException {
        Path target = pathFor(ref);
        Files.createDirectories(target.getParent());
        Path tmp = target.resolveSibling(target.getFileName().toString() + ".part");
        long n = 0L;
        try (var out = Files.newOutputStream(tmp)) {
            byte[] buf = new byte[64 * 1024];
            int r;
            while ((r = in.read(buf)) >= 0) {
                if (n + r > maxBytes) {
                    throw new TooLarge(maxBytes);
                }
                out.write(buf, 0, r);
                n += r;
            }
        } catch (TooLarge e) {
            Files.deleteIfExists(tmp);
            Files.deleteIfExists(target);
            throw e;
        }
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        log.info("[SQ-6] oci-blob stored ref={} path={} bytes={}", ref, target, n);
        return new StoredBlob(target, n);
    }

    /** Hard ceiling on overlay/public OCI tarball ingest (DoS). Digest pin is still open. */
    public static final long MAX_BLOB_BYTES = 512L * 1024 * 1024;

    public static final class TooLarge extends IOException {
        public TooLarge(long maxBytes) {
            super("OCI blob exceeded " + maxBytes + " bytes");
        }
    }

    /** A stored tarball: its on-disk path + byte count. */
    public record StoredBlob(Path path, long bytes) {
    }
}
