package com.studioquack.sqs.sandbox.oci;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Materialises OCI build context under {@code ${SQS_DATA_DIR}/oci-build/<buildId>/}. Re-validates
 * every path and enforces caps daemon-side (never trust the control plane).
 */
@Slf4j
@Component
public class OciBuildContextStore {

    /** Per-file ceiling (5 MiB). */
    public static final long MAX_FILE_BYTES = 5L * 1024 * 1024;
    /** Per-workspace total (50 MiB). */
    public static final long MAX_WORKSPACE_BYTES = 50L * 1024 * 1024;

    private final Path dataDir;
    /** Key = buildId → path → sizeBytes. */
    private final ConcurrentHashMap<String, BuildQuota> quotas = new ConcurrentHashMap<>();

    // Two constructors exist, so Spring cannot infer which to use and fails with "No default
    // constructor found" at startup. @Autowired pins the property-driven one; the package-visible
    // Path overload stays for unit tests.
    @Autowired
    public OciBuildContextStore(@Value("${SQS_DATA_DIR:./data}") String dataDir) {
        this(Path.of(dataDir));
    }

    /** Package-visible constructor for unit tests. */
    OciBuildContextStore(Path dataDir) {
        this.dataDir = dataDir.toAbsolutePath().normalize();
    }

    /**
     * Write plaintext file bytes under the build context. Re-validates path + caps.
     */
    public Path writeFile(String buildId, String path, byte[] content) throws IOException {
        String bid = OciBuildPathValidator.requireBuildId(buildId);
        String normalisedPath = OciBuildPathValidator.normalizeAndValidate(path);
        if (content == null) {
            throw new IllegalArgumentException("OCI build file content is required.");
        }
        long sizeBytes = content.length;
        if (sizeBytes > MAX_FILE_BYTES) {
            throw new IllegalArgumentException(
                    "OCI build file exceeds " + MAX_FILE_BYTES + " bytes (D66 5 MiB cap).");
        }

        Path contextRoot = contextDir(bid);
        Path resolved = contextRoot.resolve(normalisedPath).normalize();
        ensureUnderRoot(contextRoot, resolved);

        BuildQuota quota = quotas.computeIfAbsent(bid, id -> new BuildQuota());
        quota.record(normalisedPath, sizeBytes);

        Files.createDirectories(resolved.getParent());
        Files.write(resolved, content,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        // Log only buildId / path / size — never file contents.
        log.info("[T-31] OCI build context file written buildId={} path={} sizeBytes={}",
                bid, normalisedPath, sizeBytes);
        return resolved;
    }

    /** Absolute context directory for buildId; does not create it. */
    public Path contextDir(String buildId) {
        String bid = OciBuildPathValidator.requireBuildId(buildId);
        return dataDir.resolve("oci-build").resolve(bid).normalize();
    }

    /** True iff context directory exists as a directory. */
    public boolean contextExists(String buildId) {
        try {
            Path dir = contextDir(buildId);
            return Files.isDirectory(dir);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** Delete the entire context tree for buildId. Best-effort; never throws. */
    public void deleteContext(String buildId) {
        String bid;
        try {
            bid = OciBuildPathValidator.requireBuildId(buildId);
        } catch (IllegalArgumentException e) {
            log.warn("[T-31] OCI build context delete skipped — invalid buildId: {}", e.getMessage());
            return;
        }
        Path dir = dataDir.resolve("oci-build").resolve(bid).normalize();
        try {
            ensureUnderRoot(dataDir.resolve("oci-build").normalize(), dir);
        } catch (IllegalArgumentException e) {
            log.warn("[T-31] OCI build context delete refused buildId={}: {}", bid, e.getMessage());
            quotas.remove(bid);
            return;
        }
        if (Files.exists(dir)) {
            try (var walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ex) {
                        log.debug("[T-31] OCI build context delete path failed buildId={} path={}: {}",
                                bid, p, ex.getMessage());
                    }
                });
            } catch (IOException e) {
                log.warn("[T-31] OCI build context delete walk failed buildId={}: {}",
                        bid, e.getMessage());
            }
        }
        quotas.remove(bid);
        log.info("[T-31] OCI build context deleted buildId={}", bid);
    }

    /** Current tracked entry count for a buildId (for tests). */
    public int entryCount(String buildId) {
        BuildQuota q = quotas.get(buildId);
        return q == null ? 0 : q.pathSizes.size();
    }

    /** Current tracked total bytes for a buildId (for tests). */
    public long totalBytes(String buildId) {
        BuildQuota q = quotas.get(buildId);
        return q == null ? 0L : q.totalBytes;
    }

    /**
     * Defence vs symlink / {@code ..} tricks after resolve+normalize.
     * Package-visible for unit tests.
     */
    static void ensureUnderRoot(Path contextRoot, Path resolved) {
        Path root = contextRoot.toAbsolutePath().normalize();
        Path candidate = resolved.toAbsolutePath().normalize();
        if (!candidate.startsWith(root)) {
            throw new IllegalArgumentException(
                    "OCI build path escapes context root after resolve.");
        }
    }

    /** Per-buildId running totals; replace of the same path adjusts the delta. */
    static final class BuildQuota {
        final ConcurrentHashMap<String, Long> pathSizes = new ConcurrentHashMap<>();
        volatile long totalBytes;

        synchronized void record(String path, long newSize) {
            // No entry-count cap: the byte budgets (per-file and total) are the real
            // bound — a fixed file count rejected legitimate large workspaces.
            Long old = pathSizes.get(path);
            boolean replace = old != null;
            long projected = totalBytes - (replace ? old : 0L) + newSize;
            if (projected > MAX_WORKSPACE_BYTES) {
                throw new IllegalArgumentException(
                        "OCI build context would exceed " + MAX_WORKSPACE_BYTES
                                + " bytes (D66 50 MiB workspace cap).");
            }
            pathSizes.put(path, newSize);
            totalBytes = projected;
        }
    }
}
