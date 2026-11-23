package com.studioquack.sqs.channel;

import com.studioquack.sqs.secrets.DaemonBoundSealedStore;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.Optional;

/**
 * Persists the hybrid-enrolment {@code kemIkm} under {@code <dataDir>/kem/ikm.b64} (mode {@code 0600}, same
 * posture as {@link MtlsIdentity}'s private key) so a restart can rebuild
 * {@link com.studioquack.common.security.envelope.DaemonAuthCredential} and {@link EnvelopeClient#withIkm}
 * without re-running the hybrid transcript. On-disk encoding is base64url without padding — the same form the
 * BE env row stores.
 */
@Slf4j
public final class KemIkmStore {

    private final Path file;
    /**
     * Daemon-bound custody for {@link #file}; {@code null} = legacy plaintext-0600 behaviour.
     */
    private final DaemonBoundSealedStore sealed;

    public KemIkmStore(Path dataDir) {
        this(dataDir, null);
    }

    /**
     * @param dataDir channel data directory (required)
     * @param sealed optional daemon-bound custody for the same file; {@code null} keeps the legacy plaintext
     * behaviour byte-for-byte
     */
    public KemIkmStore(Path dataDir, DaemonBoundSealedStore sealed) {
        if (dataDir == null) {
            throw new IllegalArgumentException("KemIkmStore: dataDir is required");
        }
        this.file = dataDir.resolve("kem").resolve("ikm.b64");
        this.sealed = sealed;
    }

    public static KemIkmStore under(Path dataDir) {
        return new KemIkmStore(dataDir);
    }

    public static KemIkmStore under(Path dataDir, DaemonBoundSealedStore sealed) {
        return new KemIkmStore(dataDir, sealed);
    }

    /**
     * Loads persisted IKM bytes, or empty when the file is missing/blank/malformed. With custody wired, a sealed
     * blob is opened vithe OS-held key (wrong engine ⇒ empty) and a legacy plaintext file is passed through and
     * then re-sealed (one-time, idempotent migration).
     */
    public Optional<byte[]> load() {
        if (sealed != null) {
            // Snapshot the on-disk shape BEFORE opening: load() returns the decrypted payload,
            // which never carries the magic — the migration check must look at the file itself.
            boolean legacyPlaintext = !sealed.sealedFilePresent();
            Optional<byte[]> raw = sealed.load();
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            byte[] ikm = decode(raw.get());
            if (ikm == null) {
                return Optional.empty();
            }
            if (legacyPlaintext) {
                migrateIfPlaintext(raw.get());
            }
            return Optional.of(ikm);
        }
        try {
            if (!Files.isRegularFile(file)) {
                return Optional.empty();
            }
            byte[] ikm = decode(Files.readAllBytes(file));
            return ikm == null ? Optional.empty() : Optional.of(ikm);
        } catch (Exception ex) {
            return Optional.empty();
        }
    }

    /**
     * Convenience for open paths that prefer {@code null} over {@link Optional}.
     */
    public static byte[] loadOrNull(Path dataDir) {
        return under(dataDir).load().orElse(null);
    }

    /**
     * As {@link #loadOrNull(Path)} but daemon-bound-custody aware.
     */
    public static byte[] loadOrNull(Path dataDir, DaemonBoundSealedStore sealed) {
        return under(dataDir, sealed).load().orElse(null);
    }

    /**
     * Atomically persists {@code ikm}. With custody available the base64url payload is sealed (atomic tmp+replace,
     * {@code 0600}, handled by the custody store); without it the legacy plaintext base64url write at mode
     * {@code 0600} is kept and a single WARN marks the degraded posture.
     */
    public void save(byte[] ikm) throws Exception {
        if (ikm == null || ikm.length == 0) {
            throw new IllegalArgumentException("KemIkmStore.save: empty ikm");
        }
        String b64 = Base64.getUrlEncoder().withoutPadding().encodeToString(ikm);
        if (sealed != null) {
            try {
                // The sealed payload is the exact legacy file content, so load() has one decode path.
                sealed.save(b64.getBytes(StandardCharsets.UTF_8));
                return;
            } catch (DaemonBoundSealedStore.DaemonBoundCustodyException ex) {
                log.warn("channel/kem: OS custody unavailable for kemIkm ({}) — falling back to "
                        + "plaintext 0600 custody", ex.getMessage());
            }
        }
        writePlaintext(b64);
    }

    /**
     * Deletes the persisted IKM if present (best-effort; used by tests / re-enrol flows).
     */
    public void clear() throws Exception {
        Files.deleteIfExists(file);
    }

    /**
     * Path of the on-disk IKM file (for diagnostics).
     */
    public Path path() {
        return file;
    }

    /**
     * Decodes the base64url (no padding) file content; {@code null} when blank/malformed.
     */
    private static byte[] decode(byte[] rawFileBytes) {
        try {
            String b64 = new String(rawFileBytes, StandardCharsets.UTF_8).trim();
            if (b64.isEmpty()) {
                return null;
            }
            byte[] ikm = Base64.getUrlDecoder().decode(b64);
            return ikm.length == 0 ? null : ikm;
        } catch (Exception ex) {
            return null;
        }
    }

    /**
     * One-time, idempotent migration: a legacy plaintext file (caller verified the on-disk blob has no
     * {@code SQDB1} magic — the opened payload never carries it) that just loaded fine is re-sealed in place so
     * the plaintext window closes on first read. Best-effort — a custody hiccup here must never break a load that
     * already succeeded.
     */
    private void migrateIfPlaintext(byte[] rawFileBytes) {
        if (!sealed.custodyAvailable()) {
            return;
        }
        try {
            sealed.save(rawFileBytes);
            log.info("channel/kem: migrated persisted kemIkm to daemon-bound sealed custody ({})",
                    file.getFileName());
        } catch (Exception ex) {
            log.warn("channel/kem: kemIkm re-seal skipped ({}) — plaintext 0600 custody kept for now",
                    ex.getMessage());
        }
    }

    /**
     * The pre-sealing write path: base64url text, atomic tmp+replace, best-effort {@code 0600}.
     */
    private void writePlaintext(String b64) throws Exception {
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(tmp, b64, StandardCharsets.UTF_8);
        try {
            Files.setPosixFilePermissions(tmp,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX (Windows) — ACLs handled by the installer.
        }
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        try {
            Files.setPosixFilePermissions(file,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX.
        }
    }

    /**
     * Zeroes a caller-owned IKM buffer after use.
     */
    public static void zero(byte[] ikm) {
        if (ikm != null) {
            Arrays.fill(ikm, (byte) 0);
        }
    }
}
