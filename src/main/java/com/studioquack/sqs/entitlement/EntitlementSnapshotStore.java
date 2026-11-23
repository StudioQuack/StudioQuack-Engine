package com.studioquack.sqs.entitlement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.execution.crypto.ContextKeyProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Optional;

/**
 * Prerequisite — AES-GCM sealed on-disk custody for the live {@link EnvironmentEntitlements}
 * snapshot (including {@code webhookRelay} and {@code apiKeyHashes}). A restart while offline
 * therefore left the webhook answering {@code 401} to every request until the control plane came
 * back.
 */
@Slf4j
@Component
public class EntitlementSnapshotStore {

    /** GCM AAD binds ciphertext to this custody purpose (not interchangeable with other sealed files). */
    static final String AAD = "entitlement-snapshot";

    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final ObjectMapper objectMapper;
    private final ContextKeyProvider keyProvider;
    private final Path file;

    public EntitlementSnapshotStore(ObjectMapper objectMapper,
                                   ContextKeyProvider keyProvider,
                                   SqsChannelProperties channelProperties) {
        this.objectMapper = objectMapper;
        this.keyProvider = keyProvider;
        Path dataDir = Path.of(channelProperties.getDataDir());
        this.file = dataDir.resolve("entitlements").resolve("snapshot.aesgcm");
    }

    /** True when {@link ContextKeyProvider} yields a Base64 AES key of valid length (16/24/32). */
    public boolean custodyAvailable() {
        return resolveKey().isPresent();
    }

    /**
     * A sealed snapshot plus the BE {@code issuedAt} stamp of the push that produced it
     * ({@code null} for files written before the freshness guard existed). The stamp is
     * what lets a restarted daemon keep ignoring replayed older snapshots instead of
     * only the ones it has seen in this process.
     */
    public record SealedSnapshot(EnvironmentEntitlements snapshot, java.time.Instant appliedAt) {
    }

    /**
     * Seals {@code snapshot} to {@link #path()} (atomic tmp+move, mode {@code 0600}).
     * When custody is unavailable this is a WARN no-op — callers (reload-config →
     * {@link EntitlementsHolder#apply}) must not fail solely because the seal could not run.
     */
    public void save(EnvironmentEntitlements snapshot) {
        save(snapshot, null);
    }


    public void save(EnvironmentEntitlements snapshot, java.time.Instant appliedAt) {
        if (snapshot == null) {
            return;
        }
        Optional<SecretKeySpec> keyOpt = resolveKey();
        if (keyOpt.isEmpty()) {
            // In-memory apply already succeeded upstream; do not throw (reload-config must not fail on seal).
            log.warn("entitlement-snapshot: custody unavailable — skipping sealed persist "
                    + "(in-memory snapshot still applied; apiKeyHashes count={})",
                    snapshot.apiKeyHashes().size());
            return;
        }
        try {
            Files.createDirectories(file.getParent());
            byte[] plaintext;
            if (appliedAt == null) {
                plaintext = objectMapper.writeValueAsBytes(snapshot);
            } else {
                com.fasterxml.jackson.databind.node.ObjectNode envelope = objectMapper.createObjectNode();
                envelope.set("snapshot", objectMapper.valueToTree(snapshot));
                envelope.put("appliedAt", appliedAt.toString());
                plaintext = objectMapper.writeValueAsBytes(envelope);
            }
            byte[] iv = new byte[IV_BYTES];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, keyOpt.get(), new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(AAD.getBytes(StandardCharsets.UTF_8));
            byte[] ct = cipher.doFinal(plaintext);

            byte[] wire = new byte[iv.length + ct.length];
            System.arraycopy(iv, 0, wire, 0, iv.length);
            System.arraycopy(ct, 0, wire, iv.length, ct.length);

            // Atomic replace (same posture as KemIkmStore): write tmp → 0600 → move → 0600.
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.write(tmp, wire);
            restrict(tmp);
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            restrict(file);
            log.info("entitlement-snapshot: sealed persist ok (apiKeyHashes count={})",
                    snapshot.apiKeyHashes().size());
        } catch (Exception e) {
            // Loud but non-throwing: sealed persist is best-effort relative to the live apply.
            log.warn("entitlement-snapshot: failed to seal snapshot to {} ({})", file, e.getMessage());
        }
    }

    /**
     * Decrypts + parses the on-disk snapshot. Missing file, missing key, or corrupt/
     * undecryptable bytes → {@link Optional#empty()} (never restores garbage).
     */
    public Optional<EnvironmentEntitlements> load() {
        return loadSealed().map(SealedSnapshot::snapshot);
    }


    public Optional<SealedSnapshot> loadSealed() {
        Optional<SecretKeySpec> keyOpt = resolveKey();
        if (keyOpt.isEmpty()) {
            return Optional.empty();
        }
        try {
            if (!Files.isRegularFile(file)) {
                return Optional.empty();
            }
            byte[] wire = Files.readAllBytes(file);
            if (wire.length <= IV_BYTES) {
                log.warn("entitlement-snapshot: sealed file {} too short ({} bytes) — ignoring",
                        file, wire.length);
                return Optional.empty();
            }
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, keyOpt.get(),
                    new GCMParameterSpec(TAG_BITS, wire, 0, IV_BYTES));
            cipher.updateAAD(AAD.getBytes(StandardCharsets.UTF_8));
            byte[] plain = cipher.doFinal(wire, IV_BYTES, wire.length - IV_BYTES);
            com.fasterxml.jackson.databind.JsonNode root = objectMapper.readTree(plain);
            if (root == null) {
                log.warn("entitlement-snapshot: decrypted payload parsed to null — ignoring");
                return Optional.empty();
            }
            if (root.has("snapshot")) {
                EnvironmentEntitlements snap = objectMapper.treeToValue(
                        root.get("snapshot"), EnvironmentEntitlements.class);
                java.time.Instant appliedAt = null;
                if (root.path("appliedAt").isTextual()) {
                    try {
                        appliedAt = java.time.Instant.parse(root.path("appliedAt").asText());
                    } catch (Exception badStamp) {
                        log.warn("entitlement-snapshot: unreadable appliedAt stamp — treating as unstamped");
                    }
                }
                return snap == null ? Optional.empty() : Optional.of(new SealedSnapshot(snap, appliedAt));
            }
            EnvironmentEntitlements snap = objectMapper.treeToValue(root, EnvironmentEntitlements.class);
            if (snap == null) {
                log.warn("entitlement-snapshot: decrypted payload parsed to null — ignoring");
                return Optional.empty();
            }
            return Optional.of(new SealedSnapshot(snap, null));
        } catch (Exception e) {
            // Fail closed: never feed garbage into EntitlementsHolder / ingress auth.
            log.warn("entitlement-snapshot: could not decrypt/parse {} ({}) — restoring empty; "
                            + "ingress auth material will be missing until the next reload-config push",
                    file, e.getMessage());
            return Optional.empty();
        }
    }

    /** Best-effort delete of the sealed file (tests / re-enrol). */
    public void clear() {
        try {
            Files.deleteIfExists(file);
        } catch (Exception e) {
            log.warn("entitlement-snapshot: clear failed for {} ({})", file, e.getMessage());
        }
    }

    /** Path of the sealed snapshot file (tests / diagnostics). */
    public Path path() {
        return file;
    }

    /**
     * Resolves the AES key once per call from {@link ContextKeyProvider#contextEncryptionKeyBase64()}.
     * Accepts Base64 → 16/24/32 raw bytes only; anything else is treated as custody unavailable.
     */
    private Optional<SecretKeySpec> resolveKey() {
        if (keyProvider == null) {
            return Optional.empty();
        }
        String b64 = keyProvider.contextEncryptionKeyBase64();
        if (b64 == null || b64.isBlank()) {
            return Optional.empty();
        }
        try {
            byte[] raw = Base64.getDecoder().decode(b64.trim());
            if (raw.length == 16 || raw.length == 24 || raw.length == 32) {
                return Optional.of(new SecretKeySpec(raw, "AES"));
            }
            log.warn("entitlement-snapshot: context key decoded to {} bytes (need 16/24/32) — custody unavailable",
                    raw.length);
            return Optional.empty();
        } catch (IllegalArgumentException e) {
            log.warn("entitlement-snapshot: context key is not valid Base64 — custody unavailable");
            return Optional.empty();
        }
    }

    /** Best-effort owner-only mode ({@code 0600}); non-POSIX filesystems ignore. */
    private static void restrict(Path f) {
        try {
            Files.setPosixFilePermissions(f, EnumSet.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        } catch (UnsupportedOperationException | java.io.IOException ignore) {
            try {
                Files.setPosixFilePermissions(f, PosixFilePermissions.fromString("rw-------"));
            } catch (Exception ignored) {
                // Non-POSIX (Windows) — installer/ACL owns the data dir.
            }
        }
    }
}
