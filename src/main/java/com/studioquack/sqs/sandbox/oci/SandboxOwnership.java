package com.studioquack.sqs.sandbox.oci;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * The two identities every sandbox container is stamped with so a leftover one can be attributed:
 * which installed engine created it ({@link #ownerId()}) and which run of that engine ({@link
 * #bootId()}). The pre-existing {@code studioquack.sqs=1} label only says "a StudioQuack
 * container".
 */
@Slf4j
@Component
public class SandboxOwnership {

    /** Label value used when the identity could not be derived; disables owner-scoped reclamation. */
    public static final String UNKNOWN = "unknown";

    private final String ownerId;
    private final String bootId;

    /** Explicit: the private two-arg constructor behind {@link #of} would otherwise make this ambiguous. */
    @Autowired
    public SandboxOwnership(Environment environment) {
        this(deriveOwnerId(environment), shortId(UUID.randomUUID().toString()));
        log.info("sandbox.ownership owner={} boot={}", ownerId, bootId);
    }

    private SandboxOwnership(String ownerId, String bootId) {
        this.ownerId = blankTo(ownerId, UNKNOWN);
        this.bootId = blankTo(bootId, UNKNOWN);
    }

    /** Fixed identity for tests and for call sites that build argv without a Spring context. */
    public static SandboxOwnership of(String ownerId, String bootId) {
        return new SandboxOwnership(ownerId, bootId);
    }

    /** An identity that matches nothing, so nothing is ever reclaimed on its behalf. */
    public static SandboxOwnership unowned() {
        return new SandboxOwnership(UNKNOWN, UNKNOWN);
    }

    /** Stable across restarts of this installation. */
    public String ownerId() {
        return ownerId;
    }

    /** Unique to this JVM run. */
    public String bootId() {
        return bootId;
    }

    /** True when this identity is real enough to reclaim containers with. */
    public boolean identified() {
        return !UNKNOWN.equals(ownerId);
    }

    private static String deriveOwnerId(Environment environment) {
        String configured = environment == null ? null : environment.getProperty("SQS_DATA_DIR");
        if (configured == null || configured.isBlank()) {
            configured = System.getenv("SQS_DATA_DIR");
        }
        if (configured == null || configured.isBlank()) {
            configured = "./data";
        }
        try {
            return shortId(hash(Path.of(configured).toAbsolutePath().normalize().toString()));
        } catch (RuntimeException e) {
            log.warn("sandbox.ownership: could not derive an owner id from SQS_DATA_DIR={} ({}) — "
                    + "abandoned-container reclamation is disabled for this run", configured, e.toString());
            return UNKNOWN;
        }
    }

    private static String hash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** Label values are matched exactly by {@code podman ps --filter}, so keep them short and opaque. */
    private static String shortId(String value) {
        String compact = value.replace("-", "");
        return compact.substring(0, Math.min(16, compact.length()));
    }

    private static String blankTo(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
