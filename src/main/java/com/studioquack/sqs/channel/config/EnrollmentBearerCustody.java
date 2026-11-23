package com.studioquack.sqs.channel.config;

import com.studioquack.sqs.secrets.DaemonBoundSealedStore;
import com.studioquack.sqs.secrets.DaemonFileCustody;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

/**
 * Daemon-bound at-rest custody for the enrolment bearer — the one secret line ({@code sqs.channel.bearer})
 * inside the installer-written {@code enrollment.properties}. The other lines ({@code env-id},
 * {@code base-url}, a leftover {@code tls-ca-file}) are not secret and stay plaintext and readable.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class EnrollmentBearerCustody {

    /**
     * The one secret line in {@code enrollment.properties} — everything else stays as-is.
     */
    static final String BEARER_KEY = "sqs.channel.bearer";

    private final SqsChannelProperties props;
    private final DaemonFileCustody custody;
    private final Environment environment;
    /**
     * Explicit candidate list for tests; production builds it in the Spring constructor.
     */
    private final List<Path> enrollmentFileOverride;

    @org.springframework.beans.factory.annotation.Autowired
    public EnrollmentBearerCustody(SqsChannelProperties props, DaemonFileCustody custody,
                                   Environment environment) {
        this(props, custody, environment, null);
    }

    EnrollmentBearerCustody(SqsChannelProperties props, DaemonFileCustody custody,
                            Environment environment, List<Path> enrollmentFileOverride) {
        this.props = props;
        this.custody = custody;
        this.environment = environment;
        this.enrollmentFileOverride = enrollmentFileOverride;
    }

    /**
     * One-time migrate-or-load at refresh. Never throws.
     */
    @PostConstruct
    void migrateOrLoad() {
        try {
            run();
        } catch (Exception ex) {
            // Absolute rule: bearer custody must never break the boot.
            log.warn("channel: enrolment-bearer custody step failed ({}) — continuing with the "
                    + "configuration as-is", ex.toString());
        }
    }

    private void run() throws Exception {
        DaemonBoundSealedStore store = custody.enrollmentBearer();
        String configured = props.getInstallerBearer();
        if (configured != null && !configured.isBlank()) {
            if (!store.custodyAvailable()) {
                if (plaintextBearerChanged(configured)) {
                    forgetEnrolledIdentity();
                }
                log.warn("channel: OS secret custody unavailable — enrolment bearer stays "
                        + "plaintext in enrollment.properties (degraded at-rest custody; the "
                        + "daemon is unaffected)");
                return;
            }
            byte[] bearerBytes = configured.getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> existing = store.load();
            if (existing.isPresent() && !java.util.Arrays.equals(existing.get(), bearerBytes)) {
                forgetEnrolledIdentity();
            }
            if (existing.isEmpty() || !java.util.Arrays.equals(existing.get(), bearerBytes)) {
                store.save(bearerBytes);
                log.info("channel: enrolment bearer moved to daemon-bound sealed custody ({})",
                        store.path());
            }
            scrubBearerFromEnrollmentFiles();
            return;
        }
        // Property absent — either already migrated (sealed blob holds it) or never installed.
        Optional<byte[]> sealed = store.load();
        if (sealed.isPresent()) {
            props.setBearer(new String(sealed.get(), StandardCharsets.UTF_8));
            log.info("channel: enrolment bearer loaded from daemon-bound sealed custody");
        } else if (store.sealedFilePresent() && !store.custodyAvailable()) {
            log.warn("channel: a sealed enrolment bearer exists at {} but OS custody is "
                    + "unavailable — it cannot be opened on this boot. The channel stays idle; "
                    + "restore the OS secret manager (or re-enrol) to recover.", store.path());
        }
    }

    /**
     * Drops the persisted kemIkm so this boot enrols again — hybrid KEM on the new bearer.
     */
    private void forgetEnrolledIdentity() {
        try {
            com.studioquack.sqs.channel.KemIkmStore.under(Path.of(props.getDataDir()), custody.kemIkm()).clear();
            log.info("channel: a new install bundle replaced this engine's previous enrolment — the old "
                    + "kemIkm is discarded and the engine enrols again as its only installation");
        } catch (Exception ex) {
            log.warn("channel: could not discard the previous kemIkm ({}); enrolment fails until "
                    + "<dataDir>/kem is removed", ex.toString());
        }
    }

    /**
     * Without sealed custody there is no stored bearer to compare: a fingerprint file holds the last one.
     */
    private boolean plaintextBearerChanged(String configured) {
        Path marker = Path.of(props.getDataDir()).resolve("enrollment").resolve("installer-bearer.sha256");
        try {
            String fingerprint = java.util.HexFormat.of().formatHex(java.security.MessageDigest
                    .getInstance("SHA-256").digest(configured.getBytes(StandardCharsets.UTF_8)));
            String previous = Files.isRegularFile(marker) ? Files.readString(marker).trim() : null;
            if (!fingerprint.equals(previous)) {
                Files.createDirectories(marker.getParent());
                Files.writeString(marker, fingerprint);
            }
            return previous != null && !previous.equals(fingerprint);
        } catch (Exception ex) {
            log.debug("channel: installer-bearer fingerprint unavailable: {}", ex.toString());
            return false;
        }
    }

    /**
     * Rewrites each candidate {@code enrollment.properties} without the bearer line, preserving every other line
     * (and comment) verbatim and appending a custody note. Best-effort per file: a scrub failure leaves the
     * plaintext line in place (today's posture) and is logged.
     */
    private void scrubBearerFromEnrollmentFiles() {
        for (Path candidate : enrollmentFileCandidates()) {
            try {
                if (!Files.isRegularFile(candidate)) {
                    continue;
                }
                List<String> lines = Files.readAllLines(candidate, StandardCharsets.UTF_8);
                List<String> kept = new ArrayList<>(lines.size() + 1);
                boolean removed = false;
                for (String line : lines) {
                    if (isBearerLine(line)) {
                        removed = true;
                        continue;
                    }
                    kept.add(line);
                }
                if (!removed) {
                    continue;
                }
                kept.add("# " + BEARER_KEY + " moved to daemon-bound sealed custody by the daemon at "
                        + java.time.Instant.now() + " — a copy of this file is useless off this host.");
                rewritePreservingPermissions(candidate, kept);
                log.info("channel: scrubbed the enrolment bearer line from {}", candidate);
            } catch (Exception ex) {
                log.warn("channel: could not scrub the bearer line from {} ({}) — the plaintext "
                        + "copy remains", candidate, ex.toString());
            }
        }
    }

    /**
     * Matches an active (non-comment) {@code sqs.channel.bearer} assignment line.
     */
    static boolean isBearerLine(String line) {
        String t = line.trim();
        if (t.startsWith("#") || t.startsWith("!")) {
            return false;
        }
        return t.matches("^sqs\\.channel\\.bearer\\s*[=:].*");
    }

    /**
     * Candidate enrollment files, mirroring {@code UninstallService.invalidateEnrollmentState}: the explicit
     * {@code SQ_CHANNEL_CONFIG} pointer first, then the stock layouts relative to {@code SQS_DATA_DIR} and the
     * channel data dir. De-duplicated, non-existent files skipped by the caller.
     */
    private List<Path> enrollmentFileCandidates() {
        if (enrollmentFileOverride != null) {
            return enrollmentFileOverride;
        }
        LinkedHashSet<Path> candidates = new LinkedHashSet<>();
        String chCfg = firstNonBlank(
                environment == null ? null : environment.getProperty("SQ_CHANNEL_CONFIG"),
                System.getenv("SQ_CHANNEL_CONFIG"));
        if (chCfg != null) {
            candidates.add(Path.of(chCfg).toAbsolutePath().normalize());
        }
        String dataDir = firstNonBlank(
                environment == null ? null : environment.getProperty("SQS_DATA_DIR"),
                System.getenv("SQS_DATA_DIR"));
        if (dataDir != null) {
            Path data = Path.of(dataDir).toAbsolutePath().normalize();
            if (data.getParent() != null) {
                candidates.add(data.getParent().resolve("config").resolve("enrollment.properties"));
            }
            candidates.add(data.resolve("config").resolve("enrollment.properties"));
        }
        // Channel data dir (<prefix>/data/channel in the stock layout) → <prefix>/config.
        Path channelData = Path.of(props.getDataDir()).toAbsolutePath().normalize();
        Path prefix = channelData.getParent() == null ? null : channelData.getParent().getParent();
        if (prefix != null) {
            candidates.add(prefix.resolve("config").resolve("enrollment.properties"));
        }
        if (channelData.getParent() != null) {
            candidates.add(channelData.getParent().resolve("config").resolve("enrollment.properties"));
        }
        return List.copyOf(candidates);
    }

    /**
     * Atomic tmp+replace rewrite; carries the original POSIX permissions over when supported.
     */
    private static void rewritePreservingPermissions(Path file, List<String> lines) throws Exception {
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        Files.write(tmp, String.join("\n", lines).concat("\n").getBytes(StandardCharsets.UTF_8));
        try {
            Files.setPosixFilePermissions(tmp, Files.getPosixFilePermissions(file));
        } catch (UnsupportedOperationException ignored) {
            // Non-POSIX (Windows) — ACLs handled by the installer.
        }
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v.trim();
            }
        }
        return null;
    }
}
