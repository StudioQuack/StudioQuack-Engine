package com.studioquack.sqs.maintenance;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Daemon-executable file-log tailing ({@code logs}). File application logs are read only when a
 * {@link MaintenanceProperties#getLogDir log dir} is configured.
 */
@Slf4j
@Service
public class MaintenanceService {


    static final String LOG_DIR_INSTALL_MARKER = ".sq-install-id";

    /** Optional per-file sidecar suffix: {@code {filename}.sq-install-id}. */
    static final String LOG_FILE_INSTALL_SIDECAR_SUFFIX = ".sq-install-id";

    /** Operator-facing refuse when the log-dir marker belongs to another install. */
    static final String REFUSE_DIR_CLAIM_MISMATCH =
            "log selection refused: this log directory is owned by a different install identity "
                    + "(marker " + LOG_DIR_INSTALL_MARKER + "). Cross-install log selection is refused. "
                    + "Point sqs.maintenance.log-dir at a fresh path, or deliberately remove the marker "
                    + "to re-bind this directory to the current install.";

    /** Operator-facing refuse when the log-dir marker is blank or unreadable. */
    static final String REFUSE_DIR_MARKER_UNIDENTIFIABLE =
            "log selection refused: log directory marker " + LOG_DIR_INSTALL_MARKER
                    + " is blank or unreadable (unidentifiable install ownership). "
                    + "Cross-install log selection is refused.";

    /**
     * Operator-facing refuse when regular log files exist but none are eligible for this install
     * (all foreign or unidentifiable sidecars).
     */
    static final String REFUSE_NO_ELIGIBLE_LOGS =
            "log selection refused: log directory contains only files owned by a different or "
                    + "unidentifiable install identity. Cross-install log selection is refused.";

    private final String dataDir;
    private final MaintenanceProperties props;

    /** Lazily resolved via {@link InstallIdentity#loadOrCreate(Path)}; never overwritten once set. */
    private volatile String installId;

    public MaintenanceService(@Value("${SQS_DATA_DIR:./data}") String dataDir,
                              MaintenanceProperties props) {
        this.dataDir = (dataDir == null || dataDir.isBlank()) ? "./data" : dataDir;
        this.props = props;
    }

    /**
     * {@code logs} tail payload — a BOUNDED recent slice of the newest daemon log file.
     *
     * @param source {@code "daemon-log-file"} when tailed from the configured log dir, or
     * {@code "host-managed"} when logs are host-captured (no daemon log dir)
     * @param filename
     * @param lineCount
     * @param bytes
     * @param truncated True when older lines/bytes were dropped to stay within the caps
     * @param content
     */
    public record LogsResult(String source, String filename, int lineCount, long bytes,
                             boolean truncated, String content) {
    }

    /**
     * Returns a BOUNDED tail of the newest daemon log file for the {@code logs} command. When no
     * daemon log dir is configured the daemon's logs are host-captured (journald/launchd/Windows Event
     * Log), so this returns a delineated {@code host-managed} note (NOT a failure) pointing the
     * operator at the host tooling.
     *
     * @param maxLines Max trailing lines to return (clamped to {@code [1, 5000]})
     * @param maxBytes Max trailing bytes to read from the file (clamped to {@code [1 KiB, 1 MiB]})
     */
    public LogsResult tailLogs(int maxLines, long maxBytes) {
        int lines = Math.max(1, Math.min(maxLines, 5000));
        long byteCap = Math.max(1024L, Math.min(maxBytes, 1_048_576L));
        String configured = props.getLogDir();
        if (configured == null || configured.isBlank() || !Files.isDirectory(Path.of(configured))) {
            return new LogsResult("host-managed", null, 0, 0L, false,
                    "host-level: no daemon log directory is configured (sqs.maintenance.log-dir). "
                            + "This daemon's logs are captured by the host service manager "
                            + "(journald/launchd/Windows Event Log) — use the host log commands, or set "
                            + "sqs.maintenance.log-dir (and logging.file.path) to enable on-daemon log tailing.");
        }
        Optional<Path> newest = newestLogFile(Path.of(configured));
        if (newest.isEmpty()) {
            return new LogsResult("daemon-log-file", null, 0, 0L, false, "");
        }
        Path file = newest.get();
        try {
            long size = Files.size(file);
            long from = Math.max(0L, size - byteCap);
            byte[] buf = readFrom(file, from, size);
            boolean bytesTruncated = from > 0L;
            String text = new String(buf, StandardCharsets.UTF_8);
            if (bytesTruncated) {
                // We started mid-file: drop the first (partial) line so we never show a fragment.
                int nl = text.indexOf('\n');
                text = (nl >= 0) ? text.substring(nl + 1) : text;
            }
            String[] all = text.split("\n", -1);
            int start = Math.max(0, all.length - lines);
            boolean truncated = start > 0 || bytesTruncated;
            StringBuilder sb = new StringBuilder();
            for (int i = start; i < all.length; i++) {
                if (i > start) {
                    sb.append('\n');
                }
                sb.append(all[i]);
            }
            String content = sb.toString();
            int kept = all.length - start;
            log.info("maintenance.logs: tailed {} ({} line(s), {} bytes, truncated={})",
                    file.getFileName(), kept, content.length(), truncated);
            return new LogsResult("daemon-log-file", file.getFileName().toString(), kept,
                    content.getBytes(StandardCharsets.UTF_8).length, truncated, content);
        } catch (IOException e) {
            throw new MaintenanceException("logs: failed reading " + file + ": " + e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    /**
     * Empty when the dir has no regular log content; refuses when files exist but none are eligible
     * for this install (fail-closed).
     */
    private Optional<Path> newestLogFile(Path dir) {
        List<Path> eligible = eligibleLogFiles(dir);
        return eligible.stream().max(Comparator.comparingLong(MaintenanceService::lastModifiedQuiet));
    }

    /** Lazily load-or-create the install UUID under {@code {dataDir}/channel/install-identity}. */
    private String resolveInstallId() {
        String id = installId;
        if (id == null) {
            synchronized (this) {
                if (installId == null) {
                    installId = InstallIdentity.loadOrCreate(Path.of(dataDir));
                }
                id = installId;
            }
        }
        return id;
    }

    /**
     * Claim the log dir for this install (or verify the existing claim), then list top-level regular
     * files eligible for tailing. Uses {@link Files#list} only (no recursive walk).
     */
    private List<Path> eligibleLogFiles(Path logDir) {
        claimOrVerifyLogDir(logDir);
        String current = resolveInstallId();
        List<Path> eligible = new ArrayList<>();
        boolean sawRegularLog = false;
        try (var stream = Files.list(logDir)) {
            List<Path> topLevel = stream
                    .filter(p -> Files.isRegularFile(p, java.nio.file.LinkOption.NOFOLLOW_LINKS))
                    .toList();
            for (Path file : topLevel) {
                String name = file.getFileName().toString();
                // Markers / sidecars are not log content.
                if (name.equals(LOG_DIR_INSTALL_MARKER) || name.endsWith(LOG_FILE_INSTALL_SIDECAR_SUFFIX)) {
                    continue;
                }
                sawRegularLog = true;
                if (isEligibleLogCandidate(file, current)) {
                    eligible.add(file);
                }
            }
        } catch (IOException e) {
            throw new MaintenanceException("logs: cannot list log dir " + logDir + ": " + e.getMessage(), e);
        }
        // Regular files present but none eligible → refuse (do not fall through to empty/newest-unknown).
        if (eligible.isEmpty() && sawRegularLog) {
            throw new MaintenanceException(REFUSE_NO_ELIGIBLE_LOGS);
        }
        eligible.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return eligible;
    }

    /**
     * Dir claim: missing marker → write current install id; matching → OK; blank/unreadable or
     * different id → refuse (fail closed).
     */
    private void claimOrVerifyLogDir(Path logDir) {
        Path marker = logDir.resolve(LOG_DIR_INSTALL_MARKER);
        String current = resolveInstallId();
        try {
            if (!Files.exists(marker)) {
                Files.writeString(marker, current, StandardCharsets.UTF_8);
                return;
            }
            if (!Files.isRegularFile(marker)) {
                throw new MaintenanceException(REFUSE_DIR_MARKER_UNIDENTIFIABLE);
            }
            String claimed = Files.readString(marker, StandardCharsets.UTF_8).trim();
            if (claimed.isBlank()) {
                throw new MaintenanceException(REFUSE_DIR_MARKER_UNIDENTIFIABLE);
            }
            if (!claimed.equals(current)) {
                if (!logDirIsInsideDataDir(logDir)) {
                    throw new MaintenanceException(REFUSE_DIR_CLAIM_MISMATCH);
                }
                // Same install, next generation — re-bind instead of locking the operator out of
                // their own logs permanently (see logDirIsInsideDataDir).
                Files.writeString(marker, current, StandardCharsets.UTF_8);
                log.warn("maintenance.logs: re-bound log dir {} from install id {} to {} — the dir "
                                + "lives under this install's data dir, so the old marker is a previous "
                                + "generation of this same install, not a foreign one",
                        logDir, claimed, current);
            }
        } catch (MaintenanceException e) {
            throw e;
        } catch (IOException e) {
            throw new MaintenanceException(REFUSE_DIR_MARKER_UNIDENTIFIABLE, e);
        }
    }

    /**
     * The installers default {@code sqs.maintenance.log-dir} to {@code {SQS_DATA_DIR}/logs}, while a
     * reinstall purges {@code data/channel/} (minting a NEW install id) and keeps {@code data/logs/}
     * with its old marker. Under that layout a mismatch is a previous generation of this same install
     * at the same prefix — not a foreign tenant — and refusing left "fetch logs" broken for the life
     * of the install.
     */
    private boolean logDirIsInsideDataDir(Path logDir) {
        try {
            Path data = Path.of(dataDir).toAbsolutePath().normalize();
            return logDir.toAbsolutePath().normalize().startsWith(data);
        } catch (Exception e) {
            return false; // unresolvable path → treat as external and fail closed
        }
    }

    /**
     * Sidecar rules: matching id → eligible; different → foreign (exclude); blank/unreadable →
     * unidentifiable (exclude); missing sidecar → eligible only because the dir claim already
     * succeeded for the current install.
     */
    private static boolean isEligibleLogCandidate(Path file, String currentInstallId) {
        Path sidecar = file.resolveSibling(file.getFileName().toString() + LOG_FILE_INSTALL_SIDECAR_SUFFIX);
        if (!Files.exists(sidecar, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return true;
        }
        try {
            if (!Files.isRegularFile(sidecar, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                return false;
            }
            String sid = Files.readString(sidecar, StandardCharsets.UTF_8).trim();
            if (sid.isBlank()) {
                return false;
            }
            return sid.equals(currentInstallId);
        } catch (IOException e) {
            return false;
        }
    }

    /** Reads {@code [from, size)} of {@code file} into a byte[] (bounded by the caller's cap). */
    private static byte[] readFrom(Path file, long from, long size) throws IOException {
        int len = (int) Math.max(0, size - from);
        byte[] out = new byte[len];
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
            ch.position(from);
            ByteBuffer bb = ByteBuffer.wrap(out);
            int read = 0;
            while (read < len) {
                int n = ch.read(bb);
                if (n < 0) {
                    break;
                }
                read += n;
            }
            if (read < len) {
                byte[] trimmed = new byte[read];
                System.arraycopy(out, 0, trimmed, 0, read);
                return trimmed;
            }
        }
        return out;
    }

    private static long lastModifiedQuiet(Path p) {
        try {
            FileTime t = Files.getLastModifiedTime(p);
            return t == null ? 0L : t.toMillis();
        } catch (IOException e) {
            return 0L;
        }
    }

    /** Signals a host-level refusal or an unrecoverable IO problem in a maintenance op. */
    public static class MaintenanceException extends RuntimeException {
        public MaintenanceException(String message) {
            super(message);
        }

        public MaintenanceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
