package com.studioquack.sqs.retention;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * Measures disk usage of the daemon's data directory so the {@link LogRetentionEnforcer} can shed
 * finished-run history before the disk fills (the "log-until-disk-full" safety valve), not only by
 * the day-based {@code sqs.log-retention-days}. Thin, dependency-free wrapper over
 * {@link File#getTotalSpace} / {@link File#getUsableSpace} for the configured path (
 * {@code sqs.log-retention.disk.path}, default the daemon working dir).
 */
@Slf4j
@Component
public class DiskSpaceProbe {

    private final File dir;

    public DiskSpaceProbe(@Value("${sqs.log-retention.disk.path:.}") String path) {
        this.dir = new File(path == null || path.isBlank() ? "." : path.trim());
    }

    /** Total capacity of the volume backing the data dir, in bytes ({@code 0} when unknown). */
    public long totalBytes() {
        try {
            return dir.getTotalSpace();
        } catch (RuntimeException probe) {
            log.debug("disk-probe: totalSpace failed for {}: {}", dir, probe.getMessage());
            return 0L;
        }
    }

    /** Bytes still usable on the volume ({@code 0} when unknown). */
    public long usableBytes() {
        try {
            return dir.getUsableSpace();
        } catch (RuntimeException probe) {
            log.debug("disk-probe: usableSpace failed for {}: {}", dir, probe.getMessage());
            return 0L;
        }
    }

    /**
     * Fraction of the volume currently in use, in {@code [0.0, 1.0]}, or {@code -1.0}
     * when the volume size cannot be read (so callers can no-op rather than act on a
     * bogus reading).
     */
    public double usageRatio() {
        long total = totalBytes();
        if (total <= 0L) {
            return -1.0;
        }
        long usable = usableBytes();
        long used = Math.max(0L, total - usable);
        return (double) used / (double) total;
    }

    /** Convenience: usage as a whole-number percentage ({@code -1} when unknown). */
    public int usagePercent() {
        double ratio = usageRatio();
        return ratio < 0 ? -1 : (int) Math.round(ratio * 100.0);
    }

    /** The directory being measured (for logs/diagnostics). */
    public String path() {
        return dir.getPath();
    }
}
