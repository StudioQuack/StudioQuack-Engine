package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.engine.update.WorkIntakeGate;
import com.studioquack.sqs.retention.DiskSpaceProbe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stops new work when free disk space falls below {@code stop-free-mb}, and resumes above
 * {@code resume-free-mb}. It reports the condition and does not delete logs to make room.
 */
@Slf4j
@Component
public class DiskFullGuard {

    private static final long MB = 1024L * 1024L;

    private static volatile boolean stopped;
    private static volatile int usagePercent = -1;
    private static volatile long freeMb = -1;
    private static volatile Instant stoppedSince;

    private final DiskSpaceProbe probe;
    private final WorkIntakeGate intake;
    private final boolean enabled;
    private final long stopFreeBytes;
    private final long resumeFreeBytes;

    public DiskFullGuard(DiskSpaceProbe probe,
                         WorkIntakeGate intake,
                         @Value("${sqs.disk-guard.enabled:true}") boolean enabled,
                         @Value("${sqs.disk-guard.stop-free-mb:2048}") long stopFreeMb,
                         @Value("${sqs.disk-guard.resume-free-mb:4096}") long resumeFreeMb) {
        this.probe = probe;
        this.intake = intake;
        this.enabled = enabled;
        this.stopFreeBytes = Math.max(0L, stopFreeMb) * MB;
        this.resumeFreeBytes = Math.max(resumeFreeMb, stopFreeMb + 1) * MB;
    }

    @Scheduled(fixedDelayString = "${sqs.disk-guard.interval-ms:60000}",
            initialDelayString = "${sqs.disk-guard.initial-delay-ms:30000}")
    public void check() {
        if (!enabled) {
            return;
        }
        if (probe.totalBytes() <= 0L) {
            return; // unknown reading: never act on it
        }
        long free = probe.usableBytes();
        int pct = probe.usagePercent();
        usagePercent = pct;
        freeMb = free / MB;
        boolean low = free < stopFreeBytes;
        boolean clear = free >= resumeFreeBytes;
        if (!stopped && low) {
            stopped = true;
            stoppedSince = Instant.now();
            intake.close(WorkIntakeGate.Reason.DISK_FULL);
            log.error("disk-guard: data volume {} has {} MB free ({}% used; stop below {} MB) — engine STOPPED taking "
                            + "new work; nothing is deleted. Free space or archive/delete old logs to resume.",
                    probe.path(), free / MB, pct, stopFreeBytes / MB);
        } else if (stopped && clear) {
            stopped = false;
            stoppedSince = null;
            intake.open(WorkIntakeGate.Reason.DISK_FULL);
            log.info("disk-guard: data volume {} back to {} MB free (resume above {} MB) — engine takes new work again",
                    probe.path(), free / MB, resumeFreeBytes / MB);
        }
    }

    /** {@code {stopped, usagePercent?, freeMb?, since?}} — published in every resources snapshot. */
    public static Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("stopped", stopped);
        if (usagePercent >= 0) {
            out.put("usagePercent", usagePercent);
        }
        if (freeMb >= 0) {
            out.put("freeMb", freeMb);
        }
        Instant since = stoppedSince;
        if (stopped && since != null) {
            out.put("since", since.toString());
        }
        return out;
    }

    /** Test hook: forget process-wide state between tests. */
    static void reset() {
        stopped = false;
        usagePercent = -1;
        freeMb = -1;
        stoppedSince = null;
    }
}
