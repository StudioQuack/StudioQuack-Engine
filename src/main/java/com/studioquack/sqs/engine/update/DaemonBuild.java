package com.studioquack.sqs.engine.update;

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The running daemon's build identity and the state of its last in-place update, published in
 * every resources snapshot (beat and status reply) as {@code daemonBuild}. the control plane compares
 * {@code jarSha256} with the published release to decide whether "Update daemon" has anything to
 * do, keeps the button disabled while {@code update.state} is {@code IN_PROGRESS}, and shows why
 * the last attempt failed. Process-wide facts, so process-wide state.
 */
@Slf4j
public final class DaemonBuild {

    /** Where the last update attempt stands. */
    public enum UpdateState { IDLE, IN_PROGRESS, FAILED }

    private static volatile String jarSha256;
    private static volatile UpdateState state = UpdateState.IDLE;
    private static volatile String error;
    /** Progress detail while IN_PROGRESS (e.g. "stopping 2 running instance(s)") — {@code null} otherwise. */
    private static volatile String detail;
    private static volatile Instant at;

    private DaemonBuild() {
    }

    /** Blocks until the async jar hash lands (bounded); {@code null} when it never does. */
    static String awaitJarSha256(java.time.Duration max) {
        long deadline = System.nanoTime() + max.toNanos();
        while (jarSha256 == null && System.nanoTime() < deadline) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return jarSha256;
    }

    /** Hashes the running jar once, off every request path (the jar is ~100 MB). */
    static void computeJarSha256Async() {
        Thread t = new Thread(() -> {
            try {
                jarSha256 = DaemonUpdateService.sha256(DaemonUpdateService.runningJar());
            } catch (Exception ex) {
                log.warn("daemon-build: could not hash the running jar: {}", ex.toString());
            }
        }, "sqs-daemon-build-hash");
        t.setDaemon(true);
        t.start();
    }

    static void updateStarted() {
        set(UpdateState.IN_PROGRESS, null);
    }

    /** Progress note shown next to "Updating…" — the operator sees WHY it is still updating. */
    static void updateDetail(String note) {
        detail = note;
    }

    static void updateFailed(String message) {
        set(UpdateState.FAILED, message == null ? "update failed"
                : message.length() > 300 ? message.substring(0, 300) : message);
    }

    static void updateIdle() {
        set(UpdateState.IDLE, null);
    }

    /** True when the last update is already reported as failed (keeps the first, precise reason). */
    static boolean hasFailure() {
        return state == UpdateState.FAILED;
    }

    private static void set(UpdateState next, String message) {
        state = next;
        error = message;
        detail = null;
        at = Instant.now();
    }

    /**
     * {@code {jarSha256?, update: {state, error?, detail?, blockedReason?, at?}}} — {@code jarSha256}
     * appears once hashed; {@code blockedReason} while an update could not run now (low disk).
     */
    public static Map<String, Object> snapshot() {
        Map<String, Object> update = new LinkedHashMap<>();
        update.put("state", state.name());
        if (error != null) {
            update.put("error", error);
        }
        if (state == UpdateState.IN_PROGRESS && detail != null) {
            update.put("detail", detail);
        }
        if (state != UpdateState.IN_PROGRESS) {
            String blocked = UpdateDiskCheck.currentShortfall();
            if (blocked != null) {
                update.put("blockedReason", blocked);
            }
        }
        if (at != null) {
            update.put("at", at.toString());
        }
        Map<String, Object> build = new LinkedHashMap<>();
        if (jarSha256 != null) {
            build.put("jarSha256", jarSha256);
        }
        build.put("update", update);
        return build;
    }
}
