package com.studioquack.sqs.engine.update;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Free-space preflight of an in-place update. The verified jar is staged beside the running one
 * (the previous build stays for rollback), and the updated build must write its engine database
 * the moment it boots: on a full disk it cannot come up, and the engine goes offline instead of
 * updating. So the update refuses before it downloads or kills anything, and the same verdict
 * rides every resources snapshot as {@code daemonBuild.update.blockedReason} — Engine Settings
 * disables Update and says why before anyone presses it.
 */
@Slf4j
final class UpdateDiskCheck {

    /** Beyond the staged jar: the first boot's database, queue and log writes. */
    static final long HEADROOM_BYTES = 2L * 1024 * 1024 * 1024;

    private static final long CACHE_MS = 30_000L;
    private static final double GB = 1024.0 * 1024 * 1024;

    private static volatile long checkedAt;
    private static volatile String cached;

    private UpdateDiskCheck() {
    }

    /** The cached verdict for the running jar ({@code null}: an update fits, or it cannot be judged). */
    static String currentShortfall() {
        long now = System.currentTimeMillis();
        if (now - checkedAt < CACHE_MS) {
            return cached;
        }
        String verdict;
        try {
            Path jar = DaemonUpdateService.runningJar();
            verdict = shortfall(jar, dataDir(jar));
        } catch (Exception ex) {
            verdict = null; // not a boot jar (dev run) or an unreadable volume: never block on a guess
        }
        cached = verdict;
        checkedAt = now;
        return verdict;
    }

    /** {@code null} when an update of {@code jar} fits on its volume and the data volume; else why not. */
    static String shortfall(Path jar, Path dataDir) throws IOException {
        FileStore jarStore = Files.getFileStore(jar);
        long jarBytes = Files.size(jar);
        FileStore dataStore = dataDir == null || !Files.exists(dataDir) ? jarStore : Files.getFileStore(dataDir);
        return shortfall(jarBytes, jarStore.getUsableSpace(), jarStore.toString(),
                dataStore.getUsableSpace(), dataStore.toString());
    }

    /** Pure verdict: the jar's volume needs the staged jar plus headroom, the data volume the headroom. */
    static String shortfall(long jarBytes, long jarStoreFree, String jarStoreName,
                            long dataStoreFree, String dataStoreName) {
        long needOnJarStore = jarBytes + HEADROOM_BYTES;
        if (jarStoreFree < needOnJarStore) {
            return message(jarStoreFree, jarStoreName, needOnJarStore);
        }
        if (dataStoreFree < HEADROOM_BYTES) {
            return message(dataStoreFree, dataStoreName, HEADROOM_BYTES);
        }
        return null;
    }

    /** The data directory the launcher exports; the jar's own directory when unset. */
    static Path dataDir(Path jar) {
        String env = System.getenv("SQS_DATA_DIR");
        return env == null || env.isBlank() ? jar.toAbsolutePath().getParent() : Path.of(env.trim());
    }

    private static String message(long free, String where, long need) {
        return String.format(Locale.ROOT,
                "not enough free disk space to update: %.1f GB free on %s, about %.1f GB needed - free some space, "
                        + "then update again", free / GB, where, need / GB);
    }

    /** Test hook: forget the cached verdict. */
    static void reset() {
        checkedAt = 0L;
        cached = null;
    }
}
