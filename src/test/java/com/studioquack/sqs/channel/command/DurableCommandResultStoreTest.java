package com.studioquack.sqs.channel.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The store is the crash-safe backing of the registry's dedup cache: a result must survive the
 * process (and host) dying right after the handler ran, because the backend re-ships the command
 * until a result arrives — and for self-terminating commands ({@code restart},
 * {@code update-daemon}) the result is exactly what the old process took with it.
 */
class DurableCommandResultStoreTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path dir;

    private Path wal() {
        return dir.resolve("sqs-command-results.wal");
    }

    private static DaemonCommandResult result(String id, String status) {
        return new DaemonCommandResult(id, "update-daemon", status, "{\"accepted\":true}", null);
    }

    @Test
    @DisplayName("record then load round-trips the terminal result")
    void recordThenLoad() throws Exception {
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            store.record(result("cmd-1", "ok"));

            Optional<DaemonCommandResult> loaded = store.load("cmd-1");

            assertThat(loaded).isPresent();
            assertThat(loaded.orElseThrow().status()).isEqualTo("ok");
            assertThat(loaded.orElseThrow().output()).isEqualTo("{\"accepted\":true}");
        }
    }

    @Test
    @DisplayName("results survive a close + reopen (the daemon-restart case)")
    void survivesReopen() throws Exception {
        try (DurableCommandResultStore first = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            first.record(result("cmd-restart", "ok"));
        }

        try (DurableCommandResultStore second = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            assertThat(second.load("cmd-restart")).isPresent();
            assertThat(second.load("cmd-never-seen")).isEmpty();
        }
    }

    @Test
    @DisplayName("entries older than the TTL are dropped at replay and on read")
    void expiredEntriesDropped() throws Exception {
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal(), mapper, 1L)) {
            store.record(result("cmd-old", "ok"));
            Thread.sleep(5L);
            assertThat(store.load("cmd-old")).isEmpty();
        }
        try (DurableCommandResultStore replayed = DurableCommandResultStore.openAt(wal(), mapper, 1L)) {
            assertThat(replayed.load("cmd-old")).isEmpty();
        }
    }

    @Test
    @DisplayName("a torn tail write is tolerated — everything before it still loads")
    void tornTailTolerated() throws Exception {
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            store.record(result("cmd-good", "ok"));
        }
        // Simulate a crash mid-append: garbage bytes after the last intact record.
        try (RandomAccessFile file = new RandomAccessFile(wal().toFile(), "rw")) {
            file.seek(file.length());
            file.write(new byte[]{0x10, 0x00, 0x00, 0x00, (byte) 0xFF, (byte) 0xFE, 0x01});
        }

        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            assertThat(store.load("cmd-good")).isPresent();
        }
    }

    @Test
    @DisplayName("a blank command id is ignored, never persisted")
    void blankIdIgnored() throws Exception {
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            store.record(new DaemonCommandResult("  ", "restart", "ok", null, null));
            store.record(null);

            assertThat(store.load("  ")).isEmpty();
        }
    }

    @Test
    @DisplayName("the latest record per command id wins")
    void latestWins() throws Exception {
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            store.record(result("cmd-x", "failed"));
            store.record(new DaemonCommandResult("cmd-x", "update-daemon", "ok", null, null));

            assertThat(store.load("cmd-x")).isPresent();
            assertThat(store.load("cmd-x").orElseThrow().status()).isEqualTo("ok");
        }
        try (DurableCommandResultStore replayed = DurableCommandResultStore.openAt(wal(), mapper, 60_000L)) {
            assertThat(replayed.load("cmd-x").orElseThrow().status()).isEqualTo("ok");
        }
    }
}
