package com.studioquack.sqs.channel.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bug this pins: the backend keeps a queued command row until a result arrives and re-ships
 * the command on every beat until then. {@code restart} / {@code update-daemon} terminate the
 * daemon process before its in-memory result outbox can ship, so the result died with the old
 * process — the row stayed pending forever (the FE showed it "waiting for the next heartbeat")
 * and the freshly booted daemon, its dedup cache empty, executed the command AGAIN. With the
 * durable store, the post-restart registry answers the re-ship from disk.
 */
class DaemonCommandRegistryDurableResultTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path dir;

    /** Counting handler standing in for restart / update-daemon. */
    private static final class CountingHandler implements DaemonCommandHandler {
        private final AtomicInteger executions = new AtomicInteger();

        @Override
        public String type() {
            return "update-daemon";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) {
            executions.incrementAndGet();
            return DaemonCommandOutcome.ok("{\"status\":\"accepted\"}");
        }
    }

    private static DaemonCommand command(String id) {
        return new DaemonCommand(id, "update-daemon", "2026-11-07T00:00:00Z", null);
    }

    private DaemonCommandRegistry registry(CountingHandler handler, DurableCommandResultStore store) {
        return new DaemonCommandRegistry(List.of(handler), 60_000L, false, null, store);
    }

    @Test
    @DisplayName("post-restart re-ship re-reports the stored result instead of re-executing")
    void restartedRegistryReReportsStoredResult() throws Exception {
        CountingHandler handler = new CountingHandler();
        Path wal = dir.resolve("sqs-command-results.wal");
        DaemonCommand cmd = command("cmd-update-1");

        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal, mapper, 60_000L)) {
            Optional<DaemonCommandResult> first =
                    registry(handler, store).dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);
            assertThat(first).isPresent();
            assertThat(handler.executions.get()).isEqualTo(1);
        }

        // The process died before its in-memory outbox shipped the result: a brand-new registry
        // (empty claim cache) on the SAME store answers the backend's re-ship.
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal, mapper, 60_000L)) {
            Optional<DaemonCommandResult> second =
                    registry(handler, store).dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);

            assertThat(handler.executions.get())
                    .as("the handler must never run twice for one commandId")
                    .isEqualTo(1);
            assertThat(second).isPresent();
            assertThat(second.orElseThrow().status()).isEqualTo("ok");
            assertThat(second.orElseThrow().output()).isEqualTo("{\"status\":\"accepted\"}");
        }
    }

    @Test
    @DisplayName("a store miss executes the handler exactly once and persists its result")
    void storeMissExecutesAndPersists() throws Exception {
        CountingHandler handler = new CountingHandler();
        Path wal = dir.resolve("sqs-command-results.wal");

        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal, mapper, 60_000L)) {
            registry(handler, store).dispatch(command("cmd-new"), DaemonCommandRegistry.INBOUND_OVERLAY);

            assertThat(handler.executions.get()).isEqualTo(1);
            assertThat(store.load("cmd-new")).isPresent();
        }
    }

    @Test
    @DisplayName("high-frequency poll results stay in-memory-only")
    void pollResultsNotPersisted() throws Exception {
        DaemonCommandHandler status = new DaemonCommandHandler() {
            @Override
            public String type() {
                return "status";
            }

            @Override
            public DaemonCommandOutcome handle(DaemonCommand command) {
                return DaemonCommandOutcome.ok("{}");
            }
        };
        Path wal = dir.resolve("sqs-command-results.wal");

        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal, mapper, 60_000L)) {
            new DaemonCommandRegistry(List.of(status), 60_000L, false, null, store)
                    .dispatch(new DaemonCommand("cmd-poll", "status", "2026-11-07T00:00:00Z", null),
                            DaemonCommandRegistry.INBOUND_HEARTBEAT);

            assertThat(store.load("cmd-poll")).isEmpty();
        }
    }

    @Test
    @DisplayName("peekCompleted falls back to the store after a restart")
    void peekCompletedReadsStore() throws Exception {
        CountingHandler handler = new CountingHandler();
        Path wal = dir.resolve("sqs-command-results.wal");
        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal, mapper, 60_000L)) {
            registry(handler, store).dispatch(command("cmd-peek"), DaemonCommandRegistry.INBOUND_OVERLAY);
        }

        try (DurableCommandResultStore store = DurableCommandResultStore.openAt(wal, mapper, 60_000L)) {
            Optional<DaemonCommandResult> peeked = registry(handler, store).peekCompleted("cmd-peek");

            assertThat(peeked).isPresent();
            assertThat(peeked.orElseThrow().status()).isEqualTo("ok");
        }
    }
}
