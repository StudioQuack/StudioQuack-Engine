package com.studioquack.sqs.channel.command;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The backend pushes a command down the WireGuard tunnel AND leaves the queued copy, so once the
 * overlay is up the same {@code commandId} arrives on both transports within milliseconds. Caching
 * only the finished result let both deliveries miss the cache and run the handler: two process
 * instances for one run, and two identical deploys racing into an Activiti unique-key violation.
 */
class DaemonCommandRegistryDedupTest {

    /** Blocks until released, so a second delivery is guaranteed to land mid-execution. */
    private static final class GatedHandler implements DaemonCommandHandler {
        private final AtomicInteger executions = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "start-run";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
            executions.incrementAndGet();
            entered.countDown();
            release.await(10, TimeUnit.SECONDS);
            return DaemonCommandOutcome.ok("started");
        }
    }

    private static DaemonCommand command(String id) {
        return new DaemonCommand(id, "start-run", "2026-07-26T01:29:39Z", "{}");
    }

    @Test
    @DisplayName("a concurrent delivery on the other transport waits instead of executing again")
    void concurrentDeliveryDoesNotDoubleExecute() throws Exception {
        GatedHandler handler = new GatedHandler();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(handler), 60_000L);
        DaemonCommand cmd = command("cmd-concurrent");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<Optional<DaemonCommandResult>> overlay = pool.submit(
                    () -> registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_OVERLAY));
            assertThat(handler.entered.await(5, TimeUnit.SECONDS))
                    .as("the first delivery is inside the handler").isTrue();

            // The heartbeat delivers the very same command while the overlay one is still running.
            Future<Optional<DaemonCommandResult>> heartbeat = pool.submit(
                    () -> registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT));
            handler.release.countDown();

            Optional<DaemonCommandResult> overlayResult = overlay.get(10, TimeUnit.SECONDS);
            Optional<DaemonCommandResult> heartbeatResult = heartbeat.get(10, TimeUnit.SECONDS);

            assertThat(handler.executions.get())
                    .as("the handler ran once, not once per transport")
                    .isEqualTo(1);
            assertThat(overlayResult).isPresent();
            assertThat(heartbeatResult)
                    .as("the duplicate still reports a result so the backend drops the command")
                    .isPresent();
            assertThat(heartbeatResult.orElseThrow().status())
                    .isEqualTo(overlayResult.orElseThrow().status());
        } finally {
            handler.release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("a later re-ship re-reports the cached result")
    void sequentialRedeliveryReusesResult() {
        GatedHandler handler = new GatedHandler();
        handler.release.countDown(); // never block
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(handler), 60_000L);
        DaemonCommand cmd = command("cmd-resent");

        Optional<DaemonCommandResult> first = registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);
        Optional<DaemonCommandResult> second = registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(handler.executions.get()).isEqualTo(1);
        assertThat(first).isPresent();
        assertThat(second).isPresent();
        assertThat(second.orElseThrow().status()).isEqualTo(first.orElseThrow().status());
    }

    @Test
    @DisplayName("distinct commandIds each execute")
    void distinctCommandsBothExecute() {
        GatedHandler handler = new GatedHandler();
        handler.release.countDown();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(handler), 60_000L);

        registry.dispatch(command("cmd-a"), DaemonCommandRegistry.INBOUND_HEARTBEAT);
        registry.dispatch(command("cmd-b"), DaemonCommandRegistry.INBOUND_OVERLAY);

        assertThat(handler.executions.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("a command with no id is dropped, not executed")
    void blankCommandIdDropped() {
        GatedHandler handler = new GatedHandler();
        handler.release.countDown();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(handler), 60_000L);

        assertThat(registry.dispatch(new DaemonCommand("  ", "start-run", null, "{}"),
                DaemonCommandRegistry.INBOUND_OVERLAY)).isEmpty();
        assertThat(handler.executions.get()).isZero();
    }
}
