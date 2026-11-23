package com.studioquack.sqs.channel.command;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The heartbeat command lanes exist for one reason: the beat loop is the daemon's last-resort
 * control channel and must never be held by a handler. These tests pin that contract — submit()
 * returns immediately even when a handler is wedged, emergency management commands run despite a
 * blocked serial lane, and workload commands keep their queued order.
 */
class CommandLaneDispatcherTest {

    /** Records the order handlers ran in. */
    private static final class RecordingHandler implements DaemonCommandHandler {
        private final String type;
        private final ConcurrentLinkedQueue<String> ran;

        RecordingHandler(String type, ConcurrentLinkedQueue<String> ran) {
            this.type = type;
            this.ran = ran;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) {
            ran.add(command.commandId());
            return DaemonCommandOutcome.ok("done");
        }
    }

    /** A handler that wedges until released — the Podman-after-host-sleep stand-in. */
    private static final class BlockingHandler implements DaemonCommandHandler {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "status";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) {
            entered.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return DaemonCommandOutcome.ok("unwedged");
        }
    }

    private static DaemonCommand cmd(String id, String type) {
        return new DaemonCommand(id, type, "2026-08-24T00:00:00Z", null);
    }

    private static void awaitTrue(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10L);
        }
        fail("timed out waiting for: " + what);
    }

    @Test
    void serialLanePreservesQueuedOrder() throws Exception {
        ConcurrentLinkedQueue<String> ran = new ConcurrentLinkedQueue<>();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(
                new RecordingHandler("deploy", ran), new RecordingHandler("start-run", ran)));
        ConcurrentLinkedQueue<DaemonCommandResult> results = new ConcurrentLinkedQueue<>();
        AtomicInteger wakes = new AtomicInteger();
        CommandLaneDispatcher lanes = new CommandLaneDispatcher(registry, results::add, wakes::incrementAndGet);
        try {
            lanes.submit(cmd("d1", "deploy"));
            lanes.submit(cmd("r1", "start-run"));
            awaitTrue(() -> results.size() == 2, "both commands executed");
            assertEquals(List.of("d1", "r1"), List.copyOf(ran),
                    "workload commands run in the order the backend queued them");
            assertEquals(2, wakes.get(), "each completion wakes the beat loop to ship its result");
        } finally {
            lanes.shutdown();
        }
    }

    @Test
    void submitNeverBlocksOnAWedgedHandler() throws Exception {
        BlockingHandler wedged = new BlockingHandler();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(wedged));
        CommandLaneDispatcher lanes = new CommandLaneDispatcher(registry, r -> { }, () -> { });
        try {
            lanes.submit(cmd("s1", "status"));
            assertTrue(wedged.entered.await(5, TimeUnit.SECONDS), "lane picked up the command");

            // The whole point: with the serial lane wedged, submit() of the next command — and of
            // the re-shipped SAME command — still returns immediately. Before the lanes, both ran
            // on the beat thread and this is where the heartbeat died.
            long start = System.nanoTime();
            lanes.submit(cmd("s2", "status"));
            lanes.submit(cmd("s1", "status")); // re-ship of the in-flight id
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(elapsedMs < 1_000L, "submit must not wait on the wedged lane (" + elapsedMs + "ms)");
            assertTrue(lanes.isInFlight("s1"), "wedged command still tracked as in-flight");
            assertTrue(lanes.isInFlight("s2"), "follow-up command queued behind it exactly once");
        } finally {
            wedged.release.countDown();
            lanes.shutdown();
        }
    }

    @Test
    void immediateLaneRunsDespiteAWedgedSerialLane() throws Exception {
        BlockingHandler wedged = new BlockingHandler();
        ConcurrentLinkedQueue<String> ran = new ConcurrentLinkedQueue<>();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(
                wedged, new RecordingHandler("net-reconnect", ran)));
        ConcurrentLinkedQueue<DaemonCommandResult> results = new ConcurrentLinkedQueue<>();
        CommandLaneDispatcher lanes = new CommandLaneDispatcher(registry, results::add, () -> { });
        try {
            lanes.submit(cmd("s1", "status"));
            assertTrue(wedged.entered.await(5, TimeUnit.SECONDS), "serial lane is now wedged");

            // The recovery command must not queue behind the wedge — it is the command that
            // re-establishes the overlay the blocked lane depends on.
            lanes.submit(cmd("w1", "net-reconnect"));
            awaitTrue(() -> ran.contains("w1"), "net-reconnect ran on the immediate lane");
            awaitTrue(() -> results.stream().anyMatch(r -> "w1".equals(r.commandId())),
                    "net-reconnect result queued for the next beat");
        } finally {
            wedged.release.countDown();
            lanes.shutdown();
        }
    }

    @Test
    void duplicateSubmissionOfInFlightCommandIsSuppressed() throws Exception {
        // The backend re-ships a queued command on every beat until the result arrives. If each
        // re-ship queued another lane task behind the wedged one, every task would re-report the
        // cached result when it finally ran — the sink would see N copies.
        BlockingHandler wedged = new BlockingHandler();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(wedged));
        ConcurrentLinkedQueue<DaemonCommandResult> results = new ConcurrentLinkedQueue<>();
        CommandLaneDispatcher lanes = new CommandLaneDispatcher(registry, results::add, () -> { });
        try {
            lanes.submit(cmd("c1", "status"));
            assertTrue(wedged.entered.await(5, TimeUnit.SECONDS), "lane picked up the command");
            for (int i = 0; i < 5; i++) {
                lanes.submit(cmd("c1", "status")); // the per-beat re-ships
            }
            assertTrue(lanes.isInFlight("c1"));
            assertFalse(lanes.isInFlight("never-submitted"));

            wedged.release.countDown();
            awaitTrue(() -> !results.isEmpty(), "the one execution reported its result");
            Thread.sleep(200L); // settle: a leaked duplicate task would re-report within this window
            assertEquals(1, results.size(), "re-ships of an in-flight commandId never reached the lane");
        } finally {
            wedged.release.countDown();
            lanes.shutdown();
        }
    }
}
