package com.studioquack.sqs.status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Contract of {@link AsyncSandboxLogDispatcher}: per-node FIFO, cross-node parallelism,
 * drop-oldest at {@link AsyncSandboxLogDispatcher#QUEUE_BOUND}, and enqueue never throws.
 */
class AsyncSandboxLogDispatcherTest {

    private AsyncSandboxLogDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new AsyncSandboxLogDispatcher();
    }

    @AfterEach
    void tearDown() {
        dispatcher.shutdown();
    }

    @Test
    void aFailingPostMarksItsRunLossy_once_andOtherRunsStayClean() throws Exception {
        dispatcher.enqueue("run-bad", "node-a", () -> {
            throw new IllegalStateException("BE down");
        });
        dispatcher.enqueue("run-good", "node-a", () -> { });
        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(3))).isTrue();

        assertThat(dispatcher.consumeLossy("run-bad")).isTrue();
        assertThat(dispatcher.consumeLossy("run-bad")).as("consumed once at terminal").isFalse();
        assertThat(dispatcher.consumeLossy("run-good")).isFalse();
    }

    @Test
    void anExplicitlyMarkedRunIsLossy() {
        dispatcher.markLossy("run-declined");

        assertThat(dispatcher.consumeLossy("run-declined")).isTrue();
        assertThat(dispatcher.consumeLossy(null)).isFalse();
    }

    @Test
    void sameNodeStartedThenTerminalRunInOrder() throws Exception {
        AtomicInteger sequence = new AtomicInteger();
        CopyOnWriteArrayList<String> order = new CopyOnWriteArrayList<>();
        CountDownLatch startedRunning = new CountDownLatch(1);
        CountDownLatch releaseStarted = new CountDownLatch(1);

        dispatcher.enqueue("run-1", "node-a", () -> {
            order.add("STARTED");
            sequence.incrementAndGet();
            startedRunning.countDown();
            awaitQuietly(releaseStarted);
        });
        assertThat(startedRunning.await(3, TimeUnit.SECONDS)).isTrue();

        dispatcher.enqueue("run-1", "node-a", () -> {
            order.add("TERMINAL");
            sequence.incrementAndGet();
        });
        releaseStarted.countDown();

        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(order).containsExactly("STARTED", "TERMINAL");
        assertThat(sequence.get()).isEqualTo(2);
    }

    @Test
    void differentNodesDoNotWaitForEachOther() throws Exception {
        CountDownLatch slowEntered = new CountDownLatch(1);
        CountDownLatch releaseSlow = new CountDownLatch(1);
        CountDownLatch fastDone = new CountDownLatch(1);
        AtomicBoolean fastFinishedWhileSlowHeld = new AtomicBoolean();

        dispatcher.enqueue("run-1", "slow-node", () -> {
            slowEntered.countDown();
            awaitQuietly(releaseSlow);
        });
        assertThat(slowEntered.await(3, TimeUnit.SECONDS)).isTrue();

        dispatcher.enqueue("run-1", "fast-node", () -> {
            fastFinishedWhileSlowHeld.set(releaseSlow.getCount() > 0);
            fastDone.countDown();
        });
        assertThat(fastDone.await(3, TimeUnit.SECONDS)).isTrue();
        assertThat(fastFinishedWhileSlowHeld).isTrue();

        releaseSlow.countDown();
        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void overflowDropsOldestAndKeepsQueuedCountAtBound() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch firstRunning = new CountDownLatch(1);

        dispatcher.enqueue("run-fill", "node-fill", () -> {
            firstRunning.countDown();
            awaitQuietly(release);
        });
        assertThat(firstRunning.await(3, TimeUnit.SECONDS)).isTrue();

        // Park the remaining pollers on the same lane so the global queue can fill.
        for (int i = 0; i < 32; i++) {
            dispatcher.enqueue("run-fill", "node-fill", () -> awaitQuietly(release));
        }
        assertThat(waitUntilStableQueue(Duration.ofSeconds(2))).isTrue();

        for (int i = 0; i < AsyncSandboxLogDispatcher.QUEUE_BOUND + 128; i++) {
            dispatcher.enqueue("run-fill", "node-fill", () -> {
            });
        }

        assertThat(dispatcher.queuedCount())
                .isLessThanOrEqualTo(AsyncSandboxLogDispatcher.QUEUE_BOUND)
                .isEqualTo(AsyncSandboxLogDispatcher.QUEUE_BOUND);

        release.countDown();
        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(10))).isTrue();
        assertThat(dispatcher.consumeLossy("run-fill"))
                .as("a dropped post means BE does not hold every row of that run").isTrue();
        assertThat(dispatcher.queuedCount()).isZero();
    }

    @Test
    void enqueueNullAndThrowingTaskNeverPropagate() {
        assertThatCode(() -> dispatcher.enqueue(null, "node-1", () -> {
        })).doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.enqueue("run-1", null, () -> {
        })).doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.enqueue("run-1", "node-1", null)).doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.enqueue("  ", "node-1", () -> {
        })).doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.enqueue("run-1", "  ", () -> {
        })).doesNotThrowAnyException();

        assertThatCode(() -> dispatcher.enqueue("run-1", "node-1", () -> {
            throw new IllegalStateException("boom");
        })).doesNotThrowAnyException();

        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void awaitIdleReturnsTrueAfterWorkDrains() {
        AtomicInteger completed = new AtomicInteger();
        for (int i = 0; i < 20; i++) {
            dispatcher.enqueue("run-drain", "node-" + i, completed::incrementAndGet);
        }

        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
        assertThat(completed.get()).isEqualTo(20);
        assertThat(dispatcher.queuedCount()).isZero();
    }

    @Test
    void awaitRunReturnsTrueWhenOnlyThatRunIsIdle() throws Exception {
        CountDownLatch otherEntered = new CountDownLatch(1);
        CountDownLatch releaseOther = new CountDownLatch(1);
        AtomicInteger targetDone = new AtomicInteger();

        dispatcher.enqueue("other-run", "node-hold", () -> {
            otherEntered.countDown();
            awaitQuietly(releaseOther);
        });
        assertThat(otherEntered.await(3, TimeUnit.SECONDS)).isTrue();

        dispatcher.enqueue("target-run", "node-a", targetDone::incrementAndGet);

        assertThat(dispatcher.awaitRun("target-run", Duration.ofSeconds(2))).isTrue();
        assertThat(targetDone.get()).isEqualTo(1);
        assertThat(dispatcher.awaitRun("other-run", Duration.ofMillis(80))).isFalse();

        releaseOther.countDown();
        assertThat(dispatcher.awaitIdle(Duration.ofSeconds(5))).isTrue();
    }

    @Test
    void awaitRunTimesOutWhileRunHeld() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        dispatcher.enqueue("held-run", "node-1", () -> {
            entered.countDown();
            awaitQuietly(release);
        });
        assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();

        assertThat(dispatcher.awaitRun("held-run", Duration.ofMillis(80))).isFalse();

        release.countDown();
        assertThat(dispatcher.awaitRun("held-run", Duration.ofSeconds(2))).isTrue();
    }

    @Test
    void awaitRunNeverThrowsOnBlankIdOrNullTimeout() {
        assertThatCode(() -> dispatcher.awaitRun(null, Duration.ofSeconds(2)))
                .doesNotThrowAnyException();
        assertThatCode(() -> dispatcher.awaitRun("  ", Duration.ofSeconds(2)))
                .doesNotThrowAnyException();
        assertThat(dispatcher.awaitRun(null, Duration.ofSeconds(2))).isTrue();
        assertThat(dispatcher.awaitRun("idle-run", null)).isTrue();
    }

    private boolean waitUntilStableQueue(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        int last = -1;
        int stableHits = 0;
        while (System.nanoTime() < deadline) {
            int current = dispatcher.queuedCount();
            if (current == last) {
                stableHits++;
                if (stableHits >= 8) {
                    return true;
                }
            } else {
                stableHits = 0;
                last = current;
            }
            Thread.sleep(10);
        }
        return false;
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
