package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListenerExecutorsTest {

    @Test
    void bounded_capsThreadsAndQueue() {
        ThreadPoolExecutor ex = (ThreadPoolExecutor) ListenerExecutors.bounded(
                "sqs-test", ListenerExecutors.class.getClassLoader());
        try {
            assertEquals(ListenerExecutors.MAX_THREADS, ex.getMaximumPoolSize());
            assertEquals(ListenerExecutors.QUEUE_CAP, ex.getQueue().remainingCapacity()
                    + ex.getQueue().size());
            assertTrue(ex.getRejectedExecutionHandler() instanceof ThreadPoolExecutor.AbortPolicy);
            assertTrue(ex.allowsCoreThreadTimeOut(), "a quiet listener must shrink back to zero threads");
        } finally {
            ex.shutdownNow();
        }
    }

    /**
     * The reinstalled Windows engine's failure: four busy connections (slow commands, or callers that
     * timed out and left their connection open) must not make the fifth wait — that connection's TLS
     * handshake is the next caller's whole connect budget.
     */
    @Test
    void bounded_runsTheFifthConnectionAtOnce_whileFourAreBusy() throws Exception {
        ThreadPoolExecutor ex = (ThreadPoolExecutor) ListenerExecutors.bounded(
                "sqs-test", ListenerExecutors.class.getClassLoader());
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        try {
            for (int i = 0; i < 4; i++) {
                ex.execute(() -> {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            java.util.concurrent.CountDownLatch fifthRan = new java.util.concurrent.CountDownLatch(1);
            ex.execute(fifthRan::countDown);

            assertTrue(fifthRan.await(2, java.util.concurrent.TimeUnit.SECONDS),
                    "the fifth connection must get its own thread, not queue behind the busy four");
            assertEquals(0, ex.getQueue().size());
        } finally {
            release.countDown();
            ex.shutdownNow();
        }
    }
}
