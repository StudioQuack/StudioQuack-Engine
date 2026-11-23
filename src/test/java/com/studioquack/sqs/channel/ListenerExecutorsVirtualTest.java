package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Long-open listener requests park on virtual threads with the fat-jar classloader as TCCL, and
 * in-flight requests stay bounded: one past the bound is rejected at once, and a finished request
 * frees its slot.
 */
class ListenerExecutorsVirtualTest {

    @Test
    void requestsRunOnVirtualThreadsWithTheGivenClassLoader() throws Exception {
        ClassLoader fatJar = new URLClassLoader(new URL[0]);
        ExecutorService ex = ListenerExecutors.virtual("test-listener", fatJar, 4);
        try {
            AtomicReference<Thread> ran = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            ex.execute(() -> {
                ran.set(Thread.currentThread());
                done.countDown();
            });
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertTrue(ran.get().isVirtual());
            assertSame(fatJar, ran.get().getContextClassLoader());
        } finally {
            ex.shutdownNow();
        }
    }

    @Test
    void inFlightRequestsAreBounded() throws Exception {
        ExecutorService ex = ListenerExecutors.virtual("test-listener", null, 2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(2);
        try {
            for (int i = 0; i < 2; i++) {
                ex.execute(() -> {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    finished.countDown();
                });
            }
            assertThrows(RejectedExecutionException.class, () -> ex.execute(() -> { }));

            release.countDown();
            assertTrue(finished.await(5, TimeUnit.SECONDS));
            CountDownLatch third = new CountDownLatch(1);
            for (int attempt = 0; attempt < 50 && third.getCount() > 0; attempt++) {
                try {
                    ex.execute(third::countDown);
                } catch (RejectedExecutionException slotNotYetFreed) {
                    Thread.sleep(20);
                }
            }
            assertTrue(third.await(5, TimeUnit.SECONDS), "a finished request frees its slot");
        } finally {
            ex.shutdownNow();
        }
    }
}
