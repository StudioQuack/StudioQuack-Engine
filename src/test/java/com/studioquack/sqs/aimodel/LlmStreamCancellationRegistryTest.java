package com.studioquack.sqs.aimodel;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link LlmStreamCancellationRegistry}: cancel-instance aborts the run's in-flight provider
 * streams (the LLM server stops generating on client disconnect); finished streams unregister
 * so a later cancel is a no-op; unknown/blank keys never throw.
 */
class LlmStreamCancellationRegistryTest {

    private static final class CountingCloseable implements Closeable {
        private final AtomicInteger closes;

        private CountingCloseable(AtomicInteger closes) {
            this.closes = closes;
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    @Test
    void abortRunClosesEveryRegisteredStreamExactlyOnce() {
        LlmStreamCancellationRegistry registry = new LlmStreamCancellationRegistry();
        AtomicInteger closes = new AtomicInteger();
        registry.register("run-1", new CountingCloseable(closes));
        registry.register("run-1", new CountingCloseable(closes));

        assertEquals(2, registry.abortRun("run-1"));
        assertEquals(2, closes.get());
        // Idempotent: a second cancel for the same run finds nothing.
        assertEquals(0, registry.abortRun("run-1"));
        assertEquals(2, closes.get());
    }

    @Test
    void unregisteredStreamsAreNotAborted() {
        LlmStreamCancellationRegistry registry = new LlmStreamCancellationRegistry();
        AtomicInteger closes = new AtomicInteger();
        CountingCloseable finished = new CountingCloseable(closes);
        registry.register("run-1", finished);
        registry.unregister("run-1", finished);

        assertEquals(0, registry.abortRun("run-1"));
        assertEquals(0, closes.get());
    }

    @Test
    void abortIsScopedToTheRunKey() {
        LlmStreamCancellationRegistry registry = new LlmStreamCancellationRegistry();
        AtomicInteger run1 = new AtomicInteger();
        AtomicInteger run2 = new AtomicInteger();
        registry.register("run-1", new CountingCloseable(run1));
        registry.register("run-2", new CountingCloseable(run2));

        assertEquals(1, registry.abortRun("run-1"));
        assertEquals(1, run1.get());
        assertEquals(0, run2.get());
    }

    @Test
    void blankKeysAndUnknownRunsAreNoOps() {
        LlmStreamCancellationRegistry registry = new LlmStreamCancellationRegistry();
        AtomicInteger closes = new AtomicInteger();
        registry.register(null, new CountingCloseable(closes));
        registry.register("  ", new CountingCloseable(closes));
        registry.register("run-1", null);

        assertEquals(0, registry.abortRun(null));
        assertEquals(0, registry.abortRun(""));
        assertEquals(0, registry.abortRun("never-registered"));
        assertEquals(0, closes.get());
    }

    @Test
    void aStreamThatThrowsOnCloseDoesNotFailTheAbort() {
        LlmStreamCancellationRegistry registry = new LlmStreamCancellationRegistry();
        AtomicInteger closes = new AtomicInteger();
        registry.register("run-1", () -> {
            throw new RuntimeException("already broken pipe");
        });
        registry.register("run-1", new CountingCloseable(closes));

        assertEquals(1, registry.abortRun("run-1"));
        assertEquals(1, closes.get());
    }
}
