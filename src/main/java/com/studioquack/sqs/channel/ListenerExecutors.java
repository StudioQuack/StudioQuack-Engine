package com.studioquack.sqs.channel;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded HTTPS dispatch pools for overlay/public JDK {@code HttpServer}s. Cached pools grow without limit
 * under a slow peer (SQS-Q-02).
 */
public final class ListenerExecutors {

    public static final int MAX_THREADS = 32;
    public static final int QUEUE_CAP = 64;

    private ListenerExecutors() {
    }

    /**
     * Up to {@link #MAX_THREADS} connections are served at once; only beyond that do they wait in the
     * {@link #QUEUE_CAP}-slot queue, and beyond both they are rejected. Core size equals the maximum on purpose: a
     * {@link ThreadPoolExecutor} adds threads above its core only once the queue is FULL, so a small core served
     * just that many connections while the rest queued.
     */
    public static ExecutorService bounded(String namePrefix, ClassLoader tcl) {
        AtomicLong seq = new AtomicLong();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, namePrefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            if (tcl != null) {
                t.setContextClassLoader(tcl);
            }
            return t;
        };
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                MAX_THREADS, MAX_THREADS, 60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(QUEUE_CAP),
                tf,
                new ThreadPoolExecutor.AbortPolicy());
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    /**
     * Virtual-thread dispatch for a listener whose requests may stay open a long time (cross-env llm generations
     * and sub-process calls carry no time limit): a parked request holds no platform thread, so long calls cannot
     * starve short ones. In-flight requests stay bounded by {@code maxInFlight}, so a slow or hostile peer still
     * cannot grow the daemon without limit; beyond it a request is rejected at once, like {@link #bounded}'s abort
     * policy.
     */
    public static ExecutorService virtual(String namePrefix, ClassLoader tcl, int maxInFlight) {
        Semaphore inFlight = new Semaphore(maxInFlight);
        ExecutorService threads = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name(namePrefix + "-", 1).factory());
        return new AbstractExecutorService() {
            @Override
            public void execute(Runnable task) {
                if (!inFlight.tryAcquire()) {
                    throw new RejectedExecutionException(namePrefix + ": " + maxInFlight + " requests in flight");
                }
                try {
                    threads.execute(() -> {
                        if (tcl != null) {
                            Thread.currentThread().setContextClassLoader(tcl);
                        }
                        try {
                            task.run();
                        } finally {
                            inFlight.release();
                        }
                    });
                } catch (RejectedExecutionException e) {
                    inFlight.release();
                    throw e;
                }
            }

            @Override
            public void shutdown() {
                threads.shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                return threads.shutdownNow();
            }

            @Override
            public boolean isShutdown() {
                return threads.isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return threads.isTerminated();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return threads.awaitTermination(timeout, unit);
            }
        };
    }
}
