package com.studioquack.sqs.status;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single virtual-thread emitter for BPMN node logs and ChannelSandboxLogCallback STARTED /
 * terminal posts. The global queue is bounded at {@link #QUEUE_BOUND} (2048) so a ~1000-node burst
 * of STARTED+terminal fits.
 */
@Slf4j
@Component
public class AsyncSandboxLogDispatcher {

    /** Chosen bound: 2048. A ~1000-node burst × STARTED+terminal fits; drop-oldest under overload. */
    public static final int QUEUE_BOUND = 2048;

    private static final int POLLERS = 8;

    /** Production drain budget for {@link #awaitRun}; never wait longer than this. */
    public static final Duration AWAIT_RUN_TIMEOUT = Duration.ofSeconds(2);

    private final ArrayBlockingQueue<Work> queue = new ArrayBlockingQueue<>(QUEUE_BOUND);
    private final ConcurrentHashMap<String, ReentrantLock> lanes = new ConcurrentHashMap<>();
    /** Queued + executing count per workflowExecutionId; removed when it hits zero. */
    private final ConcurrentHashMap<String, AtomicInteger> inFlightByRun = new ConcurrentHashMap<>();
    /**
     * Runs that lost at least one log post (dropped under saturation, task fault, or a post the
     * backend did not store). Consumed at the run's terminal moment: a lossy run is reported with
     * {@code logsDurable=false}, so self-hosted retention never deletes its local copy.
     */
    private final java.util.Set<String> lossyRuns = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicBoolean saturating = new AtomicBoolean();
    private final AtomicInteger droppedInBurst = new AtomicInteger();
    private final Object idleMonitor = new Object();

    public AsyncSandboxLogDispatcher() {
        for (int i = 0; i < POLLERS; i++) {
            workers.execute(this::pollLoop);
        }
    }

    /**
     * Per-node ordering: tasks for the same (runId, nodeId) run FIFO (STARTED before terminal if
     * enqueued in that order). Different nodes may run in parallel on virtual threads.
     */
    public void enqueue(String workflowExecutionId, String nodeId, Runnable task) {
        try {
            if (task == null
                    || workflowExecutionId == null || workflowExecutionId.isBlank()
                    || nodeId == null || nodeId.isBlank()) {
                return;
            }
            offerDropOldest(new Work(workflowExecutionId, nodeId, task));
        } catch (Throwable t) {
            log.warn("sandbox-log dispatcher enqueue failed (run={} node={})",
                    workflowExecutionId, nodeId, t);
        }
    }

    /** Test hook: block until the queue is empty and no lane is running, or timeout. */
    public boolean awaitIdle(Duration timeout) {
        try {
            long nanos = timeout == null ? 0L : timeout.toNanos();
            long deadline = System.nanoTime() + Math.max(0L, nanos);
            synchronized (idleMonitor) {
                while (!isIdle()) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        return isIdle();
                    }
                    idleMonitor.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
                }
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return isIdle();
        } catch (Throwable ignored) {
            return isIdle();
        }
    }

    /**
     * Block until this run has no queued or in-flight sandbox-log work, or the (capped) timeout
     * elapses. Production FAILED/CANCELED status uses this so terminal log rows reach the control
     * plane before the run-index flip.
     *
     * @return {@code true} when the run is idle, {@code false} on timeout / interrupt / fault
     */
    public boolean awaitRun(String workflowExecutionId, Duration timeout) {
        try {
            if (workflowExecutionId == null || workflowExecutionId.isBlank()) {
                return true;
            }
            Duration capped = capAwaitRunTimeout(timeout);
            long deadline = System.nanoTime() + Math.max(0L, capped.toNanos());
            synchronized (idleMonitor) {
                while (hasWorkForRun(workflowExecutionId)) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0L) {
                        return !hasWorkForRun(workflowExecutionId);
                    }
                    idleMonitor.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
                }
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return !hasWorkForRun(workflowExecutionId);
        } catch (Throwable ignored) {
            return workflowExecutionId == null
                    || workflowExecutionId.isBlank()
                    || !hasWorkForRun(workflowExecutionId);
        }
    }

    /** Records that a log post for this run did not reach the backend's store. Never throws. */
    public void markLossy(String workflowExecutionId) {
        if (workflowExecutionId != null && !workflowExecutionId.isBlank()) {
            lossyRuns.add(workflowExecutionId);
        }
    }

    /** True (and cleared) when the run lost a log post since its start. Called once at terminal. */
    public boolean consumeLossy(String workflowExecutionId) {
        return workflowExecutionId != null && lossyRuns.remove(workflowExecutionId);
    }

    /** Test hook: current queued (not yet running) count. */
    public int queuedCount() {
        return queue.size();
    }

    @PreDestroy
    public void shutdown() {
        running.set(false);
        workers.shutdownNow();
    }

    private boolean isIdle() {
        return queue.isEmpty() && inFlight.get() == 0;
    }

    private boolean hasWorkForRun(String runId) {
        AtomicInteger counter = inFlightByRun.get(runId);
        return counter != null && counter.get() > 0;
    }

    /** Null → default; never longer than {@link #AWAIT_RUN_TIMEOUT}; never negative. */
    private static Duration capAwaitRunTimeout(Duration timeout) {
        Duration effective = timeout == null ? AWAIT_RUN_TIMEOUT : timeout;
        if (effective.isNegative() || effective.isZero()) {
            return Duration.ZERO;
        }
        return effective.compareTo(AWAIT_RUN_TIMEOUT) > 0 ? AWAIT_RUN_TIMEOUT : effective;
    }

    private void offerDropOldest(Work work) {
        inFlight.incrementAndGet();
        incrementPerRun(work.runId());
        while (!queue.offer(work)) {
            Work oldest = queue.poll();
            if (oldest == null) {
                continue;
            }
            int dropped = droppedInBurst.incrementAndGet();
            markLossy(oldest.runId());
            if (saturating.compareAndSet(false, true)) {
                log.warn("sandbox-log dispatcher saturated; dropping oldest (dropped={} run={} node={})",
                        dropped, oldest.runId(), oldest.nodeId());
            }
            decrementInFlight(oldest.runId());
        }
        if (saturating.get() && queue.remainingCapacity() > 0) {
            if (saturating.compareAndSet(true, false)) {
                droppedInBurst.set(0);
            }
        }
    }

    private void pollLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Work work = queue.take();
                runWork(work);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                log.warn("sandbox-log dispatcher poller failed", t);
            }
        }
    }

    private void runWork(Work work) {
        ReentrantLock lock = lanes.computeIfAbsent(work.laneKey(), k -> new ReentrantLock(true));
        lock.lock();
        try {
            work.task().run();
        } catch (Throwable t) {
            markLossy(work.runId());
            log.warn("sandbox-log dispatcher task failed (run={} node={})",
                    work.runId(), work.nodeId(), t);
        } finally {
            lock.unlock();
            decrementInFlight(work.runId());
        }
    }

    private void incrementPerRun(String runId) {
        inFlightByRun.computeIfAbsent(runId, k -> new AtomicInteger()).incrementAndGet();
    }

    /** @return {@code true} when this run has no remaining queued/in-flight work */
    private boolean decrementPerRun(String runId) {
        AtomicInteger counter = inFlightByRun.get(runId);
        if (counter == null) {
            return true;
        }
        int remaining = counter.decrementAndGet();
        if (remaining <= 0) {
            inFlightByRun.compute(runId, (k, current) -> {
                if (current == null || current.get() <= 0) {
                    return null;
                }
                return current;
            });
            return true;
        }
        return false;
    }

    private void decrementInFlight(String runId) {
        boolean runIdle = decrementPerRun(runId);
        boolean allIdle = inFlight.decrementAndGet() == 0;
        if (runIdle || allIdle) {
            synchronized (idleMonitor) {
                idleMonitor.notifyAll();
            }
        }
    }

    private record Work(String runId, String nodeId, Runnable task) {
        String laneKey() {
            return runId + '\0' + nodeId;
        }
    }
}
