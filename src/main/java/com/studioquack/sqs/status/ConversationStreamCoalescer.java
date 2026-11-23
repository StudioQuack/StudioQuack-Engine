package com.studioquack.sqs.status;

import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Self-clocking batching layer between {@link ChannelConversationStreamCallback} and the
 * {@link AsyncSandboxLogDispatcher}: one HTTP POST per LLM token cannot keep up with the model.
 * Instead of enqueueing one POST per frame, frames accumulate in a per-(run, lane) buffer while a
 * POST for that lane is in flight; the moment it completes, ONE merged POST carries everything
 * that arrived during the round-trip.
 */
@Slf4j
final class ConversationStreamCoalescer {

    /** One merged POST never exceeds this; the BE caps an inbound delta at 256 KiB. */
    static final int MAX_BATCH_BYTES = 64 * 1024;

    /**
     * Past the cap the oldest buffered deltas drop (barriers never do). This is a catastrophic memory
     * guard only — sized far beyond any real turn's backlog, so live content is never dropped.
     */
    static final int MAX_LANE_BUFFER_BYTES = 256 * 1024 * 1024;

    /**
     * A POST task older than this without completing was dropped by a saturated dispatcher
     * (its completion callback never ran) — re-drive the lane instead of wedging it.
     * Generous margin over the callback's 10s POST timeout.
     */
    static final long STUCK_POST_MS = 30_000L;

    /** Lanes idle longer than this with nothing pending are evicted (a turn never outlives it). */
    private static final long LANE_IDLE_EVICT_MS = 15 * 60 * 1000L;

    /** Sweep for evictable lanes only past this many live entries. */
    private static final int LANE_SWEEP_THRESHOLD = 512;

    private static final String EVENT_DELTA = "stream-delta";
    private static final String EVENT_END = "stream-end";

    /** Where a drained batch goes: one dispatcher task per submitted frame. */
    interface BatchSink {
        void submit(String runId, String lane, ConversationStreamEvent frame, Runnable onComplete);
    }

    /** Mutable per-(run, lane) state. Guarded by {@code synchronized (lane)}. */
    private static final class Lane {
        private final String runId;
        private final String dispatcherLane;
        private final ArrayDeque<ConversationStreamEvent> pending = new ArrayDeque<>();
        private int pendingBytes;
        private boolean postInFlight;
        private long lastSubmitMs;
        private long lastActivityMs;
        private boolean endSeen;
        private boolean dropWarned;

        private Lane(String runId, String dispatcherLane) {
            this.runId = runId;
            this.dispatcherLane = dispatcherLane;
        }
    }

    private final BatchSink sink;
    private final long stuckPostMs;
    private final int maxLaneBufferBytes;
    /** (workflowExecutionId \0 dispatcherLane) → lane state. */
    private final ConcurrentHashMap<String, Lane> lanes = new ConcurrentHashMap<>();

    ConversationStreamCoalescer(BatchSink sink) {
        this(sink, STUCK_POST_MS);
    }

    /** Test seam: a short {@code stuckPostMs} exercises the saturation watchdog without waiting. */
    ConversationStreamCoalescer(BatchSink sink, long stuckPostMs) {
        this(sink, stuckPostMs, MAX_LANE_BUFFER_BYTES);
    }

    /** Test seam: a small {@code maxLaneBufferBytes} exercises drop-oldest without 256 MiB of payload. */
    ConversationStreamCoalescer(BatchSink sink, long stuckPostMs, int maxLaneBufferBytes) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.stuckPostMs = stuckPostMs;
        this.maxLaneBufferBytes = maxLaneBufferBytes;
    }

    /**
     * Buffer one frame and drive the lane if it is idle. Barrier frames (start/end / delta-less) flush
     * whatever precedes them and submit in order.
     */
    void offer(String dispatcherLane, ConversationStreamEvent frame) {
        try {
            if (frame == null || dispatcherLane == null) {
                return;
            }
            String key = frame.workflowExecutionId() + '\0' + dispatcherLane;
            if (lanes.size() > LANE_SWEEP_THRESHOLD) {
                evictIdleLanes();
            }
            Lane lane = lanes.computeIfAbsent(key,
                    k -> new Lane(frame.workflowExecutionId(), dispatcherLane));
            synchronized (lane) {
                lane.lastActivityMs = System.currentTimeMillis();
                lane.pending.addLast(frame);
                lane.pendingBytes += deltaBytes(frame);
                if (EVENT_END.equals(frame.event())) {
                    lane.endSeen = true;
                }
                enforceLaneCap(lane);
                if (lane.postInFlight
                        && lane.lastSubmitMs > 0
                        && lane.lastActivityMs - lane.lastSubmitMs > stuckPostMs) {
                    // The in-flight task vanished (saturation drop) — re-drive or the lane wedges.
                    lane.postInFlight = false;
                }
                if (!lane.postInFlight) {
                    drainLocked(lane);
                }
            }
        } catch (RuntimeException ex) {
            log.warn("duckbot-stream[coalesce]: offer failed event={} seq={}: {}",
                    frame == null ? null : frame.event(), frame == null ? 0 : frame.seq(), ex.toString());
        }
    }

    /** Test hook: true once every lane has nothing pending and nothing in flight. */
    boolean awaitIdle(Duration timeout) {
        long deadline = System.nanoTime() + (timeout == null ? 0L : timeout.toNanos());
        while (true) {
            boolean idle = true;
            for (Lane lane : lanes.values()) {
                synchronized (lane) {
                    if (!lane.pending.isEmpty() || lane.postInFlight) {
                        idle = false;
                        break;
                    }
                }
            }
            if (idle) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /** Best-effort final drain on daemon shutdown; losing a tail token never fails a node. */
    void shutdown() {
        for (Map.Entry<String, Lane> entry : lanes.entrySet()) {
            Lane lane = entry.getValue();
            synchronized (lane) {
                try {
                    while (!lane.pending.isEmpty()) {
                        submitLocked(lane, nextGroupLocked(lane));
                    }
                } catch (RuntimeException ex) {
                    log.debug("duckbot-stream[coalesce]: shutdown drain failed: {}", ex.toString());
                }
            }
        }
        lanes.clear();
    }

    /**
     * Submit merged batches until the pending queue empties. Each submitted task re-enters
     * {@link #onPostComplete} when its POST returns, which is what clocks the next batch.
     */
    private void drainLocked(Lane lane) {
        while (!lane.pending.isEmpty()) {
            ConversationStreamEvent group = nextGroupLocked(lane);
            if (group == null) {
                break;
            }
            submitLocked(lane, group);
        }
    }

    private void submitLocked(Lane lane, ConversationStreamEvent frame) {
        lane.postInFlight = true;
        lane.lastSubmitMs = System.currentTimeMillis();
        try {
            sink.submit(lane.runId, lane.dispatcherLane, frame, () -> onPostComplete(lane));
        } catch (RuntimeException ex) {
            // The dispatcher itself never throws, but a sink failure must not wedge the lane.
            lane.postInFlight = false;
            log.warn("duckbot-stream[coalesce]: submit failed run={} lane={}: {}",
                    lane.runId, lane.dispatcherLane, ex.toString());
        }
    }

    private void onPostComplete(Lane lane) {
        synchronized (lane) {
            lane.postInFlight = false;
            try {
                if (!lane.pending.isEmpty()) {
                    drainLocked(lane);
                } else if (lane.endSeen) {
                    // Turn over and drained — drop the lane so long-lived daemons do not
                    // accumulate one entry per (run, node) forever.
                    lanes.remove(lane.runId + '\0' + lane.dispatcherLane, lane);
                }
            } catch (RuntimeException ex) {
                log.warn("duckbot-stream[coalesce]: completion re-drain failed: {}", ex.toString());
            }
        }
    }

    /**
     * Pop the next wire unit: a barrier frame alone, or the maximal run of consecutive
     * same-channel deltas that fits {@link #MAX_BATCH_BYTES}. Null when nothing is pending.
     */
    private ConversationStreamEvent nextGroupLocked(Lane lane) {
        ConversationStreamEvent first = lane.pending.pollFirst();
        if (first == null) {
            return null;
        }
        lane.pendingBytes -= deltaBytes(first);
        if (!EVENT_DELTA.equals(first.event()) || first.delta() == null) {
            return first;
        }
        StringBuilder merged = new StringBuilder(first.delta());
        int bytes = deltaBytes(first);
        while (true) {
            ConversationStreamEvent next = lane.pending.peekFirst();
            if (next == null || !EVENT_DELTA.equals(next.event()) || next.delta() == null
                    || !Objects.equals(next.channel(), first.channel())) {
                break;
            }
            int nextBytes = deltaBytes(next);
            if (bytes + nextBytes > MAX_BATCH_BYTES) {
                break;
            }
            lane.pending.pollFirst();
            lane.pendingBytes -= nextBytes;
            merged.append(next.delta());
            bytes += nextBytes;
        }
        return new ConversationStreamEvent(first.workflowExecutionId(), first.conversationId(),
                first.agentMessageKey(), first.nodeId(), first.event(), first.channel(),
                merged.toString(), first.seq(), first.msgOrder(), first.nodeLabel());
    }

    /** Drop the oldest buffered deltas past the lane cap; barriers are never dropped. */
    private void enforceLaneCap(Lane lane) {
        if (lane.pendingBytes <= maxLaneBufferBytes) {
            return;
        }
        if (!lane.dropWarned) {
            lane.dropWarned = true;
            log.warn("duckbot-stream[coalesce]: lane buffer over {} bytes run={} lane={} — "
                    + "dropping oldest deltas (producer outran a saturated relay)",
                    maxLaneBufferBytes, lane.runId, lane.dispatcherLane);
        }
        Iterator<ConversationStreamEvent> it = lane.pending.iterator();
        while (it.hasNext() && lane.pendingBytes > maxLaneBufferBytes) {
            ConversationStreamEvent frame = it.next();
            if (!EVENT_DELTA.equals(frame.event())) {
                continue;
            }
            lane.pendingBytes -= deltaBytes(frame);
            it.remove();
        }
    }

    private void evictIdleLanes() {
        long cutoff = System.currentTimeMillis() - LANE_IDLE_EVICT_MS;
        for (Map.Entry<String, Lane> entry : lanes.entrySet()) {
            Lane lane = entry.getValue();
            synchronized (lane) {
                if (!lane.postInFlight && lane.pending.isEmpty() && lane.lastActivityMs < cutoff) {
                    lanes.remove(entry.getKey(), lane);
                }
            }
        }
    }

    private static int deltaBytes(ConversationStreamEvent frame) {
        return frame.delta() == null ? 0 : frame.delta().getBytes(StandardCharsets.UTF_8).length;
    }
}
