package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Streams Duckbot token frames to the control plane over ONE long-lived chunked POST per (run,
 * lane) — NDJSON lines, each written and flushed the moment it is produced. Here a lane opens
 * {@code POST .../conversation-stream/live} once per turn and every frame is flushed into the
 * request body immediately; the control plane relays each line to SSE as it arrives.
 */
@Slf4j
final class ConversationStreamUploader {

    /** Frames the uploader cannot carry fall back here (the coalescing per-batch POST path). */
    interface FallbackSink {
        void offer(String lane, ConversationStreamEvent frame);
    }

    private static final String EVENT_DELTA = "stream-delta";
    private static final String EVENT_END = "stream-end";

    /**
     * Per-lane queue bound; past it the oldest DELTAS drop (barriers never do). This is a
     * catastrophic memory guard only — sized far beyond any real turn's in-flight backlog
     * (tokens are tens of bytes and flush immediately), so live content is never dropped.
     */
    static final int MAX_LANE_QUEUE_BYTES = 256 * 1024 * 1024;

    /** Chunked-upload flush unit; one frame per chunk is the common case. */
    private static final int CHUNK_BYTES = 8192;

    /** Connect (incl. TLS handshake) bound, matching the channel clients. */
    private static final long CONNECT_TIMEOUT_MS = 15_000L;

    /** Bound on the final response read after the body closes at turn end. */
    private static final long RESPONSE_TIMEOUT_MS = 30_000L;

    /** No frame for this long → close the upload; the next frame opens a fresh one. */
    static final long LANE_IDLE_CLOSE_MS = 120_000L;

    /** Longest one upload stays open; a still-streaming lane reconnects past it. */
    static final long LANE_MAX_AGE_MS = 15 * 60 * 1000L;

    /** Lanes idle longer than this with no writer are evicted (a turn never outlives it). */
    private static final long LANE_IDLE_EVICT_MS = 15 * 60 * 1000L;

    /** Sweep for evictable lanes only past this many live entries. */
    private static final int LANE_SWEEP_THRESHOLD = 512;

    /** Mutable per-(run, lane) state. Guarded by {@code synchronized (lane)}. */
    private static final class Lane {
        private final String runId;
        private final String dispatcherLane;
        private final ArrayDeque<ConversationStreamEvent> pending = new ArrayDeque<>();
        private int pendingBytes;
        private boolean writerActive;
        private boolean aborted;
        private boolean endSeen;
        private boolean dropWarned;
        private long createdMs;
        private long lastActivityMs;
        /** Bumped per writer generation so a stale writer never touches a revived lane. */
        private long generation;

        private Lane(String runId, String dispatcherLane) {
            this.runId = runId;
            this.dispatcherLane = dispatcherLane;
        }
    }

    private final Function<String, URI> endpointForRun;
    private final Supplier<String> bearer;
    private final Supplier<SSLContext> sslContext;
    private final HostnameVerifier hostnameVerifier;
    private final ObjectMapper mapper;
    private final FallbackSink fallback;
    private final long laneIdleCloseMs;
    private final long laneMaxAgeMs;
    private final ExecutorService writers = Executors.newVirtualThreadPerTaskExecutor();

    /** True once a BE answers 404 — that control plane predates /live; stay on per-batch POSTs. */
    private final AtomicBoolean liveDisabled = new AtomicBoolean();

    /** (workflowExecutionId \0 dispatcherLane) → lane state. */
    private final ConcurrentHashMap<String, Lane> lanes = new ConcurrentHashMap<>();

    ConversationStreamUploader(Function<String, URI> endpointForRun,
                               Supplier<String> bearer,
                               Supplier<SSLContext> sslContext,
                               HostnameVerifier hostnameVerifier,
                               ObjectMapper mapper,
                               FallbackSink fallback) {
        this(endpointForRun, bearer, sslContext, hostnameVerifier, mapper, fallback,
                LANE_IDLE_CLOSE_MS, LANE_MAX_AGE_MS);
    }

    /** Test seam: short lane bounds exercise idle-close / age-rollover without waiting. */
    ConversationStreamUploader(Function<String, URI> endpointForRun,
                               Supplier<String> bearer,
                               Supplier<SSLContext> sslContext,
                               HostnameVerifier hostnameVerifier,
                               ObjectMapper mapper,
                               FallbackSink fallback,
                               long laneIdleCloseMs,
                               long laneMaxAgeMs) {
        this.endpointForRun = Objects.requireNonNull(endpointForRun, "endpointForRun");
        this.bearer = Objects.requireNonNull(bearer, "bearer");
        this.sslContext = Objects.requireNonNull(sslContext, "sslContext");
        this.hostnameVerifier = hostnameVerifier;
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.fallback = Objects.requireNonNull(fallback, "fallback");
        this.laneIdleCloseMs = laneIdleCloseMs;
        this.laneMaxAgeMs = laneMaxAgeMs;
    }

    /**
     * Queue one frame for immediate upload, starting the lane's writer on first use.
     * Never blocks the caller, never throws.
     */
    void offer(String dispatcherLane, ConversationStreamEvent frame) {
        try {
            if (frame == null || dispatcherLane == null) {
                return;
            }
            if (liveDisabled.get()) {
                fallback.offer(dispatcherLane, frame);
                return;
            }
            if (lanes.size() > LANE_SWEEP_THRESHOLD) {
                evictIdleLanes();
            }
            String key = frame.workflowExecutionId() + '\0' + dispatcherLane;
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
                if (!lane.writerActive) {
                    startWriterLocked(lane);
                }
                lane.notifyAll();
            }
        } catch (RuntimeException ex) {
            log.warn("duckbot-stream[live]: offer failed event={} seq={}: {}",
                    frame == null ? null : frame.event(), frame == null ? 0 : frame.seq(), ex.toString());
        }
    }

    /** Test hook: true once every lane has nothing pending and no writer running. */
    boolean awaitIdle(Duration timeout) {
        long deadline = System.nanoTime() + (timeout == null ? 0L : timeout.toNanos());
        while (true) {
            boolean idle = true;
            for (Lane lane : lanes.values()) {
                synchronized (lane) {
                    if (!lane.pending.isEmpty() || lane.writerActive) {
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

    /** Best-effort stop on daemon shutdown; losing a tail token never fails a node. */
    void shutdown() {
        for (Lane lane : lanes.values()) {
            synchronized (lane) {
                lane.aborted = true;
                lane.notifyAll();
            }
        }
        writers.shutdownNow();
        lanes.clear();
    }

    /** Test seam: force every frame onto the per-batch POST path (pins the fallback contract). */
    void disableForTests() {
        liveDisabled.set(true);
    }

    /** Test hook: true once a 404 (or this seam) has parked the live path for this process. */
    boolean isLiveDisabled() {
        return liveDisabled.get();
    }

    /** Starts a new writer generation for the lane. Caller holds the lane lock. */
    private void startWriterLocked(Lane lane) {
        lane.writerActive = true;
        lane.aborted = false;
        lane.createdMs = System.currentTimeMillis();
        long gen = ++lane.generation;
        writers.execute(() -> writerLoop(lane, gen));
    }

    private void writerLoop(Lane lane, long gen) {
        HttpURLConnection conn = null;
        try {
            conn = openConnection(lane.runId);
            OutputStream out = conn.getOutputStream();
            try {
                writeLoop(lane, gen, out);
            } finally {
                try {
                    out.close();
                } catch (IOException ignored) {
                    // already broken — the response read below still reports an early status
                }
            }
            onUploadComplete(lane, gen, safeResponseCode(conn));
        } catch (IOException ex) {
            // Broken connection mid-upload. If the server answered EARLY (e.g. 404 from a
            // control plane that predates /live), that status is still readable.
            int status = safeResponseCode(conn);
            if (status == 404) {
                parkLivePath();
            }
            laneFailed(lane, gen, ex);
        } catch (RuntimeException ex) {
            laneFailed(lane, gen, ex);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Pop frames in FIFO order and flush each one the moment it is written. Exits on
     * stream-end (turn over), on idle (a later frame reopens the lane), on connection age
     * (an active lane reconnects at once), or when aborted by shutdown.
     */
    private void writeLoop(Lane lane, long gen, OutputStream out) throws IOException {
        while (true) {
            ConversationStreamEvent frame;
            synchronized (lane) {
                while (!lane.aborted && lane.pending.isEmpty()) {
                    long waitMs = lane.lastActivityMs + laneIdleCloseMs - System.currentTimeMillis();
                    if (waitMs <= 0) {
                        // Idle close — a later frame reopens the lane on a fresh connection.
                        stopWriter(lane, gen);
                        return;
                    }
                    try {
                        lane.wait(waitMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        stopWriter(lane, gen);
                        return;
                    }
                }
                if (lane.aborted) {
                    stopWriter(lane, gen);
                    return;
                }
                frame = lane.pending.pollFirst();
                lane.pendingBytes -= deltaBytes(frame);
            }
            boolean end = EVENT_END.equals(frame.event());
            byte[] line = serialize(frame);
            if (line != null) {
                out.write(line);
                out.write('\n');
                out.flush();
            }
            if (end) {
                markEnded(lane, gen);
                return;
            }
            if (System.currentTimeMillis() - lane.createdMs > laneMaxAgeMs) {
                synchronized (lane) {
                    stopWriter(lane, gen);
                    if (!lane.pending.isEmpty()) {
                        // Bounded connection age: reconnect at once so the backlog keeps flowing.
                        startWriterLocked(lane);
                    }
                }
                return;
            }
        }
    }

    /** Final response after WE closed the body — the normal turn-end path. */
    private void onUploadComplete(Lane lane, long gen, int status) {
        if (status == 404) {
            parkLivePath();
            drainPendingToFallback(lane, gen);
        } else if (status < 200 || status >= 300) {
            log.warn("duckbot-stream[live]: BE {} for run={} lane={}", status, lane.runId, lane.dispatcherLane);
        }
    }

    /** A 404 means this control plane predates /live — stay on per-batch POSTs for this process. */
    private void parkLivePath() {
        log.warn("duckbot-stream[live]: BE answered 404 — falling back to per-batch POSTs");
        liveDisabled.set(true);
    }

    /** Transport failure: queued frames take the fallback path; written frames are lost. */
    private void laneFailed(Lane lane, long gen, Exception ex) {
        log.warn("duckbot-stream[live]: lane failed run={} lane={}: {}",
                lane.runId, lane.dispatcherLane, ex.toString());
        drainPendingToFallback(lane, gen);
        stopWriter(lane, gen);
    }

    private void drainPendingToFallback(Lane lane, long gen) {
        List<ConversationStreamEvent> leftovers;
        synchronized (lane) {
            if (lane.generation != gen || lane.pending.isEmpty()) {
                return;
            }
            leftovers = new ArrayList<>(lane.pending);
            lane.pending.clear();
            lane.pendingBytes = 0;
        }
        for (ConversationStreamEvent frame : leftovers) {
            fallback.offer(lane.dispatcherLane, frame);
        }
    }

    private void stopWriter(Lane lane, long gen) {
        synchronized (lane) {
            if (lane.generation == gen) {
                lane.writerActive = false;
            }
        }
    }

    /** Turn over and drained — drop the lane so long-lived daemons accumulate nothing. */
    private void markEnded(Lane lane, long gen) {
        synchronized (lane) {
            if (lane.generation != gen) {
                return;
            }
            lane.writerActive = false;
            lanes.remove(lane.runId + '\0' + lane.dispatcherLane, lane);
        }
    }

    /** Chunked POST with the channel TLS posture; the body streams, it is never buffered. */
    private HttpURLConnection openConnection(String runId) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) endpointForRun.apply(runId).toURL().openConnection();
        if (conn instanceof HttpsURLConnection https) {
            SSLContext ctx = sslContext.get();
            if (ctx != null) {
                https.setSSLSocketFactory(ctx.getSocketFactory());
            }
            if (hostnameVerifier != null) {
                https.setHostnameVerifier(hostnameVerifier);
            }
        }
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + bearer.get());
        conn.setRequestProperty("Content-Type", "application/x-ndjson");
        conn.setConnectTimeout((int) CONNECT_TIMEOUT_MS);
        conn.setReadTimeout((int) RESPONSE_TIMEOUT_MS);
        conn.setChunkedStreamingMode(CHUNK_BYTES);
        conn.setDoOutput(true);
        return conn;
    }

    /** Response code if one already arrived (incl. early answers); -1 when unreadable. */
    private static int safeResponseCode(HttpURLConnection conn) {
        if (conn == null) {
            return -1;
        }
        try {
            return conn.getResponseCode();
        } catch (IOException | RuntimeException ex) {
            return -1;
        }
    }

    /** Drop the oldest queued deltas past the lane cap; barriers are never dropped. */
    private void enforceLaneCap(Lane lane) {
        if (lane.pendingBytes <= MAX_LANE_QUEUE_BYTES) {
            return;
        }
        if (!lane.dropWarned) {
            lane.dropWarned = true;
            log.warn("duckbot-stream[live]: lane queue over {} bytes run={} lane={} — "
                            + "dropping oldest deltas (producer outran a stalled upload)",
                    MAX_LANE_QUEUE_BYTES, lane.runId, lane.dispatcherLane);
        }
        Iterator<ConversationStreamEvent> it = lane.pending.iterator();
        while (it.hasNext() && lane.pendingBytes > MAX_LANE_QUEUE_BYTES) {
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
                if (!lane.writerActive && lane.pending.isEmpty() && lane.lastActivityMs < cutoff) {
                    lanes.remove(entry.getKey(), lane);
                }
            }
        }
    }

    /** NDJSON line for one frame; null on a mapper fault (frame skipped, never fatal). */
    private byte[] serialize(ConversationStreamEvent frame) {
        try {
            return mapper.writeValueAsBytes(new ChannelConversationStreamCallback.Body(
                    frame.conversationId(), frame.agentMessageKey(), frame.nodeId(), frame.event(),
                    frame.channel(), frame.delta(), frame.seq(), frame.msgOrder(), frame.nodeLabel()));
        } catch (Exception ex) {
            log.warn("duckbot-stream[live]: serialize failed event={} seq={}: {}",
                    frame.event(), frame.seq(), ex.toString());
            return null;
        }
    }

    private static int deltaBytes(ConversationStreamEvent frame) {
        return frame.delta() == null ? 0 : frame.delta().getBytes(StandardCharsets.UTF_8).length;
    }
}
