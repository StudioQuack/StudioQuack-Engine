package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.helperchan.StreamEdgeRouter.RouteChain;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Live stream-IN sessions from a producing node to a {@code streamThrough} target. One session per
 * {@code (wfeId, sourceNodeId, sourcePort, targetNodeId)}: NDJSON POST into the target's running
 * library instance, SSE partials re-enter {@link StreamEdgeRouter} as the target's own output.
 */
@Slf4j
@Component
public class StreamSessionManager {

    static final Duration IDLE_TIMEOUT = Duration.ofMillis(
            com.studioquack.sqs.transport.StreamIdleLimits.DEFAULT_STREAM_IDLE_TIMEOUT_MS);
    /** Duplex connect budget — high wall so active stream-through is never cut; idle reaper applies. */
    static final Duration CONNECT_BUDGET = Duration.ofSeconds(
            com.studioquack.sqs.transport.StreamIdleLimits.STREAM_CURL_MAX_TIME_SEC);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

    private final HelperRequestRouter helperRouter;
    private final StreamInstanceLocator locator;
    private final SubProcessCompletionRegistry subProcessRegistry;
    private final StatefulInstanceManager statefulInstances;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<SessionKey, Session> sessions = new ConcurrentHashMap<>();
    private final AtomicLong droppedOldestTotal = new AtomicLong();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();

    /** Test hook: when true the writer parks instead of sending, so the queue can fill. */
    volatile boolean holdWriterForTests;

    /**
     * Test hook: the drop-oldest cap. Production keeps {@link HelperStreamLimits#MAX_QUEUED_FRAMES} (a
     * catastrophic guard sized never to fire in a real turn); a test that exercises the drop-oldest
     * path lowers it so the retained frames can still be flushed to the target within the test budget
     * once the writer is released.
     */
    volatile int queueCapForTests = HelperStreamLimits.MAX_QUEUED_FRAMES;

    /**
     * Test hook: when true the SSE reader parks until the writer has fully closed the
     * request, reproducing the race where {@code complete} arrives before the close line
     * reached the target (the reader used to kill the session on the spot).
     */
    volatile boolean holdReaderUntilWriterClosedForTests;

    /** Set after construction so the router ↔ manager cycle does not deadlock Spring. */
    private volatile StreamEdgeRouter router;

    public StreamSessionManager(@Nullable HelperRequestRouter helperRouter,
                                @Nullable StreamInstanceLocator locator,
                                @Nullable SubProcessCompletionRegistry subProcessRegistry) {
        this(helperRouter, locator, subProcessRegistry, null);
    }

    @Autowired
    public StreamSessionManager(@Nullable HelperRequestRouter helperRouter,
                                @Nullable StreamInstanceLocator locator,
                                @Nullable SubProcessCompletionRegistry subProcessRegistry,
                                @Lazy @Nullable StatefulInstanceManager statefulInstances) {
        this.helperRouter = helperRouter;
        this.locator = locator;
        this.subProcessRegistry = subProcessRegistry;
        this.statefulInstances = statefulInstances;
    }

    @Autowired
    public void setRouter(@Lazy @Nullable StreamEdgeRouter router) {
        this.router = router;
    }

    /** Test seam when Spring is not wiring the cycle. */
    public void wireRouterForTests(StreamEdgeRouter router) {
        this.router = router;
    }

    /**
     * Hands one already-JOLTed frame to the session for {@code target}. Returns {@code true} when the
     * frame was accepted (queued or written).
     */
    public boolean accept(HelperJob sourceJob, String sourcePort, String targetNodeId,
                          String targetPort, Map<String, Object> targetNode,
                          String channel, String delta, RouteChain chain) {
        if (sourceJob == null || targetNodeId == null || targetNodeId.isBlank() || delta == null) {
            return false;
        }
        if (StreamEdgeRouter.containsFileRef(delta)) {
            log.warn("stream-through: dropped non-text frame source={} port={} target={}",
                    str(sourceJob.executionContext().get("currentNodeId")), sourcePort, targetNodeId);
            return false;
        }
        Map<String, Object> ec = sourceJob.executionContext();
        String wfeId = str(ec.get("workflowExecutionId"));
        String sourceNodeId = str(ec.get("currentNodeId"));
        if (wfeId.isEmpty() || sourceNodeId.isEmpty()) {
            return false;
        }
        SessionKey key = new SessionKey(wfeId, sourceNodeId, sourcePort == null ? "" : sourcePort,
                targetNodeId);
        Session session;
        try {
            session = sessions.computeIfAbsent(key, k -> new Session(k, sourceJob, sourcePort,
                    targetNodeId, targetPort, targetNode, chain));
        } catch (RuntimeException e) {
            log.warn("stream-through: session create failed source={} target={}: {}",
                    sourceNodeId, targetNodeId, e.toString());
            return false;
        }
        if (session.failed.get()) {
            return false;
        }
        if (!session.ensureStarted()) {
            return false;
        }
        // Chunk, never truncate: each piece is enqueued as its own seq'd frame and the
        // receiver appends them in order, so an oversize delta reassembles exactly.
        for (String chunk : HelperStreamLimits.splitDelta(delta)) {
            if (!session.enqueue(channel, chunk)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Aborts every in-flight stream-through session whose source helper job is {@code dispatchId}.
     * Best-effort; never throws.
     */
    public void abortForDispatch(String dispatchId) {
        if (dispatchId == null || dispatchId.isBlank()) {
            return;
        }
        for (Session session : sessions.values()) {
            if (session.sourceJob != null && dispatchId.equals(session.sourceJob.id())) {
                session.requestClose();
            }
        }
    }

    /** Closes every session this source node opened on {@code wfeId} (revoke path). */
    public void closeBySource(String wfeId, String sourceNodeId) {
        if (wfeId == null || sourceNodeId == null) {
            return;
        }
        for (Session session : sessions.values()) {
            if (wfeId.equals(session.key.wfeId()) && sourceNodeId.equals(session.key.sourceNodeId())) {
                session.requestClose();
            }
        }
    }

    /** Test/diagnostics. */
    public int openSessions() {
        int n = 0;
        for (Session s : sessions.values()) {
            if (!s.closed.get() && !s.failed.get()) {
                n++;
            }
        }
        return n;
    }

    /** Test/diagnostics: frames shed by drop-oldest across all sessions. */
    public long droppedOldestTotal() {
        return droppedOldestTotal.get();
    }

    @PreDestroy
    public void shutdown() {
        for (Session s : sessions.values()) {
            s.requestClose();
        }
        workers.shutdownNow();
    }

    record SessionKey(String wfeId, String sourceNodeId, String sourcePort, String targetNodeId) {
    }

    private final class Session {
        private final SessionKey key;
        private final HelperJob sourceJob;
        private final String sourcePort;
        private final String targetNodeId;
        private final String targetPort;
        private final Map<String, Object> targetNode;
        private final RouteChain chain;
        private final ArrayDeque<QueuedFrame> queue = new ArrayDeque<>();
        private final AtomicBoolean failed = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicInteger seq = new AtomicInteger();
        private volatile long lastActivityNanos = System.nanoTime();
        private final Object gate = new Object();

        private volatile String requestId;
        private volatile String childWfeId;
        private volatile String helperToken;
        private volatile StreamDuplexPost duplex;
        /** Container counted as in-flight after a successful stream connect; ended once in cleanup. */
        private volatile String dispatchContainerId;
        private final AtomicBoolean dispatchInFlight = new AtomicBoolean();
        private volatile Map<String, Object> childEc;
        private final AtomicBoolean loggedDropOldest = new AtomicBoolean();
        /** Set when the writer finished the request (close line sent + request ended). */
        private final AtomicBoolean writerDone = new AtomicBoolean();
        /** Set when the SSE reader reached a terminal event or EOF. */
        private final AtomicBoolean readerDone = new AtomicBoolean();

        private Session(SessionKey key, HelperJob sourceJob, String sourcePort, String targetNodeId,
                        String targetPort, Map<String, Object> targetNode, RouteChain chain) {
            this.key = key;
            this.sourceJob = sourceJob;
            this.sourcePort = sourcePort;
            this.targetNodeId = targetNodeId;
            this.targetPort = targetPort == null ? "" : targetPort;
            this.targetNode = targetNode;
            this.chain = chain == null ? RouteChain.root() : chain;
        }

        boolean ensureStarted() {
            if (failed.get() || closed.get()) {
                return false;
            }
            if (started.get()) {
                return true;
            }
            synchronized (gate) {
                if (failed.get() || closed.get()) {
                    return false;
                }
                if (started.get()) {
                    return true;
                }
                try {
                    startLocked();
                    started.set(true);
                    return true;
                } catch (Exception e) {
                    failed.set(true);
                    if (e.getMessage() != null && e.getMessage().contains("no live instance")) {
                        log.warn("stream-through: no live instance env={} template={} target={} — dropping frame",
                                str(sourceJob.executionContext().get("envId")),
                                templateId(targetNode), targetNodeId);
                    } else {
                        log.warn("stream-through: open failed source={} port={} target={}: {}",
                                key.sourceNodeId(), sourcePort, targetNodeId, e.toString());
                    }
                    cleanupQuietly();
                    return false;
                }
            }
        }

        private void startLocked() throws Exception {
            Map<String, Object> srcEc = sourceJob.executionContext();
            String envId = str(srcEc.get("envId"));
            if (locator == null) {
                throw new IllegalStateException("no instance locator");
            }
            Optional<StreamInstanceLocator.Location> loc = locator.locate(envId, targetNode);
            if (loc.isEmpty()) {
                throw new IllegalStateException("no live instance");
            }
            StreamInstanceLocator.Location location = loc.get();
            String templateId = templateId(targetNode);
            if (templateId.isEmpty()) {
                throw new IllegalStateException("target has no templateId");
            }
            this.requestId = UUID.randomUUID().toString();
            this.childWfeId = UUID.randomUUID().toString();
            String helperJobId = "stream-" + requestId;
            this.childEc = childExecutionContext(srcEc, childWfeId);
            if (subProcessRegistry != null) {
                subProcessRegistry.register(new SubProcessCompletionRegistry.SubPiLink(
                        childWfeId, key.wfeId(), sourceJob.id(), envId, "StreamThrough", null));
            }
            if (helperRouter == null) {
                throw new IllegalStateException("helper router unavailable");
            }
            HelperJob helperJob = HelperJob.builder(helperJobId, "")
                    .executionContext(childEc)
                    .persistent(true)
                    .callerImageRef(location.imageRef())
                    .build();
            this.helperToken = helperRouter.registerPersistent(helperJob);

            // Full-duplex socket POST: JDK HttpClient HTTP/1.1 will not read SSE until
            // the request body completes, which stalls live partials until close.
            this.duplex = StreamDuplexPost.connect(URI.create(location.streamUrl()),
                    CONNECT_TIMEOUT, CONNECT_BUDGET);
            this.dispatchContainerId = location.containerId();
            StatefulInstanceManager instances = statefulInstances;
            if (instances != null && this.dispatchContainerId != null) {
                if (!instances.beginDispatch(this.dispatchContainerId)) {
                    // A drain closed the located container between locate and connect.
                    cleanupQuietly();
                    throw new IllegalStateException("stream target was replaced while connecting");
                }
                dispatchInFlight.set(true);
            }
            workers.submit(this::writeLoop);
            workers.submit(this::readSse);
        }

        boolean enqueue(String channel, String delta) {
            if (failed.get() || closed.get()) {
                return false;
            }
            if (System.nanoTime() - lastActivityNanos > IDLE_TIMEOUT.toNanos()) {
                log.warn("stream-through: idle timeout source={} target={}",
                        key.sourceNodeId(), targetNodeId);
                requestClose();
                return false;
            }
            lastActivityNanos = System.nanoTime();
            int frameSeq = seq.getAndIncrement();
            synchronized (queue) {
                int cap = queueCapForTests;
                if (queue.size() >= cap) {
                    queue.pollFirst();
                    droppedOldestTotal.incrementAndGet();
                    if (loggedDropOldest.compareAndSet(false, true)) {
                        log.warn("stream-through: drop-oldest source={} target={} cap={}",
                                key.sourceNodeId(), targetNodeId, cap);
                    }
                }
                queue.addLast(new QueuedFrame(channel, delta, frameSeq, false));
                queue.notifyAll();
            }
            return true;
        }

        void requestClose() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            if (!started.get()) {
                // Never started: no writer/reader loops exist to pair up, so finish directly.
                writerDone.set(true);
                readerDone.set(true);
                finish();
                return;
            }
            synchronized (queue) {
                queue.addLast(new QueuedFrame(null, null, -1, true));
                queue.notifyAll();
            }
            // The writer may already be gone (writer failure path ran finish() before the reader's terminal
            // event arrived). With no writer left to consume the close marker, nobody will ever set writerDone
            // — mark it so finish() can proceed once the reader parks.
            if (writerDone.get()) {
                finish();
            }
        }

        /**
         * Reader-side close: park until the writer has sent the close line and ended the request. A
         * terminal SSE event ({@code complete}/{@code done}/{@code error}) or EOF only means the TARGET
         * finished streaming its reply — the target's {@code awaitInputComplete()} still needs our close
         * line before it can run the node's completion path.
         */
        private void readerFinished() {
            readerDone.set(true);
            if (holdReaderUntilWriterClosedForTests) {
                return;
            }
            requestClose();
        }

        private void writeLoop() {
            boolean sentOpen = false;
            try {
                while (true) {
                    if (System.nanoTime() - lastActivityNanos > IDLE_TIMEOUT.toNanos()) {
                        log.warn("stream-through: writer idle timeout source={} target={}",
                                key.sourceNodeId(), targetNodeId);
                        break;
                    }
                    QueuedFrame frame;
                    synchronized (queue) {
                        frame = queue.pollFirst();
                        if (frame == null) {
                            queue.wait(IDLE_TIMEOUT.toMillis());
                            frame = queue.pollFirst();
                        }
                    }
                    if (frame == null) {
                        log.warn("stream-through: idle timeout source={} target={}",
                                key.sourceNodeId(), targetNodeId);
                        break;
                    }
                    lastActivityNanos = System.nanoTime();
                    while (holdWriterForTests && !frame.close) {
                        Thread.sleep(10);
                    }
                    if (!sentOpen) {
                        writeOpen(frame.close ? "" : frame.delta);
                        sentOpen = true;
                        if (frame.close) {
                            break;
                        }
                    }
                    if (frame.close) {
                        break;
                    }
                    writeNdjson(frameLine(frame));
                }
                if (sentOpen) {
                    writeNdjson(closeLine());
                }
                StreamDuplexPost conn = duplex;
                if (conn != null) {
                    conn.endRequest();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("stream-through: writer failed source={} target={}: {}",
                            key.sourceNodeId(), targetNodeId, e.toString());
                }
            } finally {
                // Signal BEFORE finish(): the reader may be parked in readerFinished() waiting on this flag, and
                // finish() tears the session down.
                writerDone.set(true);
                finish();
            }
        }

        private void writeOpen(String firstDelta) throws IOException {
            Map<String, Object> input = new LinkedHashMap<>();
            if (!targetPort.isEmpty()) {
                input.put(targetPort, firstDelta == null ? "" : firstDelta);
            }
            Map<String, Object> context = new LinkedHashMap<>();
            if (childEc != null) {
                context.putAll(childEc);
            }
            // Per-request helper bearer so the in-instance dispatcher can authenticate
            // log/emit during the stream. Rides in context (the open object has no token field).
            if (helperToken != null) {
                context.put("helperToken", helperToken);
            }
            Map<String, Object> open = new LinkedHashMap<>();
            open.put("type", "open");
            open.put("requestId", requestId);
            open.put("templateId", templateId(targetNode));
            open.put("input", input);
            open.put("context", context);
            writeNdjson(open);
        }

        private Map<String, Object> frameLine(QueuedFrame frame) {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("type", "frame");
            line.put("requestId", requestId);
            line.put("port", targetPort);
            line.put("channel", frame.channel == null ? "" : frame.channel);
            line.put("delta", frame.delta == null ? "" : frame.delta);
            line.put("seq", frame.seq);
            return line;
        }

        private Map<String, Object> closeLine() {
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("type", "close");
            line.put("requestId", requestId);
            return line;
        }

        private void writeNdjson(Object payload) throws IOException {
            StreamDuplexPost conn = duplex;
            if (conn == null) {
                return;
            }
            byte[] bytes = (mapper.writeValueAsString(payload) + "\n").getBytes(StandardCharsets.UTF_8);
            conn.writeNdjson(bytes);
        }

        private void readSse() {
            StreamDuplexPost conn = duplex;
            if (conn == null) {
                readerDone.set(true);
                finish();
                return;
            }
            try {
                int status = conn.awaitResponse();
                if (status < 200 || status >= 300) {
                    log.warn("stream-through: target HTTP {} source={} target={}",
                            status, key.sourceNodeId(), targetNodeId);
                    return;
                }
                try (BufferedReader reader = conn.bodyReader()) {
                    consumeSse(reader);
                }
            } catch (Exception e) {
                if (!closed.get()) {
                    log.warn("stream-through: sse reader failed source={} target={}: {}",
                            key.sourceNodeId(), targetNodeId, e.toString());
                }
            } finally {
                readerFinished();
            }
        }

        private void consumeSse(BufferedReader reader) throws IOException {
            String event = null;
            StringBuilder data = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("event:")) {
                    event = line.substring("event:".length()).trim();
                } else if (line.startsWith("data:")) {
                    String payload = line.substring("data:".length());
                    if (payload.startsWith(" ")) {
                        payload = payload.substring(1);
                    }
                    if (data.length() > 0) {
                        data.append('\n');
                    }
                    data.append(payload);
                } else if (line.isEmpty()) {
                    String ev = event;
                    handleSseEvent(ev, data.toString());
                    event = null;
                    data.setLength(0);
                    if ("done".equals(ev) || "complete".equals(ev) || "error".equals(ev)) {
                        return;
                    }
                }
            }
            if (event != null || data.length() > 0) {
                handleSseEvent(event, data.toString());
            }
        }

        private void handleSseEvent(String event, String data) {
            if (event == null || event.isBlank()) {
                return;
            }
            if ("partial".equals(event)) {
                reroutePartial(data);
                return;
            }
            if ("complete".equals(event) || "done".equals(event) || "error".equals(event)) {
                if ("error".equals(event)) {
                    log.warn("stream-through: target error event source={} target={}",
                            key.sourceNodeId(), targetNodeId);
                }
                // Terminal reply from the target: the reply stream is over, but the target's
                // completion path still needs our close line — let the writer finish.
                readerFinished();
            }
        }

        @SuppressWarnings("unchecked")
        private void reroutePartial(String data) {
            StreamEdgeRouter r = router;
            if (r == null || data == null || data.isBlank()) {
                return;
            }
            try {
                Map<String, Object> body = mapper.readValue(data, Map.class);
                String delta = body.get("delta") == null ? "" : String.valueOf(body.get("delta"));
                if (StreamEdgeRouter.containsFileRef(delta)) {
                    log.warn("stream-through: dropped non-text sse partial target={}", targetNodeId);
                    return;
                }
                String channel = body.get("channel") == null ? "" : String.valueOf(body.get("channel"));
                String port = body.get("port") == null ? "" : String.valueOf(body.get("port")).trim();
                if (port.isEmpty()) {
                    port = StreamEdgeRouter.soleOutputPort(targetNode);
                }
                if (port.isEmpty()) {
                    log.warn("stream-through: sse partial missing port and target has no unique OUTPUT — drop target={}",
                            targetNodeId);
                    return;
                }
                Map<String, Object> reEc = new LinkedHashMap<>();
                if (childEc != null) {
                    reEc.putAll(childEc);
                }
                // Parent run id so a Duckbot sink posts against the control plane-known run, not the child.
                reEc.put("workflowExecutionId", key.wfeId());
                reEc.put("currentNodeId", targetNodeId);
                reEc.put("currentNode", targetNode);
                HelperJob synthetic = HelperJob.builder("stream-out-" + requestId, "")
                        .executionContext(reEc)
                        .build();
                // Whole delta: the sink chunks oversize text into seq'd frames.
                r.route(synthetic, port, channel, delta, chain);
            } catch (Exception e) {
                log.warn("stream-through: sse partial reroute failed target={}: {}",
                        targetNodeId, e.toString());
            }
        }

        /**
         * Tears the session down only once BOTH loops are done: the writer owns the request
         * body (close line), the reader owns the reply stream. The first loop to finish
         * leaves the duplex open for the other; the second closes it.
         */
        private void finish() {
            if (!writerDone.get() || !readerDone.get()) {
                return;
            }
            closed.set(true);
            sessions.remove(key, this);
            cleanupQuietly();
        }

        private void cleanupQuietly() {
            if (dispatchInFlight.compareAndSet(true, false)) {
                StatefulInstanceManager instances = statefulInstances;
                if (instances != null) {
                    instances.endDispatch(dispatchContainerId);
                }
            }
            StreamDuplexPost conn = duplex;
            duplex = null;
            if (conn != null) {
                conn.close();
            }
            String token = helperToken;
            helperToken = null;
            if (token != null && helperRouter != null) {
                try {
                    helperRouter.revoke(token);
                } catch (RuntimeException e) {
                    log.warn("stream-through: helper revoke failed target={}: {}",
                            targetNodeId, e.toString());
                }
            }
            // The synthetic sub-PI link registered at start must not outlive the session —
            // an orphaned entry leaks the registry future (and a PENDING row when durable).
            String child = childWfeId;
            childWfeId = null;
            if (child != null && subProcessRegistry != null) {
                try {
                    subProcessRegistry.cancel(child, "stream-through session closed");
                } catch (RuntimeException e) {
                    log.warn("stream-through: sub-pi cancel failed target={}: {}",
                            targetNodeId, e.toString());
                }
            }
        }

        private Map<String, Object> childExecutionContext(Map<String, Object> srcEc, String childWfeId) {
            Map<String, Object> out = new LinkedHashMap<>();
            copyIfPresent(srcEc, out, "nodes");
            copyIfPresent(srcEc, out, "edges");
            copyIfPresent(srcEc, out, "agenticInfo");
            copyIfPresent(srcEc, out, "envId");
            copyIfPresent(srcEc, out, "workflowId");
            out.put("workflowExecutionId", childWfeId);
            out.put("currentNodeId", targetNodeId);
            out.put("currentNode", targetNode);
            return out;
        }
    }

    private record QueuedFrame(String channel, String delta, int seq, boolean close) {
    }

    static String templateId(Map<String, Object> node) {
        if (node == null || !(node.get("data") instanceof Map<?, ?> data)) {
            return "";
        }
        Object id = data.get("id");
        return id == null ? "" : String.valueOf(id).trim();
    }

    private static void copyIfPresent(Map<String, Object> src, Map<String, Object> dest, String key) {
        if (src != null && src.containsKey(key) && src.get(key) != null) {
            dest.put(key, src.get(key));
        }
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
