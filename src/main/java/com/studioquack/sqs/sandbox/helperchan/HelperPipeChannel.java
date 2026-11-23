package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Daemon-side owner of ONE warm container's broker pipe: the long-lived {@code podman exec -i
 * <cid> python3 -u sq_helper_broker.py} process ({@code OciWarmer} plants + launches it — see
 * {@code com.studioquack.sqs.sandbox.oci.HelperBroker}). This class only pumps frames once that
 * process exists, so it can be unit-tested against any process that speaks the frame protocol (a
 * real container/broker is not required).
 */
@Slf4j
public final class HelperPipeChannel implements AutoCloseable {

    /** Caps how many malformed/undiagnosable broker stderr lines are logged, so a chatty or
     * misbehaving broker can never flood the daemon log — the stream is still drained past the cap
     * so the child process's stderr pipe never fills and blocks it. */
    private static final int MAX_STDERR_LOG_LINES = 200;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** {@code accept} is optional (absent on old brokers / buffered callers). */
    private record RequestFrame(String id, String method, String path, String bearer, String body_b64,
                                String accept) {
    }

    /** One-shot wire frame — exactly these three fields; never add contentType/done/seq. */
    private record ResponseFrame(String id, int status, String body_b64) {
    }

    /** Stream wire frame. {@code seq} is 0-based per request; last frame has {@code done=true}. */
    private record StreamResponseFrame(String id, int status, String contentType, String chunk_b64,
                                       int seq, boolean done) {
    }

    private final Process process;
    private final HelperRequestRouter router;
    private final ExecutorService dispatchPool;
    private final Object writeLock = new Object();
    private final BufferedWriter stdin;
    private final Thread pumpThread;
    private final Thread stderrDrainThread;
    private volatile boolean closed;
    /** In-flight chunked requests by id; only ever touched on the single pump thread. */
    private final Map<String, ChunkedRequest> chunkedRequests = new ConcurrentHashMap<>();

    public HelperPipeChannel(Process process, HelperRequestRouter router) {
        this.process = process;
        this.router = router;
        this.dispatchPool = Executors.newThreadPerTaskExecutor(virtualThreadFactory());
        this.stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
        this.pumpThread = new Thread(this::pumpLoop, "helper-pipe-pump");
        this.pumpThread.setDaemon(true);
        this.stderrDrainThread = new Thread(this::drainStderr, "helper-pipe-stderr");
        this.stderrDrainThread.setDaemon(true);
        this.pumpThread.start();
        this.stderrDrainThread.start();
    }

    /**
     * @return true while the broker process is alive, the pump thread is still reading its
     * stdout, and this channel has not been closed. The pump check matters: a broker process
     * can stay alive while the frame path is dead (2026-08-28 — broker alive, socket present,
     * zero frames delivered; every helper call hung to its curl budget and the node died on
     * the sandbox deadline). Without the pump the channel is a wedge, not a channel.
     */
    public boolean alive() {
        return !closed && process.isAlive() && pumpThread.isAlive();
    }

    /* ------------------------------------------------------------------ */
    /* Request pump: broker stdout -> router -> broker stdin              */
    /* ------------------------------------------------------------------ */

    private void pumpLoop() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while (!closed && (line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                // Chunked-frame reassembly must stay ordered, so it runs on the single pump
                // thread; only a completed request is handed to the dispatch pool. Normal
                // frames go straight to the pool (order between independent requests is moot).
                if (line.contains("\"body_chunks\"") || line.contains("\"body_part\"")) {
                    handleChunkedFrame(line);
                } else {
                    dispatchFrame(line);
                }
            }
        } catch (IOException e) {
            if (!closed) {
                log.warn("helper-pipe: pump loop ended unexpectedly: {} — helper channel is dead; "
                        + "dispatches on this container run without helper callbacks", e.toString());
            }
        }
        if (!closed) {
            // EOF with the channel still open means the broker's stdout went away on its own —
            // the single most invisible wedge there is (broker process can even stay alive).
            log.warn("helper-pipe: broker stdout closed while the channel was still open — helper "
                    + "channel is dead; dispatches on this container run without helper callbacks");
        } else {
            log.debug("helper-pipe: broker stdout closed; channel done.");
        }
    }

    /** Hands one raw frame line off to a fresh virtual thread — never parsed/executed on the pump thread. */
    private void dispatchFrame(String line) {
        try {
            dispatchPool.execute(() -> {
                // Fat-jar TCCL fix: a dispatch thread that ends up starting a sub-PI (studioquack.execute) needs
                // the app classloader as its context CL for jakarta.el's ExpressionFactory lookup. Builder-made
                // virtual threads start with the system classloader, so set it on the task itself.
                Thread.currentThread().setContextClassLoader(HelperPipeChannel.class.getClassLoader());
                handleFrame(line);
            });
        } catch (RejectedExecutionException rejected) {
            // Pool is shutting down (channel closing) — drop; nobody is waiting on this frame's id.
            log.debug("helper-pipe: dropped a frame during shutdown");
        }
    }

    private void handleFrame(String line) {
        String id = null;
        try {
            RequestFrame req = MAPPER.readValue(line, RequestFrame.class);
            id = req.id();
            routeRequest(req);
        } catch (Exception e) {
            log.warn("helper-pipe: malformed or failed request frame (id={}): {}", id, e.toString());
            if (id != null) {
                writeResponse(id, 500, "{\"error\":\"internal\"}".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Reassembles a chunked request on the pump thread (ordering is significant — the parts are
     * base64 slices that must concatenate in arrival order). The broker splits any request whose
     * body would push a frame near the 64 KiB exec-pipe capacity into a header frame
     * ({@code body_chunks=N}) followed by N {@code body_part} frames; a single larger write
     * short-writes through the conmon relay and loses the frame tail + newline.
     */
    private void handleChunkedFrame(String line) {
        JsonNode node;
        try {
            node = MAPPER.readTree(line);
        } catch (Exception parseFailure) {
            log.warn("helper-pipe: malformed chunked frame: {}", parseFailure.toString());
            return;
        }
        String id = node.path("id").asText(null);
        if (id == null || id.isEmpty()) {
            log.warn("helper-pipe: chunked frame without an id — dropped");
            return;
        }
        if (node.has("body_chunks")) {
            chunkedRequests.put(id, new ChunkedRequest(id,
                    node.path("method").asText(""), node.path("path").asText(""),
                    node.path("bearer").asText(""), node.path("accept").asText(null),
                    node.path("body_chunks").asInt(0)));
            return;
        }
        ChunkedRequest pending = chunkedRequests.get(id);
        if (pending == null) {
            log.warn("helper-pipe: body_part for unknown id={} — dropped", id);
            return;
        }
        pending.bodyB64.append(node.path("body_part").asText(""));
        pending.receivedChunks++;
        if (pending.expectedChunks > 0 && pending.receivedChunks >= pending.expectedChunks) {
            chunkedRequests.remove(id);
            log.info("helper-pipe: reassembled chunked request id={} path={} chunks={} bodyB64Chars={}",
                    id, pathWithoutQuery(pending.path), pending.expectedChunks, pending.bodyB64.length());
            dispatchAssembled(new RequestFrame(pending.id, pending.method, pending.path,
                    pending.bearer, pending.bodyB64.toString(), pending.accept));
        }
    }

    /** Hands a fully reassembled chunked request to a fresh virtual thread (same as a normal frame). */
    private void dispatchAssembled(RequestFrame req) {
        try {
            dispatchPool.execute(() -> {
                Thread.currentThread().setContextClassLoader(HelperPipeChannel.class.getClassLoader());
                routeRequest(req);
            });
        } catch (RejectedExecutionException rejected) {
            log.debug("helper-pipe: dropped a reassembled frame during shutdown");
        }
    }

    /** Routes one complete request frame (unary or streaming); never runs on the pump thread. */
    private void routeRequest(RequestFrame req) {
        String id = req.id();
        try {
            byte[] body = (req.body_b64() == null || req.body_b64().isEmpty())
                    ? new byte[0] : Base64.getDecoder().decode(req.body_b64());
            String path = pathWithoutQuery(req.path());
            // Frame-flow trace: the 2026-08-28 ai-agent hang had ZERO daemon-side helper trace, which made a
            // wedged broker pipe indistinguishable from a node that never called. "Log" and "emit-partial"
            // stay at DEBUG (per-token / chatty by design); execute, result, fail, and stream-open stay INFO
            // so a wedged unary still brackets in the log.
            boolean quiet = "log".equals(path) || "emit-partial".equals(path);
            if (isStreamingRequest(req.path(), body, req.accept())) {
                log.info("helper-pipe: stream request id={} path={}", id, path);
                router.routeStream(req.method(), req.path(), req.bearer(), body, req.accept(),
                        new PipeSink(id));
            } else {
                if (quiet) {
                    log.debug("helper-pipe: request id={} path={}", id, path);
                } else {
                    log.info("helper-pipe: request id={} path={} bodyBytes={}", id, path, body.length);
                }
                long t0 = System.nanoTime();
                HelperRequestRouter.RoutedResponse resp =
                        router.route(req.method(), req.path(), req.bearer(), body);
                writeResponse(id, resp.status(), resp.body());
                long ms = (System.nanoTime() - t0) / 1_000_000;
                if (quiet) {
                    log.debug("helper-pipe: response id={} path={} status={} ms={}", id, path, resp.status(), ms);
                } else {
                    log.info("helper-pipe: response id={} path={} status={} ms={}", id, path, resp.status(), ms);
                }
            }
        } catch (Exception e) {
            log.warn("helper-pipe: failed request frame (id={}): {}", id, e.toString());
            writeResponse(id, 500, "{\"error\":\"internal\"}".getBytes(StandardCharsets.UTF_8));
        }
    }

    /** Accumulates one chunked request's ordered base64 parts until all {@code body_chunks} arrive. */
    private static final class ChunkedRequest {
        private final String id;
        private final String method;
        private final String path;
        private final String bearer;
        private final String accept;
        private final int expectedChunks;
        private final StringBuilder bodyB64 = new StringBuilder();
        private int receivedChunks;

        private ChunkedRequest(String id, String method, String path, String bearer, String accept,
                               int expectedChunks) {
            this.id = id;
            this.method = method;
            this.path = path;
            this.bearer = bearer;
            this.accept = accept;
            this.expectedChunks = expectedChunks;
        }
    }

    /**
     * {@code execute/stream} (query ignored) always streams. {@code llm/*} streams only when Accept
     * lists {@code text/event-stream} and the JSON body has boolean {@code stream:true}.
     */
    static boolean isStreamingRequest(String path, byte[] body, String accept) {
        String p = pathWithoutQuery(path);
        if ("execute/stream".equals(p)) {
            return true;
        }
        if (!p.startsWith("llm/")) {
            return false;
        }
        if (!HelperSse.acceptsEventStream(accept)) {
            return false;
        }
        if (body == null || body.length == 0) {
            return false;
        }
        try {
            JsonNode tree = MAPPER.readTree(body);
            if (tree == null || !tree.isObject()) {
                return false;
            }
            JsonNode stream = tree.get("stream");
            return stream != null && stream.isBoolean() && stream.booleanValue();
        } catch (Exception parseFailure) {
            return false;
        }
    }

    private static String pathWithoutQuery(String path) {
        if (path == null || path.isEmpty()) {
            return "";
        }
        int q = path.indexOf('?');
        return q < 0 ? path : path.substring(0, q);
    }

    /** Legacy three-field frame. {@code contentType} is intentionally not on the wire. */
    private void writeResponse(String id, int status, byte[] body) {
        String b64 = Base64.getEncoder().encodeToString(body == null ? new byte[0] : body);
        writeLine(id, encodeFrame(new ResponseFrame(id, status, b64)));
    }

    private void writeStreamFrame(String id, int status, String contentType, byte[] chunk, int seq,
                                  boolean done) {
        byte[] bytes = chunk == null ? new byte[0] : chunk;
        String b64 = Base64.getEncoder().encodeToString(bytes);
        String ct = contentType == null ? "" : contentType;
        log.debug("helper-pipe: stream frame id={} status={} seq={} done={} bytes={}",
                id, status, seq, done, bytes.length);
        writeLine(id, encodeFrame(new StreamResponseFrame(id, status, ct, b64, seq, done)));
    }

    private String encodeFrame(Object frame) {
        try {
            return MAPPER.writeValueAsString(frame);
        } catch (Exception e) {
            log.debug("helper-pipe: failed to encode response frame: {}", e.toString());
            return null;
        }
    }

    private void writeLine(String id, String frameJson) {
        if (frameJson == null) {
            return;
        }
        synchronized (writeLock) {
            if (closed) {
                return;
            }
            try {
                stdin.write(frameJson);
                stdin.write('\n');
                stdin.flush();
            } catch (IOException e) {
                log.debug("helper-pipe: write failed (broker likely dead) id={}: {}", id, e.toString());
            }
        }
    }

    /** Turns router one-shot/chunk callbacks into pipe frames for a single request id. */
    private final class PipeSink implements HelperStreamSink {
        private final String id;

        private PipeSink(String id) {
            this.id = id;
        }

        @Override
        public void oneShot(int status, String contentType, byte[] body) {
            writeResponse(id, status, body);
        }

        @Override
        public void chunk(int status, String contentType, byte[] chunk, int seq, boolean done) {
            writeStreamFrame(id, status, contentType, chunk, seq, done);
        }
    }

    /** Drains the broker's stderr so its pipe buffer never fills (which would stall the broker). */
    private void drainStderr() {
        AtomicInteger logged = new AtomicInteger();
        try (BufferedReader err = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = err.readLine()) != null) {
                if (logged.incrementAndGet() <= MAX_STDERR_LOG_LINES) {
                    // The broker only writes to stderr when something is wrong (dropped frames,
                    // relay failures, pipe closed) — those are the wedge signals, so WARN.
                    log.warn("helper-pipe: broker stderr: {}", line);
                }
            }
        } catch (IOException ignore) {
            // stream closed on process exit — expected
        }
    }

    private static java.util.concurrent.ThreadFactory virtualThreadFactory() {
        return Thread.ofVirtual().name("helper-pipe-dispatch-", 0).factory();
    }

    /* ------------------------------------------------------------------ */
    /* Lifecycle                                                          */
    /* ------------------------------------------------------------------ */

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        dispatchPool.shutdown();
        try {
            stdin.close();
        } catch (IOException ignore) {
            // best-effort
        }
        process.destroy();
        pumpThread.interrupt();
        stderrDrainThread.interrupt();
    }
}
