package com.studioquack.sqs.sandbox.helperchan;

import com.studioquack.sqs.sandbox.stateful.StatefulEndpointResolver;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Daemon-mediated TCP tunnel from a sandbox to a NAMED stateful instance — the daemon side of
 * {@code studioquack.connect(container_name, port)}. A sandbox container is hardened to reach the
 * public internet and nothing else: it must not have a route to a sibling container, and after its
 * only channel to the daemon is the broker pipe.
 */
@Slf4j
@Component
public class HelperConnectService {

    /** Read chunk ceiling — one {@code recv} never returns more than this many bytes. */
    private static final int MAX_CHUNK = 64 * 1024;
    /** Upper bound on a {@code recv} park, so a caller can never pin a dispatch thread indefinitely. */
    private static final long MAX_RECV_TIMEOUT_MS = 30_000L;
    private static final Duration REAP_INTERVAL = Duration.ofSeconds(60);

    /** Distinguishable failures, so the SDK can tell "no such container" from "it refused us". */
    public enum Failure {
        /** No such stateful container on this host. */
        NOT_FOUND,
        /** The container exists but this job may not reach it. */
        FORBIDDEN,
        /** Known and permitted, but not currently dialable. */
        UNAVAILABLE,
        /** This job already holds the maximum number of open streams. */
        LIMIT,
        /** The stream id is unknown, already closed, or belongs to another job. */
        CLOSED,
        /** The tunnel itself broke while moving bytes. */
        IO
    }

    /** A named failure carrying the code the router turns into a status. */
    public static class ConnectException extends RuntimeException {
        private final Failure failure;

        public ConnectException(Failure failure, String message) {
            super(message);
            this.failure = failure;
        }

        public Failure failure() {
            return failure;
        }
    }

    /** One open tunnel: the daemon-held socket plus the job it belongs to. */
    private static final class Stream implements AutoCloseable {
        private final String jobId;
        private final String target;
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;
        private final AtomicLong lastUsedMs = new AtomicLong(System.currentTimeMillis());
        private volatile boolean eof;

        private Stream(String jobId, String target, Socket socket) throws IOException {
            this.jobId = jobId;
            this.target = target;
            this.socket = socket;
            this.in = socket.getInputStream();
            this.out = socket.getOutputStream();
        }

        private void touch() {
            lastUsedMs.set(System.currentTimeMillis());
        }

        @Override
        public void close() {
            try {
                socket.close();
            } catch (IOException ignore) {
                // Best-effort: the peer may already be gone.
            }
        }
    }

    private final StatefulEndpointResolver resolver;
    private final int maxStreamsPerJob;
    private final Duration connectTimeout;
    private final Duration idleTimeout;
    private final Map<String, Stream> streams = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper;

    public HelperConnectService(
            StatefulEndpointResolver resolver,
            @Value("${sqs.sandbox.connect.max-streams-per-job:8}") int maxStreamsPerJob,
            @Value("${sqs.sandbox.connect.connect-timeout-ms:5000}") long connectTimeoutMs,
            @Value("${sqs.sandbox.connect.idle-timeout-seconds:600}") long idleTimeoutSeconds) {
        this.resolver = resolver;
        this.maxStreamsPerJob = Math.max(1, maxStreamsPerJob);
        this.connectTimeout = Duration.ofMillis(Math.max(250L, connectTimeoutMs));
        this.idleTimeout = Duration.ofSeconds(Math.max(30L, idleTimeoutSeconds));
        this.reaper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "helper-connect-reaper");
            t.setDaemon(true);
            return t;
        });
        // A script that exits without closing (or is killed mid-run) must not leak the socket. Job
        // revocation already closes streams; this catches the persistent-runner case where a job
        // legitimately lives for a long time but one of its tunnels went quiet.
        this.reaper.scheduleWithFixedDelay(this::reapIdle,
                REAP_INTERVAL.toSeconds(), REAP_INTERVAL.toSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Opens a tunnel to {@code containerName:port} on behalf of {@code job}.
     *
     * @return The stream id the sandbox uses for every subsequent byte op.
     * @throws ConnectException When the target does not exist, is not permitted, or cannot be dia.
     */
    public String open(HelperJob job, String containerName, int port) {
        if (port < 1 || port > 65535) {
            throw new ConnectException(Failure.FORBIDDEN, "port out of range: " + port);
        }
        StatefulEndpointResolver.Endpoint endpoint = resolver.resolve(containerName)
                .orElseThrow(() -> new ConnectException(Failure.NOT_FOUND,
                        "no stateful instance named '" + containerName + "' on this environment"));
        authorize(job, endpoint);
        if (!endpoint.running()) {
            throw new ConnectException(Failure.UNAVAILABLE,
                    "stateful instance '" + endpoint.containerName() + "' is not running");
        }
        if (endpoint.ip() == null) {
            throw new ConnectException(Failure.UNAVAILABLE,
                    "stateful instance '" + endpoint.containerName() + "' has no reachable address");
        }
        if (countFor(job.id()) >= maxStreamsPerJob) {
            throw new ConnectException(Failure.LIMIT,
                    "this node already holds " + maxStreamsPerJob + " open connections");
        }

        String target = endpoint.containerName() + ":" + port;
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(endpoint.ip(), port), (int) connectTimeout.toMillis());
            socket.setTcpNoDelay(true);
            String streamId = "cs_" + UUID.randomUUID();
            streams.put(streamId, new Stream(job.id(), target, socket));
            log.info("helper-connect: opened {} for job={} stream={} (openStreams={})",
                    target, job.id(), streamId, streams.size());
            return streamId;
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException ignore) {
                // nothing usable was established
            }
            // Naming the target is the whole diagnosis: the container was found and permitted, so the
            // remaining causes are "nothing is listening on that port" or "the app has not booted yet".
            throw new ConnectException(Failure.UNAVAILABLE,
                    "could not connect to " + target + " — " + e.getClass().getSimpleName()
                            + ": " + e.getMessage());
        }
    }

    /** Writes {@code data} to the tunnel. */
    public void send(HelperJob job, String streamId, byte[] data) {
        Stream s = require(job, streamId);
        if (data == null || data.length == 0) {
            return;
        }
        try {
            s.out.write(data);
            s.out.flush();
            s.touch();
        } catch (IOException e) {
            closeStream(streamId);
            throw new ConnectException(Failure.IO, "send failed on " + s.target + ": " + e.getMessage());
        }
    }

    /** One read chunk plus whether the peer has closed. */
    public record Chunk(byte[] data, boolean eof) {
    }

    /**
     * Reads up to one chunk, parking at most {@code timeoutMs} for the peer to produce bytes. A read
     * that times out is NOT an error — it is an idle connection, and the SDK simply asks again.
     */
    public Chunk recv(HelperJob job, String streamId, long timeoutMs) {
        Stream s = require(job, streamId);
        if (s.eof) {
            return new Chunk(new byte[0], true);
        }
        long bounded = Math.min(Math.max(timeoutMs, 1L), MAX_RECV_TIMEOUT_MS);
        byte[] buf = new byte[MAX_CHUNK];
        try {
            s.socket.setSoTimeout((int) bounded);
            int n = s.in.read(buf);
            s.touch();
            if (n < 0) {
                s.eof = true;
                return new Chunk(new byte[0], true);
            }
            byte[] out = new byte[n];
            System.arraycopy(buf, 0, out, 0, n);
            return new Chunk(out, false);
        } catch (SocketTimeoutException idle) {
            s.touch();
            return new Chunk(new byte[0], false);
        } catch (IOException e) {
            closeStream(streamId);
            throw new ConnectException(Failure.IO, "recv failed on " + s.target + ": " + e.getMessage());
        }
    }

    /** Closes one tunnel. Idempotent — closing an already-closed stream is not an error. */
    public void close(HelperJob job, String streamId) {
        Stream s = streamId == null ? null : streams.get(streamId);
        if (s != null && !s.jobId.equals(job.id())) {
            throw new ConnectException(Failure.CLOSED, "no such stream");
        }
        closeStream(streamId);
    }

    /**
     * Closes every tunnel a job holds. Called when the job's bearer is revoked, so a grant can never
     * outlive the script that asked for it.
     */
    public int closeAllForJob(String jobId) {
        if (jobId == null) {
            return 0;
        }
        int closed = 0;
        for (Map.Entry<String, Stream> e : streams.entrySet()) {
            if (jobId.equals(e.getValue().jobId)) {
                closeStream(e.getKey());
                closed++;
            }
        }
        if (closed > 0) {
            log.debug("helper-connect: closed {} stream(s) with job={}", closed, jobId);
        }
        return closed;
    }

    /** Open tunnel count (diagnostics/tests). */
    public int openStreams() {
        return streams.size();
    }

    private void authorize(HelperJob job, StatefulEndpointResolver.Endpoint endpoint) {
        if (!endpoint.stateful()) {
            // Reachable on the bridge but not ours: a user's own docker-compose container, an
            // unrelated tool. Naming a container is not a claim on it.
            throw new ConnectException(Failure.FORBIDDEN,
                    "'" + endpoint.containerName() + "' is not a StudioQuack stateful instance");
        }
        String callerEnv = Optional.ofNullable(job.executionContext().get("envId"))
                .map(String::valueOf).map(String::trim).filter(v -> !v.isEmpty()).orElse(null);
        String ownerEnv = endpoint.envId();
        if (callerEnv != null && ownerEnv != null && !callerEnv.equals(ownerEnv)) {
            throw new ConnectException(Failure.FORBIDDEN,
                    "'" + endpoint.containerName() + "' belongs to another environment");
        }
    }

    private Stream require(HelperJob job, String streamId) {
        Stream s = streamId == null ? null : streams.get(streamId);
        if (s == null || !s.jobId.equals(job.id())) {
            throw new ConnectException(Failure.CLOSED, "no such stream");
        }
        return s;
    }

    private int countFor(String jobId) {
        int n = 0;
        for (Stream s : streams.values()) {
            if (jobId.equals(s.jobId)) {
                n++;
            }
        }
        return n;
    }

    private void closeStream(String streamId) {
        Stream s = streamId == null ? null : streams.remove(streamId);
        if (s != null) {
            s.close();
        }
    }

    private void reapIdle() {
        long cutoff = System.currentTimeMillis() - idleTimeout.toMillis();
        for (Map.Entry<String, Stream> e : streams.entrySet()) {
            if (e.getValue().lastUsedMs.get() < cutoff) {
                log.info("helper-connect: reaping idle stream={} target={} (idle > {})",
                        e.getKey(), e.getValue().target, idleTimeout);
                closeStream(e.getKey());
            }
        }
    }

    @PreDestroy
    void shutdown() {
        reaper.shutdownNow();
        for (String id : streams.keySet()) {
            closeStream(id);
        }
    }
}
