package com.studioquack.sqs.sandbox.oci;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs {@code podman build} (or any argv) with streaming stdout/stderr. Must not use {@link
 * com.studioquack.sqs.sandbox.runtime.CliRunner}: that wrapper caps captured output at 256 KiB and
 * only returns after exit — local image builds can emit far more and need live chunks for the
 * control plane {@code build-progress} callback.
 */
public final class OciBuildLogStreamer {

    /**
     * Max characters per {@link ChunkSink#onChunk} — under the control plane's 64 KiB {@code logChunk}
     * cap so the progress reporter does not need a second truncation on the happy path.
     */
    public static final int CHUNK_CHARS = 32 * 1024;

    private OciBuildLogStreamer() {
    }

    /** Receives UTF-8 log fragments; may throw — {@link #runStreaming} catches and continues. */
    @FunctionalInterface
    public interface ChunkSink {
        void onChunk(String chunk) throws Exception;
    }

    /**
     * @param exitCode      process exit, or {@code -1} on spawn failure / timeout
     * @param timedOut      true when wall-clock expired and the process was killed
     * @param lastErrorLine last non-blank line observed (diagnostics for FAILED reports)
     */
    public record Outcome(int exitCode, boolean timedOut, String lastErrorLine) {
    }

    /** Run {@code argv} (full argv including CLI path). Merges stderr into stdout. */
    public static Outcome runStreaming(List<String> argv, Duration wallClock, ChunkSink sink) {
        if (argv == null || argv.isEmpty()) {
            return new Outcome(-1, false, "empty argv");
        }
        ProcessBuilder pb = new ProcessBuilder(argv);
        // Merge stderr into stdout so build diagnostics stream as one timeline.
        pb.redirectErrorStream(true);
        Process proc;
        try {
            proc = pb.start();
        } catch (IOException e) {
            return new Outcome(-1, false, "spawn failed: " + e.getMessage());
        }
        try {
            proc.getOutputStream().close();
        } catch (IOException ignore) {
            // best-effort — child may already have exited
        }

        StringBuilder pending = new StringBuilder();
        AtomicReference<String> lastLine = new AtomicReference<>();
        Object lock = new Object();
        // Daemon pump: blocking read so we do not spin on InputStream.ready(). Sink callbacks run outside
        // the lock so a slow the control plane POST cannot stall stdout drainage.
        Thread pump = new Thread(() -> {
            try (Reader reader = new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8)) {
                char[] buf = new char[8192];
                int n;
                while ((n = reader.read(buf)) != -1) {
                    List<String> toEmit;
                    synchronized (lock) {
                        pending.append(buf, 0, n);
                        toEmit = takeReady(pending, lastLine, false);
                    }
                    emitAll(toEmit, sink);
                }
            } catch (IOException ignore) {
                // stream closed on process exit / destroy — expected
            } finally {
                List<String> toEmit;
                synchronized (lock) {
                    toEmit = takeReady(pending, lastLine, true);
                }
                emitAll(toEmit, sink);
            }
        }, "oci-build-log-pump");
        pump.setDaemon(true);
        pump.start();

        boolean timedOut = false;
        int exit;
        try {
            boolean finished;
            if (wallClock == null || wallClock.isZero() || wallClock.isNegative()) {
                proc.waitFor();
                finished = true;
            } else {
                finished = proc.waitFor(wallClock.toMillis(), TimeUnit.MILLISECONDS);
            }
            if (!finished) {
                timedOut = true;
                proc.destroyForcibly();
                try {
                    proc.waitFor(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exit = -1;
            } else {
                exit = proc.exitValue();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            proc.destroyForcibly();
            joinPump(pump);
            return new Outcome(-1, false, lastLine.get());
        }

        joinPump(pump);
        return new Outcome(exit, timedOut, lastLine.get());
    }

    private static void joinPump(Thread pump) {
        try {
            pump.join(5_000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Package-visible for unit tests: same chunking as the live pump, but delivers via
     * {@code sink} immediately (no lock split).
     */
    static void flushReady(StringBuilder pending, ChunkSink sink, AtomicReference<String> lastLine,
                           boolean flushAll) {
        emitAll(takeReady(pending, lastLine, flushAll), sink);
    }

    /**
     * Pull complete lines (through last {@code \n}) while under {@link #CHUNK_CHARS}; when the
     * buffer hits the ceiling without a newline, force-cut {@link #CHUNK_CHARS} chars.
     *
     * @param flushAll when true (EOF), also take any trailing partial line
     */
    static List<String> takeReady(StringBuilder pending, AtomicReference<String> lastLine,
                                  boolean flushAll) {
        List<String> out = new ArrayList<>();
        if (pending == null || pending.isEmpty()) {
            return out;
        }
        while (true) {
            if (pending.length() >= CHUNK_CHARS) {
                int cut = pending.lastIndexOf("\n", CHUNK_CHARS - 1);
                int end = cut >= 0 ? cut + 1 : CHUNK_CHARS;
                String chunk = pending.substring(0, end);
                pending.delete(0, end);
                noteLastLine(chunk, lastLine);
                out.add(chunk);
                continue;
            }
            int nl = pending.lastIndexOf("\n");
            if (nl >= 0) {
                String chunk = pending.substring(0, nl + 1);
                pending.delete(0, nl + 1);
                noteLastLine(chunk, lastLine);
                out.add(chunk);
                continue;
            }
            break;
        }
        if (flushAll && !pending.isEmpty()) {
            String chunk = pending.toString();
            pending.setLength(0);
            noteLastLine(chunk, lastLine);
            out.add(chunk);
        }
        return out;
    }

    private static void noteLastLine(String chunk, AtomicReference<String> lastLine) {
        if (chunk == null || chunk.isEmpty() || lastLine == null) {
            return;
        }
        int from = 0;
        while (from < chunk.length()) {
            int nl = chunk.indexOf('\n', from);
            String line = nl < 0 ? chunk.substring(from) : chunk.substring(from, nl);
            if (!line.isBlank()) {
                lastLine.set(line.stripTrailing());
            }
            if (nl < 0) {
                break;
            }
            from = nl + 1;
        }
    }

    private static void emitAll(List<String> chunks, ChunkSink sink) {
        if (sink == null || chunks == null || chunks.isEmpty()) {
            return;
        }
        for (String chunk : chunks) {
            try {
                sink.onChunk(chunk);
            } catch (Exception ignore) {
                // Best-effort sink (e.g. progress POST) must not abort the build stream.
            }
        }
    }
}
