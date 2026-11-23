package com.studioquack.sqs.sandbox.stateful.shell;

import com.studioquack.sqs.command.BespokeFilePaths;
import com.studioquack.sqs.sandbox.oci.ShExec;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Live {@code podman exec -i &lt;container&gt; /bin/sh} session registry. This is a pipe REPL, not
 * a PTY: stdin/stdout are pipes ({@code podman exec -i}, never {@code -it}).
 */
@Slf4j
@Component
public class StatefulShellSessionManager {

    /** Daemon-wide concurrent shell sessions. */
    public static final int MAX_SESSIONS = 5;

    /** Fixed reader pool size: 2 streams × {@link #MAX_SESSIONS} sessions. */
    public static final int READER_POOL_SIZE = 10;

    public static final String SHELL = "/bin/sh";

    public static final String REASON_CLOSED_BY_USER = "closed-by-user";
    public static final String REASON_IDLE_TIMEOUT = "idle-timeout";
    public static final String REASON_HARD_CAP = "hard-cap";
    public static final String REASON_INSTANCE_STOPPED = "instance-stopped";
    public static final String REASON_DAEMON_ERROR = "daemon-error";

    /** Marker prefix printed by the post-command sentinel (stdout); never shown to the user. */
    static final String CMD_DONE_MARKER = "__SQ_CMD_DONE__";

    /** Per-command cap on user-visible stdout+stderr bytes emitted to the FE. */
    static final int MAX_BYTES_PER_COMMAND = 256 * 1024;

    /** Mutable so unit tests can use a tiny cap without draining 256&nbsp;KiB through the reader. */
    private final AtomicInteger maxBytesPerCommand = new AtomicInteger(MAX_BYTES_PER_COMMAND);

    static final Duration IDLE_TIMEOUT = Duration.ofMinutes(10);
    static final Duration HARD_CAP = Duration.ofMinutes(60);
    static final Duration BATCH_WINDOW = Duration.ofMillis(50);
    static final int BATCH_BYTES = 8 * 1024;
    static final int MAX_CHUNK_CHARS = 16_384;
    static final Duration REAPER_PERIOD = Duration.ofSeconds(30);

    private static final byte CTRL_C = 0x03;
    private static final byte CTRL_D = 0x04;

    /**
     * Shell probe written after each complete user line. Separate command so {@code $?} is
     * the user command's status; prints {@code __SQ_CMD_DONE__&lt;code&gt;} then newline.
     */
    private static final byte[] SENTINEL_SHELL_CMD =
            ("printf '%s\\n' \"__SQ_CMD_DONE__" + "$?" + "\"\n").getBytes(StandardCharsets.UTF_8);

    /** Visible when per-command emission hits {@link #MAX_BYTES_PER_COMMAND}. */
    static final String TRUNCATION_NOTICE =
            "[studioquack] output truncated at 256 KiB for this command — further output dropped";

    private static final Set<String> TTY_REQUIRED_COMMANDS = Set.of(
            "vim", "vi", "nvim", "nano", "emacs", "emacsclient",
            "top", "htop", "less", "more", "most", "watch",
            "tmux", "screen", "byobu", "mc", "lazygit", "gitui");

    private static final Pattern ENV_ASSIGNMENT = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*=.*");

    /** Starts the {@code podman exec} process; package-visible hook for tests. */
    @FunctionalInterface
    public interface ProcessFactory {
        Process start(List<String> argv) throws IOException;
    }

    /** Outcome of {@link #open}. */
    public record OpenResult(boolean ok, String sessionId, String shell, String error) {
        public static OpenResult success(String sessionId) {
            return new OpenResult(true, sessionId, SHELL, null);
        }

        public static OpenResult fail(String error) {
            return new OpenResult(false, null, null, error);
        }
    }

    private final Redactor redactor;
    private final ShellOutputCallback callback;
    private final ProcessFactory processFactory;

    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    /** Serialises open admit (cap + duplicate sessionId) before the process is started. */
    private final Object openLock = new Object();
    private final ExecutorService readerPool;
    /** One thread for interrupt side-execs — must not block readers or the reaper. */
    private final ExecutorService sideExecPool;
    private final ScheduledExecutorService reaper;

    // Two constructors exist, so Spring cannot infer which to use and fails with "No default
    // constructor found" at startup. @Autowired pins the provider-driven one; the explicit
    // ProcessFactory overload stays for tests.
    @Autowired
    public StatefulShellSessionManager(Redactor redactor,
                                       ObjectProvider<ShellOutputCallback> callbackProvider) {
        this(redactor,
                callbackProvider.getIfAvailable(() -> (envId, sessionId, frame) -> { }),
                defaultProcessFactory());
    }

    /** Test / explicit-wiring constructor (public so command-package tests can wire a fake ProcessFactory). */
    public StatefulShellSessionManager(Redactor redactor,
                                       ShellOutputCallback callback,
                                       ProcessFactory processFactory) {
        this.redactor = Objects.requireNonNull(redactor, "redactor");
        this.callback = callback == null
                ? (envId, sessionId, frame) -> { }
                : callback;
        this.processFactory = processFactory == null ? defaultProcessFactory() : processFactory;
        this.readerPool = Executors.newFixedThreadPool(READER_POOL_SIZE, daemonFactory("stateful-shell-reader-"));
        this.sideExecPool = Executors.newSingleThreadExecutor(daemonFactory("stateful-shell-side-"));
        this.reaper = Executors.newSingleThreadScheduledExecutor(daemonFactory("stateful-shell-reaper-"));
        this.reaper.scheduleAtFixedRate(this::reapExpired, REAPER_PERIOD.toMillis(),
                REAPER_PERIOD.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static ProcessFactory defaultProcessFactory() {
        return argv -> new ProcessBuilder(argv)
                .redirectErrorStream(false)
                .start();
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Open a shell session bound to the control plane-minted {@code sessionId}. {@code cols}/{@code
     * rows} are accepted for wire compatibility (pipe shell, not a PTY) and currently unused.
     */
    public OpenResult open(String envId, String instanceId, String sessionId,
                           int cols, int rows, StatefulInstance inst, OciCli cli) {
        if (inst == null) {
            return OpenResult.fail("stateful-shell-open: instance is null");
        }
        if (sessionId == null || sessionId.isBlank()) {
            return OpenResult.fail("stateful-shell-open: sessionId must not be blank");
        }
        if (cli == null || cli.cliPath() == null || cli.cliPath().isBlank()) {
            return OpenResult.fail("stateful-shell-open: OCI CLI unavailable");
        }
        if (!inst.isRunning()) {
            return OpenResult.fail("stateful-shell-open: instance is not RUNNING");
        }
        if (isPlatformManaged(inst)) {
            return OpenResult.fail("stateful-shell-open: platform-managed instance refused");
        }

        String resolvedEnv = envId == null || envId.isBlank() ? inst.envId() : envId;
        String resolvedInstance = instanceId == null || instanceId.isBlank()
                ? inst.instanceId() : instanceId;

        // Record the container-side shell PID so Ctrl-C can SIGINT its children (pipe ≠ TTY).
        String pidFile = pidFileFor(sessionId);
        String wrapper = "echo $$ > '" + pidFile + "'; export TERM=dumb; PS1=; PS2=; exec " + SHELL;
        List<String> argv = List.of(
                cli.cliPath(), "exec", "-i", inst.containerName(), SHELL, "-c", wrapper);

        Session session;
        synchronized (openLock) {
            if (sessions.size() >= MAX_SESSIONS) {
                return OpenResult.fail("stateful-shell-open: concurrent session cap ("
                        + MAX_SESSIONS + ") reached");
            }
            if (sessions.containsKey(sessionId)) {
                return OpenResult.fail("stateful-shell-open: sessionId already bound");
            }
            Process process;
            try {
                process = processFactory.start(argv);
            } catch (Exception ex) {
                log.warn("stateful-shell-open: failed to start process sessionId={}: {}",
                        sessionId, ex.toString());
                return OpenResult.fail("stateful-shell-open: failed to start shell: " + message(ex));
            }
            session = new Session(sessionId, resolvedEnv, resolvedInstance, process,
                    cli.cliPath(), inst.containerName(), pidFile);
            sessions.put(sessionId, session);
        }

        // Exactly two reader tasks per session (stdout + stderr). Process-exit is observed
        // by the reaper / stream EOF — never a third blocked waitFor on this bounded pool.
        readerPool.execute(() -> drainStream(session, session.process.getInputStream(), "stdout"));
        readerPool.execute(() -> drainStream(session, session.process.getErrorStream(), "stderr"));

        return OpenResult.success(sessionId);
    }

    /**
     * Write UTF-8 bytes of {@code data} to the session's stdin. Complete lines (ending in {@code \\n})
     * get a command-done sentinel appended so the FE learns when the command finished.
     *
     * @throws IllegalStateException If {@code sessionId} is unknown or the session is closed.
     */
    public void writeInput(String sessionId, String data) {
        Session session = sessions.get(sessionId);
        if (session == null || session.closed.get()) {
            throw new IllegalStateException("stateful-shell-input: unknown sessionId");
        }
        String payload = data == null ? "" : data;
        if (payload.isEmpty()) {
            return;
        }

        // Single-byte control keys from the FE — no sentinel probe.
        if (payload.length() == 1 && payload.charAt(0) == CTRL_C) {
            interruptRunningCommand(session);
            return;
        }
        if (payload.length() == 1 && payload.charAt(0) == CTRL_D) {
            writeRaw(session, new byte[]{CTRL_D});
            return;
        }

        if (payload.endsWith("\n")) {
            String line = payload.substring(0, payload.length() - 1);
            if (line.endsWith("\r")) {
                line = line.substring(0, line.length() - 1);
            }
            String denied = ttyDeniedCommand(line);
            if (denied != null) {
                emitSystem(session, "[studioquack] '" + denied
                        + "' needs a full TTY. This terminal is a pipe shell — interactive "
                        + "full-screen tools are not supported. Use non-interactive commands instead.");
                emitSystem(session, CMD_DONE_MARKER + ":0");
                session.touch();
                return;
            }
            byte[] userLine = (line + "\n").getBytes(StandardCharsets.UTF_8);
            synchronized (session.stdinLock) {
                try {
                    OutputStream in = session.process.getOutputStream();
                    in.write(userLine);
                    in.write(SENTINEL_SHELL_CMD);
                    in.flush();
                    session.touch();
                } catch (IOException ex) {
                    closeInternal(session, REASON_DAEMON_ERROR);
                    throw new IllegalStateException("stateful-shell-input: write failed: " + message(ex), ex);
                }
            }
            return;
        }

        // Partial chunk without newline — compat write, no sentinel.
        writeRaw(session, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Destroy the process, stop readers, emit exactly one {@code closed} frame, forget the session.
     *
     * @throws IllegalStateException if {@code sessionId} is unknown (already closed/forgotten)
     */
    public void close(String sessionId, String reason) {
        Session session = sessions.get(sessionId);
        if (session == null) {
            throw new IllegalStateException("stateful-shell-close: unknown sessionId");
        }
        String r = reason == null || reason.isBlank() ? REASON_CLOSED_BY_USER : reason;
        closeInternal(session, r);
    }

    /** Close every live session for {@code instanceId} (default reason {@code instance-stopped}). */
    public void destroySessionsForInstance(String instanceId) {
        destroySessionsForInstance(instanceId, REASON_INSTANCE_STOPPED);
    }

    public void destroySessionsForInstance(String instanceId, String reason) {
        if (instanceId == null || instanceId.isBlank()) {
            return;
        }
        String r = reason == null || reason.isBlank() ? REASON_INSTANCE_STOPPED : reason;
        List<Session> victims = new ArrayList<>();
        for (Session s : sessions.values()) {
            if (instanceId.equals(s.instanceId)) {
                victims.add(s);
            }
        }
        for (Session s : victims) {
            closeInternal(s, r);
        }
    }

    /** Package-visible for tests. */
    int liveSessionCount() {
        return sessions.size();
    }

    /** Package-visible for tests. */
    boolean hasSession(String sessionId) {
        return sessions.containsKey(sessionId);
    }

    /** Package-visible for tests — restores production default when {@code bytes <= 0}. */
    void setMaxBytesPerCommandForTest(int bytes) {
        maxBytesPerCommand.set(bytes <= 0 ? MAX_BYTES_PER_COMMAND : bytes);
    }

    @PreDestroy
    public void shutdown() {
        List<Session> all = new ArrayList<>(sessions.values());
        for (Session s : all) {
            closeInternal(s, REASON_DAEMON_ERROR);
        }
        reaper.shutdownNow();
        sideExecPool.shutdownNow();
        readerPool.shutdownNow();
    }

    /**
     * Pipe shells do not deliver TTY SIGINT. Write {@code 0x03}, SIGINT direct children of
     * the recorded shell PID inside the container, unblock the FE with {@code :130}.
     */
    private void interruptRunningCommand(Session session) {
        writeRaw(session, new byte[]{CTRL_C});
        emitSystem(session, "[studioquack] interrupt: SIGINT sent to the running command");
        emitSystem(session, CMD_DONE_MARKER + ":130");
        session.touch();
        // Side exec on a dedicated thread — must not occupy readers or the reaper.
        sideExecPool.execute(() -> signalShellChildren(session));
    }

    private void signalShellChildren(Session session) {
        if (session.closed.get() || session.cliPath == null || session.containerName == null) {
            return;
        }
        // Portable: walk /proc and SIGINT every process whose ppid is our shell (no pkill required).
        String script = "spid=$(cat '" + session.pidFile + "' 2>/dev/null) || exit 0; "
                + "[ -n \"$spid\" ] || exit 0; "
                + "for c in /proc/[0-9]*; do "
                + "ppid=$(awk '{print $4}' \"$c/stat\" 2>/dev/null) || continue; "
                + "[ \"$ppid\" = \"$spid\" ] || continue; "
                + "kill -INT \"${c#/proc/}\" 2>/dev/null || true; "
                + "done";
        // The script bears double quotes, so it travels as a single-quote-only base64 plant-and-run
        // wrapper: a bare sh -c argv element would be shattered by the Win32 command-line round-trip on
        // Windows hosts.
        List<String> argv = List.of(
                session.cliPath, "exec", session.containerName, SHELL, "-c",
                ShExec.plantAndRun(script, "sq_sigint"));
        Process side = null;
        try {
            side = processFactory.start(argv);
            if (!side.waitFor(3, TimeUnit.SECONDS)) {
                side.destroyForcibly();
            }
        } catch (Exception ex) {
            log.debug("stateful-shell interrupt side-exec failed sessionId={}: {}",
                    session.sessionId, ex.toString());
        } finally {
            if (side != null && side.isAlive()) {
                side.destroyForcibly();
            }
        }
    }

    /** Sanitize sessionId for a container tmp path (the control plane UUIDs are already safe). */
    static String pidFileFor(String sessionId) {
        String safe = sessionId == null ? "unknown" : sessionId.replaceAll("[^A-Za-z0-9._-]", "_");
        return "/tmp/.sq-shell-" + safe + ".pid";
    }

    private void writeRaw(Session session, byte[] bytes) {
        synchronized (session.stdinLock) {
            try {
                OutputStream in = session.process.getOutputStream();
                in.write(bytes);
                in.flush();
                session.touch();
            } catch (IOException ex) {
                closeInternal(session, REASON_DAEMON_ERROR);
                throw new IllegalStateException("stateful-shell-input: write failed: " + message(ex), ex);
            }
        }
    }

    private void drainStream(Session session, InputStream stream, String streamName) {
        if (stream == null) {
            return;
        }
        byte[] buf = new byte[BATCH_BYTES];
        try {
            while (!session.closed.get()) {
                int first = stream.read(buf, 0, buf.length);
                if (first < 0) {
                    break;
                }
                session.touch();
                int total = first;
                long deadline = System.nanoTime() + BATCH_WINDOW.toNanos();
                while (total < BATCH_BYTES && System.nanoTime() < deadline && !session.closed.get()) {
                    int avail = stream.available();
                    if (avail <= 0) {
                        long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                        if (remainingMs <= 0) {
                            break;
                        }
                        try {
                            Thread.sleep(Math.min(5L, remainingMs));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        continue;
                    }
                    int n = stream.read(buf, total, Math.min(avail, BATCH_BYTES - total));
                    if (n < 0) {
                        break;
                    }
                    total += n;
                    session.touch();
                }
                String raw = new String(buf, 0, total, StandardCharsets.UTF_8);
                if ("stdout".equals(streamName)) {
                    processStdoutBatch(session, raw);
                } else {
                    emitUserOutput(session, streamName, raw);
                }
            }
        } catch (IOException ex) {
            if (!session.closed.get()) {
                log.debug("stateful-shell reader {} ended sessionId={}: {}",
                        streamName, session.sessionId, ex.toString());
            }
        } finally {
            if ("stdout".equals(streamName) && session.stdoutCarry.length() > 0 && !session.closed.get()) {
                // EOF: treat a trailing marker without newline as complete.
                processStdoutBatch(session, "");
                if (session.stdoutCarry.length() > 0) {
                    flushStdoutCarryAtEof(session);
                }
            }
            // Stream EOF usually means the exec process exited; emit the terminal frame
            // promptly rather than waiting for the next reaper tick.
            if (!session.process.isAlive()) {
                closeInternal(session, REASON_CLOSED_BY_USER);
            }
        }
    }

    /**
     * Strip {@code __SQ_CMD_DONE__&lt;code&gt;} markers from stdout (with a small carry buffer
     * so the marker can span batches); emit preceding text as stdout and the done event as system.
     */
    private void processStdoutBatch(Session session, String raw) {
        if (raw == null) {
            raw = "";
        }
        String combined = session.stdoutCarry.length() == 0 ? raw : session.stdoutCarry + raw;
        session.stdoutCarry.setLength(0);
        if (combined.isEmpty()) {
            return;
        }

        int pos = 0;
        while (pos < combined.length()) {
            int idx = combined.indexOf(CMD_DONE_MARKER, pos);
            if (idx < 0) {
                int keep = markerPrefixLen(combined, pos);
                if (keep < combined.length() - pos) {
                    emitUserOutput(session, "stdout", combined.substring(pos, combined.length() - keep));
                }
                if (keep > 0) {
                    session.stdoutCarry.append(combined, combined.length() - keep, combined.length());
                }
                return;
            }
            if (idx > pos) {
                emitUserOutput(session, "stdout", combined.substring(pos, idx));
            }
            int digitStart = idx + CMD_DONE_MARKER.length();
            int i = digitStart;
            while (i < combined.length() && combined.charAt(i) >= '0' && combined.charAt(i) <= '9') {
                i++;
            }
            if (i == combined.length()) {
                // Marker + optional digits at end of batch — wait for newline / more data.
                session.stdoutCarry.append(combined, idx, combined.length());
                return;
            }
            if (combined.charAt(i) == '\n') {
                String code = i == digitStart ? "0" : combined.substring(digitStart, i);
                onCommandDone(session, code);
                pos = i + 1;
                continue;
            }
            // Digits ended without newline (non-digit follows) — treat as complete marker.
            if (i > digitStart) {
                onCommandDone(session, combined.substring(digitStart, i));
                pos = i;
                continue;
            }
            // Bare marker prefix followed by non-digit — emit literally and continue.
            emitUserOutput(session, "stdout", CMD_DONE_MARKER);
            pos = digitStart;
        }
    }

    private void flushStdoutCarryAtEof(Session session) {
        String carry = session.stdoutCarry.toString();
        session.stdoutCarry.setLength(0);
        int idx = carry.indexOf(CMD_DONE_MARKER);
        if (idx < 0) {
            emitUserOutput(session, "stdout", carry);
            return;
        }
        if (idx > 0) {
            emitUserOutput(session, "stdout", carry.substring(0, idx));
        }
        int digitStart = idx + CMD_DONE_MARKER.length();
        int i = digitStart;
        while (i < carry.length() && carry.charAt(i) >= '0' && carry.charAt(i) <= '9') {
            i++;
        }
        String code = i == digitStart ? "0" : carry.substring(digitStart, i);
        onCommandDone(session, code);
        if (i < carry.length()) {
            if (carry.charAt(i) == '\n') {
                i++;
            }
            if (i < carry.length()) {
                emitUserOutput(session, "stdout", carry.substring(i));
            }
        }
    }

    private void onCommandDone(Session session, String code) {
        session.bytesSinceCmdDone.set(0);
        session.outputSuppressed.set(false);
        session.truncationNotified.set(false);
        emitSystem(session, CMD_DONE_MARKER + ":" + (code == null || code.isEmpty() ? "0" : code));
    }

    /** Longest suffix of {@code s[from..)} that is a proper prefix of {@link #CMD_DONE_MARKER}. */
    private static int markerPrefixLen(String s, int from) {
        int len = s.length() - from;
        int max = Math.min(CMD_DONE_MARKER.length() - 1, len);
        for (int keep = max; keep > 0; keep--) {
            if (CMD_DONE_MARKER.startsWith(s.substring(s.length() - keep))) {
                return keep;
            }
        }
        return 0;
    }

    private void emitUserOutput(Session session, String streamName, String raw) {
        if (raw == null || raw.isEmpty() || session.closed.get()) {
            return;
        }
        if (session.outputSuppressed.get()) {
            return;
        }
        String redacted = redactor.redact(raw);
        if (redacted == null || redacted.isEmpty()) {
            return;
        }
        int cap = maxBytesPerCommand.get();
        long prev = session.bytesSinceCmdDone.get();
        if (prev >= cap) {
            suppressOutput(session);
            return;
        }
        int allowed = (int) Math.min(redacted.length(), cap - prev);
        if (allowed <= 0) {
            suppressOutput(session);
            return;
        }
        String toEmit = allowed < redacted.length() ? redacted.substring(0, allowed) : redacted;
        session.bytesSinceCmdDone.addAndGet(toEmit.length());
        emitChunked(session, streamName, toEmit);
        if (allowed < redacted.length() || session.bytesSinceCmdDone.get() >= cap) {
            suppressOutput(session);
        }
    }

    private void suppressOutput(Session session) {
        if (session.outputSuppressed.compareAndSet(false, true)
                && session.truncationNotified.compareAndSet(false, true)) {
            emitSystem(session, TRUNCATION_NOTICE);
        } else {
            session.outputSuppressed.set(true);
        }
    }

    private void emitSystem(Session session, String data) {
        if (data == null || data.isEmpty() || session.closed.get()) {
            return;
        }
        String redacted = redactor.redact(data);
        if (redacted == null || redacted.isEmpty()) {
            return;
        }
        emitChunked(session, "system", redacted);
    }

    private void emitChunked(Session session, String streamName, String text) {
        int offset = 0;
        while (offset < text.length()) {
            int end = Math.min(offset + MAX_CHUNK_CHARS, text.length());
            String chunk = text.substring(offset, end);
            offset = end;
            long seq = session.seq.incrementAndGet();
            Map<String, Object> frame = new LinkedHashMap<>();
            frame.put("sessionId", session.sessionId);
            frame.put("seq", seq);
            frame.put("stream", streamName);
            frame.put("data", chunk);
            frame.put("event", "output");
            safePost(session.envId, session.sessionId, frame);
        }
    }

    private void closeInternal(Session session, String reason) {
        if (!session.closed.compareAndSet(false, true)) {
            sessions.remove(session.sessionId, session);
            return;
        }
        try {
            session.process.destroy();
            session.process.destroyForcibly();
        } catch (Exception ignored) {
            // best-effort
        }
        try {
            session.process.getOutputStream().close();
        } catch (Exception ignored) {
            // best-effort
        }
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("sessionId", session.sessionId);
        frame.put("event", "closed");
        frame.put("reason", reason);
        safePost(session.envId, session.sessionId, frame);
        sessions.remove(session.sessionId, session);
    }

    private void safePost(String envId, String sessionId, Map<String, Object> frame) {
        try {
            callback.postOutput(envId, sessionId, frame);
        } catch (Exception ex) {
            log.warn("stateful-shell callback failed sessionId={}: {}", sessionId, ex.toString());
        }
    }

    private void reapExpired() {
        Instant now = Instant.now();
        for (Session s : sessions.values()) {
            if (s.closed.get()) {
                continue;
            }
            // Natural shell exit (e.g. user typed `exit`) — closed-by-user.
            if (!s.process.isAlive()) {
                closeInternal(s, REASON_CLOSED_BY_USER);
            } else if (Duration.between(s.openedAt, now).compareTo(HARD_CAP) >= 0) {
                closeInternal(s, REASON_HARD_CAP);
            } else if (Duration.between(s.lastActivityAt, now).compareTo(IDLE_TIMEOUT) >= 0) {
                closeInternal(s, REASON_IDLE_TIMEOUT);
            }
        }
    }

    /**
     * First whitespace token (after optional {@code env VAR=val} prefixes); basename matched
     * case-sensitively against {@link #TTY_REQUIRED_COMMANDS}. Returns the denied command
     * name, or null if the line is allowed.
     */
    static String ttyDeniedCommand(String line) {
        if (line == null) {
            return null;
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        String[] tokens = trimmed.split("\\s+");
        int i = 0;
        if (i < tokens.length && "env".equals(tokens[i])) {
            i++;
        }
        while (i < tokens.length && ENV_ASSIGNMENT.matcher(tokens[i]).matches()) {
            i++;
        }
        if (i >= tokens.length) {
            return null;
        }
        String token = tokens[i];
        if (token.startsWith("-")) {
            return null;
        }
        String base = basename(token);
        if (TTY_REQUIRED_COMMANDS.contains(base)) {
            return base;
        }
        return null;
    }

    private static String basename(String path) {
        int slash = path.lastIndexOf('/');
        String name = slash >= 0 ? path.substring(slash + 1) : path;
        if (name.startsWith("./")) {
            name = name.substring(2);
        }
        return name;
    }

    /**
     * Platform-managed Docling: instanceId {@code docling} (ignore case) or imageRef whose
     * lowercase form contains {@code docling-serve}. Delegates to {@link BespokeFilePaths}
     * which also normalises {@code /} → {@code -} for ref matching.
     */
    static boolean isPlatformManaged(StatefulInstance inst) {
        if (BespokeFilePaths.isPlatformManaged(inst)) {
            return true;
        }
        // Brief also allows a plain lowercase substring check without normalisation.
        if (inst == null) {
            return false;
        }
        String id = inst.instanceId();
        if (id != null && "docling".equalsIgnoreCase(id.trim())) {
            return true;
        }
        String ref = inst.imageRef();
        return ref != null && ref.toLowerCase(Locale.ROOT).contains("docling-serve");
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /** One live shell binding. */
    private static final class Session {
        final String sessionId;
        final String envId;
        final String instanceId;
        final Process process;
        final String cliPath;
        final String containerName;
        /** Container path written at shell start ({@code echo $$ > …}). */
        final String pidFile;
        final Instant openedAt;
        final AtomicLong seq = new AtomicLong();
        final AtomicBoolean closed = new AtomicBoolean(false);
        final Object stdinLock = new Object();
        /** Carry for stdout marker that may span BATCH_WINDOW batches. */
        final StringBuilder stdoutCarry = new StringBuilder();
        final AtomicLong bytesSinceCmdDone = new AtomicLong();
        final AtomicBoolean outputSuppressed = new AtomicBoolean(false);
        final AtomicBoolean truncationNotified = new AtomicBoolean(false);
        volatile Instant lastActivityAt;

        Session(String sessionId, String envId, String instanceId, Process process,
                String cliPath, String containerName, String pidFile) {
            this.sessionId = sessionId;
            this.envId = envId;
            this.instanceId = instanceId;
            this.process = process;
            this.cliPath = cliPath;
            this.containerName = containerName;
            this.pidFile = pidFile;
            this.openedAt = Instant.now();
            this.lastActivityAt = this.openedAt;
        }

        void touch() {
            lastActivityAt = Instant.now();
        }
    }
}
