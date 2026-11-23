package com.studioquack.sqs.sandbox.stateful.shell;

import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager.OpenResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deny-path + registry unit tests for {@link StatefulShellSessionManager}. Uses a {@link
 * StatefulShellSessionManager.ProcessFactory} that returns a fake Process with piped streams — no
 * real podman.
 */
class StatefulShellSessionManagerTest {

    private final FakeOciCli cli = new FakeOciCli();
    private final List<Map<String, Object>> frames = new CopyOnWriteArrayList<>();
    private final List<List<String>> startedArgv = Collections.synchronizedList(new ArrayList<>());

    private StatefulShellSessionManager manager;
    private FakeShellProcess lastProcess;

    @BeforeEach
    void setUp() {
        frames.clear();
        startedArgv.clear();
        lastProcess = null;
        ShellOutputCallback callback = (envId, sessionId, frame) -> frames.add(Map.copyOf(frame));
        manager = new StatefulShellSessionManager(new Redactor(), callback, argv -> {
            startedArgv.add(List.copyOf(argv));
            FakeShellProcess proc = new FakeShellProcess();
            // Interactive shell uses -i; interrupt side-exec must not overwrite lastProcess.
            if (argv.contains("-i")) {
                lastProcess = proc;
            }
            return proc;
        });
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.shutdown();
        }
    }

    @Test
    void open_refuses_when_session_cap_reached() {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        for (int i = 0; i < StatefulShellSessionManager.MAX_SESSIONS; i++) {
            OpenResult ok = manager.open("env-1", "inst-1", "sid-" + i, 80, 24, inst, cli);
            assertTrue(ok.ok(), "open " + i + " should succeed: " + ok.error());
        }
        OpenResult sixth = manager.open("env-1", "inst-1", "sid-6", 80, 24, inst, cli);
        assertFalse(sixth.ok());
        assertTrue(sixth.error().contains("cap"), sixth.error());
        assertEquals(StatefulShellSessionManager.MAX_SESSIONS, manager.liveSessionCount());
    }

    @Test
    void open_refuses_duplicate_sessionId() {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = UUID.randomUUID().toString();
        assertTrue(manager.open("env-1", "inst-1", sid, 80, 24, inst, cli).ok());
        OpenResult dup = manager.open("env-1", "inst-1", sid, 80, 24, inst, cli);
        assertFalse(dup.ok());
        assertTrue(dup.error().contains("already bound"), dup.error());
    }

    @Test
    void open_refuses_non_running_instance() {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        inst.setStatus(StatefulStatus.STOPPED);
        OpenResult r = manager.open("env-1", "inst-1", "sid-x", 80, 24, inst, cli);
        assertFalse(r.ok());
        assertTrue(r.error().contains("not RUNNING"), r.error());
        assertEquals(0, startedArgv.size(), "process must not start for deny path");
    }

    @Test
    void open_refuses_platform_managed_by_instanceId() {
        StatefulInstance inst = runningInstance("docling", "quay.io/other/app:1");
        OpenResult r = manager.open("env-1", "docling", "sid-x", 80, 24, inst, cli);
        assertFalse(r.ok());
        assertTrue(r.error().contains("platform-managed"), r.error());
        assertEquals(0, startedArgv.size());
    }

    @Test
    void open_refuses_platform_managed_by_imageRef() {
        StatefulInstance inst = runningInstance("inst-doc", "ghcr.io/org/docling-serve:latest");
        OpenResult r = manager.open("env-1", "inst-doc", "sid-x", 80, 24, inst, cli);
        assertFalse(r.ok());
        assertTrue(r.error().contains("platform-managed"), r.error());
        assertEquals(0, startedArgv.size());
    }

    @Test
    void writeInput_unknown_session_throws() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> manager.writeInput("missing", "echo hi\n"));
        assertTrue(ex.getMessage().contains("unknown sessionId"));
    }

    @Test
    void close_unknown_session_throws() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> manager.close("missing", StatefulShellSessionManager.REASON_CLOSED_BY_USER));
        assertTrue(ex.getMessage().contains("unknown sessionId"));
    }

    @Test
    void open_binds_given_sessionId_and_argv_shape() {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = "be-minted-uuid-1";
        OpenResult r = manager.open("env-1", "inst-1", sid, 120, 40, inst, cli);
        assertTrue(r.ok(), r.error());
        assertEquals(sid, r.sessionId());
        assertEquals(StatefulShellSessionManager.SHELL, r.shell());
        assertTrue(manager.hasSession(sid));
        List<String> argv = startedArgv.get(0);
        assertEquals(cli.cliPath(), argv.get(0));
        assertEquals("exec", argv.get(1));
        assertEquals("-i", argv.get(2));
        assertEquals(inst.containerName(), argv.get(3));
        assertEquals("/bin/sh", argv.get(4));
        assertEquals("-c", argv.get(5));
        assertTrue(argv.get(6).contains(StatefulShellSessionManager.pidFileFor(sid)), argv.get(6));
        assertTrue(argv.get(6).contains("exec /bin/sh"), argv.get(6));
    }

    @Test
    void destroySessionsForInstance_closes_matching() {
        StatefulInstance a = runningInstance("inst-a", "myenv/a:1");
        StatefulInstance b = runningInstance("inst-b", "myenv/b:1");
        assertTrue(manager.open("env-1", "inst-a", "sid-a", 80, 24, a, cli).ok());
        assertTrue(manager.open("env-1", "inst-b", "sid-b", 80, 24, b, cli).ok());

        manager.destroySessionsForInstance("inst-a");

        assertFalse(manager.hasSession("sid-a"));
        assertTrue(manager.hasSession("sid-b"));
        assertTrue(frames.stream().anyMatch(f ->
                "closed".equals(f.get("event"))
                        && "sid-a".equals(f.get("sessionId"))
                        && StatefulShellSessionManager.REASON_INSTANCE_STOPPED.equals(f.get("reason"))));
    }

    @Test
    void writeInput_ttyDenied_doesNotWriteCommand_emitsSystemAndDone() {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = "sid-tty";
        assertTrue(manager.open("env-1", "inst-1", sid, 80, 24, inst, cli).ok());
        String before = lastProcess.stdinString();

        manager.writeInput(sid, "vim /tmp/x\n");

        assertEquals(before, lastProcess.stdinString(), "TTY-denied command must not reach stdin");
        assertTrue(frames.stream().anyMatch(f ->
                "system".equals(f.get("stream"))
                        && String.valueOf(f.get("data")).contains("vim")
                        && String.valueOf(f.get("data")).contains("full TTY")));
        assertTrue(frames.stream().anyMatch(f ->
                "system".equals(f.get("stream"))
                        && "__SQ_CMD_DONE__:0".equals(f.get("data"))));
    }

    @Test
    void writeInput_completeLine_appendsSentinelAfterUserLine() {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = "sid-line";
        assertTrue(manager.open("env-1", "inst-1", sid, 80, 24, inst, cli).ok());
        int bootstrapLen = lastProcess.stdinBytes().length;

        manager.writeInput(sid, "echo hi\n");

        String written = lastProcess.stdinString().substring(bootstrapLen);
        assertTrue(written.startsWith("echo hi\n"), written);
        assertTrue(written.contains("printf '%s\\n' \"__SQ_CMD_DONE__" + "$?" + "\""), written);
        assertTrue(written.endsWith("\n"), written);
        assertTrue(written.indexOf("echo hi\n") < written.indexOf("__SQ_CMD_DONE__"), written);
    }

    @Test
    void writeInput_ctrlC_writesByte_signalsChildren_emitsDone130() throws Exception {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = "sid-intr";
        assertTrue(manager.open("env-1", "inst-1", sid, 80, 24, inst, cli).ok());
        int beforeLen = lastProcess.stdinBytes().length;
        int argvBefore = startedArgv.size();

        manager.writeInput(sid, "\u0003");

        byte[] after = lastProcess.stdinBytes();
        assertEquals(beforeLen + 1, after.length);
        assertEquals(0x03, after[beforeLen] & 0xff);
        assertTrue(frames.stream().anyMatch(f ->
                "system".equals(f.get("stream"))
                        && String.valueOf(f.get("data")).contains("SIGINT")));
        assertTrue(frames.stream().anyMatch(f ->
                "system".equals(f.get("stream"))
                        && "__SQ_CMD_DONE__:130".equals(f.get("data"))));
        // Side-exec: podman exec <ctr> /bin/sh -c <base64 plant-and-run wrapper> — the kill -INT
        // script is the planted payload (single-quote-only wrapper = Windows-safe, see ShExec).
        assertTrue(awaitArgv(argv -> argv.size() >= 5
                        && "exec".equals(argv.get(1))
                        && !argv.contains("-i")
                        && plantedScript(argv.get(argv.size() - 1)).contains("kill -INT"),
                2, TimeUnit.SECONDS),
                "interrupt must start a non-interactive side-exec; argvAfter=" + startedArgv.subList(argvBefore, startedArgv.size()));
    }

    /** Decodes a ShExec plant-and-run wrapper to its planted script (raw element otherwise). */
    private static String plantedScript(String element) {
        if (!element.startsWith("printf %s '")) {
            return element;
        }
        int e = element.indexOf('\'', "printf %s '".length());
        return new String(java.util.Base64.getDecoder().decode(
                element.substring("printf %s '".length(), e)), StandardCharsets.UTF_8);
    }

    @Test
    void stdout_overCap_emitsVisibleTruncationNotice() throws Exception {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = "sid-trunc";
        assertTrue(manager.open("env-1", "inst-1", sid, 80, 24, inst, cli).ok());
        manager.setMaxBytesPerCommandForTest(64);

        lastProcess.pushStdout("x".repeat(200));

        assertTrue(awaitFrames(f ->
                        "system".equals(f.get("stream"))
                                && String.valueOf(f.get("data")).contains("truncated")
                                && String.valueOf(f.get("data")).contains("256 KiB"),
                2, TimeUnit.SECONDS));
    }

    @Test
    void stdout_marker_stripped_and_emittedAsSystemDone() throws Exception {
        StatefulInstance inst = runningInstance("inst-1", "myenv/app:1");
        String sid = "sid-out";
        assertTrue(manager.open("env-1", "inst-1", sid, 80, 24, inst, cli).ok());

        lastProcess.pushStdout("hello\n__SQ_CMD_DONE__0\n");

        assertTrue(awaitFrames(f ->
                        "stdout".equals(f.get("stream")) && "hello\n".equals(f.get("data")),
                2, TimeUnit.SECONDS));
        assertTrue(awaitFrames(f ->
                        "system".equals(f.get("stream")) && "__SQ_CMD_DONE__:0".equals(f.get("data")),
                2, TimeUnit.SECONDS));
        assertFalse(frames.stream().anyMatch(f ->
                "stdout".equals(f.get("stream"))
                        && String.valueOf(f.get("data")).contains("__SQ_CMD_DONE__")));
    }

    private boolean awaitFrames(java.util.function.Predicate<Map<String, Object>> pred,
                                long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (frames.stream().anyMatch(pred)) {
                return true;
            }
            Thread.sleep(20);
        }
        return frames.stream().anyMatch(pred);
    }

    private boolean awaitArgv(java.util.function.Predicate<List<String>> pred,
                              long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            synchronized (startedArgv) {
                for (List<String> argv : startedArgv) {
                    if (pred.test(argv)) {
                        return true;
                    }
                }
            }
            Thread.sleep(20);
        }
        synchronized (startedArgv) {
            for (List<String> argv : startedArgv) {
                if (pred.test(argv)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static StatefulInstance runningInstance(String instanceId, String imageRef) {
        StatefulInstance inst = new StatefulInstance(
                instanceId, "env-1", imageRef, "ctr-" + instanceId, "vol-" + instanceId,
                StatefulCaps.defaults(), true);
        inst.setContainerId("cid-" + instanceId);
        inst.setStatus(StatefulStatus.RUNNING);
        return inst;
    }

    /**
     * Long-lived fake Process: pushable stdout for marker tests; stderr blocks until close.
     * stdin is a ByteArrayOutputStream inspectable via {@link #stdinBytes()}.
     */
    private static final class FakeShellProcess extends Process {
        private final ByteArrayOutputStream stdin = new ByteArrayOutputStream();
        private final PushableStream stdout = new PushableStream();
        private final InputStream stderr = new NeverEndingEmptyStream();
        private volatile boolean alive = true;

        byte[] stdinBytes() {
            return stdin.toByteArray();
        }

        String stdinString() {
            return stdin.toString(StandardCharsets.UTF_8);
        }

        void pushStdout(String data) {
            stdout.push(data.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public OutputStream getOutputStream() {
            return stdin;
        }

        @Override
        public InputStream getInputStream() {
            return stdout;
        }

        @Override
        public InputStream getErrorStream() {
            return stderr;
        }

        @Override
        public int waitFor() {
            while (alive) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return 0;
        }

        @Override
        public int exitValue() {
            if (alive) {
                throw new IllegalThreadStateException("alive");
            }
            return 0;
        }

        @Override
        public void destroy() {
            alive = false;
            try {
                stdout.close();
                stderr.close();
            } catch (IOException ignored) {
                // ignore
            }
        }

        @Override
        public boolean isAlive() {
            return alive;
        }
    }

    /** Blocks in read() until bytes are pushed or the stream is closed. */
    private static final class PushableStream extends InputStream {
        private final LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
        private byte[] current;
        private int pos;
        private volatile boolean closed;

        void push(byte[] chunk) {
            if (!closed && chunk != null && chunk.length > 0) {
                queue.offer(chunk);
            }
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : (one[0] & 0xff);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            while (current == null || pos >= current.length) {
                if (closed && queue.isEmpty()) {
                    return -1;
                }
                try {
                    byte[] next = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (next == null) {
                        if (closed) {
                            return -1;
                        }
                        continue;
                    }
                    current = next;
                    pos = 0;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return -1;
                }
            }
            int n = Math.min(len, current.length - pos);
            System.arraycopy(current, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public int available() {
            if (current != null && pos < current.length) {
                return current.length - pos;
            }
            byte[] peek = queue.peek();
            return peek == null ? 0 : peek.length;
        }

        @Override
        public void close() {
            closed = true;
            queue.offer(new byte[0]);
        }
    }

    /** Blocks in read() until closed, so reader tasks park without spinning. */
    private static final class NeverEndingEmptyStream extends InputStream {
        private final Object lock = new Object();
        private volatile boolean closed;

        @Override
        public int read() throws IOException {
            synchronized (lock) {
                while (!closed) {
                    try {
                        lock.wait(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return -1;
                    }
                }
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int c = read();
            return c < 0 ? -1 : 0; // closed → EOF; otherwise would have blocked
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public void close() {
            synchronized (lock) {
                closed = true;
                lock.notifyAll();
            }
        }
    }
}
