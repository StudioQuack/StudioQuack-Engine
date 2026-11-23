package com.studioquack.sqs.sandbox.runtime;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Thin, dependency-free wrapper over {@link ProcessBuilder} for driving container-runtime CLIs
 * ({@code podman}). Usage; forbids a container Java client, so every container operation goes
 * through a process exec exactly like the Go supervisor.
 */
public final class CliRunner {

    private CliRunner() {
    }

    /** Result of a CLI invocation. {@code exitCode == -1} means the process failed to start / timed out. */
    public record Result(int exitCode, String stdout, String stderr, boolean timedOut, long elapsedMs) {
        public boolean ok() {
            return exitCode == 0 && !timedOut;
        }
    }

    /** Runs {@code argv} with the given deadline, inheriting no stdin. */
    public static Result run(List<String> argv, Duration timeout) {
        return run(argv, null, timeout, 256 * 1024, 256 * 1024, null);
    }

    /** Runs {@code argv} with container env values applied to the child process, not argv. */
    public static Result run(List<String> argv, Duration timeout, java.util.Map<String, String> containerEnv) {
        return run(argv, null, timeout, 256 * 1024, 256 * 1024, containerEnv);
    }

    /**
     * Runs {@code argv}, optionally piping {@code stdin} into the process, bounding stdout/stderr to
     * {@code stdoutCap}/{@code stderrCap} bytes, and killing the process after {@code timeout}.
     */
    public static Result run(List<String> argv, byte[] stdin, Duration timeout,
                             int stdoutCap, int stderrCap) {
        return run(argv, stdin, timeout, stdoutCap, stderrCap, null);
    }

    /**
     * {@code containerEnv} values are applied to the child process environment (and a 0600 env-file
     * for names that would retarget the podman client). They must already be name-only {@code -e NAME}
     * in argv.
     */
    public static Result run(List<String> argv, byte[] stdin, Duration timeout,
                             int stdoutCap, int stderrCap, java.util.Map<String, String> containerEnv) {
        long start = System.nanoTime();
        Launched launched = launch(argv, containerEnv);
        if (launched.failure != null) {
            long ms = (System.nanoTime() - start) / 1_000_000L;
            return new Result(-1, "", launched.failure, false, ms);
        }
        ProcessBuilder pb = launched.builder;
        Process proc;
        try {
            proc = pb.start();
        } catch (IOException e) {
            ContainerEnv.deleteQuietly(launched.envFile);
            long ms = (System.nanoTime() - start) / 1_000_000L;
            return new Result(-1, "", "spawn failed: " + e.getMessage(), false, ms);
        }

        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        Thread outPump = pump(proc.getInputStream(), outBuf, stdoutCap);
        Thread errPump = pump(proc.getErrorStream(), errBuf, stderrCap);

        if (stdin != null) {
            try (var os = proc.getOutputStream()) {
                os.write(stdin);
                os.flush();
            } catch (IOException ignore) {
                // The child may have exited before reading all of stdin (e.g. `head`); not fatal.
            }
        } else {
            try {
                proc.getOutputStream().close();
            } catch (IOException ignore) {
                // best-effort
            }
        }

        boolean finished;
        try {
            finished = timeout == null || timeout.isZero() || timeout.isNegative()
                    ? await(proc)
                    : proc.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            proc.destroyForcibly();
            ContainerEnv.deleteQuietly(launched.envFile);
            long ms = (System.nanoTime() - start) / 1_000_000L;
            return new Result(-1, drain(outPump, outBuf), drain(errPump, errBuf), false, ms);
        }

        boolean timedOut = false;
        int exit;
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
        String out = drain(outPump, outBuf);
        String err = drain(errPump, errBuf);
        ContainerEnv.deleteQuietly(launched.envFile);
        long ms = (System.nanoTime() - start) / 1_000_000L;
        return new Result(exit, out, err, timedOut, ms);
    }

    /**
     * Runs {@code argv} with an activity/idle reaper instead of a fixed wall clock. The process
     * is destroyed only after {@code idleTimeout} elapses with no {@code activityClock} advance
     * — never while helper-channel frames (or stdout pumps) are still flowing.
     */
    public static Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                           LongSupplier activityClock, int stdoutCap, int stderrCap) {
        return runWithIdleReaper(argv, stdin, idleTimeout, activityClock, stdoutCap, stderrCap, null);
    }

    /** Same as {@link #runWithIdleReaper} with container env off the argv. */
    public static Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                           LongSupplier activityClock, int stdoutCap, int stderrCap,
                                           java.util.Map<String, String> containerEnv) {
        long start = System.nanoTime();
        long idleNanos = idleTimeout == null || idleTimeout.isNegative() || idleTimeout.isZero()
                ? TimeUnit.MILLISECONDS.toNanos(
                com.studioquack.sqs.transport.StreamIdleLimits.DEFAULT_STREAM_IDLE_TIMEOUT_MS)
                : idleTimeout.toNanos();
        Launched launched = launch(argv, containerEnv);
        if (launched.failure != null) {
            long ms = (System.nanoTime() - start) / 1_000_000L;
            return new Result(-1, "", launched.failure, false, ms);
        }
        ProcessBuilder pb = launched.builder;
        Process proc;
        try {
            proc = pb.start();
        } catch (IOException e) {
            ContainerEnv.deleteQuietly(launched.envFile);
            long ms = (System.nanoTime() - start) / 1_000_000L;
            return new Result(-1, "", "spawn failed: " + e.getMessage(), false, ms);
        }

        ByteArrayOutputStream outBuf = new ByteArrayOutputStream();
        ByteArrayOutputStream errBuf = new ByteArrayOutputStream();
        Thread outPump = pump(proc.getInputStream(), outBuf, stdoutCap);
        Thread errPump = pump(proc.getErrorStream(), errBuf, stderrCap);

        if (stdin != null) {
            try (var os = proc.getOutputStream()) {
                os.write(stdin);
                os.flush();
            } catch (IOException ignore) {
                // child may exit early
            }
        } else {
            try {
                proc.getOutputStream().close();
            } catch (IOException ignore) {
                // best-effort
            }
        }

        // Sleeps until the process exits (waitFor returns at once) or until the idle window could
        // have run out — never a fixed poll. While the job has a helper request open its activity
        // clock reads "now", so a long LLM call or parked execute wakes this once per window.
        long lastActivity = activityClock == null ? System.nanoTime() : activityClock.getAsLong();
        boolean timedOut = false;
        while (true) {
            long remaining = lastActivity + idleNanos - System.nanoTime();
            boolean finished;
            try {
                finished = remaining > 0 ? proc.waitFor(remaining, TimeUnit.NANOSECONDS) : !proc.isAlive();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                proc.destroyForcibly();
                ContainerEnv.deleteQuietly(launched.envFile);
                long ms = (System.nanoTime() - start) / 1_000_000L;
                return new Result(-1, drain(outPump, outBuf), drain(errPump, errBuf), false, ms);
            }
            if (finished) {
                break;
            }
            long activity = activityClock == null ? System.nanoTime() : activityClock.getAsLong();
            if (activity > lastActivity) {
                lastActivity = activity;
                continue;
            }
            if (System.nanoTime() - lastActivity >= idleNanos) {
                timedOut = true;
                proc.destroyForcibly();
                try {
                    proc.waitFor(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                break;
            }
        }

        int exit = timedOut ? -1 : proc.exitValue();
        String out = drain(outPump, outBuf);
        String err = drain(errPump, errBuf);
        ContainerEnv.deleteQuietly(launched.envFile);
        long ms = (System.nanoTime() - start) / 1_000_000L;
        return new Result(exit, out, err, timedOut, ms);
    }

    /** Child process plus the env-file to delete once podman has exited. */
    private record Launched(ProcessBuilder builder, Path envFile, String failure) {
    }

    /**
     * Name-only {@code -e} stays in argv; values go to the child environment. Client-control names
     * move to a 0600 env-file.
     */
    private static Launched launch(List<String> argv, java.util.Map<String, String> containerEnv) {
        try {
            ContainerEnv.Prepared prepared = containerEnv == null || containerEnv.isEmpty()
                    ? new ContainerEnv.Prepared(argv, java.util.Map.of(), null)
                    : ContainerEnv.prepare(argv, containerEnv, ContainerEnv.dataDir());
            ProcessBuilder pb = new ProcessBuilder(prepared.argv());
            if (!prepared.childEnv().isEmpty()) {
                pb.environment().putAll(prepared.childEnv());
            }
            pb.redirectErrorStream(false);
            return new Launched(pb, prepared.envFile(), null);
        } catch (RuntimeException e) {
            String msg = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            return new Launched(null, null, "spawn failed: " + msg);
        }
    }

    private static boolean await(Process proc) throws InterruptedException {
        proc.waitFor();
        return true;
    }

    private static Thread pump(InputStream in, ByteArrayOutputStream sink, int cap) {
        Thread t = new Thread(() -> {
            byte[] chunk = new byte[8192];
            int n;
            try {
                while ((n = in.read(chunk)) != -1) {
                    synchronized (sink) {
                        if (sink.size() < cap) {
                            sink.write(chunk, 0, Math.min(n, cap - sink.size()));
                        }
                    }
                }
            } catch (IOException ignore) {
                // stream closed on process exit — expected
            }
        }, "cli-pump");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static String drain(Thread pump, ByteArrayOutputStream sink) {
        try {
            pump.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (sink) {
            return sink.toString(java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    /** Resolves {@code name} on the PATH (portable {@code exec.LookPath}), or null when absent. */
    public static String lookPath(String name) {
        String path = System.getenv("PATH");
        if (path == null || path.isBlank()) {
            return null;
        }
        List<String> exts = new ArrayList<>();
        exts.add("");
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            exts.add(".exe");
            exts.add(".cmd");
            exts.add(".bat");
        }
        for (String dir : path.split(java.io.File.pathSeparator)) {
            if (dir.isBlank()) {
                continue;
            }
            for (String ext : exts) {
                java.io.File f = new java.io.File(dir, name + ext);
                if (f.isFile() && f.canExecute()) {
                    return f.getAbsolutePath();
                }
            }
        }
        return null;
    }
}
