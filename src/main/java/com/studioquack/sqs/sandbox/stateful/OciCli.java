package com.studioquack.sqs.sandbox.stateful;

import java.time.Duration;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * The container-runtime CLI seam the stateful runtime drives every {@code podman} operation
 * through.
 */
public interface OciCli {

    /** True when a live container runtime is available (CLI on PATH + daemon reachable). */
    boolean available();

    /** The resolved CLI binary path (e.g. {@code /usr/local/bin/podman}), or {@code null}. */
    String cliPath();

    /**
     * Runs {@code argv} (already prefixed with the CLI path), optionally piping {@code stdin},
     * bounded by {@code timeout}. Never throws — a spawn failure / timeout is a {@link Result}
     * with a non-zero {@code exitCode} so the caller stays fail-safe (lifecycle handlers must
     * never throw out of {@code handle}).
     */
    Result run(List<String> argv, byte[] stdin, Duration timeout);

    /**
     * Same as {@link #run(List, byte[], Duration)} but with an explicit stdout capture cap. The
     * stateful launcher's stdout IS the app's response transport (multi-MiB file envelopes), so it
     * must not inherit the small log-stream cap lifecycle calls use.
     */
    default Result run(List<String> argv, byte[] stdin, Duration timeout, int stdoutCapBytes) {
        return run(argv, stdin, timeout);
    }

    /**
     * {@code processEnv} is the container environment. Values must not be in {@code argv}; the
     * production CLI applies them to the podman child process.
     */
    default Result run(List<String> argv, byte[] stdin, Duration timeout,
                       java.util.Map<String, String> processEnv) {
        return run(argv, stdin, timeout);
    }

    /** Launcher stdout cap plus the container environment. */
    default Result run(List<String> argv, byte[] stdin, Duration timeout, int stdoutCapBytes,
                       java.util.Map<String, String> processEnv) {
        return run(argv, stdin, timeout, stdoutCapBytes);
    }

    /**
     * Streaming stateful launcher path: idle reaper only (slides on {@code activityClock}),
     * never a fixed wall clock while helper frames still flow.
     */
    default Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                      LongSupplier activityClock, int stdoutCapBytes) {
        return run(argv, stdin, idleTimeout, stdoutCapBytes);
    }

    /** Idle reaper plus the container environment. */
    default Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                      LongSupplier activityClock, int stdoutCapBytes,
                                      java.util.Map<String, String> processEnv) {
        return runWithIdleReaper(argv, stdin, idleTimeout, activityClock, stdoutCapBytes);
    }

    /**
     * True when {@link #run} spawns real host processes, so a host-level wrapper such as a
     * {@code systemd-run} scope ({@link ContainerScope}) can apply. Test doubles keep the default
     * and always see the plain {@code podman} argv.
     */
    default boolean spawnsHostProcesses() {
        return false;
    }

    /** Convenience: run with no stdin. */
    default Result run(List<String> argv, Duration timeout) {
        return run(argv, null, timeout);
    }

    /** Outcome of a CLI invocation. {@code exitCode == -1} = failed to start / timed out. */
    record Result(int exitCode, String stdout, String stderr, boolean timedOut) {
        public boolean ok() {
            return exitCode == 0 && !timedOut;
        }

        /** Trimmed stdout (podman prints the id/digest on stdout with a trailing newline). */
        public String out() {
            return stdout == null ? "" : stdout.trim();
        }

        /** First stderr line, for compact error surfacing. */
        public String firstErrLine() {
            if (stderr == null) {
                return "";
            }
            int nl = stderr.indexOf('\n');
            return (nl < 0 ? stderr : stderr.substring(0, nl)).trim();
        }
    }
}
