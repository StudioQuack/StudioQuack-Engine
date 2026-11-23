package com.studioquack.sqs.sandbox.stateful;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Production {@link OciCli}: availability + CLI-path from the autodetected {@code
 * ContainerRuntime}, process exec via {@code CliRunner}. This is the only class in the stateful
 * runtime that touches the real runtime, so everything above it ({@link StatefulInstanceManager},
 * {@link StatefulLauncher}) stays runtime-free + unit-testable.
 */
@Component
public class RuntimeOciCli implements OciCli {

    /** Stateful launcher/lifecycle streams can be large; cap generously (256 KiB each). */
    private static final int STREAM_CAP = 256 * 1024;

    private final ContainerRuntime runtime;

    public RuntimeOciCli(ContainerRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public boolean available() {
        return runtime.available();
    }

    @Override
    public String cliPath() {
        return runtime.cli();
    }

    @Override
    public boolean spawnsHostProcesses() {
        return true;
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout) {
        return run(argv, stdin, timeout, java.util.Map.of());
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout,
                      java.util.Map<String, String> processEnv) {
        CliRunner.Result r = CliRunner.run(argv, stdin, timeout, STREAM_CAP, STREAM_CAP, processEnv);
        return new Result(r.exitCode(), r.stdout(), r.stderr(), r.timedOut());
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout, int stdoutCapBytes) {
        return run(argv, stdin, timeout, stdoutCapBytes, java.util.Map.of());
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout, int stdoutCapBytes,
                      java.util.Map<String, String> processEnv) {
        // The launcher response transport gets its own (large) cap; stderr stays a log stream.
        int outCap = stdoutCapBytes > 0 ? stdoutCapBytes : STREAM_CAP;
        CliRunner.Result r = CliRunner.run(argv, stdin, timeout, outCap, STREAM_CAP, processEnv);
        return new Result(r.exitCode(), r.stdout(), r.stderr(), r.timedOut());
    }

    @Override
    public Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                    java.util.function.LongSupplier activityClock, int stdoutCapBytes) {
        return runWithIdleReaper(argv, stdin, idleTimeout, activityClock, stdoutCapBytes, java.util.Map.of());
    }

    @Override
    public Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                    java.util.function.LongSupplier activityClock, int stdoutCapBytes,
                                    java.util.Map<String, String> processEnv) {
        int outCap = stdoutCapBytes > 0 ? stdoutCapBytes : STREAM_CAP;
        CliRunner.Result r = CliRunner.runWithIdleReaper(argv, stdin, idleTimeout, activityClock,
                outCap, STREAM_CAP, processEnv);
        return new Result(r.exitCode(), r.stdout(), r.stderr(), r.timedOut());
    }
}
