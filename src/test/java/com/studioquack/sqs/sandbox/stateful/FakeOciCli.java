package com.studioquack.sqs.sandbox.stateful;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * Test double for {@link OciCli}: records every argv and returns a scripted {@link OciCli.Result}
 * so {@link StatefulInstanceManager}, {@link StatefulLauncher} etc. are unit-testable with NO real
 * container runtime — the same "deterministic fake" pattern the {@code WarmPoolTest} uses for
 * {@code Warmer}. A responder function maps an argv to a result (matched by subcommand tokens).
 */
public final class FakeOciCli implements OciCli {

    public boolean available = true;
    public String cliPath = "/usr/bin/podman";
    /** Copy-on-write: reconcile loops record calls on their own thread while tests poll them. */
    public final List<List<String>> calls = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Stdin bytes per recorded call, index-aligned with {@link #calls} (empty when none). */
    public final List<byte[]> stdins = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Stdin bytes from the most recent {@link #run} (empty when none). */
    public byte[] lastStdin = new byte[0];
    /**
     * Container environment passed with each call, index-aligned with {@link #calls}. Empty when
     * the caller did not supply one.
     */
    public final java.util.List<java.util.Map<String, String>> envs =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    public Function<List<String>, Result> responder = argv -> new Result(0, "", "", false);

    @Override
    public boolean available() {
        return available;
    }

    @Override
    public String cliPath() {
        return cliPath;
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout) {
        return run(argv, stdin, timeout, java.util.Map.of());
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout,
                      java.util.Map<String, String> processEnv) {
        calls.add(List.copyOf(argv));
        envs.add(processEnv == null || processEnv.isEmpty()
                ? java.util.Map.of() : java.util.Map.copyOf(processEnv));
        lastStdin = stdin == null ? new byte[0] : stdin;
        stdins.add(lastStdin);
        // SandboxNetworkEnsure exists/inspect/rm/create must not collide with image-inspect
        // responders that key only on argv containing "inspect".
        if (argv.contains("network")) {
            return networkResult(argv);
        }
        return responder.apply(argv);
    }

    @Override
    public Result run(List<String> argv, byte[] stdin, Duration timeout, int stdoutCapBytes,
                      java.util.Map<String, String> processEnv) {
        return run(argv, stdin, timeout, processEnv);
    }

    @Override
    public Result runWithIdleReaper(List<String> argv, byte[] stdin, Duration idleTimeout,
                                    java.util.function.LongSupplier activityClock, int stdoutCapBytes,
                                    java.util.Map<String, String> processEnv) {
        return run(argv, stdin, idleTimeout, processEnv);
    }

    /** Isolated network with a discoverable subnet (matches ensure + inspect expectations). */
    private static Result networkResult(List<String> argv) {
        if (argv.contains("--format")) {
            String fmt = argv.get(argv.size() - 1);
            if (fmt != null && fmt.contains("Subnets")) {
                return new Result(0, "10.89.0.0/24;10.89.0.1", "", false);
            }
            return new Result(0, "true", "", false);
        }
        return new Result(0, "", "", false);
    }

    /** True when any recorded call's argv contains all the given tokens (order-independent). */
    public boolean sawCallContaining(String... tokens) {
        return calls.stream().anyMatch(argv -> {
            for (String t : tokens) {
                if (!argv.contains(t)) {
                    return false;
                }
            }
            return true;
        });
    }

    /** The first recorded call whose argv contains {@code subcommand}, or null. */
    public List<String> firstCallWith(String subcommand) {
        return calls.stream().filter(argv -> argv.contains(subcommand)).findFirst().orElse(null);
    }
}
