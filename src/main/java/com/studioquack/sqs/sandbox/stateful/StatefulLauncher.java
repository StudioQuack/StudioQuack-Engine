package com.studioquack.sqs.sandbox.stateful;

import com.studioquack.sqs.sandbox.oci.ShExec;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/** The thin launcher invoked PER REQUEST against an already-running stateful instance. */
@Slf4j
@Component
public class StatefulLauncher {

    /** Exit code the bootstrap uses when the image ships no usable HTTP client. */
    static final int NO_HTTP_CLIENT_EXIT = 97;

    /** In-instance cancel reason posted on a boundary interrupt (cancel-instance / cancel-step). */
    public static final String BOUNDARY_INTERRUPT = "BOUNDARY_INTERRUPT";

    /** Wall budget for the in-container {@code POST /cancel} (best-effort; never blocks cancel). */
    static final Duration CANCEL_POST_TIMEOUT = Duration.ofSeconds(2);

    private final OciCli cli;
    private final StatefulRuntimeProperties props;
    /** The thread blocked in the current {@link #invoke} so a cancel can interrupt it. */
    private final AtomicReference<Thread> invokeThread = new AtomicReference<>();

    public StatefulLauncher(OciCli cli, StatefulRuntimeProperties props) {
        this.cli = cli;
        this.props = props;
    }

    /** The captured result of one launcher invocation. */
    public record LauncherOutcome(int exitCode, String stdout, String stderr, boolean timedOut) {
        public boolean ok() {
            return exitCode == 0 && !timedOut;
        }
    }

    /**
     * Invokes the launcher against {@code instance}'s live container with the per-request context.
     *
     * @param instance The live stateful instance (its {@code containerId} is the exec target).
     * @param requestJson The request body piped to the user's app (context + inputs + helper cred.
     * @param helperSocketPath In-container UDS path when the broker is alive, or null/blank to om.
     */
    public LauncherOutcome invoke(StatefulInstance instance, String requestJson,
                                  String helperSocketPath, String helperToken, String envId) {
        return invoke(instance, requestJson, helperSocketPath, helperToken, envId, false, null);
    }

    /**
     * @param streaming When {@code true}, the launcher uses an activity-driven idle reaper (slide.
     * @param activityClock Nanoseconds of last helper-channel activity; ignored when not streamin.
     */
    public LauncherOutcome invoke(StatefulInstance instance, String requestJson,
                                  String helperSocketPath, String helperToken, String envId,
                                  boolean streaming, java.util.function.LongSupplier activityClock) {
        Map<String, String> env = execEnv(helperSocketPath, helperToken, envId);
        List<String> argv = execArgv(instance, env);
        byte[] stdin = (requestJson == null ? "{}" : requestJson).getBytes(StandardCharsets.UTF_8);
        // The captured stdout IS the app's response transport (inline-base64 file envelopes run
        // multi-MiB), so it rides the configured transport cap — never the 256 KiB log-stream cap,
        // which truncated image-bearing responses mid-JSON and surfaced as silent empty outputs.
        int responseCap = (int) Math.min(Integer.MAX_VALUE,
                Math.max(1024, props.getStatefulResponseMaxBytes()));
        invokeThread.set(Thread.currentThread());
        try {
            OciCli.Result r;
            if (streaming) {
                r = cli.runWithIdleReaper(argv, stdin,
                        Duration.ofMillis(Math.max(1L, props.getStreamIdleTimeoutMs())),
                        activityClock == null ? System::nanoTime : activityClock,
                        responseCap, env);
            } else {
                r = cli.run(argv, stdin, Duration.ofMillis(props.getRequestTimeoutMs()), responseCap, env);
            }
            if (r.exitCode() == NO_HTTP_CLIENT_EXIT) {
                log.warn("[SQ-6] stateful launcher: image {} ships no curl/python3/wget — cannot reach the "
                        + "in-instance app; configure an image with an HTTP client.", instance.imageRef());
            }
            return new LauncherOutcome(r.exitCode(), r.stdout(), r.stderr(), r.timedOut());
        } finally {
            invokeThread.compareAndSet(Thread.currentThread(), null);
        }
    }

    /**
     * Interrupts the thread blocked in {@link #invoke} so {@code CliRunner} destroys the {@code podman
     * exec} process. Best-effort; never throws.
     */
    public void abortInFlight() {
        Thread t = invokeThread.get();
        if (t != null) {
            t.interrupt();
        }
    }

    /**
     * POSTs {@code {requestId, reason:BOUNDARY_INTERRUPT}} to
     * {@code http://127.0.0.1:${SQ_APP_PORT}${SQ_APP_PATH}/cancel} inside the instance via
     * {@code podman exec} (curl → python3 → wget). ~2 s timeout. Best-effort; never throws.
     */
    public void postBoundaryInterrupt(StatefulInstance instance, String requestId) {
        if (instance == null || instance.containerId() == null || instance.containerId().isBlank()
                || requestId == null || requestId.isBlank()) {
            return;
        }
        try {
            Map<String, String> env = cancelEnv();
            List<String> argv = cancelArgv(instance, env);
            byte[] body = cancelJson(requestId).getBytes(StandardCharsets.UTF_8);
            cli.run(argv, body, CANCEL_POST_TIMEOUT, env);
        } catch (RuntimeException e) {
            log.debug("[SQ-6] stateful cancel POST skipped instance={}: {}",
                    instance.instanceId(), e.toString());
        }
    }

    /** JSON body for the in-instance {@code /cancel} contract. */
    public static String cancelJson(String requestId) {
        String id = requestId == null ? "" : requestId.replace("\\", "\\\\").replace("\"", "\\\"");
        return "{\"requestId\":\"" + id + "\",\"reason\":\"" + BOUNDARY_INTERRUPT + "\"}";
    }

    /**
     * Per-request container environment. Values stay off argv; callers pass this map to the podman
     * child process.
     */
    Map<String, String> execEnv(String helperSocketPath, String helperToken, String envId) {
        Map<String, String> env = new LinkedHashMap<>();
        if (helperSocketPath != null && !helperSocketPath.isBlank()) {
            env.put("SQ_HELPER_SOCKET", helperSocketPath);
        }
        if (helperToken != null && !helperToken.isBlank()) {
            env.put("SQVM_HELPER_TOKEN", helperToken);
            env.put("SQ_HELPER_BEARER", helperToken);
        }
        if (envId != null && !envId.isBlank()) {
            env.put("SQ_HELPER_CALLER_ENV_ID", envId);
        }
        // Stateful-mode signal + app convention (also set at container start; re-passed per request
        // so a request can target a different app port/path without restarting the instance).
        env.put("SQ_STATEFUL_RUNNER", "1");
        env.put("SQ_APP_PORT", Integer.toString(props.getAppPort()));
        env.put("SQ_APP_PATH", props.getAppPath());
        return env;
    }

    /**
     * Builds the {@code podman exec} argv (pure + unit-testable). Injects the per-request helper
     * creds + the app-convention env as bare {@code -e NAME}, and pipes the request on stdin ({@code -i}).
     */
    List<String> execArgv(StatefulInstance instance, String helperSocketPath, String helperToken,
                          String envId) {
        return execArgv(instance, execEnv(helperSocketPath, helperToken, envId));
    }

    private List<String> execArgv(StatefulInstance instance, Map<String, String> env) {
        List<String> argv = new ArrayList<>();
        argv.add(cli.cliPath() == null ? "podman" : cli.cliPath());
        argv.add("exec");
        argv.add("-i");
        // Name only. The value is the podman process environment, not argv.
        com.studioquack.sqs.sandbox.runtime.ContainerEnv.appendNameOnly(argv, env);
        argv.add(instance.containerId());
        argv.add("sh");
        argv.add("-c");
        // The bootstrap rides a base64 plant-and-run wrapper (single quotes only): a quote-bearing
        // sh -c script is shattered by the Win32 command-line round-trip on Windows hosts, and
        // sh -s is impossible here because stdin already carries the request JSON (see ShExec).
        argv.add(ShExec.plantAndRun(bootstrap(), "sq_launcher"));
        return argv;
    }

    /**
     * {@code podman exec} argv for the in-instance {@code POST /cancel}. Same env convention as
     * {@link #execArgv} so {@code SQ_APP_PORT}/{@code SQ_APP_PATH} resolve the cancel URL.
     */
    Map<String, String> cancelEnv() {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("SQ_APP_PORT", Integer.toString(props.getAppPort()));
        env.put("SQ_APP_PATH", props.getAppPath());
        return env;
    }

    List<String> cancelArgv(StatefulInstance instance) {
        return cancelArgv(instance, cancelEnv());
    }

    private List<String> cancelArgv(StatefulInstance instance, Map<String, String> env) {
        List<String> argv = new ArrayList<>();
        argv.add(cli.cliPath() == null ? "podman" : cli.cliPath());
        argv.add("exec");
        argv.add("-i");
        com.studioquack.sqs.sandbox.runtime.ContainerEnv.appendNameOnly(argv, env);
        argv.add(instance.containerId());
        argv.add("sh");
        argv.add("-c");
        argv.add(ShExec.plantAndRun(cancelBootstrap(), "sq_cancel"));
        return argv;
    }

    /**
     * The portable POSIX-sh launcher bootstrap: read the request on stdin, POST it to the user's app
     * on localhost, echo the app's response to stdout. Tries {@code curl}, then {@code python3}, then
     * {@code wget}; exits {@link #NO_HTTP_CLIENT_EXIT} when the image ships none.
     */
    public static String bootstrap() {
        return String.join("\n",
                "set -e",
                "SQ_REQ=$(cat)",
                "SQ_URL=\"http://127.0.0.1:${SQ_APP_PORT:-8080}${SQ_APP_PATH:-/invoke}\"",
                "if command -v curl >/dev/null 2>&1; then",
                "  printf '%s' \"$SQ_REQ\" | curl -sS -X POST -H 'Content-Type: application/json' "
                        + "--data-binary @- \"$SQ_URL\"",
                "elif command -v python3 >/dev/null 2>&1; then",
                "  printf '%s' \"$SQ_REQ\" | python3 -c 'import sys,urllib.request; "
                        + "d=sys.stdin.buffer.read(); "
                        + "req=urllib.request.Request(sys.argv[1], data=d, "
                        + "headers={\"Content-Type\":\"application/json\"}); "
                        + "sys.stdout.buffer.write(urllib.request.urlopen(req).read())' \"$SQ_URL\"",
                "elif command -v wget >/dev/null 2>&1; then",
                "  printf '%s' \"$SQ_REQ\" | wget -q -O - --header='Content-Type: application/json' "
                        + "--post-file=/dev/stdin \"$SQ_URL\"",
                "else",
                "  echo 'SQ_LAUNCHER_ERROR: no http client (curl/python3/wget) in image' >&2",
                "  exit " + NO_HTTP_CLIENT_EXIT,
                "fi");
    }

    /**
     * Portable POSIX-sh: read the cancel JSON on stdin and POST it to
     * {@code ${SQ_URL}/cancel}. Best-effort — missing HTTP client is a quiet success
     * (the invoke abort still unblocks the launcher).
     */
    public static String cancelBootstrap() {
        return String.join("\n",
                "set +e",
                "SQ_BODY=$(cat)",
                "SQ_URL=\"http://127.0.0.1:${SQ_APP_PORT:-8080}${SQ_APP_PATH:-/invoke}/cancel\"",
                "if command -v curl >/dev/null 2>&1; then",
                "  printf '%s' \"$SQ_BODY\" | curl -sS -m 2 -X POST -H 'Content-Type: application/json' "
                        + "--data-binary @- \"$SQ_URL\"",
                "elif command -v python3 >/dev/null 2>&1; then",
                "  printf '%s' \"$SQ_BODY\" | python3 -c 'import sys,urllib.request; "
                        + "d=sys.stdin.buffer.read(); "
                        + "req=urllib.request.Request(sys.argv[1], data=d, method=\"POST\", "
                        + "headers={\"Content-Type\":\"application/json\"}); "
                        + "sys.stdout.buffer.write(urllib.request.urlopen(req, timeout=2).read())' \"$SQ_URL\"",
                "elif command -v wget >/dev/null 2>&1; then",
                "  printf '%s' \"$SQ_BODY\" | wget -q -O - --timeout=2 --header='Content-Type: application/json' "
                        + "--post-file=/dev/stdin \"$SQ_URL\"",
                "fi");
    }
}
