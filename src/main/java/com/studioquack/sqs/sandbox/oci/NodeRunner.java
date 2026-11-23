package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.SandboxLanguage;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persistent-runner support for JavaScript and TypeScript warm slots — the Node counterpart of
 * {@link PythonRunner}. The warm container keeps one Node process ({@code sq_node_runner.js}) that
 * requires the operator's preload modules at warm time and then runs the slot's single job in that
 * same, already-primed process.
 */
public final class NodeRunner {

    private NodeRunner() {
    }

    /** In-container base path (the {@code /tmp} tmpfs). Must match both scripts' defaults. */
    public static final String BASE = "/tmp/sq-node-runner";
    public static final String RUNNER_PATH = BASE + ".js";
    public static final String CLIENT_PATH = BASE + "-client.js";
    public static final String READY_PATH = BASE + ".ready";
    public static final String LOG_PATH = BASE + ".log";

    /**
     * Node reads these only when a process starts, so a runner started at warm time cannot honour a
     * per-job value: such a job gets a fresh process (cold bootstrap) instead of a wrong one.
     */
    static final Set<String> STARTUP_ONLY_ENV = Set.of(
            "NODE_OPTIONS", "NODE_PATH", "NODE_EXTRA_CA_CERTS", "NODE_USE_ENV_PROXY", "NODE_ICU_DATA",
            "NODE_COMPILE_CACHE", "NODE_NO_WARNINGS", "NODE_PENDING_DEPRECATION", "NODE_REDIRECT_WARNINGS",
            "NODE_DEBUG", "NODE_V8_COVERAGE", "UV_THREADPOOL_SIZE", "UV_USE_IO_URING");

    private static volatile String runnerScriptCache;
    private static volatile String clientScriptCache;

    /** JavaScript and TypeScript share the Node runner; every other language has none. */
    public static boolean supports(SandboxLanguage lang) {
        return lang == SandboxLanguage.JAVASCRIPT || lang == SandboxLanguage.TYPESCRIPT;
    }

    /**
     * True when the job must not use the pre-started runner: it sets a startup-only Node variable, or
     * sets {@code NODE_ENV} while modules were preloaded (packages pick their build from it at load).
     */
    public static boolean needsFreshProcess(Map<String, String> jobEnv, boolean hasPreloads) {
        if (jobEnv == null || jobEnv.isEmpty()) {
            return false;
        }
        for (String key : jobEnv.keySet()) {
            if (STARTUP_ONLY_ENV.contains(key) || (hasPreloads && "NODE_ENV".equals(key))) {
                return true;
            }
        }
        return false;
    }

    /** The in-container command the daemon execs per job to reach the runner. */
    public static List<String> clientCommand(SandboxLanguage lang) {
        return List.of("node", CLIENT_PATH, BASE, lang.scriptFileName());
    }

    /** The detached start script for {@code podman exec -d <id> sh -c <script>}. */
    public static String startScript(SandboxLanguage lang, String preloadCsv, String depContainerDir) {
        return startScript(lang, preloadCsv, depContainerDir, BASE, HardeningArgs.SANDBOX_DIR);
    }

    /**
     * Plants both scripts from base64, creates the output FIFOs, and starts the runner with the
     * language's own interpreter flags (TypeScript keeps its type stripping). {@code sh} stays the
     * runner's parent so it can record the true exit status, signals included.
     */
    static String startScript(SandboxLanguage lang, String preloadCsv, String depContainerDir,
                              String base, String workdir) {
        List<String> entry = lang.entrypoint(workdir);
        List<String> interpreter = new ArrayList<>(entry.subList(0, entry.size() - 1));
        StringBuilder env = new StringBuilder()
                .append("SQ_RUNNER_BASE=").append(q(base)).append(' ')
                .append("SQ_RUNNER_WORKDIR=").append(q(workdir)).append(' ')
                .append("SQ_RUNNER_PRELOADS=").append(q(preloadCsv)).append(' ');
        if (depContainerDir != null && !depContainerDir.isBlank()) {
            env.append("NODE_PATH=").append(q(depContainerDir + "/node_modules")).append(' ');
        }
        String out = q(base + ".out");
        String err = q(base + ".err");
        return String.join("\n",
                "set -eu",
                "printf %s '" + b64(runnerScript()) + "' | base64 -d > " + q(base + ".js"),
                "printf %s '" + b64(clientScript()) + "' | base64 -d > " + q(base + "-client.js"),
                "rm -f " + out + " " + err + " " + q(base + ".exit"),
                "mkfifo " + out + " " + err,
                "set +e",
                // Read-write, so opening the FIFOs never waits for the client that will read them.
                "env " + env + String.join(" ", interpreter) + " " + q(base + ".js")
                        + " 1<>" + out + " 2<>" + err + " </dev/null",
                "echo $? > " + q(base + ".exit.tmp") + " && mv " + q(base + ".exit.tmp") + " "
                        + q(base + ".exit"));
    }

    /** Polls for the ready marker up to {@code timeoutSec} (100ms steps); prints the log on failure. */
    public static String readinessScript(int timeoutSec) {
        int checks = Math.max(1, timeoutSec) * 10;
        return "for i in $(seq 1 " + checks + "); do "
                + "[ -f " + READY_PATH + " ] && exit 0; sleep 0.1; done; "
                + "test -f " + LOG_PATH + " && tail -50 " + LOG_PATH + " >&2; exit 1";
    }

    private static String runnerScript() {
        if (runnerScriptCache == null) {
            runnerScriptCache = load("/sandbox/helpers/sq_node_runner.js");
        }
        return runnerScriptCache;
    }

    private static String clientScript() {
        if (clientScriptCache == null) {
            clientScriptCache = load("/sandbox/helpers/sq_node_runner_client.js");
        }
        return clientScriptCache;
    }

    private static String load(String resource) {
        try (InputStream in = NodeRunner.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load " + resource + ": " + e.getMessage(), e);
        }
    }

    private static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Single-quotes a value for POSIX sh. */
    private static String q(String s) {
        return "'" + (s == null ? "" : s.replace("'", "'\\''")) + "'";
    }
}
