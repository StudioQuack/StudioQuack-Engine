package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.SandboxLanguage;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Persistent-runner support. The warm-pool container keeps a long-lived Python worker ({@code
 * sq_python_runner.py}) that imports the operator's preload modules ONCE and then forks a child
 * per job — so a dispatch never re-pays the multi-second {@code import openai/httpx/..} cost a
 * fresh {@code python main.py} paid on every run.
 */
public final class PythonRunner {

    private PythonRunner() {
    }

    /** In-container paths (all under the writable {@code /tmp} tmpfs). Must match the two scripts. */
    public static final String RUNNER_PATH = "/tmp/sq-python-runner.py";
    public static final String CLIENT_PATH = "/tmp/sq-python-runner-client.py";
    public static final String SOCKET_PATH = "/tmp/sq-python-runner.sock";
    public static final String READY_PATH = "/tmp/sq-python-runner.ready";
    public static final String LOG_PATH = "/tmp/sq-python-runner.log";
    /** Container-persistent bytecode cache (tmpfs) so children don't recompile .pyc every job. */
    public static final String PYCACHE_DIR = "/tmp/sq-pycache";

    private static final String INTERPRETER = "python3";

    private static volatile String runnerScriptCache;
    private static volatile String clientScriptCache;

    /** The in-container command the daemon execs per job to reach the runner over the unix socket. */
    public static List<String> clientCommand(SandboxLanguage lang) {
        return List.of(INTERPRETER, CLIENT_PATH, SOCKET_PATH,
                lang == null ? "main.py" : lang.scriptFileName());
    }

    /**
     * The detached {@code sh -c} start script: plants both python scripts into {@code /tmp} from
     * their base64 blobs, then execs the runner with the preload list + import path wired. Runs under
     * {@code podman exec -d} so it outlives the starting shell.
     */
    public static String startScript(String preloadCsv, String depContainerDir) {
        String runnerB64 = Base64.getEncoder().encodeToString(runnerScript().getBytes(StandardCharsets.UTF_8));
        String clientB64 = Base64.getEncoder().encodeToString(clientScript().getBytes(StandardCharsets.UTF_8));
        StringBuilder env = new StringBuilder()
                .append("SQ_RUNNER_SOCKET='").append(SOCKET_PATH).append("' ")
                .append("SQ_RUNNER_READY='").append(READY_PATH).append("' ")
                .append("SQ_RUNNER_WORKDIR='").append(HardeningArgs.SANDBOX_DIR).append("' ")
                .append("SQ_RUNNER_PRELOADS='").append(shSafe(preloadCsv)).append("' ")
                .append("PYTHONPYCACHEPREFIX='").append(PYCACHE_DIR).append("' ")
                .append("PYTHONDONTWRITEBYTECODE='' ");
        if (depContainerDir != null && !depContainerDir.isBlank()) {
            env.append("PYTHONPATH='").append(depContainerDir).append("' PYTHONNOUSERSITE='1' ");
        }
        return "set -eu\n"
                + "printf %s '" + runnerB64 + "' | base64 -d > " + RUNNER_PATH + "\n"
                + "printf %s '" + clientB64 + "' | base64 -d > " + CLIENT_PATH + "\n"
                + "mkdir -p " + PYCACHE_DIR + " 2>/dev/null || true\n"
                + "exec env " + env + INTERPRETER + " -u " + RUNNER_PATH
                + " > " + LOG_PATH + " 2>&1\n";
    }

    /** A {@code sh -c} script that polls for the ready marker up to {@code timeoutSec} (100ms steps). */
    public static String readinessScript(int timeoutSec) {
        int checks = Math.max(1, timeoutSec) * 10;
        return "for i in $(seq 1 " + checks + "); do "
                + "[ -f " + READY_PATH + " ] && exit 0; sleep 0.1; done; "
                + "test -f " + LOG_PATH + " && tail -50 " + LOG_PATH + " >&2; exit 1";
    }

    private static String runnerScript() {
        if (runnerScriptCache == null) {
            runnerScriptCache = load("/sandbox/helpers/sq_python_runner.py");
        }
        return runnerScriptCache;
    }

    private static String clientScript() {
        if (clientScriptCache == null) {
            clientScriptCache = load("/sandbox/helpers/sq_python_runner_client.py");
        }
        return clientScriptCache;
    }

    private static String load(String resource) {
        try (InputStream in = PythonRunner.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load " + resource + ": " + e.getMessage(), e);
        }
    }

    /** Escapes single quotes for embedding inside a single-quoted POSIX-sh string. */
    private static String shSafe(String s) {
        return s == null ? "" : s.replace("'", "'\\''");
    }
}
