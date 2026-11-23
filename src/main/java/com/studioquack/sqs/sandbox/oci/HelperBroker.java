package com.studioquack.sqs.sandbox.oci;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Plants and launches {@code sq_helper_broker.py} in a warm container — the in-container relay
 * that lets {@code studioquack.execute()}/{@code studioquack.llm.*} reach the daemon over the SAME
 * podman-exec pipe the persistent python runner uses, instead of a network hop to the host. The
 * script must not arrive on the broker's own stdin.
 */
public final class HelperBroker {

    private HelperBroker() {
    }

    /** In-container path the broker file is planted at (mirrors {@code PythonRunner}'s {@code /tmp} paths). */
    public static final String SCRIPT_PATH = "/tmp/sq_helper_broker.py";
    /** In-container UDS the broker binds — injected into the job env as {@code SQ_HELPER_SOCKET}. */
    public static final String SOCKET_PATH = "/tmp/sq-helper.sock";

    private static final String INTERPRETER = "python3";

    private static volatile String scriptCache;

    /**
     * The {@code sh -c} script that plants the broker file, run via a short-lived
     * {@code podman exec <cid> sh -c <this>} BEFORE the long-lived exec is started. Mirrors the
     * plant half of {@link PythonRunner#startScript} (same base64-via-printf technique) without the
     * {@code exec} tail — launching is a separate, long-lived process (see {@link #launchArgv}).
     */
    public static String plantScript() {
        String b64 = Base64.getEncoder().encodeToString(brokerScript().getBytes(StandardCharsets.UTF_8));
        return "set -eu\n"
                + "printf %s '" + b64 + "' | base64 -d > " + SCRIPT_PATH + "\n";
    }

    /**
     * The argv for the long-lived, daemon-attached exec: {@code podman exec -e SQ_HELPER_SOCKET -i
     * <cid> python3 -u <scriptPath>}. The socket path is in the child environment ({@link #launch}),
     * not argv.
     */
    public static List<String> launchArgv(String cli, String containerId) {
        return List.of(cli, "exec",
                "-e", "SQ_HELPER_SOCKET",
                "-i", containerId,
                INTERPRETER, "-u", SCRIPT_PATH);
    }

    /**
     * Start the broker with {@code SQ_HELPER_SOCKET} in the podman child environment so {@code -e
     * SQ_HELPER_SOCKET} resolves inside the container.
     */
    public static ProcessBuilder launch(String cli, String containerId) {
        ProcessBuilder pb = new ProcessBuilder(launchArgv(cli, containerId));
        pb.environment().put("SQ_HELPER_SOCKET", SOCKET_PATH);
        pb.redirectErrorStream(false);
        return pb;
    }

    /**
     * A {@code sh -c} script that polls for the broker's UDS to appear (up to {@code timeoutSec},
     * 100ms steps) — the plant+launch confirmation, mirroring {@link PythonRunner#readinessScript}'s
     * marker-file poll but checking for a live socket ({@code -S}) instead of a ready-flag file,
     * since the broker itself never writes one.
     */
    public static String readinessScript(int timeoutSec) {
        int checks = Math.max(1, timeoutSec) * 10;
        return "for i in $(seq 1 " + checks + "); do "
                + "[ -S " + SOCKET_PATH + " ] && exit 0; sleep 0.1; done; exit 1";
    }

    /**
     * End-to-end round-trip probe: curl the broker's UDS with a deliberately INVALID bearer and
     * require the daemon's 401 to come back. The readiness script only proves the socket FILE exists —
     * 2026-08-28 incident: broker process alive, socket present, but frames never flowed
     * broker→daemon, so every helper call parked until its curl budget and the node died on the 300s
     * sandbox deadline with zero daemon-side trace.
     */
    public static String probeScript() {
        return "code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "
                + "--unix-socket " + SOCKET_PATH + " "
                + "-H 'Authorization: Bearer " + com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter.PROBE_BEARER + "' "
                + "-X POST http://localhost/v1/helper/init 2>/dev/null); "
                + "[ \"$code\" = \"401\" ] && exit 0; "
                + "echo \"helper-broker probe failed: http=$code\" >&2; exit 1";
    }

    private static String brokerScript() {
        if (scriptCache == null) {
            scriptCache = load("/sandbox/helpers/sq_helper_broker.py");
        }
        return scriptCache;
    }

    private static String load(String resource) {
        try (InputStream in = HelperBroker.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing classpath resource " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("failed to load " + resource + ": " + e.getMessage(), e);
        }
    }
}
