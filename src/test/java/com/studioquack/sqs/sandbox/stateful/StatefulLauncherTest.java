package com.studioquack.sqs.sandbox.stateful;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The thin launcher's {@code podman exec} argv + bootstrap. Pins that the launcher targets the live
 * container with {@code exec -i}, injects the per-request helper creds + the app-convention env,
 * and pipes the request on stdin — the "invoke against the already-running instance" contract.
 */
class StatefulLauncherTest {

    private final StatefulLauncher launcher =
            new StatefulLauncher(new FakeOciCli(), new StatefulRuntimeProperties());

    private StatefulInstance instance() {
        StatefulInstance inst = new StatefulInstance("inst1", "env-1", "img", "cname",
                "vol", StatefulCaps.defaults(), true);
        inst.setContainerId("cid-live-1");
        return inst;
    }

    @Test
    void exec_argv_targets_live_container_with_per_request_helper_creds() {
        List<String> argv = launcher.execArgv(instance(),
                "/tmp/sq-helper.sock", "h_job_1", "env-1");

        assertEquals("exec", argv.get(1), "invokes exec against the running container");
        assertTrue(argv.contains("-i"), "pipes the request on stdin");
        assertTrue(argv.contains("cid-live-1"), "targets the live container id");
        assertFalse(argv.stream().anyMatch(a -> a.startsWith("SQVM_HELPER_URL=")),
                "SQVM_HELPER_URL is NEVER emitted");
        assertTrue(pair(argv, "-e", "SQ_HELPER_SOCKET") >= 0,
                "socket path name injected when provided");
        assertTrue(pair(argv, "-e", "SQVM_HELPER_TOKEN") >= 0, "per-request helper token name injected");
        assertTrue(pair(argv, "-e", "SQ_HELPER_BEARER") >= 0, "bearer alias name injected");
        assertTrue(pair(argv, "-e", "SQ_STATEFUL_RUNNER") >= 0, "stateful-runner signal name injected");
        assertTrue(pair(argv, "-e", "SQ_HELPER_CALLER_ENV_ID") >= 0, "caller env id name injected");
        assertNoEnvValues(argv);
        Map<String, String> env = launcher.execEnv("/tmp/sq-helper.sock", "h_job_1", "env-1");
        assertEquals("h_job_1", env.get("SQVM_HELPER_TOKEN"));
        assertEquals("h_job_1", env.get("SQ_HELPER_BEARER"));
        assertEquals("/tmp/sq-helper.sock", env.get("SQ_HELPER_SOCKET"));
        assertEquals("sh", argv.get(argv.size() - 3));
        assertEquals("-c", argv.get(argv.size() - 2));
        String wrapper = argv.get(argv.size() - 1);
        assertTrue(wrapper.contains("base64 -d"),
                "the bootstrap is base64-planted, not a bare sh -c script (Windows-safe transport)");
        assertFalse(wrapper.contains("\""),
                "the wrapper carries no double quotes — survives the Win32 command-line round-trip");
    }

    @Test
    void exec_argv_plants_the_verbatim_bootstrap() {
        List<String> argv = launcher.execArgv(instance(), null, "h_job_1", "env-1");

        String wrapper = argv.get(argv.size() - 1);
        int s = wrapper.indexOf('\'');
        int e = wrapper.indexOf('\'', s + 1);
        String planted = new String(
                Base64.getDecoder().decode(wrapper.substring(s + 1, e)), StandardCharsets.UTF_8);
        assertEquals(launcher.bootstrap(), planted,
                "the planted payload decodes back to the exact bootstrap script");
    }

    @Test
    void exec_argv_omits_socket_when_blank() {
        List<String> argv = launcher.execArgv(instance(), null, "h_job_1", "env-1");
        assertFalse(argv.contains("SQ_HELPER_SOCKET"),
                "no socket env when broker path is absent");
        assertTrue(pair(argv, "-e", "SQVM_HELPER_TOKEN") >= 0);
        assertNoEnvValues(argv);
    }

    @Test
    void bootstrap_posts_to_the_localhost_app_convention() {
        String boot = launcher.bootstrap();
        assertTrue(boot.contains("127.0.0.1:${SQ_APP_PORT:-8080}${SQ_APP_PATH:-/invoke}"),
                "dials the user app on localhost with the configurable port/path convention");
        assertTrue(boot.contains("curl"), "prefers curl");
        assertTrue(boot.contains("python3"), "falls back to python3");
        assertTrue(boot.contains("wget"), "falls back to wget");
        assertTrue(boot.contains("exit " + StatefulLauncher.NO_HTTP_CLIENT_EXIT),
                "signals a distinct exit when the image ships no http client");
    }

    @Test
    void cancel_argv_posts_boundary_interrupt_to_app_path_cancel() {
        List<String> argv = launcher.cancelArgv(instance());
        assertEquals("exec", argv.get(1));
        assertTrue(argv.contains("-i"));
        assertTrue(argv.contains("cid-live-1"));
        assertTrue(pair(argv, "-e", "SQ_APP_PORT") >= 0);
        assertTrue(pair(argv, "-e", "SQ_APP_PATH") >= 0);
        assertNoEnvValues(argv);
        assertEquals("8080", launcher.cancelEnv().get("SQ_APP_PORT"));
        assertEquals("/invoke", launcher.cancelEnv().get("SQ_APP_PATH"));

        String wrapper = argv.get(argv.size() - 1);
        int s = wrapper.indexOf('\'');
        int e = wrapper.indexOf('\'', s + 1);
        String planted = new String(
                Base64.getDecoder().decode(wrapper.substring(s + 1, e)), StandardCharsets.UTF_8);
        assertEquals(StatefulLauncher.cancelBootstrap(), planted);
        assertTrue(planted.contains("${SQ_APP_PATH:-/invoke}/cancel"));
        assertEquals("{\"requestId\":\"job-1\",\"reason\":\"BOUNDARY_INTERRUPT\"}",
                StatefulLauncher.cancelJson("job-1"));
    }

    /** No argv element is an env value (bearer, socket path, port). */
    private static void assertNoEnvValues(List<String> argv) {
        assertFalse(argv.stream().anyMatch(a -> a.contains("h_job_1") || a.contains("/tmp/sq-helper.sock")
                || a.contains("=8080") || a.contains("=/invoke") || a.contains("env-1")),
                "env values must not appear in argv: " + argv);
    }

    private static int pair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return i;
            }
        }
        return -1;
    }
}
