package com.studioquack.sqs.sandbox.testrun;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.sandbox.egress.StubSandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter.RoutedResponse;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService.SyncBuildResult;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Daemon-owned test build+run: happy path, build failure, stateless markers, cleanup on
 * exception, image-ref gating, 400 validation, and the concurrency semaphore.
 */
class TestSandboxRunServiceTest {

    private static final String CID = "testciddeadbeef";

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private FakeOciCli cli;
    private OciBuildContextStore store;
    private OciImageBuildService buildService;
    private HelperRequestRouter router;
    private TestBuildProperties props;
    private AtomicInteger python3Exit;
    private AtomicInteger runningFlag; // 1 = true, 0 = false

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        python3Exit = new AtomicInteger(1);
        runningFlag = new AtomicInteger(1);
        cli.responder = this::scripted;
        store = new OciBuildContextStore(tempDir.toString());
        buildService = mock(OciImageBuildService.class);
        when(buildService.buildSync(anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(new SyncBuildResult(true, "STEP ok", ""));
        router = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(new AiModelProviderHolder(), mapper),
                null);
        props = new TestBuildProperties();
    }

    @Test
    void happyPath_statefulInvokeAndCleanup() throws Exception {
        TestSandboxRunService svc = newService((cliPath, cid, r) -> null);

        RoutedResponse resp = svc.buildRun(caller(), happyBody());
        Map<String, Object> body = envelope(resp);

        assertEquals(200, resp.status());
        assertEquals(Boolean.TRUE, body.get("image_built"));
        assertEquals(Boolean.TRUE, body.get("run_attempted"));
        assertEquals(Boolean.TRUE, body.get("app_listening"));
        assertEquals(Boolean.FALSE, body.get("broker_ready"));
        assertEquals("", body.get("error"));
        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) body.get("output");
        assertNotNull(output);
        assertEquals(1, ((Number) output.get("out")).intValue());

        assertTrue(cli.sawCallContaining("rm"));
        assertTrue(cli.sawCallContaining("rmi"));
        assertContextGone();
        assertEquals(0, router.activeJobs());

        List<String> runArgv = cli.firstCallWith("run");
        assertNotNull(runArgv, "podman run of the test image");
        assertTrue(runArgv.get(runArgv.size() - 1).startsWith("sq-local/node-test-"),
                "no command after the image (ENTRYPOINT kept)");
        assertTrue(runArgv.contains("--network"));
        assertTrue(runArgv.contains(SandboxNetworks.JOBS));
        assertFalse(runArgv.contains("--rm"));
        assertTrue(runArgv.contains("-d"));
        // Env travels as a bare name on argv; the value rides the podman process environment, so it
        // never shows in /proc/<pid>/cmdline.
        assertTrue(runArgv.contains("SQ_APP_PORT"));
        assertFalse(runArgv.contains("SQ_APP_PORT=8080"));
    }

    @Test
    void buildFailure_skipsRunAndDeletesContext() throws Exception {
        when(buildService.buildSync(anyString(), anyString(), anyString(), anyInt()))
                .thenReturn(new SyncBuildResult(false, "fail log", "STEP failed"));
        TestSandboxRunService svc = newService((cliPath, cid, r) -> null);

        RoutedResponse resp = svc.buildRun(caller(), happyBody());
        Map<String, Object> body = envelope(resp);

        assertEquals(200, resp.status());
        assertEquals(Boolean.FALSE, body.get("image_built"));
        assertEquals(Boolean.FALSE, body.get("run_attempted"));
        assertEquals("STEP failed", body.get("error"));
        assertTrue(String.valueOf(body.get("build_log")).contains("fail log"));
        assertEquals(null, cli.firstCallWith("run"));
        assertContextGone();
        assertEquals(0, router.activeJobs());
    }

    @Test
    void statelessMarkerPath() throws Exception {
        runningFlag.set(0);
        cli.responder = argv -> {
            if (argv.contains("logs")) {
                return new OciCli.Result(0, "portValue:answer:::result:42\n", "", false);
            }
            return scripted(argv);
        };
        TestSandboxRunService svc = newService((cliPath, cid, r) -> null);

        RoutedResponse resp = svc.buildRun(caller(), happyBody());
        Map<String, Object> body = envelope(resp);

        assertEquals(200, resp.status());
        assertEquals(Boolean.TRUE, body.get("image_built"));
        assertEquals(Boolean.TRUE, body.get("run_attempted"));
        assertEquals(Boolean.FALSE, body.get("app_listening"));
        @SuppressWarnings("unchecked")
        Map<String, Object> output = (Map<String, Object>) body.get("output");
        assertEquals("42", String.valueOf(output.get("answer")));
    }

    @Test
    void cleanupOnBrokerStarterException() throws Exception {
        python3Exit.set(0);
        TestSandboxRunService svc = newService((cliPath, cid, r) -> {
            throw new RuntimeException("broker boom");
        });

        RoutedResponse resp = svc.buildRun(caller(), happyBody());
        assertEquals(200, resp.status());
        Map<String, Object> body = envelope(resp);
        assertTrue(String.valueOf(body.get("error")).contains("broker boom"));

        assertTrue(cli.sawCallContaining("rm"));
        assertTrue(cli.sawCallContaining("rmi"));
        assertContextGone();
        assertEquals(0, router.activeJobs());
    }

    @Test
    void allowsCaller_gating() {
        TestSandboxRunService svc = newService((cliPath, cid, r) -> null);

        props.setAllowedImages("");
        assertFalse(svc.allowsCaller("sq-local/node-test-emulator:1"));
        props.setAllowedImages(null);
        assertFalse(svc.allowsCaller("sq-local/node-test-emulator:1"));

        props.setAllowedImages("sq-local/node-test-emulator");
        assertTrue(svc.allowsCaller("sq-local/node-test-emulator"));
        assertTrue(svc.allowsCaller("sq-local/node-test-emulator:1"));
        assertFalse(svc.allowsCaller("sq-local/other:1"));
    }

    @Test
    void validation_emptyFilesAndMissingDockerfile() throws Exception {
        TestSandboxRunService svc = newService((cliPath, cid, r) -> null);

        RoutedResponse empty = svc.buildRun(caller(), mapper.writeValueAsBytes(Map.of(
                "files", List.of())));
        assertEquals(400, empty.status());
        assertTrue(new String(empty.body(), StandardCharsets.UTF_8).contains("files"));

        RoutedResponse missing = svc.buildRun(caller(), mapper.writeValueAsBytes(Map.of(
                "files", List.of(Map.of("path", "src/main.java", "content", "class X {}")))));
        assertEquals(400, missing.status());
        assertTrue(new String(missing.body(), StandardCharsets.UTF_8).contains("Dockerfile")
                || new String(missing.body(), StandardCharsets.UTF_8).contains("Containerfile"));
    }

    @Test
    void saturated_whenMaxConcurrentZero() throws Exception {
        props.setMaxConcurrent(0);
        TestSandboxRunService svc = newService((cliPath, cid, r) -> null);

        RoutedResponse resp = svc.buildRun(caller(), happyBody());
        assertEquals(200, resp.status());
        Map<String, Object> body = envelope(resp);
        assertEquals("test build-run saturated", body.get("error"));
        assertEquals(Boolean.FALSE, body.get("image_built"));
        assertEquals(Boolean.FALSE, body.get("run_attempted"));
    }

    /* ------------------------------------------------------------------ */

    private TestSandboxRunService newService(BrokerStarter starter) {
        return new TestSandboxRunService(cli, store, buildService, router,
                StubSandboxEgressManager.enforced(), SandboxOwnership.unowned(),
                props, mapper, null, starter);
    }

    private HelperJob caller() {
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("envId", "env-1");
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "n1");
        return HelperJob.builder("caller-1", "python")
                .executionContext(ec)
                .callerImageRef("sq-local/node-test-emulator:1")
                .build();
    }

    private byte[] happyBody() throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("files", List.of(Map.of("path", "Dockerfile", "content", "FROM scratch")));
        body.put("mock_function_responses", Map.of());
        body.put("input_data", Map.of());
        return mapper.writeValueAsBytes(body);
    }

    private Map<String, Object> envelope(RoutedResponse resp) throws Exception {
        return mapper.readValue(resp.body(), new TypeReference<Map<String, Object>>() {});
    }

    private void assertContextGone() throws Exception {
        Path root = tempDir.resolve("oci-build");
        if (!Files.exists(root)) {
            return;
        }
        try (var stream = Files.list(root)) {
            assertTrue(stream.findAny().isEmpty(), "staged OCI context must be deleted");
        }
    }

    private OciCli.Result scripted(List<String> argv) {
        if (argv.contains("exec")) {
            String script = execScript(argv);
            if (script.contains("SQ_REQ") || script.contains("-X POST")) {
                return new OciCli.Result(0, "{\"out\":1}", "", false);
            }
            if (script.contains("8080") || script.contains("/invoke")) {
                return new OciCli.Result(0, "200", "", false);
            }
            if (script.contains("command -v python3")) {
                return new OciCli.Result(python3Exit.get(), "", "", false);
            }
        }
        if (argv.contains("inspect")) {
            if (argv.stream().anyMatch(s -> s.contains("Running"))) {
                return new OciCli.Result(0, runningFlag.get() == 1 ? "true" : "false", "", false);
            }
            if (argv.stream().anyMatch(s -> s.contains("ExitCode"))) {
                return new OciCli.Result(0, "0", "", false);
            }
        }
        if (argv.contains("run")) {
            return new OciCli.Result(0, CID, "", false);
        }
        return new OciCli.Result(0, "", "", false);
    }

    /**
     * The effective script an exec call runs, across the three transports: the planted payload
     * for a {@code ShExec} base64 plant-and-run wrapper, stdin for {@code sh -s}, or the raw
     * {@code -c} element otherwise (helper-broker plant/readiness).
     */
    private String execScript(List<String> argv) {
        int c = argv.indexOf("-c");
        String raw = (c >= 0 && c + 1 < argv.size()) ? argv.get(c + 1) : "";
        if (raw.startsWith("printf %s '")) {
            int e = raw.indexOf('\'', "printf %s '".length());
            return new String(java.util.Base64.getDecoder().decode(
                    raw.substring("printf %s '".length(), e)), StandardCharsets.UTF_8);
        }
        if (argv.contains("-s")) {
            return new String(cli.lastStdin, StandardCharsets.UTF_8);
        }
        return raw;
    }
}
