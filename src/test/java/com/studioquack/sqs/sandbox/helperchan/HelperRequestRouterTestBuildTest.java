package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.sandbox.testrun.TestSandboxRunService;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Router-level coverage for daemon-owned {@code test/build-run} and TEST-MOCK mode
 * ({@link HelperJob#mockResponses()} non-null, including empty).
 */
class HelperRequestRouterTestBuildTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final AiModelProviderProxy llmProxy = new AiModelProviderProxy(
            new AiModelProviderHolder(), mapper);

    @Test
    void testBuildRun_nonPersistentJob_403() {
        TestSandboxRunService testService = mock(TestSandboxRunService.class);
        when(testService.allowsCaller(any())).thenReturn(true);
        HelperExecuteService executeService = mock(HelperExecuteService.class);
        HelperRequestRouter router = routerWithTestService(executeService, testService);

        HelperJob job = HelperJob.builder("job-np", "python")
                .callerImageRef("sq-local/node-test-emulator:1")
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "test/build-run", "{}");
        assertEquals(403, r.status());
        assertTrue(body(r).contains("\"error\":\"test_build_forbidden\""));
        verify(testService, never()).buildRun(any(), any());
    }

    @Test
    void testBuildRun_wrongCallerImageRef_403() {
        TestSandboxRunService testService = mock(TestSandboxRunService.class);
        when(testService.allowsCaller(any())).thenReturn(false);
        when(testService.buildRun(any(), any())).thenReturn(
                HelperRequestRouter.RoutedResponse.json(200, "{\"ok\":true}".getBytes(StandardCharsets.UTF_8)));
        HelperRequestRouter router = routerWithTestService(mock(HelperExecuteService.class), testService);

        HelperJob job = HelperJob.builder("job-wrong", "python")
                .persistent(true)
                .callerImageRef("sq-local/other:1")
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "test/build-run", "{}");
        assertEquals(403, r.status());
        assertTrue(body(r).contains("\"error\":\"test_build_forbidden\""));
        verify(testService, never()).buildRun(any(), any());
    }

    @Test
    void testBuildRun_serviceAbsent_503() {
        HelperRequestRouter router = new HelperRequestRouter(mapper, llmProxy,
                mock(HelperExecuteService.class));
        HelperJob job = HelperJob.builder("job-nosvc", "python")
                .persistent(true)
                .callerImageRef("sq-local/node-test-emulator:1")
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "test/build-run", "{}");
        assertEquals(503, r.status());
        assertTrue(body(r).contains("\"error\":\"test_build_unavailable\""));
    }

    @Test
    void testBuildRun_allowed_delegatesToService() {
        TestSandboxRunService testService = mock(TestSandboxRunService.class);
        when(testService.allowsCaller(any())).thenReturn(true);
        byte[] ok = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
        when(testService.buildRun(any(), any())).thenReturn(HelperRequestRouter.RoutedResponse.json(200, ok));
        HelperRequestRouter router = routerWithTestService(mock(HelperExecuteService.class), testService);

        HelperJob job = HelperJob.builder("job-ok", "python")
                .persistent(true)
                .callerImageRef("sq-local/node-test-emulator:1")
                .build();
        String token = router.register(job);

        // Prefix path (sub starts with test/) and the switch case share the same sub string.
        HelperRequestRouter.RoutedResponse viaPrefix = post(router, token, "test/build-run", "{}");
        assertEquals(200, viaPrefix.status());
        assertEquals("{\"ok\":true}", body(viaPrefix));

        HelperRequestRouter.RoutedResponse viaSwitch = post(router, token, "test/build-run", "{}");
        assertEquals(200, viaSwitch.status());
        assertEquals("{\"ok\":true}", body(viaSwitch));
        verify(testService, times(2)).buildRun(any(HelperJob.class), any());
    }

    @Test
    void mockExecute_hit_recordsAndNeverCallsExecuteService() throws Exception {
        HelperExecuteService executeService = mock(HelperExecuteService.class);
        HelperRequestRouter router = new HelperRequestRouter(mapper, llmProxy, executeService);

        HelperJob job = HelperJob.builder("job-hit", "python")
                .mockResponses(Map.of("fn", Map.of("x", 1)))
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "execute",
                "{\"portValue\":\"fn\",\"payload\":{\"a\":1}}");
        assertEquals(200, r.status());
        Map<?, ?> parsed = mapper.readValue(r.body(), Map.class);
        assertEquals(1, ((Map<?, ?>) parsed.get("result")).get("x"));

        assertEquals(1, job.mockInvocations().size());
        Map<String, Object> inv = job.mockInvocations().get(0);
        assertEquals("fn", inv.get("portValue"));
        assertEquals(Boolean.TRUE, inv.get("mocked"));
        assertNotNull(inv.get("payload"));
        assertNotNull(inv.get("at"));
        verifyNoInteractions(executeService);
    }

    @Test
    void mockExecute_miss_emptyResult() throws Exception {
        HelperExecuteService executeService = mock(HelperExecuteService.class);
        HelperRequestRouter router = new HelperRequestRouter(mapper, llmProxy, executeService);

        HelperJob job = HelperJob.builder("job-miss", "python")
                .mockResponses(new LinkedHashMap<>())
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "execute",
                "{\"portValue\":\"missing\"}");
        assertEquals(200, r.status());
        Map<?, ?> parsed = mapper.readValue(r.body(), Map.class);
        assertEquals(Map.of(), parsed.get("result"));

        assertEquals(1, job.mockInvocations().size());
        assertEquals(Boolean.FALSE, job.mockInvocations().get(0).get("mocked"));
        assertEquals("missing", job.mockInvocations().get(0).get("portValue"));
        verifyNoInteractions(executeService);
    }

    @Test
    void mockLlm_501() {
        HelperRequestRouter router = new HelperRequestRouter(mapper, llmProxy,
                mock(HelperExecuteService.class));
        HelperJob job = HelperJob.builder("job-llm", "python")
                .mockResponses(Map.of())
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "llm/chat", "{}");
        assertEquals(501, r.status());
        assertTrue(body(r).contains("\"error\":\"llm_disabled_in_test\""));
    }

    @Test
    void mockConnect_403() {
        HelperConnectService connectService = mock(HelperConnectService.class);
        HelperRequestRouter router = new HelperRequestRouter(mapper, llmProxy, null, connectService);
        HelperJob job = HelperJob.builder("job-conn", "python")
                .mockResponses(Map.of())
                .build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse r = post(router, token, "connect/open",
                "{\"containerName\":\"db\",\"port\":5432}");
        assertEquals(403, r.status());
        assertTrue(body(r).contains("\"error\":\"connect_disabled_in_test\""));
        verifyNoInteractions(connectService);
    }

    @Test
    void normalJob_executeUnaffected() {
        HelperExecuteService executeService = mock(HelperExecuteService.class);
        when(executeService.handle(any(HelperJob.class), any(String.class), nullable(Object.class),
                nullable(String.class), nullable(String.class)))
                .thenReturn(Map.of("result", List.of()));
        HelperRequestRouter router = new HelperRequestRouter(mapper, llmProxy, executeService);

        HelperJob job = HelperJob.builder("job-live", "python").build();
        String token = router.register(job);

        HelperRequestRouter.RoutedResponse exec = post(router, token, "execute",
                "{\"portValue\":\"fn\",\"payload\":{}}");
        assertEquals(200, exec.status());
        assertTrue(body(exec).contains("\"result\""));
        verify(executeService).handle(any(HelperJob.class), eq("fn"), any(),
                nullable(String.class), nullable(String.class));

        HelperRequestRouter.RoutedResponse init = router.route("GET", "init", token, new byte[0]);
        assertEquals(200, init.status());
        assertFalse(body(init).contains("llm_disabled_in_test"));
    }

    /** 10-arg constructor with the test-sandbox service; overlay/connect/cross-env legs unused. */
    private HelperRequestRouter routerWithTestService(HelperExecuteService executeService,
                                                      TestSandboxRunService testService) {
        return new HelperRequestRouter(mapper, llmProxy, executeService, null, null, null, null, null,
                8443, testService);
    }

    private static HelperRequestRouter.RoutedResponse post(HelperRequestRouter router, String token,
                                                           String sub, String json) {
        return router.route("POST", sub, token, json.getBytes(StandardCharsets.UTF_8));
    }

    private static String body(HelperRequestRouter.RoutedResponse r) {
        return new String(r.body(), StandardCharsets.UTF_8);
    }
}
