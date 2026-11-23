package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proves the helper router serves MULTIPLE persistent, per-request jobs on ONE stateful instance in
 * parallel, WITHOUT revoking between them: each request gets its own random {@code h_…} bearer,
 * posts its own result + serves its own context, and only its OWN token is revoked when it finishes
 * — the instance and its sibling requests stay open.
 */
class HelperChannelPersistentTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private HelperRequestRouter router;

    @BeforeEach
    void setUp() {
        router = new HelperRequestRouter(mapper,
                new com.studioquack.sqs.aimodel.AiModelProviderProxy(
                        new com.studioquack.sqs.aimodel.AiModelProviderHolder(), mapper), null);
    }

    private String registerRequest(String jobId, String nodeId) {
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", nodeId);
        return router.registerPersistent(
                HelperJob.builder(jobId, "python").persistent(true).executionContext(ec).build());
    }

    @Test
    void three_parallel_requests_on_one_instance_each_serve_and_record_without_cross_revoke() {
        String a = registerRequest("req-a", "nodeA");
        String b = registerRequest("req-b", "nodeB");
        String c = registerRequest("req-c", "nodeC");
        assertEquals(3, router.activeJobs(), "three concurrent per-request tokens coexist on one instance");

        post(a, "result", "{\"ok\":true,\"items\":[{\"port\":\"out\",\"value\":\"A\"}]}");
        post(b, "result", "{\"ok\":true,\"items\":[{\"port\":\"out\",\"value\":\"B\"}]}");
        post(c, "result", "{\"ok\":true,\"items\":[{\"port\":\"out\",\"value\":\"C\"}]}");

        assertEquals("A", router.lookup(a).orElseThrow().result().items().get(0).value());
        assertEquals("B", router.lookup(b).orElseThrow().result().items().get(0).value());
        assertEquals("C", router.lookup(c).orElseThrow().result().items().get(0).value());

        assertTrue(get(a, "current-node-id").contains("nodeA"));
        assertTrue(get(c, "current-node-id").contains("nodeC"));
        assertTrue(router.lookup(b).orElseThrow().persistent(), "registered as a persistent request");

        router.revoke(a);
        assertEquals(2, router.activeJobs(), "only the finished request's token is revoked");
        assertTrue(router.lookup(b).isPresent(), "sibling request b still open");
        assertTrue(router.lookup(c).isPresent(), "sibling request c still open");
    }

    @Test
    void a_persistent_request_can_post_repeatedly_without_being_revoked() {
        String t = registerRequest("req-loop", "nodeX");
        assertEquals(200, post(t, "log", "{\"message\":\"step 1\"}").status());
        assertEquals(200, post(t, "metric", "{\"name\":\"progress\",\"value\":0.5}").status());
        assertEquals(200, post(t, "dispatch", "{\"portValue\":\"tool\",\"payload\":{}}").status());
        assertEquals(200, post(t, "result", "{\"ok\":true,\"items\":[]}").status());
        assertTrue(router.lookup(t).isPresent(), "token stays valid across many mid-run calls");
        HelperJob job = router.lookup(t).orElseThrow();
        assertEquals(1, job.logs().size());
        assertEquals(1, job.metrics().size());
        assertEquals(1, job.dispatch().size());
    }

    private String get(String token, String sub) {
        HelperRequestRouter.RoutedResponse r = router.route("GET", sub, token, new byte[0]);
        assertEquals(200, r.status(), "GET " + sub);
        return new String(r.body(), StandardCharsets.UTF_8);
    }

    private HelperRequestRouter.RoutedResponse post(String token, String sub, String json) {
        return router.route("POST", sub, token, json.getBytes(StandardCharsets.UTF_8));
    }
}
