package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The helper-channel bearer is scoped to exactly ONE job. A sandboxed script holds its job's random
 * {@code h_…} token; it must never read or mutate a sibling job's state — not by asking for a key
 * that only exists on the other job, not by smuggling the other job's id into a body or path, and
 * not by replaying its token after revocation (even while the sibling job is still live).
 */
class HelperChannelBearerScopeTest {

    /** Every routed surface a revoked token must be dead on (auth precedes dispatch). */
    private static final List<String> ALL_ROUTES = List.of(
            "init", "inputs", "input/a-key", "execution-context", "full-context", "current-node",
            "current-node-id", "workflow-execution-id", "parameters", "connections", "result",
            "fail", "metric", "log", "dispatch", "transform-context", "execute", "engine-ip",
            "llm/chat", "connect/open", "no-such-endpoint");

    private final ObjectMapper mapper = new ObjectMapper();
    private HelperRequestRouter router;
    private String tokenA;
    private String tokenB;

    private static HelperJob job(String id, String nodeId, Map<String, byte[]> inputs) {
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-" + id);
        ec.put("currentNodeId", nodeId);
        ec.put("currentNode", Map.of("id", nodeId));
        return HelperJob.builder(id, "python").executionContext(ec).inputs(inputs).build();
    }

    @BeforeEach
    void setUp() {
        router = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(new AiModelProviderHolder(), mapper),
                null);
        tokenA = router.register(job("job-a", "nodeA", Map.of("a-key", "A-BYTES".getBytes(StandardCharsets.UTF_8))));
        tokenB = router.register(job("job-b", "nodeB", Map.of("b-key", "B-BYTES".getBytes(StandardCharsets.UTF_8))));
    }

    private HelperRequestRouter.RoutedResponse call(String token, String method, String sub, String jsonBody) {
        return router.route(method, sub, token,
                jsonBody == null ? new byte[0] : jsonBody.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void a_token_reads_only_its_own_jobs_inputs() {
        HelperRequestRouter.RoutedResponse own = call(tokenA, "GET", "input/a-key", null);
        assertEquals(200, own.status());
        assertArrayEquals("A-BYTES".getBytes(StandardCharsets.UTF_8), own.body());

        // A key that exists ONLY on job B is a 404 for token A — the handler never sees job B,
        // so from A's container the key simply does not exist (no cross-job read, no oracle).
        HelperRequestRouter.RoutedResponse foreign = call(tokenA, "GET", "input/b-key", null);
        assertEquals(404, foreign.status(),
                "token A must not read a key that lives only on job B");

        assertEquals(200, call(tokenB, "GET", "input/b-key", null).status());
        assertEquals(404, call(tokenB, "GET", "input/a-key", null).status(),
                "token B must not read a key that lives only on job A");

        // Context endpoints are equally bound: each token serves its own node's identity only.
        assertTrue(new String(call(tokenA, "GET", "current-node-id", null).body(), StandardCharsets.UTF_8)
                .contains("nodeA"));
        assertTrue(new String(call(tokenB, "GET", "current-node-id", null).body(), StandardCharsets.UTF_8)
                .contains("nodeB"));
    }

    @Test
    void a_smuggled_job_id_never_cross_acts() {
        // Body-smuggled job id: the result lands on the AUTHORIZED job (A), B stays untouched.
        assertEquals(200, call(tokenA, "POST", "result",
                "{\"jobId\":\"job-b\",\"ok\":true,\"items\":[{\"port\":\"out\",\"value\":\"PWNED\"}]}").status());
        assertEquals("PWNED", router.lookup(tokenA).orElseThrow().result().items().get(0).value());
        assertNull(router.lookup(tokenB).orElseThrow().result(),
                "a body-carried jobId must never retarget the write at job B");

        assertEquals(200, call(tokenA, "POST", "transform-context",
                "{\"jobId\":\"job-b\",\"executionContext\":{\"injected\":true}}").status());
        assertEquals(Map.of("injected", true), router.lookup(tokenA).orElseThrow().transformedContext());
        assertNull(router.lookup(tokenB).orElseThrow().transformedContext(),
                "transform-context must not be retargetable at job B");

        assertEquals(200, call(tokenA, "POST", "fail",
                "{\"jobId\":\"job-b\",\"errorCode\":\"PWN\"}").status());
        assertNull(router.lookup(tokenB).orElseThrow().failure(),
                "fail must not be retargetable at job B");

        // Path-smuggled job id: the router matches endpoint prefixes only — a job-id prefix is
        // just an unknown route, and a query suffix becomes part of the (missing) input key.
        assertEquals(404, call(tokenA, "GET", "job-b/input/a-key", null).status());
        assertEquals(404, call(tokenA, "GET", "input/a-key?jobId=job-b", null).status());
    }

    @Test
    void after_revoke_every_route_401s_while_the_sibling_job_lives() {
        router.revoke(tokenA);

        for (String sub : ALL_ROUTES) {
            for (String method : List.of("GET", "POST")) {
                HelperRequestRouter.RoutedResponse r = call(tokenA, method, sub, "{}");
                assertEquals(401, r.status(),
                        method + " /v1/helper/" + sub + " must 401 for a revoked token, got "
                                + r.status());
                assertTrue(new String(r.body(), StandardCharsets.UTF_8).contains("unauthorized"),
                        "revoked token gets the standard unauthorized body on " + sub);
            }
        }
        // The sibling job is untouched by A's revocation and keeps serving.
        assertEquals(200, call(tokenB, "GET", "init", null).status());
        assertEquals(200, call(tokenB, "GET", "input/b-key", null).status());

        // Re-presenting the revoked token (exact, padded, or re-cased) never resurrects it.
        assertEquals(401, call(tokenA, "GET", "init", null).status());
        assertEquals(401, call(tokenA + " ", "GET", "init", null).status());
        assertEquals(401, call(tokenA.toUpperCase(java.util.Locale.ROOT), "GET", "init", null).status());
    }

    @Test
    void persistent_registrations_follow_the_same_single_job_binding() {
        String tokenC = router.registerPersistent(HelperJob.builder("job-c", "python")
                .persistent(true)
                .executionContext(Map.of("workflowExecutionId", "wfe-job-c", "currentNodeId", "nodeC"))
                .inputs(Map.of("c-key", "C-BYTES".getBytes(StandardCharsets.UTF_8)))
                .build());

        assertEquals(200, call(tokenC, "GET", "input/c-key", null).status());
        assertEquals(404, call(tokenC, "GET", "input/a-key", null).status(),
                "a persistent token reads only its own job's inputs");
        assertEquals(404, call(tokenB, "GET", "input/c-key", null).status(),
                "job B cannot read the persistent job's inputs either");

        router.revoke(tokenC);
        for (String sub : ALL_ROUTES) {
            assertEquals(401, call(tokenC, "POST", sub, "{}").status(),
                    "revoked persistent token must be as dead as a revoked one-shot token");
        }
        assertEquals(200, call(tokenB, "GET", "init", null).status(), "siblings stay live");
    }

    /**
     * Structural pin: the router's entrypoint has no job-id slot, and every state-touching handler
     * receives the already-authorized {@link HelperJob} — never a caller-supplied id to look one
     * up. The two job-less handlers are pinned with their exact signatures: they touch no per-job
     * state (LLM proxy forward; same-owner peer directory read), so they need no job — and any
     * future variant that takes one (or worse, a job id String) fails this test.
     */
    @Test
    void handlers_receive_only_the_authorized_job() throws Exception {
        Method route = HelperRequestRouter.class.getMethod(
                "route", String.class, String.class, String.class, byte[].class);
        assertEquals(4, route.getParameterCount(),
                "route(method, sub, bearer, body) — there is no job-id parameter to abuse");

        // handler name → expected job-less signature, for the ONLY methods allowed no HelperJob.
        Map<String, List<Class<?>>> jobLessAllowed = Map.of(
                "handleLlm", List.of(String.class, String.class, byte[].class),
                "handleEngineIp", List.of(byte[].class));

        for (Method m : HelperRequestRouter.class.getDeclaredMethods()) {
            boolean handler = m.getName().startsWith("handle") || m.getName().equals("contextValue");
            if (!handler) {
                continue;
            }
            Class<?>[] params = m.getParameterTypes();
            if (params.length > 0 && params[0] == HelperJob.class) {
                continue; // authorized-job-first: the safe shape
            }
            List<Class<?>> allowed = jobLessAllowed.get(m.getName());
            if (allowed != null && List.of(params).equals(allowed)) {
                continue; // pinned job-less handler
            }
            fail("handler '" + m.getName() + "' must take the authorized HelperJob as its first "
                    + "parameter (never a job id); found " + java.util.Arrays.toString(params));
        }
    }
}
