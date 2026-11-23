package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * {@link ExecChannelClient} against a mock server: asserts the EXACT {@code /exec-commands} poll
 * path + query, that a dispatch is parsed and run via the {@link DispatchExecutor}, that the result
 * is POSTed to {@code /exec-results/{dispatchId}} with the {@code X-SQ-Delivery-Token} header, and
 * that re-delivery of the same {@code dispatchId} is de-duplicated.
 */
class ExecChannelClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String BEARER = "sqvm_bearer_ec";

    private String base(MockBackend be) {
        return be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
    }

    @Test
    void poll_emits_exact_path_dispatches_and_posts_result_with_delivery_token() throws Exception {
        AtomicReference<String> seenRequest = new AtomicReference<>();
        DispatchExecutor executor = (requestJson, deadline) -> {
            seenRequest.set(requestJson);
            return "{\"status\":\"COMPLETED\",\"outputVariables\":{}}";
        };

        try (MockBackend be = new MockBackend()) {
            String p = "/be/v1/sqvm/environments/" + ENV_ID;
            be.stubJson(p + "/exec-commands", 200,
                    "{\"envId\":\"" + ENV_ID + "\",\"commands\":[{"
                            + "\"dispatchId\":\"d1\",\"envId\":\"" + ENV_ID + "\",\"jobKey\":5,"
                            + "\"deliveryToken\":\"tok-1\",\"request\":\"{\\\"jobKey\\\":5}\"}]}");
            be.stubJson(p + "/exec-results/d1", 200, "{\"applied\":true,\"dispatchId\":\"d1\"}");

            ExecChannelClient client = new ExecChannelClient(
                    base(be), BEARER, "inst-1", new ChannelHttpClients(null), MAPPER, executor,
                    900_000L, 15_000L, 600_000L);

            int received = client.pollAndDispatchOnce(32, 15_000L);
            assertEquals(1, received);

            MockBackend.Recorded poll = be.firstMatching("/exec-commands");
            assertNotNull(poll);
            assertEquals("GET", poll.method());
            assertEquals("limit=32&waitMs=15000", poll.rawQuery());
            assertEquals("Bearer " + BEARER, poll.headers().get("authorization"));
            assertEquals("inst-1", poll.headers().get("x-sqvm-instance-id"));

            assertEquals("{\"jobKey\":5}", seenRequest.get(), "executor ran the verbatim request payload");

            MockBackend.Recorded result = be.firstMatching("/exec-results/d1");
            assertNotNull(result, "result POSTed to exec-results/{dispatchId}");
            assertEquals("POST", result.method());
            assertEquals("tok-1", result.headers().get("x-sq-delivery-token"),
                    "delivery token echoed in the X-SQ-Delivery-Token header");
            Map<?, ?> resultBody = MAPPER.readValue(result.body(), Map.class);
            assertEquals("COMPLETED", resultBody.get("status"));
        }
    }

    @Test
    void redelivery_of_same_dispatchId_is_deduplicated() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        DispatchExecutor executor = (requestJson, deadline) -> {
            runs.incrementAndGet();
            return "{\"status\":\"COMPLETED\"}";
        };

        try (MockBackend be = new MockBackend()) {
            be.stubJson("/be/v1/sqvm/environments/" + ENV_ID + "/exec-results/d9", 200, "{\"applied\":true}");
            ExecChannelClient client = new ExecChannelClient(
                    base(be), BEARER, null, new ChannelHttpClients(null), MAPPER, executor,
                    900_000L, 15_000L, 600_000L);

            ExecChannelClient.DispatchEvent ev = new ExecChannelClient.DispatchEvent(
                    "d9", ENV_ID, 5L, "tok-9", "{\"jobKey\":5}", null);
            client.handleDispatch(ev);
            client.handleDispatch(ev); // re-delivery

            assertEquals(1, runs.get(), "the same dispatchId executes exactly once");
            assertEquals(1, be.countMatching("/exec-results/d9"), "result posted once");
        }
    }

    @Test
    void missing_executor_posts_error() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson("/be/v1/sqvm/environments/" + ENV_ID + "/exec-results/d3/error", 200, "{\"applied\":true}");
            ExecChannelClient client = new ExecChannelClient(
                    base(be), BEARER, null, new ChannelHttpClients(null), MAPPER, null,
                    900_000L, 15_000L, 600_000L);

            client.handleDispatch(new ExecChannelClient.DispatchEvent(
                    "d3", ENV_ID, 1L, "tok-3", "{}", null));

            MockBackend.Recorded err = be.firstMatching("/exec-results/d3/error");
            assertNotNull(err, "a missing executor reports a structured error, not a silent drop");
            Map<?, ?> body = MAPPER.readValue(err.body(), Map.class);
            assertNotNull(body.get("error"), "error message included in the /error body");
        }
    }
}
