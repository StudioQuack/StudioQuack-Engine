package com.studioquack.sqs.channel.wg.mesh;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.MockBackend;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CrossEnvAuthorizationClient} against the {@link MockBackend} harness: the on-miss
 * authorize call rides {@code POST {environmentBase}/cross-env/authorize} with the enrollment
 * bearer, caches allows for the backend ttl and denies for the capped ttl, and FAILS CLOSED as
 * {@link CrossEnvAuthorizationClient.Outcome#UNAVAILABLE} when the backend is unreachable or
 * answers non-200 — never throwing into the caller path.
 */
class CrossEnvAuthorizationClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SELF_ENV = "env-self-1";
    private static final String BEARER = "sqvm_bearer_authz";

    private static SqsChannelProperties props() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId(SELF_ENV);
        props.setBearer(BEARER);
        return props;
    }

    /** The client with an UNBOUND outbound-base holder → the public environment base is used. */
    private static CrossEnvAuthorizationClient client(String backendBaseUrl) {
        return new CrossEnvAuthorizationClient(props(), backendBaseUrl, new OutboundBaseHolder(),
                new ChannelHttpClients(null), MAPPER);
    }

    private static String authorizePath(String envId) {
        return "/be/v1/sqvm/environments/" + envId + "/cross-env/authorize";
    }

    @Test
    void allowedAnswer_isReturned_andCachedForTheTtl() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(authorizePath(SELF_ENV), 200, "{\"allowed\":true,\"ttlSeconds\":300}");
            CrossEnvAuthorizationClient client = client(be.baseUrl());

            assertEquals(CrossEnvAuthorizationClient.Outcome.ALLOWED,
                    client.authorize(SELF_ENV, "env-caller-9", "execute"));
            assertEquals(CrossEnvAuthorizationClient.Outcome.ALLOWED,
                    client.authorize(SELF_ENV, "env-caller-9", "execute"));

            assertEquals(1, be.countMatching("/cross-env/authorize"),
                    "the second call is served from the cache — no extra control-plane hop");
            MockBackend.Recorded req = be.firstMatching("/cross-env/authorize");
            assertEquals("Bearer " + BEARER, req.headers().get("authorization"),
                    "the daemon's own enrollment bearer authenticates the call");
            Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
            assertEquals("env-caller-9", body.get("callerEnvId"));
            assertEquals("execute", body.get("scope"));
        }
    }

    @Test
    void deniedAnswer_isReturned_andCached() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(authorizePath(SELF_ENV), 200, "{\"allowed\":false,\"ttlSeconds\":30}");
            CrossEnvAuthorizationClient client = client(be.baseUrl());

            assertEquals(CrossEnvAuthorizationClient.Outcome.DENIED,
                    client.authorize(SELF_ENV, "env-caller-9", "execute"));
            assertEquals(CrossEnvAuthorizationClient.Outcome.DENIED,
                    client.authorize(SELF_ENV, "env-caller-9", "execute"));
            assertEquals(1, be.countMatching("/cross-env/authorize"),
                    "a deny is cached too (capped at 30s)");
        }
    }

    @Test
    void unreachableBackend_isUnavailable_neverThrows() {
        // Port 1 is closed: a connect-refused transport error with nothing cached.
        CrossEnvAuthorizationClient client = client("http://127.0.0.1:1");

        assertEquals(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE,
                client.authorize(SELF_ENV, "env-caller-9", "execute"),
                "backend unreachable + no cache → UNAVAILABLE (a deny upstream, never a pass)");
    }

    @Test
    void non200Answer_isUnavailable() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson(authorizePath(SELF_ENV), 500, "{\"errorCode\":\"Error_Internal\"}");
            CrossEnvAuthorizationClient client = client(be.baseUrl());

            assertEquals(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE,
                    client.authorize(SELF_ENV, "env-caller-9", "execute"));
        }
    }

    @Test
    void cachedAllow_stillServes_whileTheBackendIsDown() throws Exception {
        CrossEnvAuthorizationClient client;
        try (MockBackend be = new MockBackend()) {
            be.stubJson(authorizePath(SELF_ENV), 200, "{\"allowed\":true,\"ttlSeconds\":300}");
            client = client(be.baseUrl());
            assertEquals(CrossEnvAuthorizationClient.Outcome.ALLOWED,
                    client.authorize(SELF_ENV, "env-caller-9", "execute"));
        }
        // The backend is now closed; the fresh cached allow must still serve (spec).
        assertEquals(CrossEnvAuthorizationClient.Outcome.ALLOWED,
                client.authorize(SELF_ENV, "env-caller-9", "execute"),
                "a cached entry may still serve while the backend is unreachable");
    }

    @Test
    void pathEnvId_isAParameter_theTrailingSegmentIsSwapped() throws Exception {
        try (MockBackend be = new MockBackend()) {
            // The later-slice CALLER pre-check reuses the same endpoint with the PEER's id.
            be.stubJson(authorizePath("env-peer-7"), 200, "{\"allowed\":true,\"ttlSeconds\":60}");
            CrossEnvAuthorizationClient client = client(be.baseUrl());

            assertEquals(CrossEnvAuthorizationClient.Outcome.ALLOWED,
                    client.authorize("env-peer-7", SELF_ENV, "execute"));
            assertEquals(1, be.countMatching(authorizePath("env-peer-7")),
                    "the request went to the PEER's environment path, not this daemon's own");
            assertEquals(0, be.countMatching(authorizePath(SELF_ENV)));
        }
    }

    @Test
    void blankParameters_areRefused_withoutACall() throws Exception {
        try (MockBackend be = new MockBackend()) {
            CrossEnvAuthorizationClient client = client(be.baseUrl());

            assertEquals(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE,
                    client.authorize(" ", "env-caller-9", "execute"));
            assertEquals(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE,
                    client.authorize(SELF_ENV, null, "execute"));
            assertEquals(CrossEnvAuthorizationClient.Outcome.UNAVAILABLE,
                    client.authorize(SELF_ENV, "env-caller-9", ""));
            assertTrue(be.requests().isEmpty(), "a malformed call never reaches the wire");
        }
    }
}
