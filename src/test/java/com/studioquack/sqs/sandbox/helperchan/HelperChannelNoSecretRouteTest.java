package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.sandbox.oci.HelperBroker;
import com.studioquack.sqs.sandbox.secrets.OsSecretKeyStore;
import com.studioquack.sqs.sandbox.secrets.SecretKeyStore;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Proves the helper channel offers NO way for sandboxed (hostile) node code to resolve an arbitrary
 * secret reference. Secrets reach a container ONLY as the current node's own just-in-time-resolved
 * plaintext (inline context + per-job tmpfs {@code.env}); the daemon deliberately exposes no
 * "resolve this ref for me" endpoint, because such an endpoint would let one node's container pull
 * every secret bound anywhere in the environment.
 */
class HelperChannelNoSecretRouteTest {

    /** Canary planted in the registered job's context: any route leaking context plaintext trips it. */
    private static final String CANARY = "canary-plaintext-value-7f3d";

    /** Plausible attacker paths a hostile script would probe for a resolve/enumeration route. */
    private static final List<String> ATTACKER_PATHS = List.of(
            "resolve-secret",
            "resolve-secret/sec_0000000000000000000000aa:v1",
            "secret",
            "secrets",
            "secret/resolve",
            "secret/resolve/sec_0000000000000000000000aa:v1",
            "secret/get",
            "secret/get/sec_0000000000000000000000aa:v1",
            "secret/sec_0000000000000000000000aa:v1",
            "secret/list",
            "secrets/list",
            "secrets/resolve",
            "resolve/sec_0000000000000000000000aa:v1",
            "keystore",
            "keystore/resolve",
            "keystore/sec_0000000000000000000000aa:v1",
            "keystore/list",
            "keys",
            "keys/sec_0000000000000000000000aa:v1",
            "keychain",
            "keychain/get",
            "secretRef/resolve",
            "vault/secret",
            "os/secret/sec_0000000000000000000000aa:v1");

    private final ObjectMapper mapper = new ObjectMapper();

    private record CanaryJob(HelperRequestRouter router, String bearer) {}

    private CanaryJob routerWithCanaryJob() {
        HelperRequestRouter router = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(new AiModelProviderHolder(), mapper),
                null);
        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "nodeA");
        // A resolved secret would look exactly like this canary once bound inline — a route that
        // (wrongly) resolved or dumped context on an attacker path would echo it back.
        currentNode.put("password", CANARY);
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("workflowExecutionId", "wfe-1");
        ec.put("currentNodeId", "nodeA");
        ec.put("currentNode", currentNode);
        String bearer = router.register(HelperJob.builder("job-a", "python").executionContext(ec).build());
        return new CanaryJob(router, bearer);
    }

    @Test
    void no_attacker_path_resolves_or_leaks_with_a_valid_bearer() {
        CanaryJob canary = routerWithCanaryJob();
        HelperRequestRouter router = canary.router();
        String bearer = canary.bearer();
        byte[] body = "{\"ref\":\"sec_0000000000000000000000aa:v1\"}".getBytes(StandardCharsets.UTF_8);

        for (String path : ATTACKER_PATHS) {
            for (String method : List.of("GET", "POST")) {
                HelperRequestRouter.RoutedResponse r = router.route(method, path, bearer, body);
                String responseBody = new String(r.body(), StandardCharsets.UTF_8);
                assertEquals(404, r.status(),
                        method + " /v1/helper/" + path + " must be 404 (no resolve route), got "
                                + r.status() + " body=" + responseBody);
                assertFalse(responseBody.contains(CANARY),
                        method + " /v1/helper/" + path + " must never echo context plaintext");
                assertFalse(responseBody.contains("value-"),
                        method + " /v1/helper/" + path + " must never carry a resolved value");
            }
        }
    }

    @Test
    void an_unauthenticated_attacker_gets_401_on_the_same_paths() {
        HelperRequestRouter router = routerWithCanaryJob().router();
        for (String path : ATTACKER_PATHS) {
            HelperRequestRouter.RoutedResponse r = router.route("POST", path, "h_forged", new byte[0]);
            assertEquals(401, r.status(), "unknown bearer must 401 even on non-existent routes");
        }
    }

    /**
     * Structural guard that survives future edits: none of the three classes behind the helper
     * channel may hold (or be constructed with) a secret-resolving collaborator. A future
     * "resolve" route would need exactly such a wiring — and would fail here first.
     */
    @Test
    void helper_channel_classes_hold_no_secret_resolving_collaborator() {
        List<Class<?>> forbidden = List.of(SecretResolver.class, SecretKeyStore.class, OsSecretKeyStore.class);
        for (Class<?> cls : List.of(HelperRequestRouter.class, HelperExecuteService.class,
                HelperConnectService.class)) {
            for (Field f : cls.getDeclaredFields()) {
                for (Class<?> bad : forbidden) {
                    assertFalse(bad.isAssignableFrom(f.getType()),
                            cls.getSimpleName() + " must not declare field '" + f.getName()
                                    + "' of secret-resolving type " + f.getType().getSimpleName());
                }
            }
            for (Constructor<?> c : cls.getDeclaredConstructors()) {
                for (Class<?> paramType : c.getParameterTypes()) {
                    for (Class<?> bad : forbidden) {
                        assertFalse(bad.isAssignableFrom(paramType),
                                cls.getSimpleName() + " constructor must not take secret-resolving "
                                        + paramType.getSimpleName());
                    }
                }
            }
        }
    }

    /**
     * The broker payload asserted here is the EXACT script {@link HelperBroker#plantScript()} would
     * plant into a container (base64-decoded back out of the plant command) — not a second copy
     * that could drift from what actually runs.
     */
    @Test
    void the_planted_broker_script_has_no_secret_or_keystore_route() {
        String plant = HelperBroker.plantScript();
        String marker = "printf %s '";
        int start = plant.indexOf(marker);
        int end = plant.indexOf("' | base64 -d", start);
        assertTrue(start >= 0 && end > start, "plant script shape changed — re-pin this extraction");
        String brokerSource = new String(
                Base64.getDecoder().decode(plant.substring(start + marker.length(), end)),
                StandardCharsets.UTF_8);
        assertFalse(brokerSource.isBlank(), "broker payload must decode to the real script");

        String lower = brokerSource.toLowerCase(java.util.Locale.ROOT);
        for (String token : List.of("secret", "keystore", "keychain")) {
            if (lower.contains(token)) {
                fail("sq_helper_broker.py must not reference '" + token + "' — the broker relays "
                        + "/v1/helper/* verbatim and needs no secret awareness of its own");
            }
        }
    }
}
