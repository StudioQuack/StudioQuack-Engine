package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code studioquack.execute} in the JavaScript / TypeScript helper ({@code node.js}): same wire
 * body as Python ({@code portValue}, {@code payload}, optional {@code nodeId} / {@code envId}) on
 * {@code POST /v1/helper/execute}, and {@code []} — never a rejection — when the call fails.
 */
class NodeHelperExecuteTransportTest {

    @Test
    void execute_withEnvId_postsWebhookShapedPayloadAndReturnsResult(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200, "{\"result\":{\"approved\":true}}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    const order = { messageName: 'order-approved', correlationKey: 'order-42',
                                    businessKey: 'ORD-42', variables: { amount: 99 } };
                    studioquack.execute('approve_order', order, null, 'env-b')
                      .then((r) => console.log('RETURNED ' + JSON.stringify(r)));
                    """);
            assertTrue(out.contains("RETURNED {\"approved\":true}"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/execute"), seen);
            assertTrue(seen.contains("Bearer job-token"), seen);
            assertTrue(seen.contains("\"portValue\":\"approve_order\""), seen);
            assertTrue(seen.contains("\"envId\":\"env-b\""), seen);
            assertTrue(seen.contains("\"messageName\":\"order-approved\""), seen);
            assertFalse(seen.contains("\"nodeId\""), "null nodeId must not be sent: " + seen);
        }
    }

    @Test
    void execute_withoutEnvId_sendsNoEnvId(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200, "{\"result\":[]}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    studioquack.execute('log', { sum: 3 })
                      .then((r) => console.log('RETURNED ' + JSON.stringify(r)));
                    """);
            assertTrue(out.contains("RETURNED []"), out);
            assertFalse(String.join("\n", stub.requests()).contains("envId"), stub.requests().toString());
        }
    }

    @Test
    void execute_refusedOrUnreachable_resolvesToEmptyList(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 403,
                "{\"errorCode\":\"Error_CrossEnvNotGranted\"}")) {
            String refused = runHelperScript(tmp, socketPath.toString(), """
                    studioquack.execute('approve_order', {}, null, 'env-b')
                      .then((r) => console.log('RETURNED ' + JSON.stringify(r)), () => console.log('REJECTED'));
                    """);
            assertTrue(refused.contains("RETURNED []"), refused);
        }
        String unreachable = runHelperScript(tmp, "/tmp/sq-helper-unreachable-invalid.sock", """
                studioquack.execute('log', {})
                  .then((r) => console.log('RETURNED ' + JSON.stringify(r)), () => console.log('REJECTED'));
                """);
        assertTrue(unreachable.contains("RETURNED []"), unreachable);
    }

    /** Runs the node.js preamble plus {@code body} under node and returns its combined output. */
    private static String runHelperScript(Path tmp, String helperSocket, String body) throws Exception {
        String helper = loadHelper().replace("__SQ_CONTEXT__", "");
        Path script = tmp.resolve("execute_transport_smoke.js");
        Files.writeString(script, helper + "\n" + body, StandardCharsets.UTF_8);

        ProcessBuilder pb = new ProcessBuilder("node", script.toString());
        pb.environment().remove("SQVM_HELPER_URL");
        pb.environment().put("SQ_HELPER_SOCKET", helperSocket);
        pb.environment().put("SQ_HELPER_BEARER", "job-token");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "node smoke timed out; output=" + out);
        assertNotEquals("", out.trim(), "node smoke produced no output");
        return out;
    }

    private static String loadHelper() throws IOException {
        try (InputStream in = NodeHelperExecuteTransportTest.class
                .getResourceAsStream("/sandbox/helpers/node.js")) {
            if (in == null) {
                throw new IOException("missing classpath resource /sandbox/helpers/node.js");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean nodeAvailable() {
        try {
            Process p = new ProcessBuilder("node", "-e", "process.exit(0)").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
