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
 * Guards the {@code studioquack.execute} transport contract: "the helper channel is unreachable"
 * and "the daemon matched no connection on this port" are DIFFERENT failures and must stay
 * distinguishable. While both collapsed into an empty list, a sandbox→daemon network fault surfaced
 * several layers up as an agent-level "the platform did not route the provider's output back",
 * which sends every investigation to the workflow graph instead of the transport.
 */
class PythonHelperExecuteTransportTest {

    /** A path that does not exist to simulate unreachable socket. */
    private static final String UNREACHABLE = "/tmp/sq-helper-unreachable-invalid.sock";

    @Test
    void execute_unreachableHelperChannel_failsLoudlyNamingTheTarget(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        String out = runHelperScript(tmp, UNREACHABLE, """
                try:
                    studioquack.execute("model", {"messages": []})
                    print("NO_RAISE")
                except RuntimeError as exc:
                    print("RAISED " + str(exc))
                """);

        assertTrue(out.contains("RAISED"),
                "an unreachable helper channel must raise, never return an empty result: " + out);
        assertTrue(out.contains("studioquack.execute('model')"),
                "the error must name the FUNCTION port that failed: " + out);
        assertTrue(out.contains("could not reach the daemon helper channel"),
                "the error must say the transport failed, not that the provider was empty: " + out);
        assertTrue(out.contains(UNREACHABLE),
                "the error must name the target so the host alias/port is diagnosable: " + out);
    }

    @Test
    void execute_daemonMatchedNoConnection_stillReturnsEmptyWithoutRaising(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        // Exactly what the daemon returns when the current node's connectionList carries no entry
        // for the requested port — a real wiring gap, not a transport fault.
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200, "{\"result\":[]}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    result = studioquack.execute("model", {"messages": []})
                    print("RETURNED " + repr(result))
                    """);
            assertTrue(out.contains("RETURNED []"),
                    "an unmatched port must stay a plain empty result, not an exception: " + out);
            assertTrue(stub.requests().stream().anyMatch(r -> r.contains("/v1/helper/execute")),
                    "the call must still go to the canonical execute path: " + stub.requests());
        }
    }

    @Test
    void execute_returnsTheProviderResultOverTheSocket(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200,
                "{\"result\":{\"role\":\"assistant\",\"content\":\"hi\"}}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    result = studioquack.execute("model", {"messages": [{"role": "user", "content": "ping"}]})
                    print("RETURNED " + repr(result))
                    """);
            assertTrue(out.contains("'content': 'hi'"),
                    "the provider's result must round-trip back to the caller: " + out);
            assertTrue(stub.requests().stream().anyMatch(r -> r.contains("Bearer job-token")),
                    "the per-job bearer must still authenticate every call: " + stub.requests());
            assertTrue(stub.requests().stream().anyMatch(r -> r.contains("ping")),
                    "the payload must reach the daemon intact: " + stub.requests());
        }
    }

    @Test
    void execute_withEnv_postsEnvIdInTheWireBody(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200, "{\"result\":[]}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    result = studioquack.execute("model", {"messages": []}, env="prod-eu")
                    print("RETURNED " + repr(result))
                    """);
            assertTrue(out.contains("RETURNED []"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("\"envId\": \"prod-eu\""),
                    "execute(env=...) must put envId on the wire for the daemon to resolve: " + seen);
        }
    }

    @Test
    void execute_withoutEnv_sendsNoEnvId_localPathUnchanged(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200, "{\"result\":[]}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    result = studioquack.execute("model", {"messages": []})
                    print("RETURNED " + repr(result))
                    """);
            assertTrue(out.contains("RETURNED []"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/execute"), seen);
            assertFalse(seen.contains("/v1/helper/execute/stream"),
                    "stream=False must keep posting the atomic execute path: " + seen);
            assertTrue(!seen.contains("envId"),
                    "omitting env must keep the wire body exactly as before (no envId key): " + seen);
        }
    }

    @Test
    void execute_unsetHelperSocket_namesMissingBrokerAndPython3(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        String out = runHelperScript(tmp, null, """
                try:
                    studioquack.execute("model", {"messages": []})
                    print("NO_RAISE")
                except RuntimeError as exc:
                    print("RAISED " + str(exc))
                """);

        assertTrue(out.contains("RAISED"), "unset SQ_HELPER_SOCKET must raise: " + out);
        assertTrue(out.contains("no helper broker"),
                "error must name the missing helper broker: " + out);
        assertTrue(out.contains("python3"),
                "error must name python3 as the required runtime: " + out);
    }

    @Test
    void pythonHelperResource_emitPartialAndStreamExecuteUseUnixHelperPaths() throws Exception {
        String helper = loadHelper();
        assertTrue(helper.contains("/v1/helper/emit-partial"),
                "emit_partial must POST the dedicated helper path, not dispatch");
        assertTrue(helper.contains("/v1/helper/execute/stream"),
                "execute(stream=True) must POST the SSE helper path");
        assertFalse(helper.contains("emitPartial"),
                "emit_partial must not reuse the dispatch {emitPartial} envelope: " + helper);
        assertFalse(helper.contains("import requests"),
                "the preamble must stay requests-free; streaming uses http.client over AF_UNIX");
    }

    @Test
    void emit_partial_postsEmitPartialPathWithDeltaAndChannel(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200, "{\"delivered\":true}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    ok = studioquack.emit_partial("hel", channel="answer")
                    print("DELIVERED " + repr(ok))
                    """);
            assertTrue(out.contains("DELIVERED True"), "delivered=true must return True: " + out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/emit-partial"),
                    "emit_partial must POST /v1/helper/emit-partial: " + seen);
            assertTrue(seen.contains("\"delta\""), "body must carry delta: " + seen);
            assertTrue(seen.contains("\"channel\""), "body must carry channel: " + seen);
            assertTrue(seen.contains("answer"), "default/explicit channel must be on the wire: " + seen);
            assertFalse(seen.contains("/v1/helper/dispatch"),
                    "emit_partial must not post to /v1/helper/dispatch: " + seen);
            assertFalse(seen.contains("emitPartial"),
                    "emit_partial must not send the old dispatch emitPartial key: " + seen);
        }
    }

    @Test
    void emit_partial_noSocketOrUnreachable_returnsFalseWithoutRaising(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        String missing = runHelperScript(tmp, null, """
                try:
                    print("NO_SOCKET " + repr(studioquack.emit_partial("x")))
                except Exception as exc:
                    print("RAISED " + str(exc))
                """);
        assertTrue(missing.contains("NO_SOCKET False"),
                "no helper socket must no-op as False: " + missing);
        assertFalse(missing.contains("RAISED"), "emit_partial must never raise: " + missing);

        String unreachable = runHelperScript(tmp, UNREACHABLE, """
                try:
                    print("UNREACHABLE " + repr(studioquack.emit_partial("x")))
                except Exception as exc:
                    print("RAISED " + str(exc))
                """);
        assertTrue(unreachable.contains("UNREACHABLE False"),
                "an unreachable socket must swallow the fault as False: " + unreachable);
        assertFalse(unreachable.contains("RAISED"), "emit_partial must never raise: " + unreachable);
    }

    @Test
    void execute_stream_unreachableHelperChannel_failsLoudlyNamingTheTarget(@TempDir Path tmp)
            throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        String out = runHelperScript(tmp, UNREACHABLE, """
                try:
                    list(studioquack.execute("model", {"messages": []}, stream=True))
                    print("NO_RAISE")
                except RuntimeError as exc:
                    print("RAISED " + str(exc))
                """);

        assertTrue(out.contains("RAISED"),
                "an unreachable helper channel must raise, never return an empty result: " + out);
        assertFalse(out.contains("NO_RAISE"), out);
        assertTrue(out.contains("studioquack.execute('model')"),
                "the error must name the FUNCTION port that failed: " + out);
        assertTrue(out.contains("could not reach the daemon helper channel"),
                "the error must say the transport failed, not that the provider was empty: " + out);
        assertTrue(out.contains(UNREACHABLE),
                "the error must name the target so the host alias/port is diagnosable: " + out);
    }

    @Test
    void execute_stream_yieldsSsePartialThenCompleteFrames(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        String sse = """
                event: partial
                data: {"delta":"hel"}

                event: partial
                data: {"delta":"lo"}

                event: complete
                data: {"result":"hello"}

                """;
        try (UnixHttpStub stub = UnixHttpStub.servingSse(socketPath, sse)) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    chunks = list(studioquack.execute("model", {"messages": [{"role": "user", "content": "ping"}]}, stream=True))
                    print("N " + str(len(chunks)))
                    print("CHUNKS " + repr(chunks))
                    """);
            assertTrue(out.contains("N 3"), "partial, partial, complete must all yield: " + out);
            assertTrue(out.contains("'delta': 'hel'"), "first partial frame must round-trip: " + out);
            assertTrue(out.contains("'delta': 'lo'"), "second partial frame must round-trip: " + out);
            assertTrue(out.contains("'result': 'hello'"), "complete payload must yield last: " + out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/execute/stream"),
                    "stream=True must POST the SSE path: " + seen);
            assertTrue(seen.contains("Accept: text/event-stream")
                            || seen.contains("accept: text/event-stream"),
                    "stream=True must ask for SSE: " + seen);
            assertTrue(seen.contains("Bearer job-token"),
                    "the per-job bearer must still authenticate every call: " + seen);
            assertTrue(seen.contains("ping"),
                    "the payload must reach the daemon intact: " + seen);
            assertTrue(seen.contains("\"portValue\""),
                    "stream body must match non-stream execute: " + seen);
        }
    }

    @Test
    void execute_stream_non2xx_failsLoudlyNamingPortAndStatus(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 500, "{\"error\":\"nope\"}")) {
            String out = runHelperScript(tmp, socketPath.toString(), """
                    try:
                        list(studioquack.execute("model", {"messages": []}, stream=True))
                        print("NO_RAISE")
                    except RuntimeError as exc:
                        print("RAISED " + str(exc))
                    """);
            assertTrue(out.contains("RAISED"), "non-2xx must raise: " + out);
            assertTrue(out.contains("studioquack.execute('model')"),
                    "the error must name the FUNCTION port: " + out);
            assertTrue(out.contains("status=500"), "the error must name the HTTP status: " + out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/execute/stream"), seen);
        }
    }

    @Test
    void iter_sse_partialCompleteErrorDoneAndComments(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        String out = runHelperScript(tmp, null, """
                frames = list(_sq_iter_sse([
                    ": heartbeat",
                    "event: partial",
                    'data: {"delta":"a"}',
                    "",
                    "event: unknown",
                    'data: {"x":1}',
                    "",
                    "event: complete",
                    'data: {"result":"a"}',
                    "",
                    "event: done",
                    "data: {}",
                    "",
                ]))
                print("FRAMES " + repr(frames))
                try:
                    list(_sq_iter_sse([
                        "event: error",
                        'data: {"message":"boom"}',
                        "",
                    ]))
                    print("NO_RAISE")
                except RuntimeError as exc:
                    print("ERR " + str(exc))
                """);
        assertTrue(out.contains("FRAMES [{'delta': 'a'}, {'result': 'a'}]"),
                "comments/unknown events skip; partial then complete yield: " + out);
        assertTrue(out.contains("ERR boom"), "error events must raise data.message: " + out);
        assertFalse(out.contains("NO_RAISE"), out);
    }

    /** Runs the sandbox preamble plus {@code body} under python3 and returns its combined output. */
    private static String runHelperScript(Path tmp, String helperSocket, String body) throws Exception {
        String helper = loadHelper().replace("__SQ_CONTEXT__", "");
        Path script = tmp.resolve("execute_transport_smoke.py");
        Files.writeString(script, helper + "\n" + body, StandardCharsets.UTF_8);

        ProcessBuilder pb = new ProcessBuilder("python3", script.toString());
        if (helperSocket != null) {
            pb.environment().put("SQ_HELPER_SOCKET", helperSocket);
        } else {
            // Simulate a stateful dispatch that omitted the socket because the broker is down.
            pb.environment().remove("SQ_HELPER_SOCKET");
        }
        pb.environment().put("SQ_HELPER_BEARER", "job-token");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "python smoke timed out; output=" + out);
        assertNotEquals("", out.trim(), "python smoke produced no output");
        return out;
    }

    private static String loadHelper() throws IOException {
        try (InputStream in = PythonHelperExecuteTransportTest.class
                .getResourceAsStream("/sandbox/helpers/python.py")) {
            if (in == null) {
                throw new IOException("missing classpath resource /sandbox/helpers/python.py");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean python3Available() {
        try {
            Process p = new ProcessBuilder("python3", "-c", "print(1)").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
