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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the Local LLM contract: seeded nodes call {@code studioquack.llm.completion()},
 * which must exist on the in-sandbox {@code _LlmHelper} and POST to
 * {@code /v1/helper/llm/completion} (proxied to LM Studio / OpenAI-compatible chat URL).
 */
class PythonLlmHelperCompletionTest {

    @Test
    void pythonHelperResource_exposesCompletionMethod() throws Exception {
        String helper = loadHelper();
        assertTrue(helper.contains("def completion("),
                "Local LLM node calls studioquack.llm.completion(); helper must define it");
        assertTrue(helper.contains("/v1/helper/llm/completion"),
                "completion() must POST the legacy helper-channel path the daemon already routes");
    }

    @Test
    void completion_postsEnvelopeAndReturnsProviderBody(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        // The SDK reaches the daemon over a Unix socket, never TCP — the in-container broker serves
        // it, so there is no host address to dial and nothing for a host firewall to drop.
        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200,
                "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}]}")) {
            String helper = loadHelper().replace("__SQ_CONTEXT__", "");
            Path script = tmp.resolve("llm_completion_smoke.py");
            Files.writeString(script, helper + """

resp = studioquack.llm.completion(
    messages=[{"role": "user", "content": "ping"}],
    model="local-model",
    temperature=0.1,
)
assert isinstance(resp, dict), resp
assert resp["choices"][0]["message"]["content"] == "pong"
print("OK")
""", StandardCharsets.UTF_8);

            ProcessBuilder pb = new ProcessBuilder("python3", script.toString());
            pb.environment().put("SQ_HELPER_SOCKET", socketPath.toString());
            pb.environment().put("SQ_HELPER_BEARER", "job-token");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            boolean finished = p.waitFor(20, TimeUnit.SECONDS);
            assertTrue(finished, "python smoke timed out; output=" + out);
            assertEquals(0, p.exitValue(), "python smoke failed; output=" + out);
            assertTrue(out.contains("OK"), out);

            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/llm/completion"),
                    "completion() must keep posting the legacy helper-channel path: " + seen);
            assertTrue(seen.contains("ping"), seen);
            assertTrue(seen.contains("local-model"), seen);
            assertTrue(seen.contains("Bearer job-token"), seen);
            assertFalse(seen.toLowerCase().contains("text/event-stream"),
                    "llm.completion must not send Accept: text/event-stream: " + seen);
        }
    }

    @Test
    void completion_withEnv_sendsEnvId_butNeverAsAModelParam(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200,
                "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}]}")) {
            String out = runLlmScript(tmp, socketPath, """
resp = studioquack.llm.completion(
    messages=[{"role": "user", "content": "ping"}],
    model="local-model",
    temperature=0.1,
    env="gpu-box",
)
print("OK")
""");
            assertTrue(out.contains("OK"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("\"envId\": \"gpu-box\""),
                    "completion(env=...) must put envId on the wire: " + seen);
            assertTrue(!seen.contains("\"env\":"),
                    "env is an EXPLICIT named parameter — it must never leak into the model "
                            + "params via **params (the named worst-case failure): " + seen);
            assertTrue(seen.contains("\"temperature\": 0.1"),
                    "ordinary model params still flow through: " + seen);
        }
    }

    @Test
    void chat_withLegacyLeadingEnvId_postsEnvId_theParameterIsNowLive(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200,
                "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}]}")) {
            String out = runLlmScript(tmp, socketPath, """
content = studioquack.llm.chat("legacy-env", prompt="hi")
assert content == "pong", content
print("OK")
""");
            assertTrue(out.contains("OK"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("/v1/helper/llm/chat/completions"), seen);
            assertTrue(seen.contains("\"envId\": \"legacy-env\""),
                    "the legacy leading env_id positional is LIVE now — it must reach the wire: " + seen);
        }
    }

    @Test
    void chat_explicitEnv_winsOverLegacyEnvId(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200,
                "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}]}")) {
            String out = runLlmScript(tmp, socketPath, """
content = studioquack.llm.chat("legacy-env", prompt="hi", env="explicit-env")
assert content == "pong", content
print("OK")
""");
            assertTrue(out.contains("OK"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("\"envId\": \"explicit-env\""),
                    "env must win when both env and env_id are given: " + seen);
            assertTrue(!seen.contains("legacy-env"),
                    "the overridden env_id must not reach the wire: " + seen);
        }
    }

    @Test
    void chat_withoutEnv_sendsNoEnvId(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required for in-sandbox helper smoke");

        Path socketPath = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(socketPath, 200,
                "{\"choices\":[{\"message\":{\"role\":\"assistant\",\"content\":\"pong\"}}]}")) {
            String out = runLlmScript(tmp, socketPath, """
content = studioquack.llm.chat(prompt="hi")
assert content == "pong", content
print("OK")
""");
            assertTrue(out.contains("OK"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(!seen.contains("envId"),
                    "omitting env keeps the chat body exactly as before: " + seen);
        }
    }

    /** Runs the sandbox preamble plus {@code body} under python3 against the stub socket. */
    private static String runLlmScript(Path tmp, Path socketPath, String body) throws Exception {
        String helper = loadHelper().replace("__SQ_CONTEXT__", "");
        Path script = tmp.resolve("llm_env_smoke.py");
        Files.writeString(script, helper + "\n" + body, StandardCharsets.UTF_8);
        ProcessBuilder pb = new ProcessBuilder("python3", script.toString());
        pb.environment().put("SQ_HELPER_SOCKET", socketPath.toString());
        pb.environment().put("SQ_HELPER_BEARER", "job-token");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean finished = p.waitFor(20, TimeUnit.SECONDS);
        assertTrue(finished, "python smoke timed out; output=" + out);
        assertEquals(0, p.exitValue(), "python smoke failed; output=" + out);
        return out;
    }

    private static String loadHelper() throws IOException {
        try (InputStream in = PythonLlmHelperCompletionTest.class
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
