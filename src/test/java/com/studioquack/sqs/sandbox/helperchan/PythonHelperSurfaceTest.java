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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Python helper methods carried over from the legacy helper: {@code llm.models / load / unload /
 * download / download_status} with their capitalised aliases, and {@code llm_local}. Same routes
 * and bodies as the JS / TS and Java helpers.
 */
class PythonHelperSurfaceTest {

    @Test
    void llmModelOperations_useTheDaemonProxyRoutes(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required");
        Path sock = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(sock, 200, "{\"ok\":true}")) {
            String out = run(tmp, sock.toString(), """
                    studioquack.llm.models()
                    studioquack.llm.Load('qwen')
                    studioquack.llm.unload('qwen')
                    studioquack.llm.Download('org/model')
                    studioquack.llm.downloadStatus('job 1')
                    studioquack.llm.DownloadStatus()
                    try:
                        studioquack.llm_local([])
                    except ValueError as exc:
                        print("EMPTY " + str(exc))
                    print("ENVELOPE " + repr(studioquack.llmLocal([{'role': 'user', 'content': 'q'}], 'm1', temperature=0.1)))
                    """);
            assertTrue(out.contains("EMPTY llm_local requires a non-empty"), out);
            assertTrue(out.contains("ENVELOPE {'ok': True}"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("GET /v1/helper/llm/models"), seen);
            assertTrue(seen.contains("POST /v1/helper/llm/load"), seen);
            assertTrue(seen.contains("\"model_id\": \"qwen\""), seen);
            assertTrue(seen.contains("POST /v1/helper/llm/unload"), seen);
            assertTrue(seen.contains("\"repo\": \"org/model\""), seen);
            assertTrue(seen.contains("GET /v1/helper/llm/download/status?job_id=job%201"), seen);
            assertTrue(seen.contains("GET /v1/helper/llm/download/status "), seen);
            assertTrue(seen.contains("POST /v1/helper/llm/completion"), seen);
            assertTrue(seen.contains("\"temperature\": 0.1"), seen);
        }
    }

    @Test
    void llmModelOperation_refused_raises(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(python3Available(), "python3 required");
        Path sock = tmp.resolve("helper.sock");
        try (UnixHttpStub ignored = UnixHttpStub.serving(sock, 503, "{\"error\":\"provider down\"}")) {
            String out = run(tmp, sock.toString(), """
                    try:
                        studioquack.llm.load('qwen')
                        print("NO_RAISE")
                    except RuntimeError as exc:
                        print("RAISED " + str(exc))
                    """);
            assertTrue(out.contains("RAISED"), out);
            assertTrue(out.contains("status=503"), out);
        }
    }

    private static String run(Path tmp, String helperSocket, String body) throws Exception {
        Path script = tmp.resolve("surface_smoke.py");
        Files.writeString(script, loadHelper().replace("__SQ_CONTEXT__", "") + "\n" + body, StandardCharsets.UTF_8);
        ProcessBuilder pb = new ProcessBuilder("python3", script.toString());
        pb.environment().put("SQ_HELPER_SOCKET", helperSocket);
        pb.environment().put("SQ_HELPER_BEARER", "job-token");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "python smoke timed out; output=" + out);
        return out;
    }

    private static String loadHelper() throws IOException {
        try (InputStream in = PythonHelperSurfaceTest.class.getResourceAsStream("/sandbox/helpers/python.py")) {
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
