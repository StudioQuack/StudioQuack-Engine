package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour of the JavaScript / TypeScript helper ({@code node.js}) methods that mirror the Python
 * helper: context readers, secrets, {@code llm.*}, {@code engineIP}, {@code emitPartial},
 * {@code executeStream} and {@code connect} — same routes and bodies as {@code python.py}.
 */
class NodeHelperSurfaceTest {

    private static final String CONTEXT = """
            {"workflowExecutionId":"run-1","currentNodeId":"n1","currentNode":{"id":"n1",
             "inputList":[{"targetPortValue":"in","nodeId":"src","value":{"a":3}},
                          {"targetPortValue":"in","nodeId":"other","value":{"a":4}}],
             "outputList":[],
             "connectionList":[{"portValue":"model","connectionDetailList":[
                 {"nodeId":"llm-node","configurationParameters":[{"shortCode":"temp","value":0.2}]}]}],
             "edgeTransformListForCurrentNode":[{"sourceNodeId":"n1","sourcePortShortCode":"out",
                 "targetNodeId":"t1","targetPortShortCode":"in"}],
             "data":{"ports":[{"value":"empty","schema":"{\\"children\\":[{\\"shortCode\\":\\"x\\"}]}"}],
                     "configurationParameters":[{"shortCode":"limit","value":5}]}}}
            """;

    @Test
    void contextReadersAndSecrets_matchThePythonHelper(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required");
        Path secrets = tmp.resolve("secrets.env");
        Files.writeString(secrets, "# comment\nSQVM_SECRET__DB_PASS=\"p\\\"w\"\n", StandardCharsets.UTF_8);
        Map<String, String> env = new LinkedHashMap<>();
        env.put("SQVM_SECRET__API_KEY", "k1");
        env.put("SQVM_SECRETS_FILE", secrets.toString());

        String out = run(tmp, null, env, """
                const sq = studioquack;
                const detail = sq.getConnectionDetailListByPortValue('model')[0];
                sq.setOutput('out', 9);
                console.log('R ' + JSON.stringify({
                  wf: sq.getWorkflowExecutionId(),
                  id: sq.getCurrentNodeId(),
                  inAll: sq.getInputValuesByPortValue('in'),
                  inSrc: sq.getInputValuesByPortValue('in', 'src'),
                  defaults: sq.getInputValuesByPortValue('empty'),
                  limit: sq.getParameterValueByShortCode('limit', 0),
                  missing: sq.getParameterValueByShortCode('nope', 'dflt'),
                  firstNode: sq.findFirstConnectionNodeIdByPortValue('model'),
                  temp: sq.getConnectionDetailParameterByShortCode(sq.getConnectionDetailParameters(detail), 'temp'),
                  ducktape: sq.getDucktapeConfigurations('t1', 'out', 'in').length,
                  output: sq.getOutput(),
                  secretEnv: sq.getSecret('api_key'),
                  secretFile: sq.getSecret('db_pass'),
                  noSecret: sq.getSecret('absent'),
                }));
                """);
        assertTrue(out.contains("\"wf\":\"run-1\""), out);
        assertTrue(out.contains("\"id\":\"n1\""), out);
        assertTrue(out.contains("\"inAll\":[{\"a\":3},{\"a\":4}]"), out);
        assertTrue(out.contains("\"inSrc\":[{\"a\":3}]"), out);
        assertTrue(out.contains("\"defaults\":[{\"x\":null}]"), out);
        assertTrue(out.contains("\"limit\":5"), out);
        assertTrue(out.contains("\"missing\":\"dflt\""), out);
        assertTrue(out.contains("\"firstNode\":\"llm-node\""), out);
        assertTrue(out.contains("\"temp\":0.2"), out);
        assertTrue(out.contains("\"ducktape\":1"), out);
        assertTrue(out.contains("\"output\":[{\"portValue\":\"out\",\"value\":9}]"), out);
        assertTrue(out.contains("\"secretEnv\":\"k1\""), out);
        assertTrue(out.contains("\"secretFile\":\"p\\\"w\""), out);
        assertTrue(out.contains("\"noSecret\":null"), out);
    }

    @Test
    void llm_usesTheDaemonProxyRoutes_andChatReturnsText(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required");
        Path sock = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(sock, 200,
                "{\"choices\":[{\"message\":{\"content\":\"hi\"}}]}")) {
            String out = run(tmp, sock.toString(), Map.of(), """
                    (async () => {
                      const text = await studioquack.llm.chat({ messages: [{ role: 'user', content: 'q' }], env: 'env-b' });
                      const envelope = await studioquack.llm.completion({ prompt: 'q', model: 'm1', temperature: 0.1 });
                      await studioquack.llm.load('qwen');
                      await studioquack.llm.download('org/model');
                      await studioquack.llm.downloadStatus('job 1');
                      await studioquack.llm.models();
                      let empty = null;
                      try { await studioquack.llmLocal([]); } catch (e) { empty = e.message; }
                      console.log('R ' + JSON.stringify({ text, choices: envelope.choices.length, empty }));
                    })();
                    """);
            assertTrue(out.contains("\"text\":\"hi\""), out);
            assertTrue(out.contains("\"choices\":1"), out);
            assertTrue(out.contains("non-empty messages"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("POST /v1/helper/llm/chat/completions"), seen);
            assertTrue(seen.contains("\"envId\":\"env-b\""), seen);
            assertTrue(seen.contains("POST /v1/helper/llm/completion "), seen);
            assertTrue(seen.contains("\"temperature\":0.1"), seen);
            assertTrue(seen.contains("POST /v1/helper/llm/load") && seen.contains("\"model_id\":\"qwen\""), seen);
            assertTrue(seen.contains("\"repo\":\"org/model\""), seen);
            assertTrue(seen.contains("GET /v1/helper/llm/download/status?job_id=job%201"), seen);
            assertTrue(seen.contains("GET /v1/helper/llm/models"), seen);
            assertTrue(seen.contains("Bearer job-token"), seen);
        }
    }

    @Test
    void llm_refusedCall_rejectsWithStatus(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required");
        Path sock = tmp.resolve("helper.sock");
        try (UnixHttpStub ignored = UnixHttpStub.serving(sock, 502,
                "{\"error\":\"Error_CrossEnvNotGranted\"}")) {
            String out = run(tmp, sock.toString(), Map.of(), """
                    studioquack.llm.chat({ prompt: 'q', env: 'env-b' })
                      .then(() => console.log('RESOLVED'), (e) => console.log('REJECTED ' + e.message));
                    """);
            assertTrue(out.contains("REJECTED studioquack.llm.chat failed: status=502"), out);
            assertTrue(out.contains("Error_CrossEnvNotGranted"), out);
        }
    }

    @Test
    void engineIp_andEmitPartial(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required");
        Path sock = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(sock, 200, "{\"ip\":\"100.64.0.7\",\"delivered\":true}")) {
            String out = run(tmp, sock.toString(), Map.of(), """
                    (async () => {
                      const ip = await studioquack.engineIP('engine-1');
                      const delivered = studioquack.emitPartial('answer', 'tok', 'main_output');
                      console.log('R ' + JSON.stringify({ ip, delivered }));
                    })();
                    """);
            assertTrue(out.contains("\"ip\":\"100.64.0.7\""), out);
            assertTrue(out.contains("\"delivered\":true"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("\"engineId\":\"engine-1\""), seen);
            assertTrue(seen.contains("POST /v1/helper/emit-partial"), seen);
            assertTrue(seen.contains("\"port\":\"main_output\""), seen);
        }
    }

    @Test
    void executeStream_yieldsPartialsThenComplete(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required");
        Path sock = tmp.resolve("helper.sock");
        String sse = """
                : heartbeat
                event: partial
                data: {"delta":"hel"}

                event: partial
                data: {"delta":"lo"}

                event: complete
                data: {"result":"hello"}

                """;
        try (UnixHttpStub stub = UnixHttpStub.servingSse(sock, sse)) {
            String out = run(tmp, sock.toString(), Map.of(), """
                    (async () => {
                      const chunks = [];
                      for await (const c of studioquack.executeStream('model', { q: 1 }, null, 'env-b')) chunks.push(c);
                      console.log('R ' + JSON.stringify(chunks));
                    })();
                    """);
            assertTrue(out.contains("R [{\"delta\":\"hel\"},{\"delta\":\"lo\"},{\"result\":\"hello\"}]"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("POST /v1/helper/execute/stream"), seen);
            assertTrue(seen.contains("text/event-stream"), seen);
            assertTrue(seen.contains("\"envId\":\"env-b\""), seen);
        }
    }

    @Test
    void connect_probesTheInstance_andReturnsALoopbackAddress(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(nodeAvailable(), "node required");
        Path sock = tmp.resolve("helper.sock");
        try (UnixHttpStub stub = UnixHttpStub.serving(sock, 200, "{\"streamId\":\"s1\"}")) {
            String out = run(tmp, sock.toString(), Map.of(), """
                    (async () => {
                      const t = await studioquack.connect('my-postgres', 5432);
                      const again = await studioquack.connect('my-postgres', 5432);
                      console.log('R ' + JSON.stringify({ host: t.host, ok: t.port > 0, same: t === again, url: t.url('http', '/h') }));
                      t.close();
                    })();
                    """);
            assertTrue(out.contains("\"host\":\"127.0.0.1\""), out);
            assertTrue(out.contains("\"ok\":true"), out);
            assertTrue(out.contains("\"same\":true"), out);
            assertTrue(out.contains("\"url\":\"http://127.0.0.1:"), out);
            String seen = String.join("\n", stub.requests());
            assertTrue(seen.contains("POST /v1/helper/connect/open"), seen);
            assertTrue(seen.contains("\"containerName\":\"my-postgres\""), seen);
            assertTrue(seen.contains("POST /v1/helper/connect/close"), seen);
            assertFalse(out.contains("REJECTED"), out);
        }
    }

    private static String run(Path tmp, String helperSocket, Map<String, String> env, String body) throws Exception {
        String ctx = Base64.getEncoder().encodeToString(CONTEXT.getBytes(StandardCharsets.UTF_8));
        Path script = tmp.resolve("surface_smoke.js");
        Files.writeString(script, loadHelper().replace("__SQ_CONTEXT__", ctx) + "\n" + body, StandardCharsets.UTF_8);
        ProcessBuilder pb = new ProcessBuilder("node", script.toString());
        pb.environment().remove("SQVM_HELPER_URL");
        pb.environment().remove("SQ_HELPER_SOCKET");
        if (helperSocket != null) {
            pb.environment().put("SQ_HELPER_SOCKET", helperSocket);
        }
        pb.environment().put("SQ_HELPER_BEARER", "job-token");
        pb.environment().putAll(env);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "node smoke timed out; output=" + out);
        return out;
    }

    private static String loadHelper() throws IOException {
        try (InputStream in = NodeHelperSurfaceTest.class.getResourceAsStream("/sandbox/helpers/node.js")) {
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
