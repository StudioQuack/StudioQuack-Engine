package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulLauncher;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import com.studioquack.sqs.sandbox.stateful.StatefulSandboxExecution;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WebhookHelperSdkTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void helperRequestRouter_handlesAllFourWebhookEndpointsAndStdoutFolding() throws Exception {
        HelperRequestRouter router = new HelperRequestRouter(mapper, null, null);
        HelperJob job = HelperJob.builder("job-1", "python").build();
        String bearer = router.register(job);

        // 1. webhook-challenge
        var chResp = router.route("POST", "webhook-challenge", bearer,
                mapper.writeValueAsBytes(Map.of("status", 200, "contentType", "text/plain", "body", "ok-123")));
        assertEquals(200, chResp.status());
        assertEquals("ok-123", job.webhookChallenge().body());

        // 2. webhook-events with >100 events enforces max 100 cap
        List<Map<String, Object>> many = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            many.add(Map.of("idempotencyKey", "k" + i, "correlationKey", "c" + i, "payload", Map.of("idx", i)));
        }
        var evResp = router.route("POST", "webhook-events", bearer,
                mapper.writeValueAsBytes(Map.of("events", many)));
        assertEquals(200, evResp.status());
        assertEquals(WebhookIngressOutcome.MAX_EVENTS, job.webhookEvents().size());

        // 3. webhook-reject
        var rejResp = router.route("POST", "webhook-reject", bearer,
                mapper.writeValueAsBytes(Map.of("status", 401, "reason", "bad_hmac")));
        assertEquals(200, rejResp.status());
        assertEquals("bad_hmac", job.webhookReject().reason());

        // 4. correlation-key
        var corrResp = router.route("POST", "correlation-key", bearer,
                mapper.writeValueAsBytes(Map.of("correlationKey", "wa-999")));
        assertEquals(200, corrResp.status());
        assertEquals("wa-999", job.correlationKey());

        // 5. foldStdoutEvent mirrors all 4 sqEvent::: kinds
        HelperJob job2 = HelperJob.builder("job-2", "javascript").build();
        assertTrue(router.foldStdoutEvent(job2, "webhook_challenge",
                Map.of("status", 200, "contentType", "text/plain", "body", "folded")));
        assertTrue(router.foldStdoutEvent(job2, "webhook_events",
                Map.of("events", List.of(Map.of("idempotencyKey", "e1", "payload", Map.of("x", 1))))));
        assertTrue(router.foldStdoutEvent(job2, "webhook_reject",
                Map.of("status", 403, "reason", "denied")));
        assertTrue(router.foldStdoutEvent(job2, "correlation_key",
                Map.of("correlationKey", "corr-2")));
        assertEquals("folded", job2.webhookChallenge().body());
        assertEquals(1, job2.webhookEvents().size());
        assertEquals(403, job2.webhookReject().status());
        assertEquals("corr-2", job2.correlationKey());
    }

    @Test
    void statefulSandboxExecution_forwardsIngressFieldsAndParsesDirectJsonKeys() throws Exception {
        HelperRequestRouter router = new HelperRequestRouter(mapper, null, null);
        StatefulInstanceManager manager = mock(StatefulInstanceManager.class);
        StatefulLauncher launcher = mock(StatefulLauncher.class);
        StatefulInstance inst = new StatefulInstance(
                "inst-1", "env1", "sq-local/lib:latest", "sq-stateful-inst-1", "sq-vol-inst-1",
                com.studioquack.sqs.sandbox.stateful.StatefulCaps.uncapped(), true);
        inst.setContainerId("cid-1");
        inst.setStatus(StatefulStatus.RUNNING);
        when(manager.brokerAlive(inst)).thenReturn(true);
        when(manager.beginDispatch("cid-1")).thenReturn(true);

        String directJsonReply = mapper.writeValueAsString(Map.of(
                "output", Map.of("sent", true),
                "__sq_correlation_key", "chat-42",
                "__sq_webhook_events", List.of(Map.of(
                        "idempotencyKey", "m1",
                        "correlationKey", "chat-42",
                        "payload", Map.of("text", "hi")))));
        when(launcher.invoke(any(), any(), any(), any(), any(), anyBoolean(), any()))
                .thenReturn(new StatefulLauncher.LauncherOutcome(0, directJsonReply, "", false));

        StatefulSandboxExecution exec = new StatefulSandboxExecution(
                router, manager, launcher, new Redactor(), mapper,
                new StatefulRuntimeProperties(), mock(SubProcessCompletionRegistry.class));

        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("invocationMode", "ingress");
        ec.put("activeSelectRole", "listen");
        ec.put("webhookRequest", Map.of("method", "POST", "rawBody", "{}"));
        SandboxJobRequest job = SandboxJobRequest.builder("j1", null, "")
                .envId("env1")
                .workflowExecutionId("wfe1")
                .currentNodeId("n1")
                .templateId("tpl-1")
                .ociImageRef("sq-local/lib:latest")
                .executionContext(ec)
                .build();

        SandboxJobResponse resp = exec.execute(job, inst);
        assertTrue(resp.ok());
        assertEquals("chat-42", resp.correlationKey());
        assertNotNull(resp.webhookOutcome());
        assertEquals(1, resp.webhookOutcome().events().size());
        assertFalse(resp.outputVariables().containsKey("__sq_correlation_key"));
        assertFalse(resp.outputVariables().containsKey("__sq_webhook_events"));
        assertTrue(resp.outputVariables().containsKey("output"));

        ArgumentCaptor<String> reqJsonCap = ArgumentCaptor.forClass(String.class);
        verify(launcher).invoke(any(), reqJsonCap.capture(), any(), any(), any(), anyBoolean(), any());
        JsonNode sent = mapper.readTree(reqJsonCap.getValue());
        assertEquals("ingress", sent.get("invocationMode").asText());
        assertEquals("listen", sent.get("activeSelectRole").asText());
        assertEquals("POST", sent.path("webhookRequest").path("method").asText());
    }

    @Test
    void pythonHelper_validatesAllWebhookIngressMethods(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(available("python3", "-c", "print(1)"), "python3 required");
        String secret = "top-secret-hmac-key";
        String rawJson = "{\"entry\":[{\"changes\":[{\"value\":{\"messages\":[{\"id\":\"wamid.1\",\"from\":\"15551234\",\"text\":{\"body\":\"hello\"}}]}}]}],\"value\":[{\"resourceData\":{\"id\":\"teams-msg-9\"}}]}";
        String sig = "sha256=" + hmacSha256Hex(secret, rawJson);
        String ctxB64 = buildIngressContextBase64(rawJson, sig, "application/json");

        String script = loadHelper("python.py").replace("__SQ_CONTEXT__", ctxB64) + """

                import json as _j
                assert studioquack.isWebhookInvocation() is True
                assert studioquack.is_webhook_invocation() is True
                req = studioquack.getWebhookRequest()
                assert req["method"] == "POST"
                assert studioquack.getWebhookHeader("X-Hub-Signature-256") == "%SIG%"
                assert studioquack.getWebhookPayload("entry[0].changes[0].value.messages[0].text.body") == "hello"
                assert studioquack.get_webhook_payload("value[0].resourceData.id") == "teams-msg-9"
                assert studioquack.getWebhookPayload("missing[9].nope", "fallback") == "fallback"
                assert studioquack.verifyHmacSha256("%SECRET%", "X-Hub-Signature-256") is True
                assert studioquack.verifyHmacSha256("%SECRET%", "%SIG%") is True
                assert studioquack.verifyHmacSha256("wrong", "X-Hub-Signature-256") is False
                assert studioquack.verifyHmacSha256("%SECRET%", "X-Hub-Signature-256", prefix="v0=") is False
                # Custom payloadOverride (Slack/Stripe style)
                import hmac as _hm, hashlib as _hl
                custom_sig = "v0=" + _hm.new(b"%SECRET%", b"v0:170000:raw", _hl.sha256).hexdigest()
                assert studioquack.verifyHmacSha256("%SECRET%", custom_sig, prefix="v0=", payloadOverride="v0:170000:raw") is True
                # Form-urlencoded fallback when body is None
                form_helper = StudioQuackHelper(execution_context={
                    "invocationMode": "ingress",
                    "webhookRequest": {
                        "method": "POST",
                        "headers": {"Content-Type": "application/x-www-form-urlencoded"},
                        "rawBody": "From=%2B15550001&Body=Hi+Twilio",
                        "body": None,
                    }
                })
                assert form_helper.getWebhookPayload("From") == "+15550001"
                assert form_helper.getWebhookPayload("Body") == "Hi Twilio"
                # Emit cap 100 + correlation key + stateful challenge/reject markers
                res = studioquack.emitWebhookEvents([{"idempotencyKey": f"id-{i}", "payload": {"i": i}} for i in range(110)])
                assert res["count"] == 100
                assert studioquack.setCorrelationKey("wa-15551234") == "wa-15551234"
                ch = studioquack.respondWebhookChallenge("challenge-ok")
                assert ch["body"] == "challenge-ok"
                rj = studioquack.rejectWebhook(401, "bad_sig")
                assert rj["reason"] == "bad_sig"
                print("PY_WEBHOOK_OK")
                """.replace("%SIG%", sig).replace("%SECRET%", secret);

        String out = run(tmp, "webhook_test.py", List.of("python3"), script);
        assertTrue(out.contains("PY_WEBHOOK_OK"), "python webhook test failed: " + out);
        assertTrue(out.contains("sqEvent:::"), "fallback stdout markers should emit when no socket: " + out);
    }

    @Test
    void nodeHelper_validatesAllWebhookIngressMethods(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(available("node", "-e", "process.exit(0)"), "node required");
        String secret = "top-secret-hmac-key";
        String rawJson = "{\"entry\":[{\"changes\":[{\"value\":{\"messages\":[{\"id\":\"wamid.1\",\"from\":\"15551234\",\"text\":{\"body\":\"hello\"}}]}}]}],\"value\":[{\"resourceData\":{\"id\":\"teams-msg-9\"}}]}";
        String sig = "sha256=" + hmacSha256Hex(secret, rawJson);
        String ctxB64 = buildIngressContextBase64(rawJson, sig, "application/json");

        String script = loadHelper("node.js").replace("__SQ_CONTEXT__", ctxB64) + """

                const assert = require("assert");
                const crypto = require("crypto");
                assert.strictEqual(studioquack.isWebhookInvocation(), true);
                assert.strictEqual(studioquack.getWebhookRequest().method, "POST");
                assert.strictEqual(studioquack.getWebhookHeader("X-Hub-Signature-256"), "%SIG%");
                assert.strictEqual(studioquack.getWebhookPayload("entry[0].changes[0].value.messages[0].text.body"), "hello");
                assert.strictEqual(studioquack.getWebhookPayload("value[0].resourceData.id"), "teams-msg-9");
                assert.strictEqual(studioquack.getWebhookPayload("missing[9].nope", "fallback"), "fallback");
                assert.strictEqual(studioquack.verifyHmacSha256("%SECRET%", "X-Hub-Signature-256"), true);
                assert.strictEqual(studioquack.verifyHmacSha256("%SECRET%", "%SIG%"), true);
                assert.strictEqual(studioquack.verifyHmacSha256("wrong", "X-Hub-Signature-256"), false);
                assert.strictEqual(studioquack.verifyHmacSha256("%SECRET%", "X-Hub-Signature-256", "v0="), false);
                const customSig = "v0=" + crypto.createHmac("sha256", "%SECRET%").update("v0:170000:raw").digest("hex");
                assert.strictEqual(studioquack.verifyHmacSha256("%SECRET%", customSig, "v0=", "v0:170000:raw"), true);
                // Form-urlencoded parsing
                studioquack.setExecutionContext({
                  invocationMode: "ingress",
                  webhookRequest: {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    rawBody: "From=%2B15550001&Body=Hi+Twilio",
                    body: null
                  }
                });
                assert.strictEqual(studioquack.getWebhookPayload("From"), "+15550001");
                assert.strictEqual(studioquack.getWebhookPayload("Body"), "Hi Twilio");
                // Cap 100 + correlationKey + challenge + reject (stateful mode avoids exit)
                const evs = [];
                for (let i = 0; i < 110; i++) evs.push({ idempotencyKey: "id-" + i, payload: { i } });
                assert.strictEqual(studioquack.emitWebhookEvents(evs).count, 100);
                assert.strictEqual(studioquack.setCorrelationKey("wa-15551234"), "wa-15551234");
                assert.strictEqual(studioquack.respondWebhookChallenge("challenge-ok").body, "challenge-ok");
                assert.strictEqual(studioquack.rejectWebhook(401, "bad_sig").reason, "bad_sig");
                console.log("NODE_WEBHOOK_OK");
                """.replace("%SIG%", sig).replace("%SECRET%", secret);

        String out = run(tmp, "webhook_test.js", List.of("node"), script);
        assertTrue(out.contains("NODE_WEBHOOK_OK"), "node webhook test failed: " + out);
        assertTrue(out.contains("sqEvent:::"), "fallback stdout markers should emit when no socket: " + out);
    }

    private String buildIngressContextBase64(String rawJson, String sig, String contentType) throws Exception {
        Map<String, Object> ctx = new LinkedHashMap<>();
        ctx.put("invocationMode", "ingress");
        ctx.put("activeSelectRole", "listen");
        ctx.put("webhookRequest", Map.of(
                "method", "POST",
                "path", "/be/v1/environments/env1/runtime/message-events/<REDACTED>",
                "query", Map.of(),
                "queryMulti", Map.of(),
                "headers", Map.of("content-type", contentType, "x-hub-signature-256", sig),
                "rawBody", rawJson,
                "rawBodyBase64", Base64.getEncoder().encodeToString(rawJson.getBytes(StandardCharsets.UTF_8)),
                "rawBodyEncoding", "utf8",
                "body", mapper.readValue(rawJson, Object.class)));
        return Base64.getEncoder().encodeToString(mapper.writeValueAsBytes(ctx));
    }

    private static String hmacSha256Hex(String secret, String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)));
    }

    private static String run(Path tmp, String file, List<String> cmd, String script) throws Exception {
        Path path = tmp.resolve(file);
        Files.writeString(path, script, StandardCharsets.UTF_8);
        List<String> argv = new ArrayList<>(cmd);
        argv.add(path.toString());
        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.environment().remove("SQ_HELPER_SOCKET");
        pb.environment().remove("SQVM_HELPER_URL");
        pb.environment().put("SQ_STATEFUL_RUNNER", "1");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "script timed out: " + out);
        assertEquals(0, p.exitValue(), "non-zero exit: " + out);
        return out;
    }

    private static String loadHelper(String name) throws IOException {
        try (InputStream in = WebhookHelperSdkTest.class.getResourceAsStream("/sandbox/helpers/" + name)) {
            if (in == null) {
                throw new IOException("missing /sandbox/helpers/" + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean available(String... argv) {
        try {
            Process p = new ProcessBuilder(argv).start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
