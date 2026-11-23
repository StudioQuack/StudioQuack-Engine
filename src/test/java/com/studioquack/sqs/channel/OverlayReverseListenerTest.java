package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.net.MeshTls;
import com.studioquack.sqs.channel.overlay.OverlayReverseListener;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobIngestService;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OverlayReverseListener} over a real HTTPS round-trip on 127.0.0.1 with a self-signed test
 * identity (client-cert relaxed via the test seam). Asserts
 */
class OverlayReverseListenerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void postJobs_runsDispatchExecutor_verbatim_and_returns_sqvmJobResponse() throws Exception {
        AtomicReference<String> seenRequest = new AtomicReference<>();
        AtomicReference<java.time.Duration> seenDeadline = new AtomicReference<>();
        DispatchExecutor executor = (requestJson, deadline) -> {
            seenRequest.set(requestJson);
            seenDeadline.set(deadline);
            return "{\"status\":\"COMPLETED\",\"outputVariables\":{\"result\":\"ok\"},\"exitCode\":0}";
        };

        OverlayReverseListener listener = testListener(executor, null);
        listener.start();
        try {
            String requestJson = "{\"jobKey\":42,\"envId\":\"env-1\",\"language\":\"python\",\"script\":\"print(1)\"}";
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/jobs", requestJson, null);

            assertEquals(200, resp.statusCode());
            assertEquals(requestJson, seenRequest.get(), "executor ran the verbatim SqvmJobRequest JSON");
            assertNotNull(seenDeadline.get(), "a dispatch deadline was passed through");
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("COMPLETED", body.get("status"));
            assertTrue(body.get("outputVariables") instanceof Map, "outputVariables present");
            assertEquals("ok", ((Map<?, ?>) body.get("outputVariables")).get("result"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void postJobs_withoutExecutor_returns_503() throws Exception {
        OverlayReverseListener listener = testListener(null, null);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/jobs", "{}", null);
            assertEquals(503, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("Error_DispatchUnavailable", body.get("errorCode"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void postCommand_runsRegistry_andReturnsResult() throws Exception {
        // A tiny in-line handler for a "status" command so the registry has something to route.
        com.studioquack.sqs.channel.command.DaemonCommandHandler statusHandler =
                new com.studioquack.sqs.channel.command.DaemonCommandHandler() {
                    @Override
                    public String type() {
                        return "status";
                    }

                    @Override
                    public com.studioquack.sqs.channel.command.DaemonCommandOutcome handle(
                            com.studioquack.sqs.channel.command.DaemonCommand command) {
                        return com.studioquack.sqs.channel.command.DaemonCommandOutcome.ok(
                                "{\"containers\":2}");
                    }
                };
        com.studioquack.sqs.channel.command.DaemonCommandRegistry registry =
                new com.studioquack.sqs.channel.command.DaemonCommandRegistry(java.util.List.of(statusHandler));

        OverlayReverseListener listener = testListener(null, null, registry);
        listener.start();
        try {
            String cmdJson = "{\"commandId\":\"c-1\",\"type\":\"status\",\"payload\":null}";
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/command", cmdJson, null);

            assertEquals(200, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("c-1", body.get("commandId"));
            assertEquals("status", body.get("type"));
            assertEquals("ok", body.get("status"));
            assertEquals("{\"containers\":2}", body.get("output"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void postCommand_startRun_returnsAcceptedWithoutWaitingForHandler() throws Exception {
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        com.studioquack.sqs.channel.command.DaemonCommandHandler startRun =
                new com.studioquack.sqs.channel.command.DaemonCommandHandler() {
                    @Override
                    public String type() {
                        return "start-run";
                    }

                    @Override
                    public com.studioquack.sqs.channel.command.DaemonCommandOutcome handle(
                            com.studioquack.sqs.channel.command.DaemonCommand command) {
                        entered.countDown();
                        try {
                            release.await(30, java.util.concurrent.TimeUnit.SECONDS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                        return com.studioquack.sqs.channel.command.DaemonCommandOutcome.ok("{\"started\":true}");
                    }
                };
        com.studioquack.sqs.channel.command.DaemonCommandRegistry registry =
                new com.studioquack.sqs.channel.command.DaemonCommandRegistry(java.util.List.of(startRun));

        OverlayReverseListener listener = testListener(null, null, registry);
        listener.start();
        try {
            long t0 = System.nanoTime();
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/command",
                    "{\"commandId\":\"run-1\",\"type\":\"start-run\",\"payload\":\"{}\"}", null);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

            assertEquals(200, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("run-1", body.get("commandId"));
            assertEquals("start-run", body.get("type"));
            assertEquals("accepted", body.get("status"));
            assertTrue(elapsedMs < 2_000L, "ACCEPT must not wait for startLatest (" + elapsedMs + "ms)");
            assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS),
                    "serial lane must still pick up the command");

            // Dedup: a re-ship while in-flight is another ACCEPT, not a second execution.
            HttpResponse<String> again = post(listener.boundPort(), "/sqvm/v1/command",
                    "{\"commandId\":\"run-1\",\"type\":\"start-run\",\"payload\":\"{}\"}", null);
            assertEquals("accepted", MAPPER.readValue(again.body(), Map.class).get("status"));
        } finally {
            release.countDown();
            listener.stop();
        }
    }

    @Test
    void postCommand_startRun_unsignedAfterHybrid_refusesBeforeAccept() throws Exception {
        java.util.concurrent.atomic.AtomicInteger executions = new java.util.concurrent.atomic.AtomicInteger();
        com.studioquack.sqs.channel.command.DaemonCommandHandler startRun =
                new com.studioquack.sqs.channel.command.DaemonCommandHandler() {
                    @Override
                    public String type() {
                        return "start-run";
                    }

                    @Override
                    public com.studioquack.sqs.channel.command.DaemonCommandOutcome handle(
                            com.studioquack.sqs.channel.command.DaemonCommand command) {
                        executions.incrementAndGet();
                        return com.studioquack.sqs.channel.command.DaemonCommandOutcome.ok("{\"started\":true}");
                    }
                };
        byte[] ikm = new byte[32];
        ikm[0] = 0x7;
        com.studioquack.sqs.channel.ChannelKemIkm holder = new com.studioquack.sqs.channel.ChannelKemIkm();
        holder.install("env-mac-overlay", ikm);
        com.studioquack.sqs.channel.command.DaemonCommandRegistry registry =
                new com.studioquack.sqs.channel.command.DaemonCommandRegistry(
                        java.util.List.of(startRun), 60_000L, false, holder);

        OverlayReverseListener listener = testListener(null, null, registry);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/command",
                    "{\"commandId\":\"run-unsigned\",\"type\":\"start-run\",\"payload\":\"{}\"}", null);
            assertEquals(200, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("failed", body.get("status"));
            assertTrue(String.valueOf(body.get("error")).startsWith(
                    com.studioquack.sqs.channel.command.DaemonCommandRegistry.ERROR_COMMAND_UNAUTHENTICATED));
            Thread.sleep(200);
            assertEquals(0, executions.get(), "unsigned start-run must not reach the handler");
        } finally {
            listener.stop();
        }
    }

    @Test
    void postCommand_withoutRegistry_returns_503() throws Exception {
        OverlayReverseListener listener = testListener(null, null, null);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/command",
                    "{\"commandId\":\"c-1\",\"type\":\"status\"}", null);
            assertEquals(503, resp.statusCode());
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("Error_CommandChannelUnavailable", body.get("errorCode"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void postOciBlob_delegates_to_ingestService_with_ref_env_and_bearer() throws Exception {
        AtomicReference<String> seenRef = new AtomicReference<>();
        AtomicReference<String> seenEnv = new AtomicReference<>();
        AtomicReference<String> seenBearer = new AtomicReference<>();
        AtomicReference<Integer> seenBytes = new AtomicReference<>();
        OciImageBlobIngestService ingest = new OciImageBlobIngestService(null, null, null, null) {
            @Override
            public Result ingest(String ref, String envId, String displayName,
                                 InputStream body, String presentedBearer) {
                seenRef.set(ref);
                seenEnv.set(envId);
                seenBearer.set(presentedBearer);
                try (InputStream in = body) {
                    seenBytes.set(in.readAllBytes().length);
                } catch (Exception ignored) {
                    seenBytes.set(-1);
                }
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("ref", ref);
                out.put("digest", "sha256:abc123");
                out.put("status", "LOADED");
                return new Result(200, out);
            }
        };

        OverlayReverseListener listener = testListener(null, ingest);
        listener.start();
        try {
            HttpResponse<String> resp = post(listener.boundPort(),
                    "/sqvm/v1/oci-images/my-image/blob?envId=env-1&displayName=Hello%20World",
                    "TARBALL-BYTES", "upload-tok");

            assertEquals(200, resp.statusCode());
            assertEquals("my-image", seenRef.get(), "ref path segment decoded + delegated");
            assertEquals("env-1", seenEnv.get(), "envId query param delegated");
            assertEquals("upload-tok", seenBearer.get(), "Authorization bearer delegated");
            assertEquals(Integer.valueOf("TARBALL-BYTES".length()), seenBytes.get(), "body stream delegated");
            Map<?, ?> body = MAPPER.readValue(resp.body(), Map.class);
            assertEquals("LOADED", body.get("status"));
            assertEquals("my-image", body.get("ref"));
        } finally {
            listener.stop();
        }
    }

    @Test
    void start_withHybridNamedGroups_stillServes() throws Exception {
        OverlayReverseListener listener = testListener((r, d) -> "{}", null);
        listener.start();
        try {
            assertTrue(listener.isRunning());
            assertTrue(MeshTls.offeredGroups().length > 0, "MeshTls must offer at least classic groups");
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/jobs", "{}", null);
            assertEquals(200, resp.statusCode());
        } finally {
            listener.stop();
        }
    }

    @Test
    void rebind_sameHost_keepsServing_andRefusesWildcard() throws Exception {
        OverlayReverseListener listener = testListener((r, d) -> "{\"status\":\"COMPLETED\"}", null);
        listener.start();
        try {
            assertEquals("127.0.0.1", listener.bindHost());
            listener.rebind("127.0.0.1");
            assertTrue(listener.isRunning());
            HttpResponse<String> resp = post(listener.boundPort(), "/sqvm/v1/jobs", "{}", null);
            assertEquals(200, resp.statusCode());
            assertThrows(IllegalStateException.class, () -> listener.rebind("0.0.0.0"));
            assertTrue(listener.isRunning());
            assertEquals("127.0.0.1", listener.bindHost());
        } finally {
            listener.stop();
        }
    }

    @Test
    void start_and_stop_are_idempotent() throws Exception {
        OverlayReverseListener listener = testListener((r, d) -> "{}", null);
        assertFalse(listener.isRunning());
        listener.start();
        listener.start(); // second start is a no-op, must not throw or rebind
        assertTrue(listener.isRunning());
        int port = listener.boundPort();
        assertTrue(port > 0);

        listener.stop();
        listener.stop(); // second stop is a no-op
        assertFalse(listener.isRunning());
    }

    @Test
    void bootstrap_gate_refuses_to_start_when_wg_absent_down_or_unenrolled() {
        // Happy path: enabled + wg up + real backend + stats up + mTLS enrolled → starts (null).
        assertNull(SqsChannelBootstrap.overlaySkipReason(true, true, "userspace", true, true));

        // No data plane on this host (the bean is absent — nothing simulates a tunnel) → must NOT start.
        assertEquals("no-data-plane",
                SqsChannelBootstrap.overlaySkipReason(true, true, null, true, true));
        assertEquals("no-data-plane",
                SqsChannelBootstrap.overlaySkipReason(true, true, "  ", true, true));
        // Interface down → must NOT start. Distinct from "absent": the tooling is there, the tunnel is not.
        assertEquals("wg-down",
                SqsChannelBootstrap.overlaySkipReason(true, true, "kernel", false, true));
        // mTLS not enrolled → must NOT start.
        assertEquals("mtls-not-enrolled",
                SqsChannelBootstrap.overlaySkipReason(true, true, "kernel", true, false));
        // WG never came up → must NOT start.
        assertEquals("wg-not-up",
                SqsChannelBootstrap.overlaySkipReason(true, false, "kernel", false, true));
        // Disabled by config → must NOT start.
        assertEquals("disabled",
                SqsChannelBootstrap.overlaySkipReason(false, true, "kernel", true, true));

        // Bind host: explicit override wins; otherwise derive the overlay IP from the CIDR host part.
        assertEquals("127.0.0.1", SqsChannelBootstrap.resolveOverlayBindHost("127.0.0.1", "100.64.0.5/32"));
        assertEquals("100.64.0.5", SqsChannelBootstrap.resolveOverlayBindHost("", "100.64.0.5/32"));
        assertEquals("100.64.0.5", SqsChannelBootstrap.stripCidr("100.64.0.5/32"));
    }

    // ---- test scaffolding ---------------------------------------------------

    /** A listener bound to an ephemeral 127.0.0.1 port with a self-signed identity, client-auth off. */
    private static OverlayReverseListener testListener(DispatchExecutor executor,
                                                       OciImageBlobIngestService ingest) throws Exception {
        return testListener(executor, ingest, null);
    }

    /** As {@link #testListener(DispatchExecutor, OciImageBlobIngestService)} but with a command registry. */
    private static OverlayReverseListener testListener(DispatchExecutor executor,
                                                       OciImageBlobIngestService ingest,
                                                       com.studioquack.sqs.channel.command.DaemonCommandRegistry registry)
            throws Exception {
        return new OverlayReverseListener("127.0.0.1", 0, selfSignedServerContext(), false,
                executor, ingest, registry, MAPPER);
    }

    private static HttpResponse<String> post(int port, String pathAndQuery, String body, String bearer)
            throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("https://127.0.0.1:" + port + pathAndQuery))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (bearer != null) {
            b.header("Authorization", "Bearer " + bearer);
        }
        return trustingClient().send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Server SSLContext from a freshly-minted self-signed RSA leaf (CN=localhost). */
    private static SSLContext selfSignedServerContext() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair kp = kpg.generateKeyPair();
        X500Name dn = new X500Name("CN=localhost");
        long now = System.currentTimeMillis();
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(
                new JcaX509v3CertificateBuilder(dn, BigInteger.valueOf(now),
                        new Date(now - 60_000L), new Date(now + 3_600_000L), dn, kp.getPublic())
                        .build(signer(kp)));
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("leaf", kp.getPrivate(), new char[0], new X509Certificate[]{cert});
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);
        return ctx;
    }

    private static ContentSigner signer(KeyPair kp) throws Exception {
        return new JcaContentSignerBuilder("SHA256withRSA").build(kp.getPrivate());
    }

    /**
     * Client that trusts the self-signed server AND skips hostname verification. A custom {@link
     * X509ExtendedTrustManager} is the mechanism: the JDK HttpClient force-enables HTTPS endpoint
     * identification and ignores an SSLParameters override, but replacing the trust manager removes
     * the default hostname check (same posture the server overlay dispatcher uses for the dynamic
     * overlay IP).
     */
    private static HttpClient trustingClient() throws Exception {
        TrustManager[] trustAll = {new X509ExtendedTrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] c, String a) { }
            @Override public void checkClientTrusted(X509Certificate[] c, String a, Socket s) { }
            @Override public void checkClientTrusted(X509Certificate[] c, String a, SSLEngine e) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a, Socket s) { }
            @Override public void checkServerTrusted(X509Certificate[] c, String a, SSLEngine e) { }
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }};
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trustAll, new SecureRandom());
        return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).sslContext(ctx).build();
    }
}
