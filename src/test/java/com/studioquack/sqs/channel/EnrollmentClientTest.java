package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.HybridEnrolmentRequest;
import com.studioquack.common.security.envelope.HybridEnrolmentResponse;
import com.studioquack.common.security.envelope.HybridKemEnrolment;
import com.studioquack.common.security.envelope.KemEncapsulation;
import com.studioquack.common.security.envelope.KemSchema;
import com.studioquack.common.security.envelope.MlKem768Primitive;
import com.studioquack.common.security.envelope.X25519KemPrimitive;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link EnrollmentClient} against a mock server: asserts the EXACT {@code /enroll} path +
 * camelCase body + status parse, and proves the hybrid X25519 + ML-KEM-768 transcript yields a
 * daemon IKM byte-identical to the one the server computes via {@code
 * HybridKemEnrolment.combineToIkm}.
 */
class EnrollmentClientTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String BEARER = "sqvm_bearer_abc";

    private String base(MockBackend be) {
        return be.baseUrl() + "/be/v1/sqvm/environments/" + ENV_ID;
    }

    @Test
    void enrollLegacy_emits_exact_path_bearer_and_camelcase_body() throws Exception {
        try (MockBackend be = new MockBackend()) {
            be.stubJson("/be/v1/sqvm/environments/" + ENV_ID + "/enroll", 200,
                    "{\"status\":\"CREATED\"}");
            EnrollmentClient client = new EnrollmentClient(
                    base(be), ENV_ID, BEARER, null, new ChannelHttpClients(null), MAPPER);

            String status = client.enrollLegacy(
                    new EnrollmentClient.Params("1.2.3.4", 49152, "https://agent.example"), "0.0.1");

            assertEquals("CREATED", status);
            MockBackend.Recorded req = be.firstMatching("/enroll");
            assertNotNull(req);
            assertEquals("POST", req.method());
            assertEquals("/be/v1/sqvm/environments/" + ENV_ID + "/enroll", req.path());
            assertEquals("Bearer " + BEARER, req.headers().get("authorization"));

            Map<?, ?> body = MAPPER.readValue(req.body(), Map.class);
            assertEquals("1.2.3.4", body.get("publicIp"));
            assertEquals("0.0.1", body.get("agentVersion"));
            assertEquals(49152, ((Number) body.get("servicePort")).intValue());
            assertEquals("https://agent.example", body.get("agentEndpoint"));
        }
    }

    @Test
    void fullEnroll_derives_hybrid_ikm_byte_identical_to_backend() throws Exception {
        Path dataDir = java.nio.file.Files.createTempDirectory("sqs-enroll-mtls");
        MtlsIdentity identity = MtlsIdentity.loadOrCreate(dataDir);
        byte[][] beIkm = new byte[1][];

        try (MockBackend be = new MockBackend()) {
            String p = "/be/v1/sqvm/environments/" + ENV_ID;
            be.stubJson(p + "/enroll", 200, "{\"status\":\"CREATED\"}");
            // Server side of the hybrid transcript: encapsulate against the daemon's public keys,
            // compute the SAME IKM combineToIkm yields, return ciphertexts.
            be.stub(p + "/enrol-hybrid", req -> {
                HybridEnrolmentRequest hreq = MAPPER.readValue(req.body(), HybridEnrolmentRequest.class);
                byte[] xPub = Base64.getDecoder().decode(hreq.getPublicKeys().get(X25519KemPrimitive.NAME));
                byte[] mPub = Base64.getDecoder().decode(hreq.getPublicKeys().get(MlKem768Primitive.NAME));
                KemEncapsulation xe = new X25519KemPrimitive().encapsulate(xPub);
                KemEncapsulation me = new MlKem768Primitive().encapsulate(mPub);
                beIkm[0] = HybridKemEnrolment.combineToIkm(
                        List.of(xe, me),
                        HybridKemEnrolment.primitivesFor(KemSchema.HYBRID_X25519_MLKEM768),
                        ENV_ID);
                HybridEnrolmentResponse resp = HybridEnrolmentResponse.builder()
                        .kemSchema(KemSchema.HYBRID_X25519_MLKEM768.name())
                        .ciphertexts(Map.of(
                                X25519KemPrimitive.NAME, Base64.getEncoder().encodeToString(xe.getCiphertext()),
                                MlKem768Primitive.NAME, Base64.getEncoder().encodeToString(me.getCiphertext())))
                        .build();
                return MockBackend.Response.json(200, MAPPER.writeValueAsString(resp));
            });
            // No cert service on this server — mTLS stays off (503), keeping the test crypto-focused.
            be.stubJson(p + "/enrol-mtls", 503, "{\"errorCode\":\"Error_MtlsServiceUnavailable\"}");

            EnrollmentClient client = new EnrollmentClient(
                    base(be), ENV_ID, BEARER, identity, new ChannelHttpClients(null), MAPPER);
            EnrollmentClient.Result result = client.enroll(
                    new EnrollmentClient.Params(null, null, null), "0.0.1");

            assertEquals("CREATED", result.status());
            assertTrue(result.hybridIkm(), "hybrid IKM path taken");
            assertFalse(result.mtlsEnrolled(), "503 → stay bearer-only");
            assertNotNull(beIkm[0], "BE computed an IKM");
            assertEquals(32, result.ikm().length);
            assertArrayEquals(beIkm[0], result.ikm(),
                    "daemon hybrid IKM must be byte-identical to the BE combineToIkm output");
            // The server burns the installer bearer at hybrid enrol: enrol-mtls in the same run
            // must present the kemIkm proof, or the first boot would come up without mTLS.
            assertEquals("Bearer " + com.studioquack.common.security.envelope.DaemonAuthCredential
                            .fromKemIkm(result.ikm(), ENV_ID),
                    be.firstMatching("/enrol-mtls").headers().get("authorization"));

            // The daemon sent both public keys at the right sizes.
            HybridEnrolmentRequest sent = MAPPER.readValue(
                    be.firstMatching("/enrol-hybrid").body(), HybridEnrolmentRequest.class);
            assertEquals("HYBRID_X25519_MLKEM768", sent.getKemSchema());
            assertEquals(BEARER, sent.getPresentedBearer());
            assertEquals(32, Base64.getDecoder().decode(sent.getPublicKeys().get("X25519")).length);
            assertEquals(1184, Base64.getDecoder().decode(sent.getPublicKeys().get("ML-KEM-768")).length);
        }
    }

    @Test
    void enroll_with_persisted_ikm_skips_rehybrid_but_still_calls_legacy_enroll() throws Exception {
        Path dataDir = java.nio.file.Files.createTempDirectory("sqs-enroll-persisted");
        MtlsIdentity identity = MtlsIdentity.loadOrCreate(dataDir);
        byte[] persisted = new byte[32];
        for (int i = 0; i < persisted.length; i++) {
            persisted[i] = (byte) (0x10 + i);
        }

        try (MockBackend be = new MockBackend()) {
            String p = "/be/v1/sqvm/environments/" + ENV_ID;
            be.stubJson(p + "/enroll", 200, "{\"status\":\"CREATED\"}");
            // Hybrid must NOT be hit when persisted IKM is supplied.
            be.stubJson(p + "/enrol-hybrid", 409, "{\"errorCode\":\"ALREADY_ENROLLED\"}");
            be.stubJson(p + "/enrol-mtls", 503, "{\"errorCode\":\"Error_MtlsServiceUnavailable\"}");

            EnrollmentClient client = new EnrollmentClient(
                    base(be), ENV_ID, BEARER, identity, new ChannelHttpClients(null), MAPPER);
            EnrollmentClient.Result result = client.enroll(
                    new EnrollmentClient.Params(null, null, null), "0.0.1", persisted);

            assertEquals("CREATED", result.status());
            assertTrue(result.hybridIkm());
            assertArrayEquals(persisted, result.ikm());
            assertNotNull(be.firstMatching("/enroll"));
            org.junit.jupiter.api.Assertions.assertNull(be.firstMatching("/enrol-hybrid"),
                    "re-hybrid must be skipped when persisted IKM is present");
        }
    }
}
