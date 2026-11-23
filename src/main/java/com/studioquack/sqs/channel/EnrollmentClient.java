package com.studioquack.sqs.channel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.DaemonAuthCredential;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.HybridEnrolmentRequest;
import com.studioquack.common.security.envelope.HybridEnrolmentResponse;
import com.studioquack.common.security.envelope.HybridKemEnrolment;
import com.studioquack.common.security.envelope.HybridKemPrimitive;
import com.studioquack.common.security.envelope.KemEncapsulation;
import com.studioquack.common.security.envelope.KemSchema;
import com.studioquack.common.security.envelope.MlKem768Primitive;
import com.studioquack.common.security.envelope.X25519KemPrimitive;
import lombok.extern.slf4j.Slf4j;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Daemon enrolment driver. Runs the first-boot handshake against the control plane: {@code POST …/enroll} —
 * {@code Authorization: Bearer}; body {@code {publicIp,agentVersion,servicePort,agentEndpoint}}; flips the env
 * to {@code CREATED}.
 */
@Slf4j
public class EnrollmentClient {

    private final String environmentBase;
    private final String envId;
    private final String bearer;
    private final MtlsIdentity identity;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final Duration timeout = Duration.ofSeconds(20);

    public EnrollmentClient(String environmentBase, String envId, String bearer,
                            MtlsIdentity identity, ChannelHttpClients httpClients, ObjectMapper mapper) {
        this.environmentBase = environmentBase;
        this.envId = envId;
        this.bearer = bearer;
        this.identity = identity;
        this.httpClients = httpClients;
        this.mapper = mapper;
    }

    /**
     * Outcome of the full enrolment: the IKM to seal/open with + posture flags.
     */
    public record Result(String status, byte[] ikm, boolean hybridIkm, boolean mtlsEnrolled) {
    }

    /**
     * Per-boot fields the daemon contributes to its own enrolment record.
     */
    public record Params(String publicIp, Integer servicePort, String agentEndpoint) {
    }

    /**
     * Runs enroll → enrol-hybrid (best-effort) → enrol-mtls (best-effort) and returns the derived IKM + posture.
     * Throws only when the legacy enrol itself fails terminally (401/404/409) — the hybrid + mTLS upgrades degrade
     * gracefully so a non-hybrid / non-mTLS BE still yields a working v1 envelope channel.
     */
    public Result enroll(Params params, String agentVersion) throws Exception {
        return enroll(params, agentVersion, null);
    }

    /**
     * Same as {@link #enroll(Params, String)} but skips re-hybrid when {@code existingHybridIkm} is already
     * available (persisted kemIkm from a prior boot). Legacy enrol still runs for status sync.
     */
    public Result enroll(Params params, String agentVersion, byte[] existingHybridIkm) throws Exception {
        String status = enrollLegacy(params, agentVersion);

        byte[] ikm;
        boolean hybrid;
        if (existingHybridIkm != null && existingHybridIkm.length > 0) {
            ikm = existingHybridIkm.clone();
            hybrid = true;
            log.info("channel/enroll: using persisted hybrid IKM (skip re-hybrid) env={}", envId);
        } else {
            try {
                ikm = enrolHybrid();
                hybrid = true;
                log.info("channel/enroll: hybrid KEM IKM derived (X25519 + ML-KEM-768) env={}", envId);
            } catch (Exception ex) {
                // ALREADY_ENROLLED / burned responses also land here — without a persisted
                // IKM there is no recovery short of reinstall, so fall back to bearer-hash.
                ikm = EnvelopeCrypto.ikmFromBearer(bearer);
                hybrid = false;
                log.info("channel/enroll: hybrid enrol unavailable ({}); using v1 bearer-hash IKM env={}",
                        ex.getMessage(), envId);
            }
        }

        boolean mtls = false;
        try {
            mtls = enrolMtls(hybrid ? DaemonAuthCredential.fromKemIkm(ikm, envId) : bearer);
        } catch (Exception ex) {
            log.info("channel/enroll: mTLS enrol unavailable ({}); continuing without mTLS env={}",
                    ex.getMessage(), envId);
        }

        return new Result(status, ikm, hybrid, mtls);
    }

    /**
     * {@code POST …/enroll}. Returns the BE status string; throws on terminal auth/state errors.
     */
    public String enrollLegacy(Params params, String agentVersion) throws Exception {
        String body = mapper.writeValueAsString(new EnrollBody(
                emptyToNull(params == null ? null : params.publicIp()),
                agentVersion,
                params == null ? null : params.servicePort(),
                emptyToNull(params == null ? null : params.agentEndpoint())));
        HttpResponse<String> resp = ChannelHttp.postJson(
                httpClients.get(), environmentBase + "/enroll", bearer, body, timeout);
        if (!ChannelHttp.isOk(resp.statusCode())) {
            throw new EnrollmentException("enroll: status " + resp.statusCode()
                    + " body=" + ChannelHttp.trim(resp.body()));
        }
        EnrollResponse parsed = mapper.readValue(resp.body(), EnrollResponse.class);
        log.info("channel/enroll: enrolled env={} status={}", envId, parsed.status());
        return parsed.status();
    }

    /**
     * {@code POST …/enrol-hybrid}. Generates X25519 + ML-KEM-768 keypairs, sends both public keys, decapsulates
     * the returned ciphertexts, and combines them into the 32-byte IKM.
     */
    public byte[] enrolHybrid() throws Exception {
        X25519KemPrimitive x25519 = new X25519KemPrimitive();
        MlKem768Primitive mlkem = new MlKem768Primitive();
        var xKeys = x25519.generateKeypair();
        var mKeys = mlkem.generateKeypair();

        Map<String, String> publicKeys = new LinkedHashMap<>();
        publicKeys.put(X25519KemPrimitive.NAME, b64(xKeys.getPublicKey()));
        publicKeys.put(MlKem768Primitive.NAME, b64(mKeys.getPublicKey()));

        HybridEnrolmentRequest req = HybridEnrolmentRequest.builder()
                .envId(envId)
                .kemSchema(KemSchema.HYBRID_X25519_MLKEM768.name())
                .presentedBearer(bearer)
                .publicKeys(publicKeys)
                .build();

        HttpResponse<String> resp = ChannelHttp.postJson(
                httpClients.get(), environmentBase + "/enrol-hybrid", null,
                mapper.writeValueAsString(req), timeout);
        if (!ChannelHttp.isOk(resp.statusCode())) {
            throw new EnrollmentException("enrol-hybrid: status " + resp.statusCode()
                    + " body=" + ChannelHttp.trim(resp.body()));
        }
        HybridEnrolmentResponse parsed = mapper.readValue(resp.body(), HybridEnrolmentResponse.class);
        Map<String, String> ciphertexts = parsed.getCiphertexts();
        if (ciphertexts == null
                || !ciphertexts.containsKey(X25519KemPrimitive.NAME)
                || !ciphertexts.containsKey(MlKem768Primitive.NAME)) {
            throw new EnrollmentException("enrol-hybrid: response missing ciphertexts");
        }

        byte[] xCipher = Base64.getDecoder().decode(ciphertexts.get(X25519KemPrimitive.NAME));
        byte[] mCipher = Base64.getDecoder().decode(ciphertexts.get(MlKem768Primitive.NAME));
        byte[] xSecret = x25519.decapsulate(xCipher, xKeys.getPrivateKey());
        byte[] mSecret = mlkem.decapsulate(mCipher, mKeys.getPrivateKey());

        // Canonical order [X25519, ML-KEM-768] — MUST match the BE transcript.
        List<KemEncapsulation> encapsulations = List.of(
                KemEncapsulation.builder().ciphertext(xCipher).sharedSecret(xSecret).build(),
                KemEncapsulation.builder().ciphertext(mCipher).sharedSecret(mSecret).build());
        List<HybridKemPrimitive> primitives = HybridKemEnrolment.primitivesFor(KemSchema.HYBRID_X25519_MLKEM768);
        return HybridKemEnrolment.combineToIkm(encapsulations, primitives, envId);
    }

    /**
     * {@code POST …/enrol-mtls}. Submits the daemon SubjectPublicKeyInfo PEM, persists the signed leaf + env CA,
     * and refreshes the HTTP clients so subsequent calls present the leaf + pin the env CA.
     */
    public boolean enrolMtls() throws Exception {
        return enrolMtls(bearer);
    }

    private boolean enrolMtls(String credential) throws Exception {
        String body = mapper.writeValueAsString(new MtlsBody(identity.csrPublicKeyPem()));
        HttpResponse<String> resp = ChannelHttp.postJson(
                httpClients.get(), environmentBase + "/enrol-mtls", credential, body, timeout);
        if (resp.statusCode() == 503) {
            return false;
        }
        if (!ChannelHttp.isOk(resp.statusCode())) {
            throw new EnrollmentException("enrol-mtls: status " + resp.statusCode()
                    + " body=" + ChannelHttp.trim(resp.body()));
        }
        MtlsBundle bundle = mapper.readValue(resp.body(), MtlsBundle.class);
        if (bundle.leafCertPem() == null || bundle.leafCertPem().isBlank()
                || bundle.caCertPem() == null || bundle.caCertPem().isBlank()) {
            throw new EnrollmentException("enrol-mtls: empty cert bundle");
        }
        identity.persistSignedBundle(bundle.leafCertPem(), bundle.caCertPem());
        httpClients.refresh();
        log.info("channel/enroll: mTLS enrolled env={} (leaf + env CA pinned)", envId);
        return true;
    }

    private static String b64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }


    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record EnrollBody(String publicIp, String agentVersion, Integer servicePort,
                              String agentEndpoint) {
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record EnrollResponse(String status) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record MtlsBody(String csrPublicKey) {
    }

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
    private record MtlsBundle(String leafCertPem, String caCertPem) {
    }

    /**
     * Raised when a channel enrolment step returns a non-success status.
     */
    public static class EnrollmentException extends RuntimeException {
        public EnrollmentException(String message) {
            super(message);
        }
    }
}
