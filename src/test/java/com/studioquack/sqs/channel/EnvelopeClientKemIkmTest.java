package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.DaemonAuthCredential;
import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.SecureEnvelope;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * KemIkm-sealed envelopes open with {@link EnvelopeClient#withIkm}; bearer-derived IKM must fail to
 * open the same ciphertext.
 */
class EnvelopeClientKemIkmTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-h2-d02";
    private static final String BEARER = "sqvm_installer_bearer_for_d02";
    private static final String JOB = "secret-commit";

    private static byte[] kemIkm() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) (0xA0 + i);
        }
        return k;
    }

    @Test
    void kemIkm_sealed_envelope_opens_with_withIkm() throws Exception {
        byte[] ikm = kemIkm();
        byte[] plaintext = "phi-secret".getBytes(StandardCharsets.UTF_8);
        SecureEnvelope sealed = EnvelopeCrypto.seal(
                ikm, Direction.BE_TO_AGENT, ENV_ID, JOB, plaintext);

        EnvelopeClient opener = EnvelopeClient.withIkm(ENV_ID, ikm, MAPPER);
        assertArrayEquals(plaintext, opener.open(sealed, JOB));
    }

    @Test
    void bearer_derived_ikm_fails_to_open_kemIkm_sealed_envelope() throws Exception {
        byte[] ikm = kemIkm();
        byte[] plaintext = "phi-secret".getBytes(StandardCharsets.UTF_8);
        SecureEnvelope sealed = EnvelopeCrypto.seal(
                ikm, Direction.BE_TO_AGENT, ENV_ID, JOB, plaintext);

        EnvelopeClient bearerOpener = EnvelopeClient.fromBearer(ENV_ID, BEARER, MAPPER);
        assertThrows(GeneralSecurityException.class, () -> bearerOpener.open(sealed, JOB));
    }

    @Test
    void preferKemIkm_uses_hybrid_ikm_when_present() throws Exception {
        byte[] ikm = kemIkm();
        byte[] plaintext = "ok".getBytes(StandardCharsets.UTF_8);
        SecureEnvelope sealed = EnvelopeCrypto.seal(
                ikm, Direction.BE_TO_AGENT, ENV_ID, JOB, plaintext);

        EnvelopeClient preferred = EnvelopeClient.preferKemIkm(ENV_ID, ikm, BEARER, MAPPER);
        assertArrayEquals(plaintext, preferred.open(sealed, JOB));
    }

    @Test
    void daemonAuthCredential_fromKemIkm_has_sqikm1_prefix() {
        String cred = DaemonAuthCredential.fromKemIkm(kemIkm(), ENV_ID);
        assertTrue(DaemonAuthCredential.looksLike(cred));
        assertTrue(cred.startsWith(DaemonAuthCredential.PREFIX));
    }
}
