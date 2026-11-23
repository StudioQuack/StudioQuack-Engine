package com.studioquack.common.security;

import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.utils.EnrollmentBearerUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Proves the two bearer-derived forms both sides MUST agree on */
class EnrollmentBearerParityTest {

    private static final HexFormat HEX = HexFormat.of();
    private static final String BEARER = "sqvm_test_bearer_kat_0001";

    @Test
    void ikm_from_bearer_is_sha256_fixed_vector() {
        byte[] ikm = EnvelopeCrypto.ikmFromBearer(BEARER);
        assertEquals(32, ikm.length, "IKM is the 32-byte SHA-256 digest");
        assertEquals("701ae10be6364725dffa44c88343e674c877f1448a626b9100037d910fb7e7a4",
                HEX.formatHex(ikm));
    }

    @Test
    void ikm_from_bearer_matches_independent_sha256() throws Exception {
        byte[] expected = MessageDigest.getInstance("SHA-256").digest(BEARER.getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(expected, EnvelopeCrypto.ikmFromBearer(BEARER));
    }

    @Test
    void ikm_from_blank_bearer_is_null() {
        assertNull(EnvelopeCrypto.ikmFromBearer(""));
        assertNull(EnvelopeCrypto.ikmFromBearer(null));
    }

    @Test
    void stored_hash_is_base64url_nopad_sha256_fixed_vector() {
        assertEquals("cBrhC-Y2RyXf-kTIg0PmdMh38USKYmuRAAN9kQ-356Q",
                EnrollmentBearerUtil.sha256(BEARER));
    }

    @Test
    void stored_hash_matches_independent_recompute() throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(BEARER.getBytes(StandardCharsets.UTF_8));
        String expected = Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        assertEquals(expected, EnrollmentBearerUtil.sha256(BEARER));
    }

    @Test
    void stored_hash_of_null_is_null() {
        assertNull(EnrollmentBearerUtil.sha256(null));
    }
}
