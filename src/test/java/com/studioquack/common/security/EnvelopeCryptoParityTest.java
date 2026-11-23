package com.studioquack.common.security;

import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.KemSchema;
import com.studioquack.common.security.envelope.SecureEnvelope;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Proves the vendored {@link EnvelopeCrypto} is wire-compatible with the unchanged peer copy */
class EnvelopeCryptoParityTest {

    private static final HexFormat HEX = HexFormat.of();

    /** A 32-byte IKM (content is irrelevant to the layout assertions). */
    private static byte[] ikm() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) {
            k[i] = (byte) (0x40 + i);
        }
        return k;
    }

    @Test
    void aad_byte_layout_matches_fixed_known_answer() {
        // v=1, kemSchema=PRESHARED_HASH(ord 0), dir=AGENT_TO_BE(ord 1), envId, jobKey.
        byte[] aad = EnvelopeCrypto.buildAad(
                EnvelopeCrypto.WIRE_VERSION, KemSchema.PRESHARED_HASH, Direction.AGENT_TO_BE,
                "env-kat-0001", "42");
        assertEquals("0100010000000c656e762d6b61742d30303031000000023432", HEX.formatHex(aad),
                "AAD layout must be [u8 v][u8 kemOrd][u8 dirOrd][u32be envLen][env][u32be jobLen][job]");
    }

    @Test
    void aad_byte_layout_matches_independent_serialisation() {
        String envId = "some-env";
        String jobKey = "job-7";
        byte[] expected = independentAad(1, 0, 1, envId, jobKey);
        byte[] actual = EnvelopeCrypto.buildAad(1, KemSchema.PRESHARED_HASH, Direction.AGENT_TO_BE, envId, jobKey);
        assertArrayEquals(expected, actual);
    }

    @Test
    void enum_ordinals_are_pinned() {
        // The AAD pins ordinals directly — a reorder would silently break the wire.
        assertEquals(0, KemSchema.PRESHARED_HASH.ordinal());
        assertEquals(0, Direction.BE_TO_AGENT.ordinal());
        assertEquals(1, Direction.AGENT_TO_BE.ordinal());
    }

    @Test
    void seal_open_roundtrip_be_to_agent() throws Exception {
        byte[] ikm = ikm();
        byte[] plaintext = "hello-agent".getBytes(StandardCharsets.UTF_8);
        SecureEnvelope env = EnvelopeCrypto.seal(ikm, Direction.BE_TO_AGENT, "env-1", "9", plaintext);

        assertEquals(EnvelopeCrypto.WIRE_VERSION, env.getV());
        assertEquals(EnvelopeCrypto.ALG_AES256GCM_HKDF_SHA256, env.getAlg());
        assertEquals(KemSchema.PRESHARED_HASH.name(), env.getKemSchema());
        assertEquals(Direction.BE_TO_AGENT.name(), env.getDir());

        byte[] opened = EnvelopeCrypto.open(ikm, env, Direction.BE_TO_AGENT, "env-1", "9");
        assertArrayEquals(plaintext, opened);
    }

    @Test
    void seal_open_roundtrip_agent_to_be() throws Exception {
        byte[] ikm = ikm();
        byte[] plaintext = "hello-be".getBytes(StandardCharsets.UTF_8);
        SecureEnvelope env = EnvelopeCrypto.seal(ikm, Direction.AGENT_TO_BE, "env-1", "9", plaintext);
        byte[] opened = EnvelopeCrypto.open(ikm, env, Direction.AGENT_TO_BE, "env-1", "9");
        assertArrayEquals(plaintext, opened);
    }

    @Test
    void open_fails_on_wrong_direction() throws Exception {
        byte[] ikm = ikm();
        SecureEnvelope env = EnvelopeCrypto.seal(ikm, Direction.BE_TO_AGENT, "env-1", "9",
                "x".getBytes(StandardCharsets.UTF_8));
        assertThrows(GeneralSecurityException.class,
                () -> EnvelopeCrypto.open(ikm, env, Direction.AGENT_TO_BE, "env-1", "9"));
    }

    @Test
    void open_fails_on_wrong_envId_and_jobKey() throws Exception {
        byte[] ikm = ikm();
        SecureEnvelope env = EnvelopeCrypto.seal(ikm, Direction.BE_TO_AGENT, "env-1", "9",
                "x".getBytes(StandardCharsets.UTF_8));
        assertThrows(GeneralSecurityException.class,
                () -> EnvelopeCrypto.open(ikm, env, Direction.BE_TO_AGENT, "env-OTHER", "9"));
        assertThrows(GeneralSecurityException.class,
                () -> EnvelopeCrypto.open(ikm, env, Direction.BE_TO_AGENT, "env-1", "999"));
    }

    @Test
    void open_fails_on_wrong_ikm() throws Exception {
        SecureEnvelope env = EnvelopeCrypto.seal(ikm(), Direction.BE_TO_AGENT, "env-1", "9",
                "x".getBytes(StandardCharsets.UTF_8));
        byte[] wrongIkm = new byte[32];
        assertThrows(GeneralSecurityException.class,
                () -> EnvelopeCrypto.open(wrongIkm, env, Direction.BE_TO_AGENT, "env-1", "9"));
    }

    /** Re-serialises the AAD from scratch to cross-check {@link EnvelopeCrypto#buildAad}. */
    private static byte[] independentAad(int v, int kemOrd, int dirOrd, String envId, String jobKey) {
        byte[] env = envId.getBytes(StandardCharsets.UTF_8);
        byte[] job = jobKey.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(v);
        out.write(kemOrd);
        out.write(dirOrd);
        writeU32Be(out, env.length);
        out.write(env, 0, env.length);
        writeU32Be(out, job.length);
        out.write(job, 0, job.length);
        return out.toByteArray();
    }

    private static void writeU32Be(ByteArrayOutputStream out, int v) {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }
}
