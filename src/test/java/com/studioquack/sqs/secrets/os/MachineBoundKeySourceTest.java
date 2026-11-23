package com.studioquack.sqs.secrets.os;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link MachineBoundKeySource}: the HKDF-SHA256 primitive is pinned against the RFC 5869
 * reference vector (a wrong-but-self-consistent KDF would still pass round-trips yet break
 * interop with any future re-implementation), and machine-id reading is exercised against
 * temp files.
 */
class MachineBoundKeySourceTest {

    @TempDir
    Path dir;

    /** RFC 5869, Test Case 1 (SHA-256), truncated to the single 32-byte block our KDF emits. */
    @Test
    void hkdf_matches_rfc_5869_test_case_1() {
        byte[] ikm = new byte[22]; // 0x0b repeated 22 times
        java.util.Arrays.fill(ikm, (byte) 0x0b);
        byte[] salt = HexFormat.of().parseHex("000102030405060708090a0b0c");
        byte[] info = HexFormat.of().parseHex("f0f1f2f3f4f5f6f7f8f9");
        byte[] expectedOkmPrefix =
                HexFormat.of().parseHex("3cb25f25faacd57a90434f64d0362f2a"
                        + "2d2d0a90cf1a5a4c5db02d56ecc4c5bf");
        assertArrayEquals(expectedOkmPrefix, MachineBoundKeySource.hkdfSha256(ikm, salt, info, 32));
    }

    @Test
    void hkdf_is_deterministic_across_calls() {
        byte[] a = MachineBoundKeySource.hkdfSha256("id".getBytes(), "s".getBytes(), "i".getBytes(), 32);
        byte[] b = MachineBoundKeySource.hkdfSha256("id".getBytes(), "s".getBytes(), "i".getBytes(), 32);
        assertArrayEquals(a, b, "reboot stability comes from derivation determinism");
    }

    @Test
    void reads_the_first_valid_machine_id() throws Exception {
        Path malformed = dir.resolve("malformed");
        Files.writeString(malformed, "not-a-machine-id\n");
        Path valid = dir.resolve("machine-id");
        Files.writeString(valid, "0123456789ABCDEF0123456789ABCDEF\n");

        Optional<String> id = MachineBoundKeySource.readMachineId(
                List.of(dir.resolve("missing"), malformed, valid));
        assertTrue(id.isPresent());
        assertEquals("0123456789abcdef0123456789abcdef", id.get(),
                "normalised to lowercase; trailing newline trimmed");
    }

    @Test
    void no_valid_machine_id_means_empty() throws Exception {
        Path malformed = dir.resolve("machine-id");
        Files.writeString(malformed, "zzzz\n");
        assertTrue(MachineBoundKeySource.readMachineId(
                List.of(dir.resolve("missing"), malformed)).isEmpty());
    }

    @Test
    void derive_is_linux_only() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (!os.contains("linux")) {
            assertTrue(MachineBoundKeySource.derive("com.studioquack.sqs").isEmpty(),
                    "non-Linux hosts use their native persistent stores instead");
        }
        // On Linux the result depends on this host's machine-id readability; both outcomes valid.
    }
}
