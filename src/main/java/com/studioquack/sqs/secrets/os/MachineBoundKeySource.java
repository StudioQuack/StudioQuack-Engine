package com.studioquack.sqs.secrets.os;

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Derives the machine-bound master key for {@link MachineBoundFileSecretManager} from the host's
 * stable machine identity — on Linux the systemd {@code machine-id(5)}. The key is derived, never
 * stored: every boot re-derives the identical AES-256 key, which is exactly what makes the sealed
 * store reboot-persistent (the failure mode of the kernel-keyring backend).
 */
@Slf4j
final class MachineBoundKeySource {

    /** Fixed HKDF salt — versioned so a future format change can rotate. */
    private static final byte[] SALT =
            "com.studioquack.sqs.machine-bound.v1".getBytes(StandardCharsets.UTF_8);


    private static final List<Path> LINUX_MACHINE_ID_PATHS = List.of(
            Path.of("/etc/machine-id"),
            Path.of("/var/lib/dbus/machine-id"));

    private static final int KEY_BYTES = 32;

    private MachineBoundKeySource() {
    }

    /**
     * Derives the host's machine-bound AES-256 key, or empty when no usable machine identity exists on
     * this platform (non-Linux hosts use their native persistent stores instead).
     *
     * @param servicePrefix The custody namespace ({@code sqs.secrets.os.service-prefix}); used as the
     * HKDF {@code info} so two prefixes on one host derive distinct keys
     */
    static Optional<SecretKeySpec> derive(String servicePrefix) {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (!os.contains("linux")) {
            return Optional.empty();
        }
        return readMachineId(LINUX_MACHINE_ID_PATHS)
                .map(id -> hkdfSha256(id.getBytes(StandardCharsets.UTF_8), SALT,
                        infoBytes(servicePrefix), KEY_BYTES))
                .map(raw -> new SecretKeySpec(raw, "AES"));
    }

    /** First readable, well-formed machine-id wins; a malformed one is skipped with a warning. */
    static Optional<String> readMachineId(List<Path> paths) {
        for (Path p : paths) {
            try {
                if (!Files.isRegularFile(p)) {
                    continue;
                }
                String id = Files.readString(p, StandardCharsets.UTF_8).trim();
                if (id.matches("[0-9a-fA-F]{32}")) {
                    return Optional.of(id.toLowerCase());
                }
                log.warn("machine-bound key source: {} does not contain a 32-hex machine-id — skipping", p);
            } catch (Exception e) {
                log.warn("machine-bound key source: could not read {} ({}) — trying next", p, e.getMessage());
            }
        }
        return Optional.empty();
    }

    private static byte[] infoBytes(String servicePrefix) {
        String info = (servicePrefix == null || servicePrefix.isBlank())
                ? "com.studioquack.sqs" : servicePrefix.trim();
        return info.getBytes(StandardCharsets.UTF_8);
    }

    /** RFC 5869 HKDF-SHA256, extract + single-block expand (output ≤ 32 bytes by construction). */
    static byte[] hkdfSha256(byte[] ikm, byte[] salt, byte[] info, int length) {
        if (length < 1 || length > 32) {
            throw new IllegalArgumentException("HKDF-SHA256 single-block output must be 1..32 bytes");
        }
        try {
            Mac hmac = Mac.getInstance("HmacSHA256");
            // Extract: PRK = HMAC-Hash(salt, IKM). RFC 5869: a zero salt means HashLen zero bytes.
            byte[] actualSalt = (salt == null || salt.length == 0) ? new byte[32] : salt;
            hmac.init(new SecretKeySpec(actualSalt, "HmacSHA256"));
            byte[] prk = hmac.doFinal(ikm);
            // Expand (one block): T(1) = HMAC-Hash(PRK, info || 0x01).
            hmac.init(new SecretKeySpec(prk, "HmacSHA256"));
            hmac.update(info == null ? new byte[0] : info);
            hmac.update((byte) 0x01);
            byte[] t = hmac.doFinal();
            byte[] out = new byte[length];
            System.arraycopy(t, 0, out, 0, length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("HKDF-SHA256 derivation failed", e);
        }
    }
}
