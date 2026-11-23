package com.studioquack.sqs.secrets.os;

import com.studioquack.sqs.sandbox.runtime.CliRunner;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

/**
 * Shared plumbing for the platform {@link OsSecretManager}s that drive a native OS CLI (
 * {@code security} / {@code secret-tool} / {@code keyctl} / {@code powershell}) through
 * {@link CliRunner}. Keeps each concrete manager down to just its CLI verbs.
 */
abstract class CliBackedSecretManager implements OsSecretManager {

    /** A generous wall-clock cap: keychain unlock + item lookup is sub-second, but be lenient. */
    protected static final Duration CLI_TIMEOUT = Duration.ofSeconds(15);

    private final String osName = System.getProperty("os.name", "").toLowerCase();

    /** @return true when the current host OS name contains {@code token} (e.g. {@code "mac"}). */
    protected boolean osIs(String token) {
        return osName.contains(token);
    }

    /** Base64 of the UTF-8 bytes of {@code raw} — the on-the-wire form handed to the CLI. */
    protected String enc(String raw) {
        return Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /** Inverse of {@link #enc}; tolerates a trailing newline the CLI may append on read. */
    protected String dec(String wire) {
        String trimmed = wire == null ? "" : wire.strip();
        return new String(Base64.getDecoder().decode(trimmed), StandardCharsets.UTF_8);
    }

    /** Runs {@code argv} with no stdin under the shared timeout. */
    protected CliRunner.Result run(List<String> argv) {
        return CliRunner.run(argv, CLI_TIMEOUT);
    }


    protected CliRunner.Result run(List<String> argv, String stdin) {
        byte[] in = stdin == null ? null : stdin.getBytes(StandardCharsets.UTF_8);
        return CliRunner.run(argv, in, CLI_TIMEOUT, 256 * 1024, 64 * 1024);
    }
}
