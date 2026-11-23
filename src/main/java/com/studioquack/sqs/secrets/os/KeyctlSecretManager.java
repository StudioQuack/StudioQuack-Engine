package com.studioquack.sqs.secrets.os;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * Linux {@link OsSecretManager} fallback backed by the kernel keyring via {@code keyctl}, used on
 * headless/minimal hosts where libsecret ({@code secret-tool}) is not installed. Keys live in the
 * user keyring ({@code @u}), described by {@code <service>:<account>}.
 */
@Slf4j
public class KeyctlSecretManager extends CliBackedSecretManager {

    private static final String KEYCTL = "keyctl";
    private static final String USER_KEYRING = "@u";

    private final String keyctlPath;

    public KeyctlSecretManager() {
        this.keyctlPath = CliRunner.lookPath(KEYCTL);
    }

    @Override
    public boolean available() {
        return osIs("linux") && keyctlPath != null;
    }

    @Override
    public String name() {
        return "linux-keyctl";
    }

    private String desc(String service, String account) {
        return service + ":" + account;
    }

    /** Resolves the key id for a description, or empty when the keyring has no such key. */
    private Optional<String> keyId(String service, String account) {
        CliRunner.Result r = run(List.of(keyctlPath, "request", "user", desc(service, account)));
        if (r.ok() && r.stdout() != null && !r.stdout().isBlank()) {
            return Optional.of(r.stdout().strip());
        }
        return Optional.empty();
    }

    @Override
    public Optional<String> get(String service, String account) {
        Optional<String> id = keyId(service, account);
        if (id.isEmpty()) {
            return Optional.empty();
        }
        CliRunner.Result r = run(List.of(keyctlPath, "pipe", id.get()));
        if (r.ok() && r.stdout() != null && !r.stdout().isBlank()) {
            try {
                return Optional.of(dec(r.stdout()));
            } catch (IllegalArgumentException e) {
                log.warn("keyctl: payload for desc={} not valid Base64 — treating as absent",
                        desc(service, account));
            }
        }
        return Optional.empty();
    }

    @Override
    public void put(String service, String account, String secret) {
        CliRunner.Result r = run(List.of(keyctlPath, "padd", "user", desc(service, account), USER_KEYRING),
                enc(secret));
        if (!r.ok()) {
            throw new OsSecretException("keyctl padd failed desc=" + desc(service, account)
                    + " exit=" + r.exitCode() + " err=" + r.stderr());
        }
    }

    @Override
    public boolean delete(String service, String account) {
        Optional<String> id = keyId(service, account);
        if (id.isEmpty()) {
            return false;
        }
        CliRunner.Result r = run(List.of(keyctlPath, "unlink", id.get(), USER_KEYRING));
        return r.ok();
    }
}
