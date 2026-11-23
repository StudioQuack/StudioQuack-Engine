package com.studioquack.sqs.secrets.os;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * Linux {@link OsSecretManager} backed by libsecret / the Secret Service via the
 * {@code secret-tool} CLI (GNOME Keyring, KWallet, or any Secret Service provider). Preferred
 * Linux backend because it is persistent, per-user, and unlocked with the login session.
 */
@Slf4j
public class SecretToolSecretManager extends CliBackedSecretManager {

    private static final String SECRET_TOOL = "secret-tool";

    private final String secretToolPath;

    public SecretToolSecretManager() {
        this.secretToolPath = CliRunner.lookPath(SECRET_TOOL);
    }

    @Override
    public boolean available() {
        return osIs("linux") && secretToolPath != null;
    }

    @Override
    public String name() {
        return "linux-secret-tool";
    }

    @Override
    public Optional<String> get(String service, String account) {
        CliRunner.Result r = run(List.of(secretToolPath, "lookup", "service", service, "account", account));
        if (r.ok() && r.stdout() != null && !r.stdout().isBlank()) {
            try {
                return Optional.of(dec(r.stdout()));
            } catch (IllegalArgumentException e) {
                log.warn("secret-tool: value at service={} account={} not valid Base64 — treating as absent",
                        service, account);
            }
        }
        return Optional.empty();
    }

    @Override
    public void put(String service, String account, String secret) {
        // The secret is streamed on stdin (secret-tool's contract), keeping it off argv.
        CliRunner.Result r = run(List.of(secretToolPath, "store", "--label=" + service,
                "service", service, "account", account), enc(secret));
        if (!r.ok()) {
            throw new OsSecretException("secret-tool store failed service=" + service
                    + " account=" + account + " exit=" + r.exitCode() + " err=" + r.stderr());
        }
    }

    @Override
    public boolean delete(String service, String account) {
        CliRunner.Result r = run(List.of(secretToolPath, "clear", "service", service, "account", account));
        return r.ok();
    }
}
