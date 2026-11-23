package com.studioquack.sqs.secrets.os;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

/**
 * MacOS {@link OsSecretManager} backed by the login Keychain via {@code /usr/bin/security} generic
 * passwords — real Apple Keychain custody, driven through {@link CliRunner} exactly like the
 * container-runtime CLIs (no JNI/native dependency). get →
 * {@code security find-generic-password -a <acct> -s <svc> -w} (prints the value; exit 44 when
 * absent);.
 */
@Slf4j
public class MacKeychainSecretManager extends CliBackedSecretManager {

    private static final String SECURITY = "security";
    /** {@code find-generic-password}'s "item not found" exit status. */
    private static final int ERR_NOT_FOUND = 44;

    private final String securityPath;

    public MacKeychainSecretManager() {
        this.securityPath = CliRunner.lookPath(SECURITY);
    }

    @Override
    public boolean available() {
        // macOS host + the security CLI resolvable on PATH. A create/read probe is deferred to the
        // first real use (get-or-create) so construction stays cheap and side-effect free.
        return osIs("mac") && securityPath != null;
    }

    @Override
    public String name() {
        return "macos-keychain";
    }

    @Override
    public Optional<String> get(String service, String account) {
        CliRunner.Result r = run(List.of(securityPath, "find-generic-password",
                "-a", account, "-s", service, "-w"));
        if (r.ok()) {
            String out = r.stdout();
            if (out == null || out.isBlank()) {
                return Optional.empty();
            }
            try {
                return Optional.of(dec(out));
            } catch (IllegalArgumentException e) {
                log.warn("keychain: value at service={} account={} is not valid Base64 — treating as absent",
                        service, account);
                return Optional.empty();
            }
        }
        if (r.exitCode() != ERR_NOT_FOUND) {
            // Not a clean "absent" — surface as a breadcrumb but still return empty so get-or-create
            // can proceed (a put with -U will repair a corrupt/locked slot).
            log.debug("keychain: find-generic-password service={} account={} exit={} err={}",
                    service, account, r.exitCode(), r.stderr());
        }
        return Optional.empty();
    }

    @Override
    public void put(String service, String account, String secret) {
        CliRunner.Result r = run(List.of(securityPath, "add-generic-password",
                "-a", account, "-s", service, "-U", "-w", enc(secret)));
        if (!r.ok()) {
            throw new OsSecretException("keychain add-generic-password failed service=" + service
                    + " account=" + account + " exit=" + r.exitCode() + " err=" + r.stderr());
        }
    }

    @Override
    public boolean delete(String service, String account) {
        CliRunner.Result r = run(List.of(securityPath, "delete-generic-password",
                "-a", account, "-s", service));
        return r.ok();
    }
}
