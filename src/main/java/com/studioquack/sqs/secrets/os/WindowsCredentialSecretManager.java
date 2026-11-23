package com.studioquack.sqs.secrets.os;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

/**
 * Windows {@link OsSecretManager} backed by DPAPI (Data Protection API, {@code CurrentUser} scope)
 * driven through PowerShell — no external module required, works on stock Windows. The value is
 * streamed to PowerShell on stdin (Base64) and returned on stdout (Base64), so no secret material
 * is placed on the argv line.
 */
@Slf4j
public class WindowsCredentialSecretManager extends CliBackedSecretManager {

    private final String powershellPath;
    private final Path storeDir;

    public WindowsCredentialSecretManager(Path dataDir) {
        String ps = CliRunner.lookPath("powershell");
        this.powershellPath = ps != null ? ps : CliRunner.lookPath("pwsh");
        this.storeDir = dataDir.resolve("dpapi");
    }

    @Override
    public boolean available() {
        return osIs("win") && powershellPath != null;
    }

    @Override
    public String name() {
        return "windows-dpapi";
    }

    /** Filesystem-safe, collision-resistant file for a {@code (service, account)} pair. */
    private Path blobFile(String service, String account) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest((service + "\u0000" + account).getBytes(StandardCharsets.UTF_8));
            return storeDir.resolve(HexFormat.of().formatHex(h) + ".bin");
        } catch (Exception e) {
            throw new OsSecretException("windows dpapi: cannot derive blob file name", e);
        }
    }

    @Override
    public Optional<String> get(String service, String account) {
        String file = blobFile(service, account).toString().replace("'", "''");
        String script = "if (Test-Path -LiteralPath '" + file + "') {"
                + " Add-Type -AssemblyName System.Security;"
                + " $p=[IO.File]::ReadAllBytes('" + file + "');"
                + " $b=[Security.Cryptography.ProtectedData]::Unprotect($p,$null,'CurrentUser');"
                + " [Console]::Out.Write([Convert]::ToBase64String($b)) }";
        CliRunner.Result r = run(List.of(powershellPath, "-NoProfile", "-NonInteractive", "-Command", script));
        if (r.ok() && r.stdout() != null && !r.stdout().isBlank()) {
            try {
                return Optional.of(dec(r.stdout()));
            } catch (IllegalArgumentException e) {
                log.warn("windows dpapi: blob for service={} account={} not valid Base64 — treating as absent",
                        service, account);
            }
        }
        return Optional.empty();
    }

    @Override
    public void put(String service, String account, String secret) {
        String file = blobFile(service, account).toString().replace("'", "''");
        String dir = storeDir.toString().replace("'", "''");
        String script = "New-Item -ItemType Directory -Force -Path '" + dir + "' | Out-Null;"
                + " Add-Type -AssemblyName System.Security;"
                + " $in=[Console]::In.ReadToEnd().Trim();"
                + " $b=[Convert]::FromBase64String($in);"
                + " $p=[Security.Cryptography.ProtectedData]::Protect($b,$null,'CurrentUser');"
                + " [IO.File]::WriteAllBytes('" + file + "',$p)";
        CliRunner.Result r = run(List.of(powershellPath, "-NoProfile", "-NonInteractive", "-Command", script),
                enc(secret));
        if (!r.ok()) {
            throw new OsSecretException("windows dpapi protect failed service=" + service
                    + " account=" + account + " exit=" + r.exitCode() + " err=" + r.stderr());
        }
    }

    @Override
    public boolean delete(String service, String account) {
        String file = blobFile(service, account).toString().replace("'", "''");
        String script = "if (Test-Path -LiteralPath '" + file + "') { Remove-Item -LiteralPath '"
                + file + "' -Force; 'deleted' } else { 'absent' }";
        CliRunner.Result r = run(List.of(powershellPath, "-NoProfile", "-NonInteractive", "-Command", script));
        return r.ok() && r.stdout() != null && r.stdout().contains("deleted");
    }
}
