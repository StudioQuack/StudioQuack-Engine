package com.studioquack.sqs.secrets.os;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

/**
 * Pure logic (no Spring), so the selection rules are unit-testable in isolation. Selection order
 * for {@code provider=auto} (the default): the native platform store if reachable — macOS
 * Keychain, Linux libsecret, Windows DPAPI;.
 */
@Slf4j
public final class OsSecretManagerFactory {

    private OsSecretManagerFactory() {
    }

    public static OsSecretManager create(OsSecretManagerProperties props) {
        if (!props.isEnabled()) {
            log.info("OS secret custody DISABLED by config (sqs.secrets.os.enabled=false) — "
                    + "context crypto stays plaintext-disabled and $secretRef resolution no-op.");
            return new UnavailableSecretManager();
        }
        Path dataDir = Path.of(props.getDataDir());
        String provider = props.getProvider() == null ? "auto" : props.getProvider().trim().toLowerCase();

        OsSecretManager selected = switch (provider) {
            case "off", "unavailable", "none" -> new UnavailableSecretManager();
            case "macos", "keychain" -> new MacKeychainSecretManager();
            case "secret-tool", "libsecret" -> new SecretToolSecretManager();
            case "keyctl", "keyring" -> new KeyctlSecretManager();
            case "machine-file", "machine" -> new MachineBoundFileSecretManager(
                    dataDir, props.getServicePrefix());
            case "windows", "dpapi" -> new WindowsCredentialSecretManager(dataDir);
            default -> auto(props, dataDir);
        };

        if (selected.available()) {
            log.info("OS secret custody: using backend '{}' (provider={}).", selected.name(), provider);
        } else {
            log.warn("OS secret custody backend '{}' (provider={}) is NOT available on this host — "
                    + "context crypto degrades to plaintext-disabled and $secretRef resolution to no-op. "
                    + "Install the platform secret tool (macOS security / Linux secret-tool|keyctl / "
                    + "Windows PowerShell).",
                    selected.name(), provider);
        }
        return selected;
    }

    /** Auto-detect the native platform store, then fall back to unavailable. */
    private static OsSecretManager auto(OsSecretManagerProperties props, Path dataDir) {
        String os = System.getProperty("os.name", "").toLowerCase();
        OsSecretManager nativeMgr = null;
        if (os.contains("mac")) {
            nativeMgr = new MacKeychainSecretManager();
        } else if (os.contains("linux")) {
            nativeMgr = linuxAuto(props, dataDir);
        } else if (os.contains("win")) {
            nativeMgr = new WindowsCredentialSecretManager(dataDir);
        }
        if (nativeMgr != null && nativeMgr.available()) {
            return nativeMgr;
        }
        return new UnavailableSecretManager();
    }

    /**
     * Linux custody order: libsecret when present (a real, persistent Secret Service store); otherwise
     * the reboot-persistent machine-bound file store. When keyctl is present it is kept as a migration
     * source only: entries sealed there by a pre-upgrade daemon move into the persistent store on
     * first read.
     */
    private static OsSecretManager linuxAuto(OsSecretManagerProperties props, Path dataDir) {
        OsSecretManager secretTool = new SecretToolSecretManager();
        if (secretTool.available()) {
            return secretTool;
        }
        OsSecretManager machineFile = new MachineBoundFileSecretManager(dataDir, props.getServicePrefix());
        if (machineFile.available()) {
            KeyctlSecretManager keyctl = new KeyctlSecretManager();
            if (keyctl.available()) {
                log.info("OS secret custody: machine-file selected; legacy kernel-keyring entries "
                        + "will migrate into it on first read.");
                return new MigratingSecretManager(machineFile, keyctl);
            }
            return machineFile;
        }
        return new KeyctlSecretManager();
    }
}
