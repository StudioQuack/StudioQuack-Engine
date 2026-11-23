package com.studioquack.sqs.execution.crypto;

import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Optional;

/**
 * Production {@link ContextKeyProvider} that sources the {@code execution_context} at-rest AES key
 * from the host {@link OsSecretManager} (macOS Keychain / Linux libsecret|machine-file|keyring /
 * Windows DPAPI).
 *
 * <p><b>Key resolution:</b>
 * <ol>
 * <li><b>OS secret manager</b> — when a manager is {@link OsSecretManager#available available},
 * the key is fetched from it and <b>created on first run</b> (a fresh AES-256 key, persisted
 * back into the keychain) so the host seals at rest with a key that never lives in a config
 * file or environment variable;</li>
 * <li><b>disabled</b> — otherwise {@code null}, which leaves {@link ExecutionContextCrypto} in
 * transparent plaintext pass-through with a single boot WARN.</li>
 * </ol>
 *
 * <p>Custody failures (manager throws / CLI error) are caught and degrade to {@code null} rather than
 * failing the boot — availability of the store is a deployment concern, not a crash condition. The
 * resolved key is cached for the process lifetime so the keychain is touched at most once per boot.
 */
@Slf4j
@Primary
@Component
public class OsKeychainContextKeyProvider implements ContextKeyProvider {

    /** Purpose suffix under the configured service prefix (e.g. {@code com.studioquack.sqs.execution-context}). */
    private static final String CONTEXT_KEY_PURPOSE = "execution-context";
    /** AES-256: created-on-first-run key length. */
    private static final int NEW_KEY_BYTES = 32;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final OsSecretManager osSecretManager;
    private final OsSecretManagerProperties props;

    /** Cached resolution ("" = resolved-to-absent) so we hit the OS store at most once. */
    private volatile String cachedKey;
    private volatile boolean resolved;

    public OsKeychainContextKeyProvider(OsSecretManager osSecretManager,
                                        OsSecretManagerProperties props) {
        this.osSecretManager = osSecretManager;
        this.props = props;
    }

    @Override
    public String contextEncryptionKeyBase64() {
        if (resolved) {
            return StringUtils.isEmpty(cachedKey) ? null : cachedKey;
        }
        String key = resolve();
        this.cachedKey = key == null ? "" : key;
        this.resolved = true;
        return key;
    }

    private String resolve() {
        // OS secret manager: fetch, creating on first run.
        if (osSecretManager != null && osSecretManager.available()) {
            String service = contextService();
            String account = props.getAccount();
            try {
                Optional<String> existing = osSecretManager.get(service, account);
                if (existing.isPresent() && StringUtils.isNotBlank(existing.get())) {
                    return existing.get();
                }
                String fresh = newKeyBase64();
                osSecretManager.put(service, account, fresh);
                log.info("execution_context at-rest key CREATED on first run in OS secret manager '{}' "
                        + "(service={} account={}).", osSecretManager.name(), service, account);
                return fresh;
            } catch (RuntimeException e) {
                // OsSecretException (a RuntimeException) and any CLI/IO error land here.
                log.warn("execution_context at-rest key custody via OS secret manager '{}' failed "
                        + "({}); at-rest encryption will stay DISABLED (plaintext) this boot.",
                        osSecretManager.name(), e.getMessage());
                return null;
            }
        }
        // No custody available → disabled.
        return null;
    }

    private String contextService() {
        return props.getServicePrefix() + "." + CONTEXT_KEY_PURPOSE;
    }

    private static String newKeyBase64() {
        byte[] raw = new byte[NEW_KEY_BYTES];
        RANDOM.nextBytes(raw);
        return Base64.getEncoder().encodeToString(raw);
    }

    @Override
    public String source() {
        if (osSecretManager != null && osSecretManager.available()) {
            return "os-keychain:" + osSecretManager.name();
        }
        return "disabled(no-os-secret-manager)";
    }
}
