package com.studioquack.sqs.sandbox.secrets;

import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the default {@link SecretKeyStore} for the sandbox. The production keystore backed by the
 * OS secret manager (Keychain / DPAPI / libsecret / keyring) is now the default whenever a manager
 * is available ({@link OsSecretKeyStore}); on a host with no reachable platform store it falls
 * back to {@link NoopSecretKeyStore}, exactly as the deliverable requires ("falling back to Noop
 * only when it isn't [available]").
 */
@Slf4j
@Configuration
public class SandboxSecretsConfig {

    @Bean
    @ConditionalOnMissingBean(SecretKeyStore.class)
    public SecretKeyStore secretKeyStore(OsSecretManager osSecretManager, SecretStoreNamespace namespace) {
        if (osSecretManager != null && osSecretManager.available()) {
            return new OsSecretKeyStore(osSecretManager, namespace);
        }
        log.info("SecretKeyStore: no OS secret manager available — using no-op keystore "
                + "($secretRef resolution stays a structured no-op).");
        return new NoopSecretKeyStore();
    }
}
