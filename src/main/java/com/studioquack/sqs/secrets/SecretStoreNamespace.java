package com.studioquack.sqs.secrets;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.springframework.stereotype.Component;

/**
 * Addresses the OS secret-store service this engine's committed secrets live under. That made a
 * stale entry resolvable by a different engine on the same host: uninstall the engine, reinstall a
 * fresh one, and any workflow still carrying the old ref would resolve the previous engine's
 * leftover plaintext whenever the.
 */
@Component
public class SecretStoreNamespace {


    public static final String SECRET_PURPOSE = "secret";

    private final OsSecretManagerProperties osProps;
    private final SqsChannelProperties channel;

    public SecretStoreNamespace(OsSecretManagerProperties osProps, SqsChannelProperties channel) {
        this.osProps = osProps;
        this.channel = channel;
    }


    public String legacySecretService() {
        return osProps.getServicePrefix() + "." + SECRET_PURPOSE;
    }


    public String secretService() {
        String envId = channel.getEnvId();
        if (envId == null || envId.isBlank()) {
            return legacySecretService();
        }
        return legacySecretService() + "." + sanitize(envId);
    }

    /** OS service names are free-form, but keep the suffix tooling-friendly (secret-tool / keyctl). */
    static String sanitize(String envId) {
        return envId.trim().replaceAll("[^A-Za-z0-9_.-]", "_");
    }
}
