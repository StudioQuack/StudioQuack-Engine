package com.studioquack.sqs.secrets.os;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Sources the local database {@code CIPHER=AES} file password from the {@link OsSecretManager} at
 * boot, so the base-tier engine database is encrypted at rest with a key held by the OS secret
 * manager rather than a value living in a config. the local database expects the connection
 * password for a ciphered database to be two space-separated tokens —
 * {@code "<filePassword> <userPassword>"} — neither containing a space.
 */
public class H2CipherPasswordEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final Logger log = LoggerFactory.getLogger(H2CipherPasswordEnvironmentPostProcessor.class);

    private static final String PROPERTY_SOURCE_NAME = "sqs-h2-cipher-password";
    private static final String H2_FILE_PURPOSE = "h2-file";
    private static final String H2_USER_PURPOSE = "h2-user";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Function<OsSecretManagerProperties, OsSecretManager> secretManagerFactory;

    public H2CipherPasswordEnvironmentPostProcessor() {
        this(OsSecretManagerFactory::create);
    }

    /** Test seam: supplies a deterministic secret manager without touching the host keychain. */
    H2CipherPasswordEnvironmentPostProcessor(Function<OsSecretManagerProperties, OsSecretManager> secretManagerFactory) {
        this.secretManagerFactory = Objects.requireNonNull(secretManagerFactory, "secretManagerFactory");
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        try {
            String url = environment.getProperty("spring.datasource.url");
            if (url == null || !url.contains("CIPHER=")) {
                return;
            }
            String existing = environment.getProperty("spring.datasource.password");
            if (existing != null && existing.trim().contains(" ")) {
                return; // operator already supplied a two-token cipher password.
            }

            OsSecretManagerProperties osProps = Binder.get(environment)
                    .bind("sqs.secrets.os", OsSecretManagerProperties.class)
                    .orElseGet(OsSecretManagerProperties::new);

            OsSecretManager mgr = secretManagerFactory.apply(osProps);
            if (!mgr.available()) {
                log.warn("H2 datasource requests CIPHER=AES but no OS secret manager is available to "
                        + "custody the file password. Either install the platform secret tool or remove "
                        + "CIPHER=AES from spring.datasource.url. Leaving the password untouched.");
                return;
            }

            String filePw = getOrCreate(mgr, service(osProps, H2_FILE_PURPOSE), osProps.getAccount());
            String userPw = (existing != null && !existing.isBlank())
                    ? existing.trim()
                    : getOrCreate(mgr, service(osProps, H2_USER_PURPOSE), osProps.getAccount());

            Map<String, Object> injected = new LinkedHashMap<>();
            injected.put("spring.datasource.password", filePw + " " + userPw);
            environment.getPropertySources().addFirst(new MapPropertySource(PROPERTY_SOURCE_NAME, injected));
            log.info("H2 at-rest encryption ENABLED (CIPHER=AES); file password sourced from OS secret "
                    + "manager '{}'.", mgr.name());
        } catch (Throwable t) {
            // Never fail the boot from here — degrade to the operator-configured password.
            log.warn("H2 cipher password wiring skipped ({}). spring.datasource.password left as-is.",
                    t.getMessage());
        }
    }

    private static String service(OsSecretManagerProperties osProps, String purpose) {
        return osProps.getServicePrefix() + "." + purpose;
    }

    /** Returns the stored token, creating a fresh 32-hex (space-free) one on first run. */
    private static String getOrCreate(OsSecretManager mgr, String service, String account) {
        Optional<String> existing = mgr.get(service, account);
        if (existing.isPresent() && !existing.get().isBlank()) {
            return existing.get().trim();
        }
        byte[] raw = new byte[16];
        RANDOM.nextBytes(raw);
        String token = HexFormat.of().formatHex(raw); // 32 hex chars, never contains a space
        mgr.put(service, account, token);
        return token;
    }

    @Override
    public int getOrder() {
        // Run after Boot's config-data processing so application.yml (the datasource url) and
        // sqs.* properties are already bound and resolvable.
        return Ordered.LOWEST_PRECEDENCE;
    }
}
