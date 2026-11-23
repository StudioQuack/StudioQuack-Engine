package com.studioquack.sqs.secrets.os;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring wiring for OS secret-manager custody. Publishes exactly one {@link OsSecretManager} bean,
 * chosen at boot by {@link OsSecretManagerFactory} from {@link OsSecretManagerProperties}.
 */
@Configuration
@EnableConfigurationProperties(OsSecretManagerProperties.class)
public class OsSecretManagerConfig {

    @Bean
    @ConditionalOnMissingBean(OsSecretManager.class)
    public OsSecretManager osSecretManager(OsSecretManagerProperties props) {
        return OsSecretManagerFactory.create(props);
    }
}
