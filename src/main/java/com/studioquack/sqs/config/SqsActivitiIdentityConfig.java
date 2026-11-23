package com.studioquack.sqs.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

/**
 * Satisfies the one dependency Activiti's {@code activiti-spring-boot-starter} identity
 * auto-configuration requires to boot the engine: a {@link UserDetailsService} backing its
 * {@code userGroupManager}. On the backend this bean comes from the shared security config.
 */
@Slf4j
@Configuration
public class SqsActivitiIdentityConfig {

    @Bean
    public UserDetailsService sqsEngineUserDetailsService() {
        log.info("SQS Activiti identity: empty in-memory UserDetailsService (single-tenant; "
                + "engine identity model unused — real auth is owned by WP-E4).");
        return new InMemoryUserDetailsManager();
    }
}
