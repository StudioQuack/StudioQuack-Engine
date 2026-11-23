package com.studioquack.sqs.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * {@code spring-boot-starter-security} is on the classpath; without this chain, auto-config would
 * lock every route behind a generated password. Health/liveness and a few ops matchers are
 * {@code permitAll}; those ops routes re-check loopback + {@code X-SQS-Host-Op} in-controller (
 * {@code LoopbackOpsGuard}).
 */
@Configuration
public class SqsWebSecurityConfig {

    @Bean
    public SecurityFilterChain sqsFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/info", "/sqs/v1/health").permitAll()
                        // The local-uninstall purge endpoint is loopback-only (verified
                        // in-controller against the request's remote address, fail-closed), so it is
                        // permitted here rather than behind httpBasic.
                        .requestMatchers("/sqs/v1/ops/local-uninstall").permitAll()
                        // Another engine's installer asks this daemon to report that it is being
                        // replaced on this host. Loopback-only, re-checked in LocalReplacedController.
                        .requestMatchers("/sqs/v1/ops/replaced").permitAll()
                        // Health detail is host-local: the same loopback-only, verified-in-controller
                        // posture as the purge above (HealthController → LoopbackOpsGuard).
                        .requestMatchers("/sqs/v1/ops/health").permitAll()

                        .anyRequest().denyAll())
                .httpBasic(basic -> basic.disable())
                .formLogin(form -> form.disable());
        return http.build();
    }
}
