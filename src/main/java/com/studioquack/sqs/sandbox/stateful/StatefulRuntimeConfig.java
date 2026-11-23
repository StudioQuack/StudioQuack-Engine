package com.studioquack.sqs.sandbox.stateful;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Spring wiring for the stateful custom-OCI runtime. Binds {@link StatefulRuntimeProperties}
 * ({@code sqs.sandbox.oci.*}) so operators can see/set the blob store, upload credential,
 * container naming, hardening deltas, and the in-instance app convention.
 */
@Configuration
@EnableConfigurationProperties(StatefulRuntimeProperties.class)
public class StatefulRuntimeConfig {
}
