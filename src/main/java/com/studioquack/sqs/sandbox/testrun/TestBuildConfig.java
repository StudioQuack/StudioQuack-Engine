package com.studioquack.sqs.sandbox.testrun;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Spring wiring for daemon-owned test build+run. Binds {@link TestBuildProperties} ({@code
 * sqs.testbuild.*}).
 */
@Configuration
@EnableConfigurationProperties(TestBuildProperties.class)
public class TestBuildConfig {
}
