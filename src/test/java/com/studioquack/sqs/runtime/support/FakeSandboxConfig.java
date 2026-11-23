package com.studioquack.sqs.runtime.support;

import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Test configuration that replaces the real (container-backed) {@link SandboxExecutionPort} with
 * the in-process {@link FakeSandboxExecutionPort} for conformance tests. Marked {@code @Primary} so
 * it wins over {@code PodmanSandboxExecution} without disabling any other bean.
 */
@TestConfiguration
public class FakeSandboxConfig {

    @Bean
    @Primary
    public FakeSandboxExecutionPort fakeSandboxExecutionPort() {
        return new FakeSandboxExecutionPort();
    }
}
