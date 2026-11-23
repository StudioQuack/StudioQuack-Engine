package com.studioquack.sqs.maintenance;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression: Spring must construct {@link HostServiceUninstaller} via constructor injection
 * (ProcessRunner bean), not {@code instantiateBean} no-arg. A second package-private test
 * constructor previously caused startup failure:
 * {@code NoSuchMethodException: HostServiceUninstaller.<init>()}.
 */
@SpringJUnitConfig
@Import({MaintenanceConfig.class, HostServiceUninstaller.class})
class HostServiceUninstallerSpringWiringTest {

    @Autowired
    private HostServiceUninstaller hostServiceUninstaller;

    @Autowired
    private DaemonLifecycleService.ProcessRunner processRunner;

    @Test
    void springConstructsHostServiceUninstaller_withProcessRunner() {
        assertNotNull(processRunner, "ProcessRunner bean from MaintenanceConfig");
        assertNotNull(hostServiceUninstaller, "HostServiceUninstaller must autowire via @Autowired ctor");
    }
}
