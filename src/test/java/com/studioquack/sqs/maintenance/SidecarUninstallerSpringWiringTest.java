package com.studioquack.sqs.maintenance;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Regression: Spring must construct {@link SidecarUninstaller} via its single {@code @Autowired}
 * constructor (Environment), not the package-private test constructor — otherwise startup fails
 * with {@code NoSuchMethodException: <init>} (the "No default constructor" class of bug). Mirrors
 * {@link HostServiceUninstallerSpringWiringTest}.
 */
@SpringJUnitConfig
@Import(SidecarUninstaller.class)
class SidecarUninstallerSpringWiringTest {

    @Autowired
    private SidecarUninstaller sidecarUninstaller;

    @Test
    void springConstructsSidecarUninstaller_withEnvironment() {
        assertNotNull(sidecarUninstaller, "SidecarUninstaller must autowire via its @Autowired ctor");
    }
}
