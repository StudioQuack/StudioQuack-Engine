package com.studioquack.sqs.aimodel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProviderDialGuardTest {

    @Test
    void loopbackIsAllowed() {
        assertNull(ProviderDialGuard.refusal("http://127.0.0.1:1234/v1/models"));
        assertNull(ProviderDialGuard.refusal("http://localhost:1234/v1/models"));
        assertNull(ProviderDialGuard.refusal("http://[::1]:1234/v1/models"));
    }

    @Test
    void privateAndMetadataAreRefused() {
        assertNotNull(ProviderDialGuard.refusal("http://169.254.169.254/latest/meta-data"));
        assertNotNull(ProviderDialGuard.refusal("http://10.0.0.1/v1/models"));
        assertNotNull(ProviderDialGuard.refusal("http://192.168.1.10/v1/models"));
        assertNotNull(ProviderDialGuard.refusal("http://172.16.0.2/v1/models"));
        assertNotNull(ProviderDialGuard.refusal("http://100.64.0.1/v1/models"));
        assertTrue(ProviderDialGuard.refusal("file:///etc/passwd").toLowerCase().contains("http"));
    }
}
