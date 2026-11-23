package com.studioquack.sqs.secrets;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.secrets.os.OsSecretManagerProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class SecretStoreNamespaceTest {

    private SecretStoreNamespace namespace(String envId) {
        OsSecretManagerProperties props = new OsSecretManagerProperties();
        props.setServicePrefix("com.studioquack.sqs.test");
        SqsChannelProperties channel = new SqsChannelProperties();
        channel.setEnvId(envId);
        return new SecretStoreNamespace(props, channel);
    }

    @Test
    void enrolled_engine_gets_an_env_scoped_service() {
        SecretStoreNamespace ns = namespace("env-1");
        assertEquals("com.studioquack.sqs.test.secret", ns.legacySecretService());
        assertEquals("com.studioquack.sqs.test.secret.env-1", ns.secretService());
    }

    @Test
    void two_engines_on_one_host_never_share_a_namespace() {
        assertNotEquals(namespace("env-a").secretService(), namespace("env-b").secretService(),
                "a reinstall (new envId) must address a different namespace than its predecessor");
    }

    @Test
    void unenrolled_daemon_keeps_the_legacy_service() {
        SecretStoreNamespace ns = namespace("");
        assertEquals(ns.legacySecretService(), ns.secretService());
    }

    @Test
    void env_ids_are_sanitised_for_os_tooling() {
        assertEquals("com.studioquack.sqs.test.secret.env_1_x", namespace(" env 1/x ").secretService());
    }
}
