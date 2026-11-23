package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Enrolled TLS material that will not build must not fall back to a JVM-default {@code HttpClient}. */
class ChannelHttpClientsFailClosedTest {

    @Test
    void enrolledTlsBuildFailureRefusesDefaultClient() throws Exception {
        MtlsIdentity identity = mock(MtlsIdentity.class);
        when(identity.isEnrolled()).thenReturn(true);
        when(identity.keyManagers()).thenThrow(new RuntimeException("corrupt leaf"));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> new ChannelHttpClients(identity));
        assertTrue(ex.getMessage().contains("enrolled mTLS material"));
        assertTrue(ex.getMessage().contains("refusing default HttpClient fallback"));
    }

    @Test
    void unenrolledBootstrapStillBuildsAClient() {
        // null identity + no pin: ChannelTls uses the platform store; must not throw.
        ChannelHttpClients clients = new ChannelHttpClients(null);
        assertTrue(clients.get() != null);
        assertTrue(clients.streaming() != null);
    }
}
