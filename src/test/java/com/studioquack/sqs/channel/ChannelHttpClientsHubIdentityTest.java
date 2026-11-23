package com.studioquack.sqs.channel;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Test;

/**
 * HttpURLConnection applies its own hostname verifier on top of the trust manager.
 * The verifier must accept the public backend name when the hub is dialed by IP,
 * and must still reject a name that matches neither.
 */
class ChannelHttpClientsHubIdentityTest {

    private static X509Certificate edgeLeaf() throws Exception {
        X509Certificate leaf = mock(X509Certificate.class);
        when(leaf.getSubjectAlternativeNames()).thenReturn(List.of(
                List.of(2, "api.studioquack.com")));
        when(leaf.getSubjectX500Principal()).thenReturn(new X500Principal("CN=api.studioquack.com"));
        return leaf;
    }

    private static SSLSession session() throws Exception {
        X509Certificate leaf = edgeLeaf();
        SSLSession session = mock(SSLSession.class);
        when(session.getPeerCertificates()).thenReturn(new java.security.cert.Certificate[]{leaf});
        return session;
    }

    @Test
    void verifierAcceptsDialedHostOrPublicBackendAlias() throws Exception {
        ChannelHttpClients clients = new ChannelHttpClients(null, Set.of("api.studioquack.com"));
        SSLSession session = session();
        assertTrue(clients.hostnameVerifier().verify("api.studioquack.com", session));
        assertTrue(clients.hostnameVerifier().verify("192.0.2.10", session));
    }

    @Test
    void verifierRejectsANameThatMatchesNeither() throws Exception {
        ChannelHttpClients clients = new ChannelHttpClients(null, Set.of());
        assertFalse(clients.hostnameVerifier().verify("192.0.2.10", session()));
        assertFalse(clients.hostnameVerifier().verify("evil.example", session()));
    }
}
