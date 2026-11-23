package com.studioquack.sqs.channel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Test;

/**
 * The private-network hub is dialed by overlay IP but terminates on the public edge
 * certificate. Identity = trusted chain AND (dialed host OR public backend host).
 */
class BackendIdentityTrustManagerTest {

    /** Public edge certificate: DNS api.studioquack.com, no overlay address in the SAN. */
    private static X509Certificate edgeLeaf() throws Exception {
        X509Certificate leaf = mock(X509Certificate.class);
        when(leaf.getSubjectAlternativeNames()).thenReturn(List.of(
                List.of(2, "api.studioquack.com")));
        when(leaf.getSubjectX500Principal()).thenReturn(new X500Principal("CN=api.studioquack.com"));
        return leaf;
    }

    private static SSLEngine engineDialing(String host) {
        SSLEngine e = mock(SSLEngine.class);
        when(e.getPeerHost()).thenReturn(host);
        return e;
    }

    @Test
    void hubDialedByOverlayIpIsAcceptedWhenLeafNamesThePublicBackendHost() throws Exception {
        X509TrustManager chainOk = mock(X509TrustManager.class);
        BackendIdentityTrustManager tm = new BackendIdentityTrustManager(
                chainOk, Set.of("api.studioquack.com"));
        X509Certificate[] chain = {edgeLeaf()};

        assertDoesNotThrow(() -> tm.checkServerTrusted(chain, "RSA", engineDialing("192.0.2.10")));
        assertDoesNotThrow(() -> tm.checkServerTrusted(chain, "RSA", engineDialing("api.studioquack.com")));
    }

    @Test
    void unrelatedHostIsStillRefused() throws Exception {
        X509TrustManager chainOk = mock(X509TrustManager.class);
        BackendIdentityTrustManager tm = new BackendIdentityTrustManager(chainOk, Set.of("be.example.com"));
        X509Certificate[] chain = {edgeLeaf()};

        CertificateException ex = assertThrows(CertificateException.class,
                () -> tm.checkServerTrusted(chain, "RSA", engineDialing("192.0.2.10")));
        assertTrue(ex.getMessage().contains("names neither"), ex.getMessage());
    }

    @Test
    void chainValidationFailureIsNotMaskedByAlias() throws Exception {
        X509TrustManager chainBad = mock(X509TrustManager.class);
        doThrow(new CertificateException("untrusted issuer"))
                .when(chainBad).checkServerTrusted(any(X509Certificate[].class), anyString());
        BackendIdentityTrustManager tm = new BackendIdentityTrustManager(
                chainBad, Set.of("api.studioquack.com"));
        X509Certificate[] chain = {edgeLeaf()};

        assertThrows(CertificateException.class,
                () -> tm.checkServerTrusted(chain, "RSA", engineDialing("api.studioquack.com")));
    }

    @Test
    void dnsMatchingRules() throws Exception {
        assertTrue(BackendIdentityTrustManager.dnsMatches("*.studioquack.com", "app.studioquack.com"));
        assertFalse(BackendIdentityTrustManager.dnsMatches("*.studioquack.com", "a.b.studioquack.com"));
        assertFalse(BackendIdentityTrustManager.dnsMatches("*.studioquack.com", "studioquack.com"));
        assertTrue(BackendIdentityTrustManager.looksLikeIp("192.0.2.10"));
        assertTrue(BackendIdentityTrustManager.looksLikeIp("fd7a:115c::1"));
        assertFalse(BackendIdentityTrustManager.looksLikeIp("api.studioquack.com"));
        assertEquals("api.studioquack.com", BackendIdentityTrustManager.normalise("API.StudioQuack.com."));
        assertTrue(BackendIdentityTrustManager.leafNames(edgeLeaf(), "api.studioquack.com"));
        assertFalse(BackendIdentityTrustManager.leafNames(edgeLeaf(), "192.0.2.10"));
    }
}
