package com.studioquack.sqs.channel;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

/**
 * Outbound backend identity for the daemon's HTTPS clients. The backend is reached on two addresses that
 * terminate on the SAME TLS endpoint (the nginx edge / BE load balancer): the public base (
 * {@code https://<be-host>/...}) and the private-network hub ({@code https://<gateway overlay ip>:5005/...},
 * an L4 splice on the gateway node — TLS is still end-to-end daemon ↔ edge).
 */
public final class BackendIdentityTrustManager extends X509ExtendedTrustManager {

    private static final int SAN_DNS = 2;
    private static final int SAN_IP = 7;

    private final X509TrustManager delegate;
    private final Set<String> aliasHosts;

    /**
     * @param delegate chain validator (platform default trust store)
     * @param aliasHosts hosts the leaf may name instead of the dialed host (public BE host); null/blank entries
     * ignored
     */
    public BackendIdentityTrustManager(X509TrustManager delegate, Collection<String> aliasHosts) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate trust manager is required");
        }
        this.delegate = delegate;
        Set<String> hosts = new LinkedHashSet<>();
        if (aliasHosts != null) {
            for (String h : aliasHosts) {
                if (h != null && !h.isBlank()) {
                    hosts.add(normalise(h));
                }
            }
        }
        this.aliasHosts = Set.copyOf(hosts);
    }

    // Server side (what the daemon verifies when it dials the backend) -------------------

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
        String dialed = null;
        if (socket instanceof SSLSocket ssl) {
            SSLSession session = ssl.getHandshakeSession();
            dialed = session == null ? null : session.getPeerHost();
        }
        verifyIdentity(chain, dialed);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
        verifyIdentity(chain, engine == null ? null : engine.getPeerHost());
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // No dialed host available on this overload: identity must come from the alias set.
        delegate.checkServerTrusted(chain, authType);
        verifyIdentity(chain, null);
    }

    // Client side (never used on this outbound leg; pass through) -----------------------

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }

    // Identity ---------------------------------------------------------------------------

    private void verifyIdentity(X509Certificate[] chain, String dialedHost) throws CertificateException {
        if (chain == null || chain.length == 0) {
            throw new CertificateException("empty certificate chain");
        }
        X509Certificate leaf = chain[0];
        if (dialedHost != null && !dialedHost.isBlank() && leafNames(leaf, normalise(dialedHost))) {
            return;
        }
        for (String alias : aliasHosts) {
            if (leafNames(leaf, alias)) {
                return;
            }
        }
        throw new CertificateException("backend certificate names neither the dialed host "
                + (dialedHost == null ? "<unknown>" : dialedHost)
                + " nor a configured backend host " + aliasHosts
                + " (subject=" + leaf.getSubjectX500Principal().getName(X500Principal.RFC2253) + ")");
    }

    /**
     * RFC 6125-style match of {@code host} against the leaf's SANs (dNSName / iPAddress).
     */
    static boolean leafNames(X509Certificate leaf, String host) throws CertificateException {
        Collection<List<?>> sans;
        try {
            sans = leaf.getSubjectAlternativeNames();
        } catch (CertificateParsingException e) {
            throw new CertificateException("unreadable subjectAltName extension", e);
        }
        boolean hostIsIp = looksLikeIp(host);
        boolean sawDns = false;
        if (sans != null) {
            for (List<?> san : sans) {
                if (san == null || san.size() < 2 || !(san.get(0) instanceof Integer type)
                        || !(san.get(1) instanceof String value)) {
                    continue;
                }
                String v = normalise(value);
                if (type == SAN_IP && hostIsIp && v.equals(host)) {
                    return true;
                }
                if (type == SAN_DNS) {
                    sawDns = true;
                    if (!hostIsIp && dnsMatches(v, host)) {
                        return true;
                    }
                }
            }
        }
        // Legacy fallback: CN only when the certificate carries no dNSName SAN at all
        // (same tolerance as the JDK's HostnameChecker); never for IP hosts.
        if (!sawDns && !hostIsIp) {
            String cn = commonName(leaf);
            return cn != null && dnsMatches(normalise(cn), host);
        }
        return false;
    }

    /**
     * Exact match, or a single leftmost wildcard label ({@code *.example.com}).
     */
    static boolean dnsMatches(String pattern, String host) {
        if (pattern.equals(host)) {
            return true;
        }
        if (pattern.startsWith("*.") && pattern.length() > 2) {
            String suffix = pattern.substring(1); // ".example.com"
            int firstDot = host.indexOf('.');
            return firstDot > 0 && host.substring(firstDot).equals(suffix);
        }
        return false;
    }

    static boolean looksLikeIp(String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }
        if (host.indexOf(':') >= 0) {
            return true; // IPv6 literal
        }
        int dots = 0;
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if (c == '.') {
                dots++;
            } else if (c < '0' || c > '9') {
                return false;
            }
        }
        return dots == 3;
    }

    static String normalise(String host) {
        String h = host.trim().toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) {
            h = h.substring(1, h.length() - 1);
        }
        return h.endsWith(".") ? h.substring(0, h.length() - 1) : h;
    }

    private static String commonName(X509Certificate leaf) {
        String dn = leaf.getSubjectX500Principal().getName(X500Principal.RFC2253);
        for (String part : dn.split(",")) {
            String p = part.trim();
            if (p.regionMatches(true, 0, "CN=", 0, 3)) {
                return p.substring(3);
            }
        }
        return null;
    }
}
