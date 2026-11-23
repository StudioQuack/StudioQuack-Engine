package com.studioquack.sqs.channel.net;

import com.studioquack.sqs.channel.config.HttpsEndpointPolicy;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.sandbox.egress.OverlayAddresses;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Fail-closed validation of a handshake v2 response BEFORE any field reaches a privileged sink. The handshake
 * is answered by the control plane, which the daemon's threat model treats as compromisable (A5) or spoofable
 * on-path (A2); every value that becomes {@code sq-net} argv, a TLS trust anchor, a firewall grant or a peer
 * address is therefore parsed and bounded here.
 */
public final class NetHandshakeValidator {

    public static final String ERROR_MALFORMED = "Error_MalformedHandshake";

    /**
     * RFC 1123 host name: labels of ≤63 alnum/hyphen chars, ≤253 total.
     */
    private static final Pattern HOSTNAME = Pattern.compile(
            "^(?=.{1,253}$)[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
                    + "(\\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$");

    /**
     * Environment / cell identifiers: opaque tokens, never path or shell material.
     */
    private static final Pattern IDENT = Pattern.compile("^[A-Za-z0-9_-]{1,64}$");

    /**
     * Pre-auth keys: URL-safe token alphabet, bounded length.
     */
    private static final Pattern AUTH_KEY = Pattern.compile("^[A-Za-z0-9._:-]{8,512}$");

    /**
     * Grant scopes ({@code execute}, {@code llm}, …).
     */
    private static final Pattern SCOPE = Pattern.compile("^[a-z][a-z0-9_-]{0,31}$");

    private NetHandshakeValidator() {}

    /**
     * Bounds the SHAPE of every field (injection into argv / TLS trust / iptables / the peer directory).
     * {@code controlUrl} must be HTTPS except for loopback HTTP — see {@link #requireControlUrl}.
     *
     * @throws NetHandshakeException with {@link #ERROR_MALFORMED} on the first violation
     */
    public static void validate(NetHandshakeResult r) {
        if (r == null) {
            throw malformed("empty response");
        }
        requireNoControlChars("cellId", r.cellId());
        requireNoControlChars("envName", r.envName());
        requireControlUrl(r.controlUrl());
        requireAuthKey(r.authKey());
        requireHostname("hostname", r.hostname());
        requireHubs(r.hubAddresses());
        requirePeers(r.meshPeers());
        requireRules(r.networkRules(), r.outboundNetworkRules());
        requireNoControlChars("meshToken", r.meshToken());
        requireNoControlChars("meshVerificationKey", r.meshVerificationKey());
        requirePemOrBlank("meshCaCertPem", r.meshCaCertPem());
        requirePemOrBlank("listenerCertPem", r.listenerCertPem());
    }

    /**
     * {@code true} when {@code hostname} is a value {@code sq-net --hostname=} may receive.
     */
    public static boolean validHostname(String hostname) {
        return hostname != null && HOSTNAME.matcher(hostname.trim()).matches();
    }

    /**
     * {@code true} when {@code id} is an environment / cell identifier the daemon will accept.
     */
    public static boolean validIdentifier(String id) {
        return id != null && IDENT.matcher(id.trim()).matches();
    }

    /**
     * Shape plus scheme: HTTPS, or HTTP only for {@code localhost} / {@code 127.0.0.1} / {@code::1}. A public or
     * RFC1918 {@code http} login server is refused — {@code sq-net up} must not register against an
     * unauthenticated coordinator.
     */
    private static void requireControlUrl(String raw) {
        if (blank(raw)) {
            return;
        }
        String s = raw.trim();
        requireNoControlChars("controlUrl", s);
        URI uri;
        try {
            uri = new URI(s);
        } catch (URISyntaxException ex) {
            throw malformed("controlUrl is not a URI");
        }
        if (blank(uri.getHost()) || uri.getRawUserInfo() != null || uri.getRawFragment() != null) {
            throw malformed("controlUrl must be scheme://host[:port][/path] without userinfo or fragment");
        }
        String httpsRefusal = HttpsEndpointPolicy.refusal(s);
        if (httpsRefusal != null) {
            throw malformed(httpsRefusal);
        }
    }

    private static void requireAuthKey(String raw) {
        if (blank(raw)) {
            return;
        }
        if (!AUTH_KEY.matcher(raw.trim()).matches()) {
            throw malformed("authKey has an unexpected shape");
        }
    }

    private static void requireHostname(String field, String raw) {
        if (blank(raw)) {
            throw malformed(field + " is blank");
        }
        if (!validHostname(raw)) {
            throw malformed(field + " is not a valid host name");
        }
    }

    private static void requireHubs(List<HubAddress> hubs) {
        for (HubAddress h : hubs) {
            if (h == null || !h.usable()) {
                throw malformed("hubAddresses entry unusable");
            }
            String addr = h.address().trim();
            boolean ok = OverlayAddresses.isIpv4Literal(addr)
                    || OverlayAddresses.isIpv6Literal(addr)
                    || validHostname(addr);
            if (!ok) {
                throw malformed("hubAddresses entry is neither an IP literal nor a host name");
            }
            if (h.gatewayId() != null && !blank(h.gatewayId()) && !validIdentifier(h.gatewayId())) {
                throw malformed("hubAddresses gatewayId malformed");
            }
        }
    }

    private static void requirePeers(List<GrantedPeerDirectory.GrantedPeer> peers) {
        for (GrantedPeerDirectory.GrantedPeer p : peers) {
            if (p == null) {
                throw malformed("meshPeers entry null");
            }
            if (!blank(p.envId()) && !validIdentifier(p.envId())) {
                throw malformed("meshPeers envId malformed");
            }
            requireNoControlChars("meshPeers.name", p.name());
            for (String scope : p.scopes()) {
                if (scope == null || !SCOPE.matcher(scope).matches()) {
                    throw malformed("meshPeers scope malformed");
                }
            }
        }
    }

    private static void requireRules(List<NetworkRuleStore.Rule> inbound,
                                     List<NetworkRuleStore.OutboundRule> outbound) {
        for (NetworkRuleStore.Rule r : inbound) {
            if (r == null || !validIdentifier(r.sourceEnvId()) || !validPort(r.port())) {
                throw malformed("networkRules entry malformed");
            }
        }
        for (NetworkRuleStore.OutboundRule r : outbound) {
            if (r == null || !validIdentifier(r.targetEnvId()) || !validPort(r.port())) {
                throw malformed("outboundNetworkRules entry malformed");
            }
        }
    }

    private static void requirePemOrBlank(String field, String pem) {
        if (blank(pem)) {
            return;
        }
        if (!pem.contains("-----BEGIN CERTIFICATE-----") || !pem.contains("-----END CERTIFICATE-----")) {
            throw malformed(field + " is not a PEM certificate");
        }
        for (int i = 0; i < pem.length(); i++) {
            char c = pem.charAt(i);
            if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
                throw malformed(field + " contains control characters");
            }
        }
    }

    private static void requireNoControlChars(String field, String value) {
        if (value == null) {
            return;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                throw malformed(field + " contains control characters");
            }
        }
    }

    private static boolean validPort(int port) {
        return port >= 1 && port <= 65_535;
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static NetHandshakeException malformed(String detail) {
        return new NetHandshakeException(200, ERROR_MALFORMED,
                "net handshake: refused — " + detail, null);
    }
}
