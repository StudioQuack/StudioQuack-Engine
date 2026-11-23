package com.studioquack.sqs.aimodel;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * SSRF-class dial jail for operator-pushed provider URLs ({@code test-connection},
 * {@code set-ai-model}, {@code list-models}, LLM proxy). Loopback is allowed (LM Studio
 * {@code http://127.0.0.1:1234}).
 */
public final class ProviderDialGuard {

    private ProviderDialGuard() {}

    /** {@code null} when {@code url} is absent or may be dialed. */
    public static String refusalIfPresent(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        return refusal(url);
    }

    /** {@code null} when the URL may be dialed; otherwise an operator-facing reason. */
    public static String refusal(String url) {
        if (url == null || url.isBlank()) {
            return "Provider URL is empty";
        }
        URI uri;
        try {
            uri = URI.create(url.trim());
        } catch (IllegalArgumentException e) {
            return "Provider URL is not a valid URI";
        }
        String scheme = uri.getScheme();
        if (scheme == null
                || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            return "Provider URL must be http or https";
        }
        if (uri.getUserInfo() != null && !uri.getUserInfo().isBlank()) {
            return "Provider URL must not include credentials";
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            return "Provider URL has no host";
        }
        InetAddress[] addrs;
        try {
            addrs = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            return "Provider host is unresolvable";
        }
        if (addrs.length == 0) {
            return "Provider host is unresolvable";
        }
        for (InetAddress a : addrs) {
            if (a.isLoopbackAddress()) {
                continue;
            }
            if (blocked(a)) {
                return "Provider host is not allowed (" + a.getHostAddress() + ")";
            }
        }
        return null;
    }

    static boolean blocked(InetAddress a) {
        if (a.isAnyLocalAddress() || a.isLinkLocalAddress() || a.isSiteLocalAddress()
                || a.isMulticastAddress()) {
            return true;
        }
        byte[] b = a.getAddress();
        if (a instanceof Inet4Address && b.length == 4) {
            int o0 = b[0] & 0xff;
            int o1 = b[1] & 0xff;
            if (o0 == 100 && o1 >= 64 && o1 <= 127) {
                return true;
            }
            if (o0 == 0) {
                return true;
            }
        }
        if (a instanceof Inet6Address && b.length == 16 && (b[0] & 0xfe) == 0xfc) {
            return true;
        }
        return false;
    }
}
