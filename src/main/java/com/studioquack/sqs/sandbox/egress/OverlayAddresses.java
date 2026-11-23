package com.studioquack.sqs.sandbox.egress;

import java.util.regex.Pattern;

/**
 * Membership tests for the overlay address space. Every address that reaches a privileged host
 * operation — the daemon's own overlay IP handed to {@code iptables}/{@code ip}, a peer address
 * written into a per-container {@code -d IP/32} ACCEPT, a peer the daemon dials as a same-owner
 * sibling — must sit inside the CGNAT overlay range.
 */
public final class OverlayAddresses {

    /** IPv4 overlay range, {@link OverlayEgressRules#OVERLAY_CIDR}: 100.64.0.0/10. */
    private static final long V4_NETWORK = ipv4ToLong(100, 64, 0, 0);
    private static final long V4_MASK = 0xFFFFFFFFL << (32 - 10) & 0xFFFFFFFFL;

    /** IPv6 overlay range: fd7a:115c:a1e0::/48 (first three hextets fixed). */
    private static final String V6_PREFIX = "fd7a:115c:a1e0:";

    private static final Pattern IPV4 = Pattern.compile(
            "^(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}$");
    private static final Pattern IPV6_CHARS = Pattern.compile("^[0-9a-fA-F:]{2,39}$");

    private OverlayAddresses() {}

    /** {@code true} when {@code ip} is a syntactically valid IPv4 literal (no leading zeros, no DNS). */
    public static boolean isIpv4Literal(String ip) {
        return ip != null && IPV4.matcher(ip.trim()).matches();
    }

    /** {@code true} when {@code ip} is a plausible IPv6 literal (hex + colons only, no zone, no brackets). */
    public static boolean isIpv6Literal(String ip) {
        if (ip == null) {
            return false;
        }
        String s = ip.trim();
        return IPV6_CHARS.matcher(s).matches() && s.indexOf("::") == s.lastIndexOf("::")
                && s.chars().filter(c -> c == ':').count() >= 2;
    }

    /** {@code true} when {@code ip} is a host address inside 100.64.0.0/10 or fd7a:115c:a1e0::/48. */
    public static boolean isOverlayAddress(String ip) {
        if (ip == null) {
            return false;
        }
        String s = ip.trim();
        if (isIpv4Literal(s)) {
            return (ipv4ToLong(s) & V4_MASK) == V4_NETWORK;
        }
        return isIpv6Literal(s) && s.toLowerCase().startsWith(V6_PREFIX);
    }

    /**
     * {@code true} when {@code cidr} is {@code ip/prefix} whose network lies inside the overlay
     * range and whose prefix is at least as narrow as the overlay's own (so {@code 0.0.0.0/0},
     * {@code 100.64.0.0/8} and any non-overlay block are all refused). A bare host address
     * without a prefix is accepted as a /32.
     */
    public static boolean isOverlayCidr(String cidr) {
        if (cidr == null || cidr.isBlank()) {
            return false;
        }
        String s = cidr.trim();
        int slash = s.indexOf('/');
        if (slash < 0) {
            return isOverlayAddress(s);
        }
        String host = s.substring(0, slash);
        int prefix;
        try {
            prefix = Integer.parseInt(s.substring(slash + 1));
        } catch (NumberFormatException ex) {
            return false;
        }
        if (isIpv4Literal(host)) {
            return prefix >= 10 && prefix <= 32 && isOverlayAddress(host);
        }
        return isIpv6Literal(host) && prefix >= 48 && prefix <= 128 && isOverlayAddress(host);
    }

    private static long ipv4ToLong(String dotted) {
        String[] p = dotted.split("\\.");
        return ipv4ToLong(Integer.parseInt(p[0]), Integer.parseInt(p[1]),
                Integer.parseInt(p[2]), Integer.parseInt(p[3]));
    }

    private static long ipv4ToLong(int a, int b, int c, int d) {
        return ((long) a << 24) | ((long) b << 16) | ((long) c << 8) | d;
    }
}
