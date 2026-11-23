package com.studioquack.sqs.sandbox.egress;

import java.util.ArrayList;
import java.util.List;

/**
 * (DNS-hardening acceptance line: "internal DNS names do not resolve from inside a sandbox") — the
 * single source of truth for WHICH recursive resolvers a sandbox container is pinned to via
 * podman's {@code --dns} flag. The leak this closes.
 */
public final class SandboxDns {

    /** Configuration key (comma-separated resolver IPs). Operator-trusted, never workload input. */
    public static final String CONFIG_KEY = "sqs.sandbox.dns.servers";

    /**
     * Default pin set, applied when {@value #CONFIG_KEY} is unset/blank: Quad9 (9.9.9.9 —
     * malware-filtering, GDPR-stewarded non-profit) + Cloudflare (1.1.1.1 — no-log public
     * resolver). Both are PUBLIC anycast resolvers, so a sandbox can resolve public names (and
     * ONLY public names) whenever its posture allows internet at all.
     */
    public static final String DEFAULT_SERVERS = "9.9.9.9,1.1.1.1";

    private SandboxDns() {
    }

    /** The default pin set as a list (never mutated by callers — a fresh copy each time). */
    public static List<String> defaults() {
        return parse(DEFAULT_SERVERS);
    }

    /**
     * Parses the {@value #CONFIG_KEY} value into validated resolver IP literals. Blank → the {@link
     * #DEFAULT_SERVERS} set (never the platform default — that IS the leak).
     */
    public static List<String> parse(String raw) {
        String v = raw == null ? "" : raw.trim();
        if (v.isEmpty()) {
            return parse(DEFAULT_SERVERS);
        }
        List<String> out = new ArrayList<>();
        for (String part : v.split(",")) {
            String ip = part.trim();
            if (!isIpLiteral(ip)) {
                throw new IllegalArgumentException(
                        "refused " + CONFIG_KEY + " entry '" + ip + "': DNS pins must be IP "
                                + "literals (IPv4 dotted-quad or IPv6), comma-separated "
                                + "(fail closed — a sandbox must never fall back to the bridge "
                                + "gateway resolver, which forwards to the host's internal DNS)");
            }
            out.add(ip);
        }
        return List.copyOf(out);
    }

    /**
     * Validates an already-parsed list (the builders' last-line check before argv emission).
     * Null/empty → {@link #defaults()}; any non-literal entry → {@link IllegalArgumentException}.
     */
    public static List<String> validate(List<String> servers) {
        if (servers == null || servers.isEmpty()) {
            return defaults();
        }
        for (String ip : servers) {
            if (!isIpLiteral(ip)) {
                throw new IllegalArgumentException(
                        "refused sandbox DNS pin '" + ip + "': not an IP literal (fail closed)");
            }
        }
        return List.copyOf(servers);
    }

    /** IPv4 dotted-quad with each octet 0–255, or an IPv6 literal (any token with ':'). */
    private static boolean isIpLiteral(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        if (s.indexOf(':') >= 0) {
            // IPv6 literal — podman accepts full/compressed forms; a zone id (fe80::1%eth0)
            // is meaningless for an upstream resolver but harmless, so no further parsing.
            return true;
        }
        String[] o = s.split("\\.", -1);
        if (o.length != 4) {
            return false;
        }
        for (String octet : o) {
            if (octet.isEmpty() || octet.length() > 3) {
                return false;
            }
            for (int i = 0; i < octet.length(); i++) {
                if (!Character.isDigit(octet.charAt(i))) {
                    return false;
                }
            }
            if (Integer.parseInt(octet) > 255) {
                return false;
            }
        }
        return true;
    }
}
