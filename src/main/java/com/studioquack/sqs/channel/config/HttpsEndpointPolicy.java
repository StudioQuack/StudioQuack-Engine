package com.studioquack.sqs.channel.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * Fail-closed scheme check for daemon→StudioQuack endpoints (backend, releases, SQNet control). HTTPS is
 * required.
 */
public final class HttpsEndpointPolicy {

    /**
     * Token embedded in the refusal so bootstrap treats it as a terminal config error.
     */
    public static final String REFUSAL = "https-required";

    private HttpsEndpointPolicy() {
    }

    /**
     * @return {@code null} when {@code url} may be dialed; a one-line reason otherwise. Blank is not a refusal —
     * "not configured" is a separate decision.
     */
    public static String refusal(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        String trimmed = url.trim();
        URI uri;
        try {
            uri = new URI(trimmed);
        } catch (URISyntaxException ex) {
            return REFUSAL + ": not a URI (" + trimmed + ")";
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if ("https".equals(scheme)) {
            return null;
        }
        if ("http".equals(scheme) && isLoopbackHost(uri.getHost())) {
            return null;
        }
        return REFUSAL + ": " + trimmed
                + " must use https (http is allowed only for localhost, 127.0.0.1, and ::1)";
    }

    /**
     * Throws {@link IllegalStateException} when {@link #refusal} is non-null.
     */
    public static void require(String url, String what) {
        String reason = refusal(url);
        if (reason != null) {
            throw new IllegalStateException(what + " refused (" + reason + ")");
        }
    }

    /**
     * Exact loopback names. Not RFC1918, not {@code *.localhost}, not {@code 127.0.0.2}.
     */
    public static boolean isLoopbackHost(String host) {
        if (host == null || host.isBlank()) {
            return false;
        }
        String h = host.trim().toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]") && h.length() > 2) {
            h = h.substring(1, h.length() - 1);
        }
        if (h.endsWith(".")) {
            h = h.substring(0, h.length() - 1);
        }
        return h.equals("localhost") || h.equals("127.0.0.1") || h.equals("::1")
                || h.equals("0:0:0:0:0:0:0:1");
    }
}
