package com.studioquack.sqs.channel;

import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * A minimal reachability probe for the overlay hub. Before the daemon repoints its outbound control-plane
 * calls onto the WireGuard overlay, it must confirm the hub is actually reachable through the (freshly
 * brought-up) tunnel.
 */
@Slf4j
public final class OverlayHealthProbe {

    /**
     * Default connect timeout for the probe (ms). Short so bootstrap never stalls.
     */
    public static final int DEFAULT_TIMEOUT_MS = 3_000;

    private OverlayHealthProbe() {
    }

    /**
     * Attempts a TCP connect to {@code host:port} within {@code timeoutMs} (falling back to
     * {@link #DEFAULT_TIMEOUT_MS} for non-positive values). Returns {@code true} iff the connection is
     * established; any failure (refused, timeout, unresolved, unreachable) returns {@code false} and is logged at
     * debug — the caller stays on the public base.
     */
    public static boolean tcpReachable(String host, int port, int timeoutMs) {
        if (host == null || host.isBlank() || port <= 0 || port > 65_535) {
            return false;
        }
        int effectiveTimeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), effectiveTimeout);
            return true;
        } catch (Exception ex) {
            log.debug("channel/probe: overlay hub {}:{} not reachable within {}ms ({})",
                    host, port, effectiveTimeout, ex.toString());
            return false;
        }
    }

    /**
     * TLS handshake to {@code host:port} under {@code ssl} (pinned platform CA). The hub leg is HTTPS — never
     * plain HTTP.
     */
    public static boolean httpsReachable(String host, int port, javax.net.ssl.SSLContext ssl,
                                         int timeoutMs) {
        if (host == null || host.isBlank() || port <= 0 || port > 65_535) {
            return false;
        }
        int effectiveTimeout = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
        javax.net.ssl.SSLContext ctx = ssl;
        try {
            if (ctx == null) {
                ctx = com.studioquack.common.security.tls.HybridTls.newContext(null, null);
            }
            try (javax.net.ssl.SSLSocket socket = (javax.net.ssl.SSLSocket) ctx.getSocketFactory()
                    .createSocket()) {
                socket.setSoTimeout(effectiveTimeout);
                javax.net.ssl.SSLParameters sp =
                        com.studioquack.common.security.tls.HybridTls.hybridParameters(ctx, true);
                socket.setSSLParameters(sp);
                socket.connect(new InetSocketAddress(host, port), effectiveTimeout);
                socket.startHandshake();
                return true;
            }
        } catch (Exception ex) {
            log.debug("channel/probe: overlay hub https {}:{} not reachable within {}ms ({})",
                    host, port, effectiveTimeout, ex.toString());
            return false;
        }
    }

    /**
     * Convenience overload using {@link #DEFAULT_TIMEOUT_MS}.
     */
    public static boolean tcpReachable(String host, int port) {
        return tcpReachable(host, port, DEFAULT_TIMEOUT_MS);
    }
}
