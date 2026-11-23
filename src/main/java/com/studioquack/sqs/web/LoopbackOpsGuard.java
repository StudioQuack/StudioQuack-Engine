package com.studioquack.sqs.web;

import jakarta.servlet.http.HttpServletRequest;

import java.net.InetAddress;
import java.util.Optional;

/**
 * Admission check shared by the host-local ops endpoints ({@code local-uninstall},
 * {@code replaced}, {@code health} detail). "Came from 127.0.0.1" alone is not "came from the
 * operator": every browser on this host is also a loopback caller, and the ops API runs with CSRF
 * disabled and no session, so a page the operator happens to have open could {@code fetch} a.
 */
public final class LoopbackOpsGuard {

    /** Marker header the operator commands carry; value is not secret, presence is the point. */
    public static final String HOST_OP_HEADER = "X-SQS-Host-Op";

    private LoopbackOpsGuard() {
    }

    /**
     * @return a short rejection reason, or empty when the request is admissible
     */
    public static Optional<String> rejectReason(HttpServletRequest request) {
        if (!isLoopback(request)) {
            return Optional.of("loopback-only");
        }
        if (request.getHeader("Origin") != null || request.getHeader("Referer") != null) {
            return Optional.of("browser-originated request refused");
        }
        String marker = request.getHeader(HOST_OP_HEADER);
        if (marker == null || marker.isBlank()) {
            return Optional.of("missing " + HOST_OP_HEADER + " header");
        }
        return Optional.empty();
    }

    /** True when the request originates from a loopback address (127.0.0.1 / ::1). */
    static boolean isLoopback(HttpServletRequest request) {
        String remote = request.getRemoteAddr();
        if (remote == null || remote.isBlank()) {
            return false;
        }
        try {
            return InetAddress.getByName(remote).isLoopbackAddress();
        } catch (Exception e) {
            // Unresolvable remote address → fail closed.
            return false;
        }
    }
}
