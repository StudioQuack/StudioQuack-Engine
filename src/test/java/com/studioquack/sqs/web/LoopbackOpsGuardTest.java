package com.studioquack.sqs.web;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A browser tab on the operator's host is also a loopback caller; the ops guard must tell the
 * two apart without a session or CSRF token.
 */
class LoopbackOpsGuardTest {

    private static MockHttpServletRequest loopbackPost() {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/sqs/v1/ops/local-uninstall");
        req.setRemoteAddr("127.0.0.1");
        return req;
    }

    @Test
    void operatorCurlWithMarkerIsAdmitted() {
        MockHttpServletRequest req = loopbackPost();
        req.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");
        assertThat(LoopbackOpsGuard.rejectReason(req)).isEmpty();
    }

    @Test
    void loopbackWithoutMarkerIsRefused() {
        assertThat(LoopbackOpsGuard.rejectReason(loopbackPost())).contains("missing X-SQS-Host-Op header");
    }

    @Test
    void browserOriginIsRefusedEvenWithMarker() {
        MockHttpServletRequest req = loopbackPost();
        req.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");
        req.addHeader("Origin", "https://evil.example");
        assertThat(LoopbackOpsGuard.rejectReason(req)).contains("browser-originated request refused");

        MockHttpServletRequest referer = loopbackPost();
        referer.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");
        referer.addHeader("Referer", "http://127.0.0.1:5173/");
        assertThat(LoopbackOpsGuard.rejectReason(referer)).contains("browser-originated request refused");
    }

    @Test
    void remoteCallerIsRefusedFirst() {
        MockHttpServletRequest req = loopbackPost();
        req.setRemoteAddr("10.0.0.7");
        req.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");
        assertThat(LoopbackOpsGuard.rejectReason(req)).contains("loopback-only");
    }

    @Test
    void ipv6LoopbackIsLoopback() {
        MockHttpServletRequest req = loopbackPost();
        req.setRemoteAddr("::1");
        req.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");
        assertThat(LoopbackOpsGuard.rejectReason(req)).isEmpty();
    }
}
