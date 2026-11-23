package com.studioquack.sqs.web;

import com.studioquack.sqs.channel.EnrollmentStatus;
import com.studioquack.sqs.retention.DiskSpaceProbe;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Anonymous liveness has no tier/profiles/disk; detail is LoopbackOpsGuard-gated. */
class HealthControllerTest {

    @Test
    void anonymousHealth_isStatusOnly() {
        HealthController controller = controller();
        Map<String, Object> body = controller.health();
        assertEquals("UP", body.get("status"));
        assertEquals(1, body.size());
        assertFalse(body.containsKey("tier"));
        assertFalse(body.containsKey("profiles"));
        assertFalse(body.containsKey("disk"));
    }

    @Test
    void detail_withoutHostOp_isForbiddenAndEmptyOfInventory() {
        HealthController controller = controller();
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/sqs/v1/ops/health");
        req.setRemoteAddr("127.0.0.1");

        var resp = controller.detail(req);
        assertEquals(HttpStatus.FORBIDDEN, resp.getStatusCode());
        assertTrue(resp.getBody() instanceof Map<?, ?> body && !body.containsKey("tier"));
    }

    @Test
    void detail_withHostOp_includesTierProfilesDisk() {
        HealthController controller = controller();
        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/sqs/v1/ops/health");
        req.setRemoteAddr("127.0.0.1");
        req.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");

        var resp = controller.detail(req);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) resp.getBody();
        assertEquals("base", body.get("tier"));
        assertTrue(body.containsKey("profiles"));
        assertTrue(body.containsKey("disk"));
    }

    @Test
    void detail_reportsEnrolmentOutcomeForTheInstaller() {
        HealthController controller = controller();
        EnrollmentStatus status = new EnrollmentStatus();
        status.failed(new RuntimeException("enroll: status 401 body={\"errorCode\":\"Error_BearerExpired\"}"));
        controller.setEnrollmentStatus(status);

        @SuppressWarnings("unchecked")
        Map<String, Object> enrolment = (Map<String, Object>) controller.detailBody().get("enrolment");
        assertEquals("TOKEN_EXPIRED", enrolment.get("state"));
        assertEquals("Error_BearerExpired", enrolment.get("errorCode"));
    }

    private static HealthController controller() {
        Environment env = mock(Environment.class);
        when(env.getActiveProfiles()).thenReturn(new String[]{"base"});
        DiskSpaceProbe probe = mock(DiskSpaceProbe.class);
        when(probe.usagePercent()).thenReturn(10);
        when(probe.path()).thenReturn(".");
        HealthController controller = new HealthController(env, probe);
        ReflectionTestUtils.setField(controller, "tier", "base");
        ReflectionTestUtils.setField(controller, "diskHighWatermarkPercent", 90);
        return controller;
    }
}
