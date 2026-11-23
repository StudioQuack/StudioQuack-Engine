package com.studioquack.sqs.channel;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** The installer's final "enrolled?" line is only as honest as this classification. */
class EnrollmentStatusTest {

    @Test
    void expiredAndReplacedBearersAreTokenStates() {
        EnrollmentStatus status = new EnrollmentStatus();

        assertEquals(EnrollmentStatus.State.TOKEN_EXPIRED, status.failed(new EnrollmentClient.EnrollmentException(
                "enroll: status 401 body={\"errorCode\":\"Error_BearerExpired\"}")));
        assertEquals(EnrollmentStatus.State.TOKEN_REJECTED, status.failed(new IllegalStateException("bootstrap",
                new EnrollmentClient.EnrollmentException("enroll: status 401 body={\"errorCode\":\"Error_BearerMismatch\"}"))));
        assertEquals("Error_BearerMismatch", status.snapshot().get("errorCode"));
    }

    @Test
    void networkFailureIsRetrying_andSuccessClearsTheErrorCode() {
        EnrollmentStatus status = new EnrollmentStatus();

        assertEquals(EnrollmentStatus.State.RETRYING,
                status.failed(new java.net.ConnectException("Connection refused")));
        status.enrolled();

        Map<String, Object> snapshot = status.snapshot();
        assertEquals("ENROLLED", snapshot.get("state"));
        assertFalse(snapshot.containsKey("errorCode"));
    }
}
