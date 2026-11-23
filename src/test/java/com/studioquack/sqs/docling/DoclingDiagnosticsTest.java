package com.studioquack.sqs.docling;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Failure classification for Docling: convert-budget exhaustion must not be reported as a
 * side-car crash, and connect timeouts must not be mistaken for read-budget expiry.
 */
class DoclingDiagnosticsTest {

    @Test
    @DisplayName("a Read timed out ResourceAccessException is convert-budget exhaustion")
    void readTimedOutIsBudgetExceeded() {
        Exception e = new ResourceAccessException(
                "I/O error on POST \"http://127.0.0.1:5001/v1/convert/file/async\": Read timed out",
                new SocketTimeoutException("Read timed out"));
        assertTrue(DoclingDiagnostics.isConvertBudgetExceeded(e));
        assertTrue(DoclingDiagnostics.isClientReadTimeout(e));
    }

    @Test
    @DisplayName("HTTP 504 is convert-budget exhaustion")
    void gatewayTimeoutFromSideCarIsBudgetExceeded() {
        Exception e = HttpServerErrorException.create(HttpStatus.GATEWAY_TIMEOUT, "Gateway Timeout",
                null,
                "gateway timeout".getBytes(StandardCharsets.UTF_8),
                StandardCharsets.UTF_8);
        assertTrue(DoclingDiagnostics.isConvertBudgetExceeded(e));
    }

    @Test
    @DisplayName("budget message names the property and does not claim the side-car failed")
    void budgetMessageNamesPropertyNotSideCarFault() {
        String msg = DoclingDiagnostics.formatClientTimeoutMessage(900);
        assertTrue(msg.contains("sqs.docling.convert-timeout-seconds"));
        assertTrue(msg.contains("900"));
        assertTrue(msg.contains("probably still converting"));
        assertFalse(msg.toLowerCase().contains("did not complete the request"));
        assertFalse(msg.toLowerCase().contains("side-car failed"));
    }

    @Test
    @DisplayName("a connect timeout is not convert-budget exhaustion")
    void connectTimeoutIsNotBudgetExceeded() {
        Exception e = new ResourceAccessException(
                "I/O error on POST: Connect timed out",
                new SocketTimeoutException("Connect timed out"));
        assertFalse(DoclingDiagnostics.isConvertBudgetExceeded(e));
    }

    @Test
    @DisplayName("an ordinary conversion error is not convert-budget exhaustion")
    void ordinaryErrorIsNotBudgetExceeded() {
        assertFalse(DoclingDiagnostics.isConvertBudgetExceeded(
                new IllegalStateException("unsupported media type")));
    }
}
