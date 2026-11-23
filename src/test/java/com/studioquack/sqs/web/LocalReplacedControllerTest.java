package com.studioquack.sqs.web;

import com.studioquack.sqs.channel.HostReplacedReporter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The replaced report is irreversible from the host's side (the installer purges this engine right
 * after it), so only the operator's installer on this machine may trigger it.
 */
class LocalReplacedControllerTest {

    private final HostReplacedReporter reporter = mock(HostReplacedReporter.class);

    @SuppressWarnings("unchecked")
    private LocalReplacedController controller(HostReplacedReporter available) {
        ObjectProvider<HostReplacedReporter> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(available);
        return new LocalReplacedController(provider);
    }

    private static MockHttpServletRequest installerCall() {
        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/sqs/v1/ops/replaced");
        req.setRemoteAddr("127.0.0.1");
        req.addHeader(LoopbackOpsGuard.HOST_OP_HEADER, "1");
        return req;
    }

    @Test
    void aRemoteCallerCannotReportThisEngineReplaced() {
        MockHttpServletRequest req = installerCall();
        req.setRemoteAddr("203.0.113.7");

        ResponseEntity<?> resp = controller(reporter).replaced(Map.of("replacedBy", "env-new"), req);

        assertThat(resp.getStatusCode().value()).isEqualTo(403);
        verify(reporter, never()).reportReplaced(any());
    }

    @Test
    void aBrowserTabOnThisHostCannotReportIt() {
        MockHttpServletRequest req = installerCall();
        req.addHeader("Origin", "https://evil.example");

        ResponseEntity<?> resp = controller(reporter).replaced(null, req);

        assertThat(resp.getStatusCode().value()).isEqualTo(403);
        verify(reporter, never()).reportReplaced(any());
    }

    @Test
    void theInstallerCallIsForwardedWithTheNewEnvironmentId() {
        when(reporter.reportReplaced("env-new")).thenReturn(true);

        ResponseEntity<?> resp = controller(reporter).replaced(Map.of("replacedBy", "env-new"), installerCall());

        assertThat(resp.getStatusCode().value()).isEqualTo(200);
        assertThat(resp.getBody()).isEqualTo(Map.of("reported", true));
    }

    @Test
    void aRefusedReportIsNotPresentedAsSuccess() {
        when(reporter.reportReplaced(null)).thenReturn(false);

        ResponseEntity<?> resp = controller(reporter).replaced(null, installerCall());

        assertThat(resp.getStatusCode().value()).isEqualTo(502);
    }

    @Test
    void anEngineWithoutAChannelSaysSo() {
        ResponseEntity<?> resp = controller(null).replaced(null, installerCall());

        assertThat(resp.getStatusCode().value()).isEqualTo(409);
    }
}
