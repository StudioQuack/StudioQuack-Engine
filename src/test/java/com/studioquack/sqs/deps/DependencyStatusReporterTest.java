package com.studioquack.sqs.deps;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.BackendBaseResolver;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DependencyStatusReporter}: the daemon callback callback URL contract (the
 * per-env {@code the per-environment dependencies status path} path every daemon callback hangs
 * off) and the best-effort degrade when the secure channel is not enabled.
 */
class DependencyStatusReporterTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private DependencyEntry entry() {
        return new DependencyEntry("python", "requests", "2.31.0", "queued", false, null, null, null, null);
    }

    private DependencyStatusReporter reporter(ObjectProvider<ChannelHttpClients> clients,
                                              SqsChannelProperties props,
                                              String base) {
        return reporter(clients, props, base, new OutboundBaseHolder());
    }

    private DependencyStatusReporter reporter(ObjectProvider<ChannelHttpClients> clients,
                                              SqsChannelProperties props,
                                              String base,
                                              OutboundBaseHolder outbound) {
        return new DependencyStatusReporter(clients, props, mapper, base, outbound);
    }

    /**
     * The installer bearer is burned at hybrid enrolment; a report sent with it just before bootstrap
     * switched to the kemIkm credential gets a 401. It is retried on the new credential, and reporting
     * stays on: it is not an uninstalled environment.
     */
    @Test
    @SuppressWarnings("unchecked")
    void a401OnTheInstallerBearerDuringTheCredentialSwitch_isRetried_andReportingStaysOn() throws Exception {
        String kemCredential = "sqikm1.test-proof";
        try (com.studioquack.sqs.channel.MockBackend be = new com.studioquack.sqs.channel.MockBackend()) {
            be.stub("/be/v1/sqvm/environments/env-1/dependencies/status", req ->
                    com.studioquack.sqs.channel.MockBackend.Response.json(
                            ("Bearer " + kemCredential).equals(req.headers().get("authorization")) ? 200 : 401,
                            "{}"));
            SqsChannelProperties props = new SqsChannelProperties();
            props.setEnvId("env-1");
            props.setBearer("sqb-installer-bearer");
            ObjectProvider<ChannelHttpClients> clients = mock(ObjectProvider.class);
            when(clients.getIfAvailable()).thenReturn(new ChannelHttpClients(null));
            DependencyStatusReporter reporter = reporter(clients, props, be.baseUrl());
            DependencyEntry entry = new DependencyEntry("python", "aiohappyeyeballs", "", "installed",
                    null, null, null, null, null);
            Thread bootstrap = new Thread(() -> {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException ignored) {
                    return;
                }
                props.installChannelCredential(kemCredential);
            });
            bootstrap.start();

            assertTrue(reporter.report(entry, "installed", "2.7.1", null, null, null),
                    "the report is retried on the kemIkm credential");
            assertTrue(reporter.report(entry, "installed", "2.7.1", null, null, null),
                    "reporting stays on after the switch");
            bootstrap.join();
        }
    }

    /** A 401 on the kemIkm credential itself still means the backend no longer knows the engine. */
    @Test
    @SuppressWarnings("unchecked")
    void a401OnTheKemIkmCredential_stillDisablesReporting() throws Exception {
        try (com.studioquack.sqs.channel.MockBackend be = new com.studioquack.sqs.channel.MockBackend()) {
            be.stubJson("/be/v1/sqvm/environments/env-1/dependencies/status", 401, "{}");
            SqsChannelProperties props = new SqsChannelProperties();
            props.setEnvId("env-1");
            props.installChannelCredential("sqikm1.revoked-proof");
            ObjectProvider<ChannelHttpClients> clients = mock(ObjectProvider.class);
            when(clients.getIfAvailable()).thenReturn(new ChannelHttpClients(null));
            DependencyStatusReporter reporter = reporter(clients, props, be.baseUrl());
            DependencyEntry entry = new DependencyEntry("python", "x", "", "installed",
                    null, null, null, null, null);

            assertFalse(reporter.report(entry, "installed", "1", null, null, null));
            assertEquals(1, be.requests().size(), "no wait, no retry on a credential that is already the engine's");
            assertFalse(reporter.report(entry, "installed", "1", null, null, null));
            assertEquals(1, be.requests().size(), "breaker tripped: no further reports");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusUrlIsTheDaemonCallbackDependencyStatusPath() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId("env-9");
        ObjectProvider<ChannelHttpClients> clients = mock(ObjectProvider.class);
        DependencyStatusReporter reporter = reporter(clients, props, "https://be.example.com/");

        assertEquals(
                "https://be.example.com/be/v1/sqvm/environments/env-9/dependencies/status",
                reporter.statusUrl("env-9"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void statusUrlFollowsTheOverlayBaseWhenTheTunnelIsUp() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId("env-9");
        ObjectProvider<ChannelHttpClients> clients = mock(ObjectProvider.class);
        String publicBase = "https://be.example.com/be/v1/sqvm/environments/env-9";
        BackendBaseResolver resolver = new BackendBaseResolver(publicBase);
        resolver.activateOverlay("https://10.8.0.1/be/v1/sqvm/environments/env-9");
        OutboundBaseHolder outbound = new OutboundBaseHolder();
        outbound.bind(resolver);

        DependencyStatusReporter reporter = reporter(clients, props, "https://be.example.com", outbound);

        assertEquals(
                "https://10.8.0.1/be/v1/sqvm/environments/env-9/dependencies/status",
                reporter.statusUrl("env-9"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportSkipsWhenSecureChannelUnavailable() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId("env-9");
        ObjectProvider<ChannelHttpClients> clients = mock(ObjectProvider.class);
        when(clients.getIfAvailable()).thenReturn(null); // channel not enabled (offline/dev)
        DependencyStatusReporter reporter = reporter(clients, props, "https://be.example.com");

        // Best-effort: no channel → returns false (never throws), so an install task can't die on it.
        assertFalse(reporter.report(entry(), "installing", null, null, null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void isChannelReadyReflectsChannelAndBaseUrl() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId("env-9");

        ObjectProvider<ChannelHttpClients> down = mock(ObjectProvider.class);
        when(down.getIfAvailable()).thenReturn(null);
        assertFalse(reporter(down, props, "https://be.example.com").isChannelReady());

        ChannelHttpClients live = mock(ChannelHttpClients.class);
        ObjectProvider<ChannelHttpClients> up = mock(ObjectProvider.class);
        when(up.getIfAvailable()).thenReturn(live);
        assertTrue(reporter(up, props, "https://be.example.com").isChannelReady());
        // Channel up but no server base URL configured → not ready (a report would be undeliverable).
        assertFalse(reporter(up, props, "").isChannelReady());
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportDiscoveredSkipsWhenSecureChannelUnavailable() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId("env-9");
        ObjectProvider<ChannelHttpClients> clients = mock(ObjectProvider.class);
        when(clients.getIfAvailable()).thenReturn(null);
        DependencyStatusReporter reporter = reporter(clients, props, "https://be.example.com");

        // Same best-effort degrade as report(): no channel → false, never throws.
        assertFalse(reporter.reportDiscovered(entry(), "2.31.0", "sha256:abc", 4096L, Boolean.TRUE));
    }
}
