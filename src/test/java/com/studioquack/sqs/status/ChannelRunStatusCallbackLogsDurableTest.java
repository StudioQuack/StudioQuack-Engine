package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.MockBackend;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.retention.ReportedRunRegistry;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The 401 log-loss regression gate: a terminal status ack must mark the run reported (purge-
 * eligible) ONLY when its logs are durable at the backend. A BATCH run whose flush was refused
 * ({@code logsDurable=false}) keeps its local rows — the status POST still goes out either way.
 */
class ChannelRunStatusCallbackLogsDurableTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ENV_ID = "env-1";
    private static final String RUN_ID = "wx-durable";
    private static final String ENV_BASE = "/be/v1/sqvm/environments/" + ENV_ID;

    /** No sub-process registry: these run ids are top-level. */
    private static final ObjectProvider<SubProcessCompletionRegistry> NO_REGISTRY =
            new ObjectProvider<>() {
                @Override
                public SubProcessCompletionRegistry getObject() {
                    return null;
                }

                @Override
                public SubProcessCompletionRegistry getIfAvailable() {
                    return null;
                }
            };

    private static SqsChannelProperties props() {
        SqsChannelProperties props = new SqsChannelProperties();
        props.setEnvId(ENV_ID);
        props.setBearer("sqvm_bearer_cb");
        return props;
    }

    private ChannelRunStatusCallback newCallback(ReportedRunRegistry registry, String baseUrl) {
        ChannelRunStatusCallback callback = new ChannelRunStatusCallback(props(), baseUrl,
                new ChannelHttpClients(null), MAPPER, NO_REGISTRY, new OutboundBaseHolder());
        callback.wireReportedRegistryForTests(registry);
        return callback;
    }

    @Test
    void terminalAck_marksReported_whenLogsDurable() throws Exception {
        ReportedRunRegistry registry = new ReportedRunRegistry();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(ENV_BASE + "/runs/" + RUN_ID + "/status", 200, "{}");

            newCallback(registry, be.baseUrl())
                    .report(RUN_ID, RunStatusCallback.Status.COMPLETED, "done", null, true);

            assertThat(registry.contains(RUN_ID)).isTrue();
            assertThat(be.countMatching("/status")).isEqualTo(1);
        }
    }

    @Test
    void terminalAck_withholdsReported_whenLogsRefused() throws Exception {
        ReportedRunRegistry registry = new ReportedRunRegistry();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(ENV_BASE + "/runs/" + RUN_ID + "/status", 200, "{}");

            newCallback(registry, be.baseUrl())
                    .report(RUN_ID, RunStatusCallback.Status.COMPLETED, "done", null, false);

            // Status still went out — the run index must flip terminal…
            assertThat(be.countMatching("/status")).isEqualTo(1);
            // …but the run must NOT become purge-eligible: the local rows are the only copy.
            assertThat(registry.contains(RUN_ID)).isFalse();
        }
    }

    @Test
    void fourArgForm_staysDurableByDefault() throws Exception {
        ReportedRunRegistry registry = new ReportedRunRegistry();
        try (MockBackend be = new MockBackend()) {
            be.stubJson(ENV_BASE + "/runs/" + RUN_ID + "/status", 200, "{}");

            // STREAM path (and every pre-existing caller) uses the 4-arg form: durable by default.
            newCallback(registry, be.baseUrl())
                    .report(RUN_ID, RunStatusCallback.Status.FAILED, "boom", null);

            assertThat(registry.contains(RUN_ID)).isTrue();
        }
    }
}
