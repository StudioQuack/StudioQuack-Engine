package com.studioquack.sqs.runtime;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Targeted unit tests for {@link InMemoryGatewayDecisionRecorder}: round-trip, overwrite,
 * isolation, never-throw guards, max-size eviction of the oldest write, and TTL expiry.
 */
class InMemoryGatewayDecisionRecorderTest {

    @Test
    void recordThenLatest_roundTrip() {
        InMemoryGatewayDecisionRecorder rec = new InMemoryGatewayDecisionRecorder(256, 1_800_000L);
        GatewayDecisionRecorder.GatewayDecision decision = decision("wfe-1", "gw-a", "t1");

        rec.record(decision);

        assertThat(rec.latest("wfe-1", "gw-a")).isEqualTo(decision);
    }

    @Test
    void record_overwriteSameRunAndGateway_keepsLatest() {
        InMemoryGatewayDecisionRecorder rec = new InMemoryGatewayDecisionRecorder(256, 1_800_000L);
        GatewayDecisionRecorder.GatewayDecision first = decision("wfe-1", "gw-a", "t1");
        GatewayDecisionRecorder.GatewayDecision second = decision("wfe-1", "gw-a", "t2");

        rec.record(first);
        rec.record(second);

        assertThat(rec.latest("wfe-1", "gw-a")).isEqualTo(second);
        assertThat(rec.size()).isEqualTo(1);
    }

    @Test
    void latest_isolatesDifferentGatewaysAndRuns() {
        InMemoryGatewayDecisionRecorder rec = new InMemoryGatewayDecisionRecorder(256, 1_800_000L);
        GatewayDecisionRecorder.GatewayDecision a = decision("wfe-1", "gw-a", "t1");
        GatewayDecisionRecorder.GatewayDecision b = decision("wfe-1", "gw-b", "t2");
        GatewayDecisionRecorder.GatewayDecision c = decision("wfe-2", "gw-a", "t3");

        rec.record(a);
        rec.record(b);
        rec.record(c);

        assertThat(rec.latest("wfe-1", "gw-a")).isEqualTo(a);
        assertThat(rec.latest("wfe-1", "gw-b")).isEqualTo(b);
        assertThat(rec.latest("wfe-2", "gw-a")).isEqualTo(c);
        assertThat(rec.latest("wfe-2", "gw-b")).isNull();
    }

    @Test
    void recordAndLatest_nullOrBlank_neverThrow_latestReturnsNull() {
        InMemoryGatewayDecisionRecorder rec = new InMemoryGatewayDecisionRecorder(256, 1_800_000L);

        assertThatCode(() -> rec.record(null)).doesNotThrowAnyException();
        assertThatCode(() -> rec.record(decision(null, "gw-a", "t1"))).doesNotThrowAnyException();
        assertThatCode(() -> rec.record(decision("  ", "gw-a", "t1"))).doesNotThrowAnyException();
        assertThatCode(() -> rec.record(decision("wfe-1", null, "t1"))).doesNotThrowAnyException();
        assertThatCode(() -> rec.record(decision("wfe-1", "", "t1"))).doesNotThrowAnyException();

        assertThat(rec.size()).isZero();
        assertThatCode(() -> rec.latest(null, "gw-a")).doesNotThrowAnyException();
        assertThat(rec.latest(null, "gw-a")).isNull();
        assertThat(rec.latest("", "gw-a")).isNull();
        assertThat(rec.latest("wfe-1", null)).isNull();
        assertThat(rec.latest("wfe-1", "  ")).isNull();
        assertThat(rec.latest("missing", "gw-a")).isNull();
    }

    @Test
    void record_overCap_evictsOldestWrite() {
        InMemoryGatewayDecisionRecorder rec = new InMemoryGatewayDecisionRecorder(2, 1_800_000L);
        GatewayDecisionRecorder.GatewayDecision oldest = decision("wfe-1", "gw-a", "t1");
        GatewayDecisionRecorder.GatewayDecision mid = decision("wfe-1", "gw-b", "t2");
        GatewayDecisionRecorder.GatewayDecision newest = decision("wfe-2", "gw-a", "t3");

        rec.record(oldest);
        rec.record(mid);
        rec.record(newest);

        assertThat(rec.size()).isEqualTo(2);
        assertThat(rec.latest("wfe-1", "gw-a")).isNull();
        assertThat(rec.latest("wfe-1", "gw-b")).isEqualTo(mid);
        assertThat(rec.latest("wfe-2", "gw-a")).isEqualTo(newest);
    }

    @Test
    void latest_expiredEntry_returnsNull() throws InterruptedException {
        InMemoryGatewayDecisionRecorder rec = new InMemoryGatewayDecisionRecorder(256, 1L);
        rec.record(decision("wfe-1", "gw-a", "t1"));
        assertThat(rec.size()).isEqualTo(1);

        Thread.sleep(5);

        assertThat(rec.latest("wfe-1", "gw-a")).isNull();
        assertThat(rec.size()).isZero();
    }

    private static GatewayDecisionRecorder.GatewayDecision decision(
            String workflowExecutionId, String gatewayNodeId, String selectedTarget) {
        return new GatewayDecisionRecorder.GatewayDecision(
                workflowExecutionId,
                gatewayNodeId,
                "Gateway",
                List.of(new GatewayDecisionRecorder.BranchEvaluation(
                        selectedTarget, "${route__gw == '" + selectedTarget + "'}", true, null)),
                selectedTarget,
                false,
                false,
                1L);
    }
}
