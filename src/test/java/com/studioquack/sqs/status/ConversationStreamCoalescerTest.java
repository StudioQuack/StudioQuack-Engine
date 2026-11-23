package com.studioquack.sqs.status;

import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * {@link ConversationStreamCoalescer} merges the per-token frame burst into one POST per
 * round-trip while a lane is busy, and submits immediately while it is idle — the Duckbot
 * screen must paint at the speed the model produces, never at one-POST-per-token speed.
 */
class ConversationStreamCoalescerTest {

    /** Captures submitted frames; the test decides when each POST "completes". */
    private static final class FakeSink implements ConversationStreamCoalescer.BatchSink {
        private final List<ConversationStreamEvent> submitted = new ArrayList<>();
        private final List<Runnable> completions = new ArrayList<>();

        @Override
        public void submit(String runId, String lane, ConversationStreamEvent frame, Runnable onComplete) {
            submitted.add(frame);
            completions.add(onComplete);
        }

        private void completeLast() {
            completions.get(completions.size() - 1).run();
        }

        private void completeAll() {
            while (!completions.isEmpty()) {
                Runnable c = completions.remove(0);
                c.run();
            }
        }
    }

    private static ConversationStreamEvent delta(int seq, String channel, String text) {
        return new ConversationStreamEvent("run-1", "conv-1", "msg-1", "node-1",
                "stream-delta", channel, text, seq, null, null);
    }

    private static ConversationStreamEvent barrier(int seq, String event) {
        return new ConversationStreamEvent("run-1", "conv-1", "msg-1", "node-1",
                event, "thinking", null, seq, null, null);
    }

    @Test
    @DisplayName("idle lane submits the first frame immediately — zero added latency")
    void idleLane_submitsImmediately() {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink);
        coalescer.offer("node-1", delta(0, "thinking", "a"));
        assertThat(sink.submitted).hasSize(1);
        assertThat(sink.submitted.get(0).delta()).isEqualTo("a");
    }

    @Test
    @DisplayName("frames arriving during an in-flight POST merge into ONE batch on completion")
    void inFlight_mergesBurstIntoOnePost() {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink);
        coalescer.offer("node-1", delta(0, "thinking", "a"));   // submitted at once
        coalescer.offer("node-1", delta(1, "thinking", "b"));   // buffered (POST in flight)
        coalescer.offer("node-1", delta(2, "thinking", "c"));
        coalescer.offer("node-1", delta(3, "thinking", "d"));
        assertThat(sink.submitted).hasSize(1);

        sink.completeLast();

        assertThat(sink.submitted).hasSize(2);
        ConversationStreamEvent merged = sink.submitted.get(1);
        assertThat(merged.event()).isEqualTo("stream-delta");
        assertThat(merged.channel()).isEqualTo("thinking");
        assertThat(merged.delta()).isEqualTo("bcd");
        assertThat(merged.seq())
                .as("merged batch carries the group's first seq — the FE stale-seq guard stays monotonic")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a channel flip splits the batch but preserves order")
    void channelFlip_splitsInOrder() {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink);
        coalescer.offer("node-1", delta(0, "thinking", "t0"));
        coalescer.offer("node-1", delta(1, "thinking", "t1"));
        coalescer.offer("node-1", delta(2, "answer", "a2"));
        coalescer.offer("node-1", delta(3, "answer", "a3"));
        sink.completeLast();   // drains the burst

        assertThat(sink.submitted).hasSize(3);
        assertThat(sink.submitted.get(1).channel()).isEqualTo("thinking");
        assertThat(sink.submitted.get(1).delta()).isEqualTo("t1");
        assertThat(sink.submitted.get(2).channel()).isEqualTo("answer");
        assertThat(sink.submitted.get(2).delta()).isEqualTo("a2a3");
        assertThat(sink.submitted.get(2).seq()).isEqualTo(2);
    }

    @Test
    @DisplayName("start/end barriers ride alone and in order")
    void barriers_rideAloneInOrder() {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink);
        coalescer.offer("node-1", barrier(0, "stream-start"));
        coalescer.offer("node-1", delta(1, "answer", "x"));
        coalescer.offer("node-1", barrier(2, "stream-end"));
        sink.completeAll();

        assertThat(sink.submitted).extracting(ConversationStreamEvent::event)
                .containsExactly("stream-start", "stream-delta", "stream-end");
        assertThat(sink.submitted.get(2).delta()).isNull();
    }

    @Test
    @DisplayName("a batch never exceeds MAX_BATCH_BYTES — it splits instead")
    void batchBytes_capSplits() {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink);
        String big = "x".repeat(ConversationStreamCoalescer.MAX_BATCH_BYTES);
        coalescer.offer("node-1", delta(0, "answer", "first"));
        coalescer.offer("node-1", delta(1, "answer", big));
        coalescer.offer("node-1", delta(2, "answer", "tail"));
        sink.completeAll();

        assertThat(sink.submitted).hasSize(3);
        assertThat(sink.submitted.get(1).delta()).isEqualTo(big);
        assertThat(sink.submitted.get(2).delta()).isEqualTo("tail");
    }

    @Test
    @DisplayName("stream-end drains the lane and the lane evicts after completion")
    void streamEnd_drainsAndEvicts() {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink);
        coalescer.offer("node-1", delta(0, "answer", "x"));
        coalescer.offer("node-1", barrier(1, "stream-end"));
        sink.completeAll();
        assertThat(coalescer.awaitIdle(Duration.ofMillis(50))).isTrue();
    }

    @Test
    @DisplayName("lane buffer cap drops oldest deltas, never barriers")
    void laneCap_dropsOldestDeltas() {
        FakeSink sink = new FakeSink();
        // Test seam: exercise the catastrophic guard with a small cap (production is 256 MiB).
        int cap = 64 * 1024;
        ConversationStreamCoalescer coalescer =
                new ConversationStreamCoalescer(sink, ConversationStreamCoalescer.STUCK_POST_MS, cap);
        coalescer.offer("node-1", barrier(0, "stream-start"));
        // No completion → everything buffers. Push past the lane cap.
        String chunk = "y".repeat(4096);
        int frames = cap / 4096 + 8;
        for (int i = 1; i <= frames; i++) {
            coalescer.offer("node-1", delta(i, "thinking", chunk));
        }
        coalescer.offer("node-1", barrier(frames + 1, "stream-end"));
        sink.completeAll();

        assertThat(sink.submitted).last().satisfies(f -> assertThat(f.event()).isEqualTo("stream-end"));
        long totalDeltaBytes = sink.submitted.stream()
                .filter(f -> "stream-delta".equals(f.event()))
                .mapToLong(f -> f.delta().length())
                .sum();
        assertThat(totalDeltaBytes)
                .isLessThanOrEqualTo(cap + ConversationStreamCoalescer.MAX_BATCH_BYTES);
    }

    @Test
    @DisplayName("a dropped in-flight task (saturation) is re-driven by the stuck watchdog")
    void stuckPost_reDriven() throws Exception {
        FakeSink sink = new FakeSink();
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(sink, 50L);
        coalescer.offer("node-1", delta(0, "answer", "a"));
        // Simulate the dispatcher dropping the queued its completion never runs.
        sink.completions.clear();
        coalescer.offer("node-1", delta(1, "answer", "b"));
        assertThat(sink.submitted).hasSize(1);   // still in flight — nothing new submitted

        Thread.sleep(80L);   // age the in-flight marker past the (test-short) watchdog window
        coalescer.offer("node-1", delta(2, "answer", "c"));

        assertThat(sink.submitted).hasSize(2);
        assertThat(sink.submitted.get(1).delta())
                .as("the wedged lane re-drives and merges everything buffered meanwhile")
                .isEqualTo("bc");
    }

    @Test
    @DisplayName("null frame / null lane never throw")
    void offer_nullSafe() {
        ConversationStreamCoalescer coalescer = new ConversationStreamCoalescer(new FakeSink());
        assertThatCode(() -> coalescer.offer(null, delta(0, "answer", "x"))).doesNotThrowAnyException();
        assertThatCode(() -> coalescer.offer("node-1", null)).doesNotThrowAnyException();
    }
}
