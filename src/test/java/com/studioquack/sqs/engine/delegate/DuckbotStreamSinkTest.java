package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.sandbox.helperchan.HelperStreamLimits;
import com.studioquack.sqs.sandbox.helperchan.StreamRouteSink.StreamFrame;
import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckbotStreamSinkTest {

    private static StreamFrame frame(String wfe, String source, String channel, String delta) {
        return new StreamFrame(wfe, "conv-1", "amk-1", source, "node-end", channel, delta, null);
    }

    @Test
    void boundaryDetectionMatchesDuckbotEndOnly() {
        DuckbotStreamSink sink = new DuckbotStreamSink(null);
        assertTrue(sink.isStreamTarget(Map.of("data", Map.of("category", "conversion", "nodeType", "end"))));
        assertTrue(sink.isStreamTarget(Map.of("data", Map.of("category", "application", "nodeType", "end"))));
        assertFalse(sink.isStreamTarget(Map.of("data", Map.of("category", "conversion", "nodeType", "start"))));
        assertFalse(sink.isStreamTarget(Map.of("data", Map.of("category", "ai", "nodeType", "end"))));
        assertFalse(sink.isStreamTarget(Map.of("id", "x")));
        assertFalse(sink.isStreamTarget(null));
    }

    @Test
    void chainedAgentsGetSeparateSessions() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        sink.onFrame(frame("wfe", "agent-1", "thinking", "a1 thinks"));
        sink.onFrame(frame("wfe", "agent-2", "thinking", "a2 thinks"));
        sink.onSourceFinished("wfe", "agent-1");
        sink.onSourceFinished("wfe", "agent-2");

        // start+delta per agent, then one end per agent — nothing overwrites.
        assertEquals(6, seen.size());
        assertEquals("agent-1", seen.get(1).nodeId());
        assertEquals("a1 thinks", seen.get(1).delta());
        assertEquals("agent-2", seen.get(3).nodeId());
        assertEquals("a2 thinks", seen.get(3).delta());
        assertEquals("stream-end", seen.get(4).event());
        assertEquals("agent-1", seen.get(4).nodeId());
        assertEquals("stream-end", seen.get(5).event());
        assertEquals("agent-2", seen.get(5).nodeId());
    }

    @Test
    void oversizeDeltaIsChunkedIntoSeqOrderedFramesNeverTruncated() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        String huge = "x".repeat(HelperStreamLimits.MAX_DELTA_BYTES * 2 + 100);
        sink.onFrame(frame("wfe", "agent-1", "thinking", huge));

        // start + 3 chunks; concatenation reproduces the exact input, seqs ascending.
        assertEquals(4, seen.size());
        assertEquals("stream-start", seen.get(0).event());
        StringBuilder reassembled = new StringBuilder();
        int prevSeq = -1;
        for (int i = 1; i < seen.size(); i++) {
            assertEquals("stream-delta", seen.get(i).event());
            assertTrue(seen.get(i).delta().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                    <= HelperStreamLimits.MAX_DELTA_BYTES);
            assertTrue(seen.get(i).seq() > prevSeq);
            prevSeq = seen.get(i).seq();
            reassembled.append(seen.get(i).delta());
        }
        assertEquals(huge, reassembled.toString());
    }

    @Test
    void fallbackRenderChunksTheAssembledText() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        String huge = "y".repeat(HelperStreamLimits.MAX_DELTA_BYTES + 10);
        sink.renderFallback("wfe", "conv-1", "amk-1", "agent-1", "Agent One", "thinking", huge);

        StringBuilder reassembled = new StringBuilder();
        for (ConversationStreamEvent e : seen) {
            if ("stream-delta".equals(e.event())) {
                reassembled.append(e.delta());
            }
        }
        assertEquals(huge, reassembled.toString());
        assertTrue(seen.size() > 2, "assembled render must arrive as multiple frames");
    }

    @Test
    void channelNormalisationMapsThoughtsToThinking() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));
        sink.onFrame(frame("wfe", "agent-1", "thoughts", "t"));
        sink.onFrame(frame("wfe", "agent-1", "answer", "a"));
        assertEquals("thinking", seen.get(0).channel());
        assertEquals("answer", seen.get(2).channel());
    }

    @Test
    void fallbackRenderSkipsWhenLiveStreamAlreadyRan() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        sink.onFrame(frame("wfe", "agent-1", "thinking", "live"));
        int afterLive = seen.size();
        sink.renderFallback("wfe", "conv-1", "amk-1", "agent-1", "Agent One", "thinking", "assembled");
        assertEquals(afterLive, seen.size(), "live stream already painted it — no duplicate");

        // A different source node with no live history DOES render, under its own label —
        // one agent's live stream never suppresses another agent's fallback.
        sink.renderFallback("wfe", "conv-1", "amk-1", "agent-2", "Agent Two", "thinking", "assembled");
        assertTrue(seen.stream().anyMatch(e -> "assembled".equals(e.delta())
                && "agent-2".equals(e.nodeId()) && "Agent Two".equals(e.nodeLabel())));

        // A different conversation with no live history renders, then ends.
        sink.renderFallback("wfe", "conv-2", "amk-1", "agent-1", "Agent One", "thinking", "assembled");
        sink.endFallback("conv-2");
        assertTrue(seen.stream().anyMatch(e -> "stream-end".equals(e.event())
                && "conv-2".equals(e.conversationId())));
    }

    @Test
    void hadRecentStreamTracksLiveActivityPerSourceNode() {
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> { });
        assertFalse(sink.hadRecentStream("wfe", "conv-1", "agent-1", "thinking"));
        sink.onFrame(frame("wfe", "agent-1", "thinking", "x"));
        assertTrue(sink.hadRecentStream("wfe", "conv-1", "agent-1", "thinking"));
        assertFalse(sink.hadRecentStream("wfe", "conv-1", "agent-1", "answer"));
        assertFalse(sink.hadRecentStream("wfe", "conv-1", "agent-2", "thinking"));
        // A later run on the same conversation is a new turn — turn 1's live paint
        // must not suppress turn 2's fallback or steal its stream-end.
        assertFalse(sink.hadRecentStream("wfe-turn-2", "conv-1", "agent-1", "thinking"));
    }

    @Test
    void secondTurnOnSameConversationFallsBackAndSealsIndependently() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        sink.onFrame(frame("run-1", "agent-1", "answer", "first"));
        sink.onSourceFinished("run-1", "agent-1");
        sink.endFallback("conv-1");
        int afterTurn1 = seen.size();

        // Turn 2: no live stream (e.g. the live lane was refused after run-1 went COMPLETED).
        // Fallback must still paint once and emit its own stream-end on run-2.
        sink.renderFallback("run-2", "conv-1", "amk-1", "agent-1", "Agent One", "answer", "second");
        sink.endFallback("conv-1");

        List<ConversationStreamEvent> turn2 = seen.subList(afterTurn1, seen.size());
        assertTrue(turn2.stream().anyMatch(e -> "second".equals(e.delta()) && "run-2".equals(e.workflowExecutionId())),
                "turn 2 fallback must not be dropped as a duplicate of turn 1");
        assertEquals(1, turn2.stream().filter(e -> "second".equals(e.delta())).count());
        assertTrue(turn2.stream().anyMatch(e -> "stream-end".equals(e.event())
                && "run-2".equals(e.workflowExecutionId())));
    }

    @Test
    void thinkingThenAnswerOnSameNodeShareMonotonicSeq() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        sink.onFrame(frame("wfe", "agent-1", "thinking", "t0"));
        sink.onFrame(frame("wfe", "agent-1", "thinking", "t1"));
        sink.onFrame(frame("wfe", "agent-1", "thinking", "t2"));
        sink.onFrame(frame("wfe", "agent-1", "answer", "a0"));
        sink.onFrame(frame("wfe", "agent-1", "answer", "a1"));

        int[] seqs = seen.stream().mapToInt(ConversationStreamEvent::seq).toArray();
        for (int i = 1; i < seqs.length; i++) {
            assertTrue(seqs[i] > seqs[i - 1], "seq must be monotonic across channels of one source");
        }
        assertEquals(0, seqs[0]);
        assertEquals(seqs.length - 1, seqs[seqs.length - 1]);
        int lastThinking = seen.stream()
                .filter(e -> "thinking".equals(e.channel()))
                .mapToInt(ConversationStreamEvent::seq)
                .max().orElseThrow();
        int firstAnswerDelta = seen.stream()
                .filter(e -> "answer".equals(e.channel()) && "stream-delta".equals(e.event()))
                .mapToInt(ConversationStreamEvent::seq)
                .min().orElseThrow();
        assertTrue(firstAnswerDelta > lastThinking);
    }

    @Test
    void seqSurvivesSourceFinishWhileFallbackContinues() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        sink.onFrame(frame("wfe", "agent-1", "thinking", "live"));
        sink.onSourceFinished("wfe", "agent-1");
        int lastLive = seen.get(seen.size() - 1).seq();
        assertEquals("stream-end", seen.get(seen.size() - 1).event());

        // Answer channel had no live stream, so fallback is not skipped. Seq must not restart at 0.
        sink.renderFallback("wfe", "conv-1", "amk-1", "agent-1", "Agent One", "answer", "assembled");
        int firstFallback = seen.stream()
                .filter(e -> "answer".equals(e.channel()))
                .mapToInt(ConversationStreamEvent::seq)
                .min().orElseThrow();
        assertTrue(firstFallback > lastLive, "fallback after source-finish must continue seq, not restart at 0");

        sink.endFallback("conv-1");
        assertTrue(seen.stream().anyMatch(e -> "stream-end".equals(e.event())
                && "answer".equals(e.channel())));
    }

    @Test
    void fallbackSeqContinuesAcrossSourceFinishOnOpenSession() {
        List<ConversationStreamEvent> seen = new ArrayList<>();
        DuckbotStreamSink sink = new DuckbotStreamSink(e -> seen.add(e));

        sink.renderFallback("wfe", "conv-1", "amk-1", "agent-1", "Agent One", "thinking", "a");
        sink.onSourceFinished("wfe", "agent-1");
        sink.renderFallback("wfe", "conv-1", "amk-1", "agent-1", "Agent One", "thinking", "b");

        int[] seqs = seen.stream().mapToInt(ConversationStreamEvent::seq).toArray();
        for (int i = 1; i < seqs.length; i++) {
            assertTrue(seqs[i] > seqs[i - 1], "open fallback must keep monotonic seq across source-finish");
        }
        assertEquals(0, seqs[0]);
    }
}
