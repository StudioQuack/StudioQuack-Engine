package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.sandbox.helperchan.HelperStreamLimits;
import com.studioquack.sqs.sandbox.helperchan.StreamRouteSink;
import com.studioquack.sqs.status.ConversationStreamCallback;
import com.studioquack.sqs.status.ConversationStreamCallback.ConversationStreamEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Duckbot end as the graph-routed live renderer: the ONLY component that turns streamed
 * node output into conversation-stream frames. Sessions are keyed by
 * (workflowExecutionId, sourceNodeId, channel) so chained agents on one run each paint
 * their own thinking/answer stream — the FE keys its buffer by the same source-node
 * identity, so nothing overwrites anything else. Seq is shared across every channel of
 * one (workflowExecutionId, sourceNodeId) and lives as long as any session for that
 * source — live or fallback — so a source-job finish cannot restart the counter at 0.
 * The FE keeps one lastSeq per stream key; a second channel (or late fallback) restarting
 * at 0 would be dropped as stale or treated as a new turn and wipe the buffers.
 *
 * <p>Channel normalisation: {@code thinking}/{@code thoughts}/{@code reasoning} render as
 * the thinking bubble; everything else is the answer. The assembled final value still
 * arrives through the normal completion fan-out and is persisted by
 * {@link DuckbotBridge#applyEnd} — these frames are the live preview only.
 *
 * <p>Best-effort: the callback swallows its own failures; this sink never throws.
 */
@Slf4j
@Component
public class DuckbotStreamSink implements StreamRouteSink {

    private static final String EVENT_START = "stream-start";
    private static final String EVENT_DELTA = "stream-delta";
    private static final String EVENT_END = "stream-end";
    /** Fallback/assembled renders count as "recent" for this long (a turn never outlives it). */
    private static final long RECENT_STREAM_MS = 15 * 60 * 1000L;
    private static final int RECENT_CAP = 512;

    /** One open live-render session. seq is shared across channels of the same source node. */
    private static final class Session {
        private final String workflowExecutionId;
        private final String conversationId;
        private final String agentMessageKey;
        private final String sourceNodeId;
        private final String channel;
        private final AtomicInteger seq;
        private final String nodeLabel;
        private volatile boolean started;

        private Session(String workflowExecutionId, String conversationId, String agentMessageKey,
                        String sourceNodeId, String channel, AtomicInteger seq, String nodeLabel) {
            this.workflowExecutionId = workflowExecutionId;
            this.conversationId = conversationId;
            this.agentMessageKey = agentMessageKey;
            this.sourceNodeId = sourceNodeId;
            this.channel = channel;
            this.seq = seq;
            this.nodeLabel = nodeLabel;
        }
    }

    private final ConversationStreamCallback callback;
    /**
     * Cancel marker set by {@code cancel-instance}: frames of a cancelled run are dropped so a
     * dying LLM stream cannot keep painting (or feeding) a stopped turn. Null in slice tests.
     */
    @Autowired(required = false)
    @Nullable
    private com.studioquack.sqs.runtime.CancelledRunGuard cancelledRunGuard;
    /** (wfeId \0 sourceNodeId \0 channel) → session. */
    private final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();
    /** (wfeId \0 sourceNodeId) → next seq, shared by every channel of that source. */
    private final ConcurrentHashMap<String, AtomicInteger> seqBySource = new ConcurrentHashMap<>();
    /** (wfeId \0 conversationId \0 sourceNodeId \0 channel) → last activity — live already painted. */
    private final ConcurrentHashMap<String, Long> recentStreams = new ConcurrentHashMap<>();
    /** (wfeId \0 conversationId \0 sourceNodeId \0 channel) → fallback session for assembled renders. */
    private final ConcurrentHashMap<String, Session> fallbackSessions = new ConcurrentHashMap<>();

    @Autowired
    public DuckbotStreamSink(@Nullable ConversationStreamCallback callback) {
        this.callback = callback;
    }

    /** Duckbot END boundary: template category conversion/application + nodeType "end". */
    @Override
    public boolean isStreamTarget(Map<String, Object> targetNode) {
        if (targetNode == null || !(targetNode.get("data") instanceof Map<?, ?> data)) {
            return false;
        }
        Object category = data.get("category");
        Object nodeType = data.get("nodeType");
        if (category == null || nodeType == null) {
            return false;
        }
        String cat = String.valueOf(category);
        boolean duck = "conversion".equalsIgnoreCase(cat) || "application".equalsIgnoreCase(cat);
        return duck && "end".equalsIgnoreCase(String.valueOf(nodeType));
    }

    @Override
    public void onFrame(StreamFrame frame) {
        if (callback == null || frame == null || frame.conversationId() == null
                || frame.conversationId().isBlank()) {
            return;
        }
        if (cancelledRunGuard != null && cancelledRunGuard.isCancelled(frame.workflowExecutionId())) {
            return; // the turn was stopped — late tokens of the dying stream go nowhere
        }
        String channel = normalizeChannel(frame.channel());
        AtomicInteger seq = seqFor(frame.workflowExecutionId(), frame.sourceNodeId());
        String key = frame.workflowExecutionId() + '\0' + frame.sourceNodeId() + '\0' + channel;
        Session session = sessions.computeIfAbsent(key, k -> new Session(frame.workflowExecutionId(),
                frame.conversationId(), frame.agentMessageKey(), frame.sourceNodeId(), channel,
                seq, blankToNull(frame.nodeLabel())));
        if (!session.started) {
            synchronized (session) {
                if (!session.started) {
                    emit(session, EVENT_START, null);
                    session.started = true;
                }
            }
        }
        // Chunk, never truncate: each piece rides its own seq'd frame so the FE appends
        // them in order and an oversize delta reassembles byte-for-byte.
        for (String chunk : HelperStreamLimits.splitDelta(frame.delta())) {
            emit(session, EVENT_DELTA, chunk);
        }
        touch(frame.workflowExecutionId(), frame.conversationId(), frame.sourceNodeId(), channel);
    }

    @Override
    public void onSourceFinished(String workflowExecutionId, String sourceNodeId) {
        if (callback == null || workflowExecutionId == null || sourceNodeId == null) {
            return;
        }
        String prefix = workflowExecutionId + '\0' + sourceNodeId + '\0';
        Iterator<Map.Entry<String, Session>> it = sessions.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Session> entry = it.next();
            if (!entry.getKey().startsWith(prefix)) {
                continue;
            }
            Session session = entry.getValue();
            it.remove();
            if (session.started) {
                emit(session, EVENT_END, null);
            }
        }
        // Seq outlives the source job: a fallback session for this stream may still be
        // open (or open in applyEnd after this returns). Clearing the counter here
        // restarted late frames at 0, which the FE treats as a new turn and wipes.
    }

    @Override
    public boolean hadRecentStream(String workflowExecutionId, String conversationId, String sourceNodeId,
                                   String channel) {
        if (workflowExecutionId == null || conversationId == null || sourceNodeId == null) {
            return false;
        }
        Long at = recentStreams.get(recentKey(workflowExecutionId, conversationId, sourceNodeId,
                normalizeChannel(channel)));
        return at != null && System.currentTimeMillis() - at < RECENT_STREAM_MS;
    }

    @Override
    public void renderFallback(String workflowExecutionId, String conversationId, String agentMessageKey,
                               String rendererNodeId, String nodeLabel, String channel, String text) {
        if (callback == null || conversationId == null || conversationId.isBlank()
                || text == null || text.isBlank()) {
            return;
        }
        if (cancelledRunGuard != null && cancelledRunGuard.isCancelled(workflowExecutionId)) {
            return; // cancelled run — no fallback render after the turn stopped
        }
        String ch = normalizeChannel(channel);
        if (hadRecentStream(workflowExecutionId, conversationId, rendererNodeId, ch)) {
            // This run's live stream already painted this content — re-rendering would duplicate it.
            return;
        }
        String key = recentKey(workflowExecutionId, conversationId, rendererNodeId, ch);
        AtomicInteger seq = seqFor(workflowExecutionId, rendererNodeId);
        Session session = fallbackSessions.computeIfAbsent(key, k -> new Session(
                workflowExecutionId, conversationId, agentMessageKey, rendererNodeId, ch, seq,
                blankToNull(nodeLabel)));
        if (!session.started) {
            synchronized (session) {
                if (!session.started) {
                    emit(session, EVENT_START, null);
                    session.started = true;
                }
            }
        }
        // Assembled renders arrive as one whole string — chunk it into seq'd frames.
        for (String chunk : HelperStreamLimits.splitDelta(text)) {
            emit(session, EVENT_DELTA, chunk);
        }
    }

    @Override
    public void endFallback(String conversationId) {
        if (callback == null || conversationId == null) {
            return;
        }
        Iterator<Map.Entry<String, Session>> it = fallbackSessions.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Session> entry = it.next();
            if (!keyBelongsToConversation(entry.getKey(), conversationId)) {
                continue;
            }
            Session session = entry.getValue();
            it.remove();
            if (session.started) {
                emit(session, EVENT_END, null);
            }
        }
        // Drop this conversation's live-already-painted markers so the next turn on the
        // same thread can fallback-render (and seal) if its own live stream never ran.
        recentStreams.entrySet().removeIf(e -> keyBelongsToConversation(e.getKey(), conversationId));
    }

    /** thinking/thoughts/reasoning render as the thinking bubble; everything else is the answer. */
    static String normalizeChannel(String channel) {
        if (channel == null) {
            return "answer";
        }
        String c = channel.toLowerCase(java.util.Locale.ROOT);
        return (c.contains("think") || c.contains("thought") || c.contains("reason")) ? "thinking" : "answer";
    }

    private void touch(String workflowExecutionId, String conversationId, String sourceNodeId, String channel) {
        if (recentStreams.size() > RECENT_CAP) {
            long cutoff = System.currentTimeMillis() - RECENT_STREAM_MS;
            recentStreams.entrySet().removeIf(e -> e.getValue() < cutoff);
        }
        recentStreams.put(recentKey(workflowExecutionId, conversationId, sourceNodeId, channel),
                System.currentTimeMillis());
    }

    /** (wfeId \0 conversationId \0 sourceNodeId \0 channel). */
    private static String recentKey(String workflowExecutionId, String conversationId, String sourceNodeId,
                                    String channel) {
        return String.valueOf(workflowExecutionId) + '\0' + conversationId + '\0' + sourceNodeId + '\0' + channel;
    }

    /** True when {@code key} is {@code * \0 conversationId \0 …}. */
    private static boolean keyBelongsToConversation(String key, String conversationId) {
        if (key == null || conversationId == null) {
            return false;
        }
        int first = key.indexOf('\0');
        if (first < 0) {
            return false;
        }
        return key.startsWith(conversationId + '\0', first + 1);
    }

    private AtomicInteger seqFor(String workflowExecutionId, String sourceNodeId) {
        if (seqBySource.size() > RECENT_CAP) {
            seqBySource.keySet().removeIf(k -> !seqKeyInUse(k));
        }
        return seqBySource.computeIfAbsent(workflowExecutionId + '\0' + sourceNodeId, k -> new AtomicInteger(0));
    }

    private boolean seqKeyInUse(String key) {
        int split = key.indexOf('\0');
        if (split < 0) {
            return false;
        }
        String wfe = key.substring(0, split);
        String source = key.substring(split + 1);
        String livePrefix = wfe + '\0' + source + '\0';
        for (String sessionKey : sessions.keySet()) {
            if (sessionKey.startsWith(livePrefix)) {
                return true;
            }
        }
        for (Session session : fallbackSessions.values()) {
            if (wfe.equals(session.workflowExecutionId) && source.equals(session.sourceNodeId)) {
                return true;
            }
        }
        return false;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private void emit(Session session, String event, String delta) {
        int seq = session.seq.getAndIncrement();
        try {
            callback.emit(new ConversationStreamEvent(session.workflowExecutionId, session.conversationId,
                    session.agentMessageKey, session.sourceNodeId, event, session.channel, delta, seq,
                    null, session.nodeLabel));
        } catch (RuntimeException e) {
            log.warn("duckbot.stream: {} failed node={} channel={}: {}",
                    event, session.sourceNodeId, session.channel, e.toString());
        }
    }
}
