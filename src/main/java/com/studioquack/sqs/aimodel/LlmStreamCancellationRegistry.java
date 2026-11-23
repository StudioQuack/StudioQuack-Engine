package com.studioquack.sqs.aimodel;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks in-flight streaming LLM provider calls by the run that started them (
 * {@code workflowExecutionId}) so {@code cancel-instance} can abort the generation itself — not
 * just the sandbox container around it. The server never learns the run died, keeps generating,
 * and the late answer still lands in the conversation history.
 */
@Slf4j
@Component
public class LlmStreamCancellationRegistry {

    /** run key → live provider streams. A run chats with at most a couple of models at once. */
    private final Map<String, Set<Closeable>> streamsByRun = new ConcurrentHashMap<>();

    /** Registers {@code stream} under {@code runKey}; no-op for blank keys or null streams. */
    public void register(String runKey, Closeable stream) {
        if (runKey == null || runKey.isBlank() || stream == null) {
            return;
        }
        streamsByRun.computeIfAbsent(runKey, k -> ConcurrentHashMap.newKeySet()).add(stream);
    }

    /** Drops {@code stream} (stream finished on its own); removes the run bucket when empty. */
    public void unregister(String runKey, Closeable stream) {
        if (runKey == null || stream == null) {
            return;
        }
        Set<Closeable> streams = streamsByRun.get(runKey);
        if (streams == null) {
            return;
        }
        streams.remove(stream);
        if (streams.isEmpty()) {
            streamsByRun.remove(runKey, streams);
        }
    }

    /**
     * Closes every stream the run still has open, failing their parked reads. Returns how many streams
     * were aborted.
     */
    public int abortRun(String runKey) {
        if (runKey == null || runKey.isBlank()) {
            return 0;
        }
        Set<Closeable> streams = streamsByRun.remove(runKey);
        if (streams == null || streams.isEmpty()) {
            return 0;
        }
        int aborted = 0;
        for (Closeable stream : streams) {
            try {
                stream.close();
                aborted++;
            } catch (Exception ignored) {
                // Already closed by EOF — the abort goal still holds.
            }
        }
        if (aborted > 0) {
            log.info("llm-proxy: aborted {} in-flight provider stream(s) for cancelled run {}", aborted, runKey);
        }
        return aborted;
    }
}
