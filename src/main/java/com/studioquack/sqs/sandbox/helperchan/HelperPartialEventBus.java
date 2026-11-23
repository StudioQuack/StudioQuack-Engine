package com.studioquack.sqs.sandbox.helperchan;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory fan-in of {@code /v1/helper/emit-partial} frames to a parent {@code execute/stream}
 * caller. Keyed by the CHILD workflowExecutionId (the sub-PI the parent started).
 */
@Slf4j
@Component
public class HelperPartialEventBus {

    /** One parent execute/stream waiting on a child's emit-partials. */
    @FunctionalInterface
    public interface PartialListener {
        void onPartial(String channel, Object payload);
    }

    private final ConcurrentHashMap<String, PartialListener> listeners = new ConcurrentHashMap<>();

    /**
     * Binds {@code childWorkflowExecutionId} to the parent's stream listener.
     * Replaces any previous listener for the same child id.
     */
    public void register(String childWorkflowExecutionId, PartialListener listener) {
        if (childWorkflowExecutionId == null || childWorkflowExecutionId.isBlank() || listener == null) {
            return;
        }
        listeners.put(childWorkflowExecutionId, listener);
    }

    /**
     * Delivers one partial to the registered parent, synchronously so the pipe can write
     * immediately. Returns {@code false} when no parent is registered.
     */
    public boolean emit(String childWorkflowExecutionId, String channel, Object payload) {
        if (childWorkflowExecutionId == null || childWorkflowExecutionId.isBlank()) {
            return false;
        }
        PartialListener listener = listeners.get(childWorkflowExecutionId);
        if (listener == null) {
            return false;
        }
        String ch = (channel == null || channel.isBlank()) ? "answer" : channel;
        try {
            listener.onPartial(ch, payload);
        } catch (RuntimeException e) {
            log.warn("helper-partial: listener failed childWfeId={}: {}",
                    childWorkflowExecutionId, e.toString());
        }
        return true;
    }

    /** Removes the parent binding when execute/stream finishes (success, error, or cancel). */
    public void unregister(String childWorkflowExecutionId) {
        if (childWorkflowExecutionId == null || childWorkflowExecutionId.isBlank()) {
            return;
        }
        listeners.remove(childWorkflowExecutionId);
    }

    /** Test/diagnostics: whether a parent is parked on this child. */
    public boolean isRegistered(String childWorkflowExecutionId) {
        return childWorkflowExecutionId != null && listeners.containsKey(childWorkflowExecutionId);
    }
}
