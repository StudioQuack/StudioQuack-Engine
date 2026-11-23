package com.studioquack.sqs.command;

import com.studioquack.sqs.status.RunStatusCallback;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Test double for {@link RunStatusCallback} that records every reported transition so tests can
 * assert the daemon emitted RUNNING at {@code start-run} and COMPLETED/FAILED off the engine event
 * bridge. Thread-safe: COMPLETED/FAILED are reported from Activiti async-executor threads.
 */
public class RecordingRunStatusCallback implements RunStatusCallback {

    /** One recorded transition. */
    public record Event(String workflowExecutionId, Status status, String detail) {
    }

    private final List<Event> events = new CopyOnWriteArrayList<>();

    @Override
    public void report(String workflowExecutionId, Status status, String detail) {
        events.add(new Event(workflowExecutionId, status, detail));
    }

    /** All recorded transitions, in arrival order. */
    public List<Event> events() {
        return events;
    }

    /** True when a {@code (workflowExecutionId, status)} transition was recorded. */
    public boolean received(String workflowExecutionId, Status status) {
        return events.stream().anyMatch(e ->
                e.workflowExecutionId().equals(workflowExecutionId) && e.status() == status);
    }

    /** Clears recorded transitions (per-test isolation when a context is reused across methods). */
    public void clear() {
        events.clear();
    }
}
