package com.studioquack.sqs.runtime.support;

import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Conformance test double for {@link SandboxExecutionPort}. It lets the FULL in-process dispatch →
 * resume flow (engine → {@code ScriptDispatchDelegate} → coordinator → {@code
 * EngineTriggerService}) run deterministically WITHOUT a real container, so the carry-over
 * conformance cases are not gated on a live OCI runtime (the plan explicitly allows a fake {@code
 * SandboxExecutionPort}).
 */
public class FakeSandboxExecutionPort implements SandboxExecutionPort {

    private final Map<String, Function<SandboxJobRequest, SandboxJobResponse>> byNode = new ConcurrentHashMap<>();

    /** Program a node to succeed, returning the given per-port outputs (portValue → value). */
    public FakeSandboxExecutionPort succeed(String nodeId, Map<String, Object> portOutputs) {
        byNode.put(nodeId, req -> SandboxJobResponse.success(0, "ok", "", portOutputs, 1L, "fake"));
        return this;
    }

    /** Program a node to fail with a typed error code → routes through the error gate/boundary. */
    public FakeSandboxExecutionPort fail(String nodeId, String errorCode, String errorMessage) {
        byNode.put(nodeId, req -> SandboxJobResponse.failure(1, "", errorMessage, Map.of(),
                errorCode, errorMessage, 1L, "fake"));
        return this;
    }

    /** Program a fully bespoke response (fast-return, delayed, etc.). */
    public FakeSandboxExecutionPort program(String nodeId, Function<SandboxJobRequest, SandboxJobResponse> fn) {
        byNode.put(nodeId, fn);
        return this;
    }

    /** Clears all programmed responses (call from {@code @BeforeEach}). */
    public void reset() {
        byNode.clear();
    }

    @Override
    public SandboxJobResponse execute(SandboxJobRequest job) {
        Function<SandboxJobRequest, SandboxJobResponse> fn = byNode.get(job.currentNodeId());
        if (fn != null) {
            return fn.apply(job);
        }
        return SandboxJobResponse.success(0, "ok", "", Map.of(), 1L, "fake");
    }

    @Override
    public String name() {
        return "fake-sandbox (WP-E5 conformance)";
    }

    @Override
    public boolean runtimeAvailable() {
        return true;
    }
}
