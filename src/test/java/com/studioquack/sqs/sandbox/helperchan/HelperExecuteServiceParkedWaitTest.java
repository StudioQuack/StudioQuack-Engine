package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.LlmStreamCancellationRegistry;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A parked {@code studioquack.execute()} has no time limit: it waits until the child finishes,
 * and ends early only when its caller's dispatch is over — then the orphaned child run is
 * cancelled with it (its LLM calls, its sandbox and its process instance).
 */
class HelperExecuteServiceParkedWaitTest {

    private final SqsEngine engine = mock(SqsEngine.class);
    private final SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
    private final RefOnlyVarBuilder refOnlyVars = mock(RefOnlyVarBuilder.class);
    private final DispatchCancellationRegistry dispatchCancellation = mock(DispatchCancellationRegistry.class);
    private final LlmStreamCancellationRegistry llmCancellation = mock(LlmStreamCancellationRegistry.class);
    private final CompletableFuture<Map<String, Object>> childCompletion = new CompletableFuture<>();

    private HelperExecuteService service;

    @BeforeEach
    void setUp() {
        when(engine.latestDefinitionExists(any())).thenReturn(true);
        when(engine.processInstances()).thenThrow(new RuntimeException("no runtime"));
        when(engine.resolveProcessInstanceId(anyString())).thenReturn("pi-child");
        when(engine.isProcessInstanceActive("pi-child")).thenReturn(true);
        when(registry.register(any())).thenReturn(childCompletion);
        when(refOnlyVars.toEngineVariables(any())).thenReturn(Map.of());
        ExecutionContextStore store = mock(ExecutionContextStore.class);
        when(store.collectOutputsByNode(any())).thenReturn(Map.of());
        service = new HelperExecuteService(
                new ObjectMapper(), engine, registry, refOnlyVars, mock(TransformDirectiveApplier.class),
                store, new OverlayPeerDirectory(), new MeshIdentity());
        service.dispatchCancellation = dispatchCancellation;
        service.llmCancellation = llmCancellation;
    }

    @Test
    void parkedCall_endsAndCancelsTheChildRunWhenTheCallerIsGone() throws Exception {
        HelperJob caller = job();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread parked = new Thread(() -> {
            try {
                service.handle(caller, "model", Map.of("q", "hi"), null, null);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        parked.start();
        parked.join(1_200);
        assertTrue(parked.isAlive(), "the call waits for the child, with no limit of its own");

        caller.markRevoked();
        parked.join(5_000);
        assertFalse(parked.isAlive(), "the wait ends once the caller's dispatch is over");
        assertInstanceOf(IllegalStateException.class, failure.get());

        ArgumentCaptor<SubProcessCompletionRegistry.SubPiLink> link =
                ArgumentCaptor.forClass(SubProcessCompletionRegistry.SubPiLink.class);
        verify(registry).register(link.capture());
        String childId = link.getValue().childWorkflowExecutionId();
        verify(llmCancellation).abortRun(childId);
        verify(dispatchCancellation).cancelRun("pi-child");
        verify(engine).deleteProcessInstance(eq("pi-child"), anyString());
        verify(registry).cancel(eq(childId), anyString());
    }

    @Test
    void parkedCall_returnsTheChildResultHoweverLateItArrives() throws Exception {
        HelperJob caller = job();
        AtomicReference<Object> result = new AtomicReference<>();
        Thread parked = new Thread(() -> result.set(service.handle(caller, "model", Map.of(), null, null)));
        parked.start();
        parked.join(1_200);
        assertTrue(parked.isAlive());

        childCompletion.complete(Map.of());
        parked.join(5_000);
        assertFalse(parked.isAlive());
        assertInstanceOf(Map.class, result.get());
        assertTrue(((Map<?, ?>) result.get()).containsKey("result"));
        verify(engine, never()).deleteProcessInstance(anyString(), anyString());
    }

    /** FUNCTION-port job whose blank {@code env=} stays on the local execute path. */
    private static HelperJob job() {
        Map<String, Object> portDto = new LinkedHashMap<>();
        portDto.put("value", "model_in");
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("nodeId", "node-target");
        detail.put("portDto", portDto);
        Map<String, Object> conn = new LinkedHashMap<>();
        conn.put("portValue", "model");
        conn.put("connectionDetailList", List.of(detail));
        Map<String, Object> currentNode = new LinkedHashMap<>();
        currentNode.put("id", "node-agent");
        currentNode.put("connectionList", List.of(conn));
        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("envId", "env-local");
        ec.put("workflowExecutionId", "parent-wfe");
        ec.put("currentNodeId", "node-agent");
        ec.put("currentNode", currentNode);
        return HelperJob.builder("job-parked", "python").executionContext(ec).build();
    }
}
