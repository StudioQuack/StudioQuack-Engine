package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.MeshIdentity;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins {@code executeLocal}/{@code executeLocalStream} recording a sub-PI invocation after the
 * definition check and before {@code startLatest}, and that a store failure is swallowed so the run
 * still starts.
 */
class HelperExecuteServiceSubPiIndexTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final SqsEngine engine = mock(SqsEngine.class);
    private final SubProcessCompletionRegistry registry = mock(SubProcessCompletionRegistry.class);
    private final RefOnlyVarBuilder refOnlyVars = mock(RefOnlyVarBuilder.class);
    private final ExecutionContextStore store = mock(ExecutionContextStore.class);

    private HelperExecuteService service;

    @BeforeEach
    void setUp() {
        when(engine.latestDefinitionExists(any())).thenReturn(true);
        // namespaceSuffixOfRun swallows this and treats the run as published ("").
        when(engine.processInstances()).thenThrow(new RuntimeException("no runtime"));
        when(registry.register(any())).thenReturn(CompletableFuture.completedFuture(Map.of()));
        when(refOnlyVars.toEngineVariables(any())).thenReturn(Map.of());
        when(store.collectOutputsByNode(any())).thenReturn(Map.of());
        service = new HelperExecuteService(
                mapper, engine, registry, refOnlyVars, mock(TransformDirectiveApplier.class),
                store, new OverlayPeerDirectory(), new MeshIdentity());
    }

    @Test
    void executeLocal_recordsInvocationWithCallerCurrentNodeId() {
        Object out = service.handle(job(), "model", Map.of("q", "hi"), null, null);
        assertNotNull(out);

        ArgumentCaptor<String> childId = ArgumentCaptor.forClass(String.class);
        verify(store).recordSubPiInvocation(
                eq("parent-wfe"), childId.capture(), eq("node-agent"), eq("node-target"));
        assertFalse(childId.getValue().isBlank(), "child wfe id is minted before start");
        assertNotEquals("parent-wfe", childId.getValue());
        UUID.fromString(childId.getValue());

        InOrder order = inOrder(store, engine);
        order.verify(store).recordSubPiInvocation(
                eq("parent-wfe"), anyString(), eq("node-agent"), eq("node-target"));
        order.verify(engine).startLatest(any(), any(), any());
    }

    @Test
    void executeLocal_recordingFailureDoesNotBreakExecute() {
        doThrow(new RuntimeException("index down"))
                .when(store).recordSubPiInvocation(any(), any(), any(), any());

        assertDoesNotThrow(() -> service.handle(job(), "model", Map.of(), null, null));
        verify(engine).startLatest(any(), any(), any());
        verify(registry).register(any());
    }

    @Test
    void executeLocalStream_recordsInvocation() {
        AtomicReference<Object> completed = new AtomicReference<>();
        service.handleStream(job(), "model", Map.of(), null, null,
                new HelperExecuteService.ExecuteStreamListener() {
                    @Override
                    public void onPartial(String channel, Object payload) {
                        // unused — this test only asserts the index write
                    }

                    @Override
                    public void onComplete(Object result) {
                        completed.set(result);
                    }

                    @Override
                    public void onError(String message) {
                        fail(message);
                    }
                });
        assertNotNull(completed.get(), "stream path must complete after the local sub-PI");
        verify(store).recordSubPiInvocation(
                eq("parent-wfe"), anyString(), eq("node-agent"), eq("node-target"));
        verify(engine).startLatest(any(), any(), any());
    }

    @Test
    void executeLocal_doesNotRecordWhenSubProcessDefinitionMissing() {
        when(engine.latestDefinitionExists(any())).thenReturn(false);

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> service.handle(job(), "model", Map.of(), null, null));
        assertTrue(ex.getMessage().contains("not deployed"), ex.getMessage());
        verify(store, never()).recordSubPiInvocation(any(), any(), any(), any());
        verify(engine, never()).startLatest(any(), any(), any());
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
        return HelperJob.builder("job-subpi", "python").executionContext(ec).build();
    }
}
