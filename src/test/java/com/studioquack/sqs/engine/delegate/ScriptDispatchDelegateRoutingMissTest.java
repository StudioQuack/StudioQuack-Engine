package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.runtime.DispatchResultCoordinator;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.BranchEvaluation;
import com.studioquack.sqs.runtime.GatewayDecisionRecorder.GatewayDecision;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.queue.DurableJobQueue;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.activiti.engine.ActivitiException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.env.MockEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Resume-time routing miss must name the gateway, not {@code dispatch error: ActivitiException}.
 */
class ScriptDispatchDelegateRoutingMissTest {

    private static final String WFE = "wfe-live";
    private static final String AGENT = "882fdb05";
    private static final String GATEWAY = "61359ea8";
    private static final String DISPATCH = "disp-1";

    private DispatchResultCoordinator coordinator;
    private DurableJobQueue jobQueue;
    private CapturingLogCallback logs;
    private RecordingRecorder recorder;
    private ScriptDispatchDelegate delegate;

    @BeforeEach
    void setUp() {
        coordinator = mock(DispatchResultCoordinator.class);
        jobQueue = mock(DurableJobQueue.class);
        logs = new CapturingLogCallback();
        recorder = new RecordingRecorder();
        delegate = new ScriptDispatchDelegate(
                null, null, null, null, null, null, null, null, null,
                coordinator,
                null,
                jobQueue,
                logs,
                null, null, null, null, null, null, null, null, null, null,
                new MockEnvironment(),
                null,
                providerOf(recorder),
                null,
                null,
                null);
    }

    @Test
    void deliverSuccess_unknownProperty_withDeadEndDecision_namesGatewayNotActivitiException() {
        recorder.record(deadEndDecision(WFE, GATEWAY));
        doThrow(new ActivitiException(
                "Unknown property used in expression: ${route__61359ea8_x}"))
                .when(coordinator).deliverSuccess(eq(DISPATCH), anyMap(), anyMap());

        delegate.resumeSuccessOrReportFailure(
                DISPATCH, AGENT, ref(), graphNodes(), graphEdges(), Map.of(), Map.of());

        org.mockito.ArgumentCaptor<String> message = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(coordinator).deliverFailure(
                eq(DISPATCH),
                eq(SandboxJobResponse.ERROR_SANDBOX_FAILED),
                message.capture(),
                eq(SandboxJobResponse.FailureKind.ENGINE.name()),
                isNull());
        String reported = message.getValue();
        assertTrue(reported.contains(GATEWAY), reported);
        assertTrue(reported.contains("no outgoing flow matched"), reported);
        assertFalse(reported.contains("dispatch error: ActivitiException"), reported);
        assertFalse(reported.contains("Unknown property"), reported);

        assertTrue(logs.lastError.contains(GATEWAY), logs.lastError);
        assertTrue(logs.lastError.contains("no outgoing flow matched"), logs.lastError);
        assertFalse(logs.lastError.contains("dispatch error: ActivitiException"), logs.lastError);
        assertTrue("ENGINE".equals(logs.lastKind), logs.lastKind);
    }

    @Test
    void duckbotDeliverSuccess_unknownProperty_namesGateway() {
        recorder.record(deadEndDecision(WFE, GATEWAY));
        doThrow(new ActivitiException(
                "Unknown property used in expression: ${route__61359ea8_x}"))
                .when(coordinator).deliverSuccess(eq(DISPATCH), anyMap(), anyMap());

        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(WFE);
        delegate.resumeDuckbotSuccessOrReportFailure(
                DISPATCH, AGENT, ref(), ctx, graphNodes(), graphEdges(), Map.of(), Map.of());

        org.mockito.ArgumentCaptor<String> message = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(coordinator).deliverFailure(
                eq(DISPATCH),
                eq(SandboxJobResponse.ERROR_SANDBOX_FAILED),
                message.capture(),
                eq(SandboxJobResponse.FailureKind.ENGINE.name()),
                isNull());
        String reported = message.getValue();
        assertTrue(reported.contains(GATEWAY), reported);
        assertTrue(reported.contains("no outgoing flow matched"), reported);
        assertFalse(reported.contains("duckbot boundary error: ActivitiException"), reported);
        assertTrue(logs.lastError.contains(GATEWAY), logs.lastError);
    }

    @Test
    void unknownProperty_withoutDecision_stillNamesGatewayFromRouteToken() {
        doThrow(new ActivitiException(
                "Unknown property used in expression: ${route__61359ea8 == 'payload-must-not-leak'}"))
                .when(coordinator).deliverSuccess(any(), any(), any());

        delegate.resumeSuccessOrReportFailure(
                DISPATCH, AGENT, ref(), graphNodes(), graphEdges(), Map.of(), Map.of());

        org.mockito.ArgumentCaptor<String> message = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(coordinator).deliverFailure(
                eq(DISPATCH),
                eq(SandboxJobResponse.ERROR_SANDBOX_FAILED),
                message.capture(),
                eq(SandboxJobResponse.FailureKind.ENGINE.name()),
                isNull());
        String reported = message.getValue();
        assertTrue(reported.contains(GATEWAY), reported);
        assertTrue(reported.contains("no outgoing flow matched"), reported);
        assertFalse(reported.contains("payload-must-not-leak"), reported);
        assertFalse(reported.contains("dispatch error: ActivitiException"), reported);
    }

    private static ProcessContextRef ref() {
        return ProcessContextRef.builder()
                .workflowExecutionId(WFE)
                .currentNodeRefId("ref-1")
                .envId("env-1")
                .build();
    }

    private static List<NodeDto> graphNodes() {
        return List.of(node(AGENT, "script"), node(GATEWAY, "exclusive.gateway"));
    }

    private static List<EdgeDto> graphEdges() {
        EdgeDto e = new EdgeDto();
        e.setId("e1");
        e.setSource(AGENT);
        e.setTarget(GATEWAY);
        return List.of(e);
    }

    private static NodeDto node(String id, String bpmnType) {
        TemplateDto t = new TemplateDto();
        t.setBpmnType(bpmnType);
        NodeDto n = new NodeDto();
        n.setId(id);
        n.setData(t);
        return n;
    }

    /** Dead-end without relying on {@code deadEnd()} in the miss check (selected sentinel). */
    private static GatewayDecision deadEndDecision(String wfeId, String gatewayId) {
        return new GatewayDecision(
                wfeId, gatewayId, "XOR",
                List.of(new BranchEvaluation("t1", "@x == 1", false, null)),
                "__dead_end__",
                false,
                true,
                System.currentTimeMillis());
    }

    private static <T> ObjectProvider<T> providerOf(T value) {
        return new ObjectProvider<>() {
            @Override
            public T getObject() {
                return value;
            }

            @Override
            public T getIfAvailable() {
                return value;
            }

            @Override
            public Stream<T> stream() {
                return value == null ? Stream.empty() : Stream.of(value);
            }
        };
    }

    private static final class RecordingRecorder implements GatewayDecisionRecorder {
        private final List<GatewayDecision> held = new ArrayList<>();

        @Override
        public void record(GatewayDecision decision) {
            held.add(decision);
        }

        @Override
        public GatewayDecision latest(String workflowExecutionId, String gatewayNodeId) {
            for (int i = held.size() - 1; i >= 0; i--) {
                GatewayDecision d = held.get(i);
                if (workflowExecutionId.equals(d.workflowExecutionId())
                        && gatewayNodeId.equals(d.gatewayNodeId())) {
                    return d;
                }
            }
            return null;
        }
    }

    private static final class CapturingLogCallback implements SandboxLogCallback {
        String lastError;
        String lastKind;

        @Override
        public void report(SandboxLogRecord record) {
            lastError = record.errorMessage();
            lastKind = record.failureKind();
        }

        @Override
        public void reportStarted(SandboxStartRecord record) {
            // unused
        }
    }
}
