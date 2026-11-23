package com.studioquack.sqs.engine.delegate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.status.SandboxLogCallback;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.engine.delegate.DelegateExecution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A fired ERROR boundary is sealed for its handler AND logged as a row of the boundary's own id, so
 * the failed call's Monitoring record can show what the boundary port published (Error tab). Other
 * boundary types are sealed but not logged.
 */
class SqBoundaryEventDelegateTest {

    private static final String RUN_ID = "run-1";
    private static final String HOST = "host-node";
    private static final String HANDLER = "handler-node";
    private static final String BOUNDARY_ELEMENT = "err_host-node_0";

    private final ExecutionContextStore contextStore = mock(ExecutionContextStore.class);
    private final ActivitiBoundaryEventHelper boundaryHelper = mock(ActivitiBoundaryEventHelper.class);
    private final SandboxLogCallback sandboxLogCallback = mock(SandboxLogCallback.class);
    private final SqBoundaryEventDelegate delegate = new SqBoundaryEventDelegate(
            contextStore, boundaryHelper, new ObjectMapper(), sandboxLogCallback, new Redactor());

    private final ProcessContextRef ref = new ProcessContextRef();
    private ExecutionContext ctx;

    @BeforeEach
    void setUp() {
        NodeDto host = new NodeDto();
        host.setId(HOST);
        NodeDto handler = new NodeDto();
        handler.setId(HANDLER);
        ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(RUN_ID);
        ctx.setNodes(List.of(host, handler));
        when(contextStore.loadByRef(ref)).thenReturn(Optional.of(ctx));
    }

    @Test
    void error_boundary_publication_is_sealed_and_logged_under_the_boundary_id() {
        BoundaryEventConfig cfg = BoundaryEventConfig.error("E_TIMEOUT").withId("boundary-error-1").withLabel("On failure");
        wireFired(cfg);

        delegate.notify(execution());

        verify(contextStore).saveOutputs(any(), anyList());
        ArgumentCaptor<SandboxLogCallback.SandboxLogRecord> captor =
                ArgumentCaptor.forClass(SandboxLogCallback.SandboxLogRecord.class);
        verify(sandboxLogCallback).report(captor.capture());
        SandboxLogCallback.SandboxLogRecord record = captor.getValue();
        assertEquals(RUN_ID, record.workflowExecutionId());
        assertEquals("boundary-error-1", record.nodeId());
        assertEquals("On failure", record.nodeName());
        assertEquals("COMPLETED", record.status());
        assertEquals("NONE", record.failureKind());

        List<?> output = assertInstanceOf(List.class, record.output());
        InOutValueDto row = assertInstanceOf(InOutValueDto.class, output.getFirst());
        assertEquals(HOST, row.getNodeId());
        assertEquals("boundary-error-1", row.getPortValue());
        assertEquals(HANDLER, row.getTargetNodeId());
        Map<?, ?> payload = assertInstanceOf(Map.class, row.getValue());
        assertEquals("SANDBOX_FAILED", payload.get("errorCode"));
        assertEquals("script exited 1", payload.get("errorMessage"));
        assertEquals(HOST, payload.get("errorSource"));
        assertEquals("SCRIPT", payload.get("errorKind"));
        assertEquals("1", payload.get("exitCode"));
    }

    @Test
    void non_error_boundary_is_sealed_but_not_logged() {
        BoundaryEventConfig cfg = BoundaryEventConfig.timer("PT30M").withId("boundary-timer-1");
        wireFired(cfg);

        delegate.notify(execution());

        verify(contextStore).saveOutputs(any(), anyList());
        verify(sandboxLogCallback, never()).report(any());
    }

    private void wireFired(BoundaryEventConfig cfg) {
        EdgeDataDto data = new EdgeDataDto();
        data.setBoundaryEventId(cfg.getId());
        EdgeDto edge = new EdgeDto();
        edge.setSource(HOST);
        edge.setTarget(HANDLER);
        edge.setData(data);
        ctx.setEdges(List.of(edge));
        when(boundaryHelper.resolveFired(ctx.getNodes(), BOUNDARY_ELEMENT))
                .thenReturn(new ActivitiBoundaryEventHelper.FiredBoundary(ctx.getNodes().getFirst(), cfg));
    }

    private DelegateExecution execution() {
        SequenceFlow flow = new SequenceFlow();
        flow.setSourceRef(BOUNDARY_ELEMENT);
        DelegateExecution execution = mock(DelegateExecution.class);
        when(execution.getCurrentFlowElement()).thenReturn(flow);
        when(execution.getProcessInstanceBusinessKey()).thenReturn(RUN_ID);
        when(execution.getVariable(EngineVariableNames.STUDIOQUACK_REF)).thenReturn(ref);
        when(execution.getVariable(EngineVariableNames.ERROR_CODE)).thenReturn("SANDBOX_FAILED");
        when(execution.getVariable(EngineVariableNames.ERROR_MESSAGE)).thenReturn("script exited 1");
        when(execution.getVariable(EngineVariableNames.ERROR_KIND)).thenReturn("SCRIPT");
        when(execution.getVariable(EngineVariableNames.ERROR_EXIT_CODE)).thenReturn("1");
        return execution;
    }
}
