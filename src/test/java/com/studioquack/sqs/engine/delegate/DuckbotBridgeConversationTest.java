package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.AgenticInfo;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PromptEditSupersede;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.conversation.ConversationStore;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.status.ConversationOutputCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * The Duckbot bridge owns conversation history on the client. Runs against the real store so the
 * append/read round trip is exercised: the start node must see the thread, not just the latest
 * line, and reading it from the CLIENT store is what removes the per-node remote round trip.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-duckbot-conv-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true"
})
@ActiveProfiles("base")
class DuckbotBridgeConversationTest {

    @Autowired
    private DuckbotBridge bridge;
    @Autowired
    private ConversationStore conversationStore;
    @Autowired
    private SubProcessCompletionRegistry subProcessRegistry;
    @MockBean
    private ConversationOutputCallback conversationOutputCallback;

    private static NodeDto duckbotNode(String id, String nodeType, String category) {
        TemplateDto template = new TemplateDto();
        template.setCategory(category);
        template.setNodeType(nodeType);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private static ExecutionContext ctxFor(NodeDto node, String conversationId, String prompt) {
        return ctxFor(node, conversationId, prompt, true);
    }

    private static ExecutionContext ctxFor(NodeDto node, String conversationId, String prompt,
                                           boolean includeFullHistory) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setCurrentNode(node);
        ctx.setUserId("user-1");
        ctx.setWorkflowExecutionId("run-1");
        AgenticInfo ai = new AgenticInfo();
        ai.setConversationId(conversationId);
        ai.setAgentMessageKey("mk");
        ai.setUserPrompt(prompt);
        ai.setIncludeFullHistory(includeFullHistory);
        ctx.setAgenticInfo(ai);
        return ctx;
    }

    /**
     * A start node wired to one downstream target with an authored transform, mirroring how the
     * designer emits a Duckbot edge ({@code msg} on the source becomes {@code prompt} on the target).
     */
    private static NodeDto startNodeWithEdge(String conversationCategory) {
        NodeDto node = duckbotNode("start-1", "start", conversationCategory);
        EdgeTransformDto edge = new EdgeTransformDto();
        edge.setSourceNodeId("start-1");
        edge.setTargetNodeId("agent-1");
        edge.setSourcePortShortCode("output");
        edge.setTargetPortShortCode("input");
        edge.setSourcePath("msg");
        edge.setTargetPath("prompt");
        node.setEdgeTransformListForCurrentNode(new ArrayList<>(List.of(edge)));
        return node;
    }

    @Test
    @DisplayName("the start node records the user's turn in the client store")
    void start_appendsTheUserTurn() {
        ExecutionContext ctx = ctxFor(startNodeWithEdge("conversion"), "conv-append", "hello there");

        bridge.applyStart(ctx);

        var history = conversationStore.history("conv-append");
        assertEquals(1, history.size());
        assertEquals("user", history.get(0).getRole());
        assertEquals("hello there", history.get(0).getContent());
    }

    @Test
    @DisplayName("the start node publishes the whole thread, not just the latest line")
    void start_publishesTheThread() throws Exception {
        // The reason a conversation node exists. Reading it from the client store is what removes
        // the per-node remote round trip.
        conversationStore.append("conv-thread", "user-1", "mk", null, "user", "first question", null);
        conversationStore.append("conv-thread", "user-1", "mk", null, "assistant", "first answer", null);
        ExecutionContext ctx = ctxFor(startNodeWithEdge("conversion"), "conv-thread", "follow up");

        bridge.applyStart(ctx);

        List<InOutValueDto> published = ctx.getCurrentNode().getOutputList();
        assertTrue(published != null && !published.isEmpty(), "the bridge published nothing");
        // The msg port is declared STRING: the thread ships as a JSON array string of
        // {role, content} so the port value honors its declared type in every mode, and
        // the consuming node parses its own port value back into messages.
        Object wire = published.get(0).getValue();
        assertTrue(wire instanceof Map<?, ?>, "source-shaped wire payload: " + wire);
        Object msg = ((Map<?, ?>) wire).get("msg");
        assertTrue(msg instanceof String, "msg honors its declared string port type: " + msg);
        List<?> thread = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue((String) msg, List.class);
        assertEquals(3, thread.size(), "whole thread: " + msg);
        String rendered = String.valueOf(msg);
        assertTrue(rendered.contains("first question"), rendered);
        assertTrue(rendered.contains("first answer"), rendered);
        assertTrue(rendered.contains("follow up"), "the current turn is part of the thread: " + rendered);
    }

    private static ExecutionContext editRerun(String conversationId, String newPrompt, String previousPrompt,
                                              int turnOrdinal, boolean dropLaterUserTurns) {
        ExecutionContext ctx = ctxFor(startNodeWithEdge("conversion"), conversationId, newPrompt);
        ctx.getAgenticInfo().setPromptEdit(new PromptEditSupersede(previousPrompt, turnOrdinal, dropLaterUserTurns));
        return ctx;
    }

    private List<String> contents(String conversationId) {
        return conversationStore.history(conversationId).stream().map(m -> m.getContent()).toList();
    }

    @Test
    @DisplayName("agent-only edit: the edited turn and everything after it leave the thread")
    void start_promptEdit_agentOnlyTruncatesTheTail() {
        conversationStore.append("conv-edit-solo", "user-1", "mk", null, "user", "hello", null);
        conversationStore.append("conv-edit-solo", "user-1", "mk", null, "assistant", "Hello!", null);
        conversationStore.append("conv-edit-solo", "user-1", "mk", null, "user", "second", null);
        conversationStore.append("conv-edit-solo", "user-1", "mk", null, "assistant", "second answer", null);

        bridge.applyStart(editRerun("conv-edit-solo", "what date is today", "hello", 1, true));

        assertEquals(List.of("what date is today"), contents("conv-edit-solo"));
    }

    @Test
    @DisplayName("room edit: later answers leave, later people's turns stay")
    void start_promptEdit_roomKeepsLaterPeople() {
        conversationStore.append("conv-edit-room", "user-1", "mk", null, "user", "first", null);
        conversationStore.append("conv-edit-room", "user-1", "mk", null, "user", "hi", null);
        conversationStore.append("conv-edit-room", "user-1", "mk", null, "assistant", "hi answer", null);
        conversationStore.append("conv-edit-room", "user-2", "mk", null, "user", "colleague", null);
        conversationStore.append("conv-edit-room", "user-1", "mk", null, "assistant", "colleague answer", null);

        bridge.applyStart(editRerun("conv-edit-room", "[@Bot](mention:agent:mk) revised",
                "[@Bot](mention:agent:mk) hi", 2, false));

        assertEquals(List.of("first", "colleague", "revised"), contents("conv-edit-room"));
    }

    @Test
    @DisplayName("edit whose turn text does not match removes nothing")
    void start_promptEdit_driftRemovesNothing() {
        conversationStore.append("conv-edit-drift", "user-1", "mk", null, "user", "something else", null);
        conversationStore.append("conv-edit-drift", "user-1", "mk", null, "assistant", "answer", null);

        bridge.applyStart(editRerun("conv-edit-drift", "revised", "hello", 1, true));

        assertEquals(List.of("something else", "answer", "revised"), contents("conv-edit-drift"));
    }

    @Test
    @DisplayName("private mode: @agent turn publishes only post-mention text, not prior history")
    void start_privateMode_isTurnOnly() {
        conversationStore.append("conv-mention", "user-1", "mk", null, "user", "earlier to someone else", null);
        conversationStore.append("conv-mention", "user-1", "mk", null, "assistant", "earlier answer", null);
        String prompt = "[@Tax Bot](mention:agent:mk-tax) what is VAT?";
        ExecutionContext ctx = ctxFor(startNodeWithEdge("conversion"), "conv-mention", prompt, false);

        bridge.applyStart(ctx);

        List<InOutValueDto> published = ctx.getCurrentNode().getOutputList();
        assertTrue(published != null && !published.isEmpty(), "the bridge published nothing");
        String rendered = String.valueOf(published.get(0).getValue());
        assertTrue(rendered.contains("what is VAT?"), rendered);
        assertTrue(!rendered.contains("earlier to someone else"), rendered);
        assertTrue(!rendered.contains("earlier answer"), rendered);
        assertTrue(!rendered.contains("mention:agent"), rendered);
        assertEquals("what is VAT?", DuckbotBridge.stripLeadingAgentAddress(prompt));
    }

    @Test
    @DisplayName("full-history mode still publishes the thread when toggle is on")
    void start_fullHistoryMode_publishesThread() {
        conversationStore.append("conv-full", "user-1", "mk", null, "user", "first question", null);
        conversationStore.append("conv-full", "user-1", "mk", null, "assistant", "first answer", null);
        ExecutionContext ctx = ctxFor(startNodeWithEdge("conversion"), "conv-full",
                "[@Bot](mention:agent:mk) follow up", true);

        bridge.applyStart(ctx);

        String rendered = String.valueOf(ctx.getCurrentNode().getOutputList().get(0).getValue());
        assertTrue(rendered.contains("first question"), rendered);
        assertTrue(rendered.contains("first answer"), rendered);
        assertTrue(rendered.contains("follow up"), rendered);
        assertTrue(!rendered.contains("mention:agent"), rendered);
    }

    @Test
    @DisplayName("a non-conversation start node does not touch history")
    void start_appCategory_doesNotRecordHistory() {
        // App pages carry flowInputs, not a chat turn; recording them would invent a conversation.
        NodeDto node = startNodeWithEdge("application");
        ExecutionContext ctx = ctxFor(node, "conv-app", null);
        ctx.getAgenticInfo().setFlowInputs(java.util.Map.of("field", "value"));

        bridge.applyStart(ctx);

        assertTrue(conversationStore.history("conv-app").isEmpty());
    }

    @Test
    @DisplayName("the end node stores the answer locally AND pushes it to the open chat")
    void end_storesLocallyAndNotifies() {
        // Local append is the durable record; the callback is live delivery so the user's chat updates.
        NodeDto end = duckbotNode("end-1", "end", "conversion");
        InOutValueDto in = new InOutValueDto();
        in.setValue("the final answer");
        end.setInputList(new ArrayList<>(List.of(in)));
        ExecutionContext ctx = ctxFor(end, "conv-end", null);

        bridge.applyEnd(ctx);

        var history = conversationStore.history("conv-end");
        assertEquals(1, history.size());
        assertEquals("assistant", history.get(0).getRole());
        assertEquals("the final answer", history.get(0).getContent());
        verify(conversationOutputCallback).report(any());
    }

    @Test
    @DisplayName("a thoughts-port delivery persists as thinking sections on the answer row, never as the answer")
    void end_thoughtsPortDeliveryPersistsAsThinkingSections() {
        NodeDto end = duckbotNode("end-1", "end", "conversion");
        InOutValueDto thoughts = new InOutValueDto();
        // Function-port deliveries keep the SOURCE port in portValue (HelperExecuteService).
        // Function-port deliveries are discriminated by the stamped SOURCE PORT TYPE
        // (graph metadata), never by the port's name.
        thoughts.setSourcePortType("FUNCTION");
        thoughts.setNodeId("agent-1");
        thoughts.setSourceNodeLabel("Tax Bot");
        thoughts.setValue(Map.of("thoughts", "<thinking>plan the lookup</thinking>"));
        InOutValueDto answer = new InOutValueDto();
        answer.setPortValue("main_output");
        answer.setValue("the final answer");
        end.setInputList(new ArrayList<>(List.of(thoughts, answer)));
        ExecutionContext ctx = ctxFor(end, "conv-thoughts", null);

        bridge.applyEnd(ctx);

        var history = conversationStore.history("conv-thoughts");
        assertEquals(1, history.size(), "one row persists — the thinking rides ON the answer row");
        assertEquals("the final answer", history.get(0).getContent());
        // The row is the record of the turn: the reasoning bubble must survive a reload and the
        // next turn's stream-slot reuse, so it persists with the answer it belongs to.
        var sections = conversationStore.thinkingSectionsOf(history.get(0));
        assertEquals(1, sections.size());
        assertEquals("agent-1", sections.get(0).nodeId());
        assertEquals("Tax Bot", sections.get(0).nodeLabel());
        assertEquals("plan the lookup", sections.get(0).text(), "tags stripped, text intact");
        // …and the same sections ride the report so the backend row + SSE update carry them too.
        var captor = org.mockito.ArgumentCaptor.forClass(
                com.studioquack.sqs.status.ConversationOutputCallback.ConversationOutput.class);
        verify(conversationOutputCallback).report(captor.capture());
        var reported = captor.getValue().thinkingSections();
        assertEquals(1, reported.size());
        assertEquals("plan the lookup", reported.get(0).text());
        assertEquals("agent-1", reported.get(0).nodeId());
    }

    @Test
    @DisplayName("reasoning parked by a function-port child is recovered onto the top-level answer row")
    void end_functionPortChildThinkingIsRecoveredOntoTheAnswerRow() {
        String conversationId = "conv-gap";
        // The function-port child runs under its own wfeId; the registry links it to the top-level run.
        subProcessRegistry.register(new SubProcessCompletionRegistry.SubPiLink(
                "child-run", "parent-run", "parent-exec", "env", "bpmn", "epoch"));

        // 1) The function-port child renders the reasoning but has no answer row to persist it on.
        NodeDto childEnd = duckbotNode("end-1", "end", "conversion");
        InOutValueDto thoughts = new InOutValueDto();
        thoughts.setSourcePortType("FUNCTION");
        thoughts.setNodeId("agent-1");
        thoughts.setSourceNodeLabel("Tax Bot");
        thoughts.setValue(Map.of("thoughts", "<thinking>plan the lookup</thinking>"));
        childEnd.setInputList(new ArrayList<>(List.of(thoughts)));
        ExecutionContext childCtx = ctxFor(childEnd, conversationId, null);
        childCtx.setWorkflowExecutionId("child-run");
        bridge.applyEnd(childCtx);
        assertTrue(conversationStore.history(conversationId).isEmpty(),
                "a function-port child persists no row of its own");

        // 2) The top-level answer arrives with no thinking in its inputList — the parked reasoning
        //    is recovered onto it so the bubble survives a reload.
        NodeDto topEnd = duckbotNode("end-1", "end", "conversion");
        InOutValueDto answer = new InOutValueDto();
        answer.setPortValue("main_output");
        answer.setNodeId("agent-1");
        answer.setValue("the final answer");
        topEnd.setInputList(new ArrayList<>(List.of(answer)));
        ExecutionContext topCtx = ctxFor(topEnd, conversationId, null);
        topCtx.setWorkflowExecutionId("parent-run");
        bridge.applyEnd(topCtx);

        var history = conversationStore.history(conversationId);
        assertEquals(1, history.size());
        assertEquals("the final answer", history.get(0).getContent());
        var sections = conversationStore.thinkingSectionsOf(history.get(0));
        assertEquals(1, sections.size());
        assertEquals("agent-1", sections.get(0).nodeId());
        assertEquals("Tax Bot", sections.get(0).nodeLabel());
        assertEquals("plan the lookup", sections.get(0).text());
    }

    @Test
    @DisplayName("two sequential turns persist each assistant once with increasing order")
    void twoSequentialTurns_eachAnswerPersistedOnce() {
        String conversationId = "conv-two-turns";
        ExecutionContext start1 = ctxFor(startNodeWithEdge("conversion"), conversationId, "q1");
        start1.setWorkflowExecutionId("run-1");
        bridge.applyStart(start1);
        NodeDto end1 = duckbotNode("end-1", "end", "conversion");
        InOutValueDto a1 = new InOutValueDto();
        a1.setValue("answer-1");
        end1.setInputList(new ArrayList<>(List.of(a1)));
        ExecutionContext endCtx1 = ctxFor(end1, conversationId, null);
        endCtx1.setWorkflowExecutionId("run-1");
        bridge.applyEnd(endCtx1);

        ExecutionContext start2 = ctxFor(startNodeWithEdge("conversion"), conversationId, "q2");
        start2.setWorkflowExecutionId("run-2");
        bridge.applyStart(start2);
        NodeDto end2 = duckbotNode("end-1", "end", "conversion");
        InOutValueDto a2 = new InOutValueDto();
        a2.setValue("answer-2");
        end2.setInputList(new ArrayList<>(List.of(a2)));
        ExecutionContext endCtx2 = ctxFor(end2, conversationId, null);
        endCtx2.setWorkflowExecutionId("run-2");
        bridge.applyEnd(endCtx2);

        var history = conversationStore.history(conversationId);
        assertEquals(List.of("q1", "answer-1", "q2", "answer-2"),
                history.stream().map(m -> m.getContent()).toList());
        assertEquals(List.of("user", "assistant", "user", "assistant"),
                history.stream().map(m -> m.getRole()).toList());
        assertEquals(4, conversationStore.messageCount(conversationId));
        var reports = org.mockito.ArgumentCaptor.forClass(
                ConversationOutputCallback.ConversationOutput.class);
        verify(conversationOutputCallback, org.mockito.Mockito.times(2)).report(reports.capture());
        assertEquals("run-1", reports.getAllValues().get(0).workflowExecutionId());
        assertEquals(2, reports.getAllValues().get(0).msgOrder());
        assertEquals("answer-1", reports.getAllValues().get(0).message());
        assertEquals("run-2", reports.getAllValues().get(1).workflowExecutionId());
        assertEquals(4, reports.getAllValues().get(1).msgOrder());
        assertEquals("answer-2", reports.getAllValues().get(1).message());
    }

    @Test
    @DisplayName("a full turn leaves the thread in order")
    void fullTurn_isOrdered() {
        ExecutionContext start = ctxFor(startNodeWithEdge("conversion"), "conv-turn", "question");
        bridge.applyStart(start);

        NodeDto end = duckbotNode("end-1", "end", "conversion");
        InOutValueDto in = new InOutValueDto();
        in.setValue("answer");
        end.setInputList(new ArrayList<>(List.of(in)));
        bridge.applyEnd(ctxFor(end, "conv-turn", null));

        var history = conversationStore.history("conv-turn");
        assertEquals(List.of("question", "answer"),
                history.stream().map(m -> m.getContent()).toList());
        assertEquals(List.of("user", "assistant"),
                history.stream().map(m -> m.getRole()).toList());
    }
}
