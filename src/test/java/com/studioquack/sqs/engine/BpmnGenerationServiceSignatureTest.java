package com.studioquack.sqs.engine;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.engine.bpmn.FeelToJuelTranslator;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * {@code bpmn-signature:v9} — the deploy-reuse key must move when a BPMN-shaping configuration
 * parameter VALUE changes (a message event's {@code name}/{@code correlationKey}, a timer
 * expression, …). Up to v8 the node shape hashed only a presence boolean, so a value-only edit
 * reused the stale deployed XML (the tt4 manual-test failure.
 */
class BpmnGenerationServiceSignatureTest {

    private BpmnGenerationService service;

    @BeforeEach
    void setUp() {
        // computeGraphSignature touches no strategy and no FEEL translation; the boundary helper
        // is a void no-op for boundary-less graphs, so a mock stands in for it.
        service = new BpmnGenerationService(List.of(), new FeelToJuelTranslator(),
                mock(ActivitiBoundaryEventHelper.class));
    }

    @Test
    void configuration_parameter_value_change_moves_the_signature() {
        String withKeyA = service.computeGraphSignature("w", nodesWithMessageStart("tt4", "1"), List.of());
        String withKeyB = service.computeGraphSignature("w", nodesWithMessageStart("tt4", "2"), List.of());
        assertNotNull(withKeyA);
        assertNotNull(withKeyB);
        assertNotEquals(withKeyA, withKeyB,
                "a correlationKey value edit must bust the deploy-reuse cache");
    }

    @Test
    void configuration_parameter_removal_moves_the_signature() {
        String filtered = service.computeGraphSignature("w", nodesWithMessageStart("tt4", "1"), List.of());
        String unfiltered = service.computeGraphSignature("w", nodesWithMessageStart("tt4", null), List.of());
        assertNotEquals(filtered, unfiltered,
                "deleting the correlationKey must redeploy — the stale filtered XML must not be reused");
    }

    @Test
    void message_name_value_change_moves_the_signature() {
        String t3 = service.computeGraphSignature("w", nodesWithMessageStart("t3", null), List.of());
        String t4 = service.computeGraphSignature("w", nodesWithMessageStart("tt4", null), List.of());
        assertNotEquals(t3, t4, "a message-name edit must redeploy the subscription");
    }

    @Test
    void identical_graphs_share_the_signature_regardless_of_node_order() {
        List<NodeDto> nodes = nodesWithMessageStart("tt4", "1");
        List<NodeDto> shuffled = new ArrayList<>(nodes);
        java.util.Collections.reverse(shuffled);
        assertEquals(service.computeGraphSignature("w", nodes, List.of()),
                service.computeGraphSignature("w", shuffled, List.of()),
                "the hash stays order-independent");
    }

    private List<NodeDto> nodesWithMessageStart(String messageName, String correlationKey) {
        NodeDto start = node("startN", "message.start.event");
        addParam(start, "name", messageName);
        if (correlationKey != null) {
            addParam(start, "correlationKey", correlationKey);
        }
        NodeDto task = node("taskN", "user.task");
        NodeDto end = node("endN", "end.event");
        return new ArrayList<>(List.of(start, task, end));
    }

    private NodeDto node(String id, String bpmnType) {
        TemplateDto template = new TemplateDto();
        template.setBpmnType(bpmnType);
        template.setLabel(id);
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setType(bpmnType);
        node.setData(template);
        return node;
    }

    private void addParam(NodeDto node, String shortCode, String value) {
        ParameterDto param = new ParameterDto();
        param.setShortCode(shortCode);
        param.setValue(value);
        List<ParameterDto> existing = node.getData().getConfigurationParameters();
        List<ParameterDto> merged = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
        merged.add(param);
        node.getData().setConfigurationParameters(merged);
    }
}
