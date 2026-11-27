package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Designer → deploy contract for {@link BoundaryEventConfig}: blank errorCode is a catch-all,
 * message correlationKey is optional, type-specific keys survive Jackson whether they arrive
 * nested under {@code config} or flattened beside id/type, and {@code INGRESS_NODE} message
 * boundaries validate {@code ingressNodeBinding.templateId} + bound {@code $webhookKeyRef}.
 */
class BoundaryEventConfigTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void errorBlankCodeIsValidCatchAll() {
        BoundaryEventConfig cfg = BoundaryEventConfig.error("");
        cfg.setId("boundary-error-a");

        assertTrue(cfg.validate().isValid());
        assertEquals("", cfg.getErrorCode());
    }

    @Test
    void errorMissingCodeIsValidCatchAll() {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setId("boundary-error-b");
        cfg.setType(BoundaryEventConfig.BoundaryEventType.ERROR);
        cfg.setInterrupting(true);

        assertTrue(cfg.validate().isValid());
    }

    @Test
    void errorNonInterruptingIsInvalid() {
        BoundaryEventConfig cfg = BoundaryEventConfig.error("ERR_TIMEOUT").nonInterrupting();
        cfg.setId("boundary-error-c");
        cfg.setInterrupting(false);

        assertFalse(cfg.validate().isValid());
    }

    @Test
    void messageWithoutCorrelationKeyIsValid() {
        BoundaryEventConfig missing = BoundaryEventConfig.message("paymentReceived", null);
        missing.setId("boundary-message-a");
        assertTrue(missing.validate().isValid());
        assertEquals(BoundaryEventConfig.MESSAGE_SOURCE_GENERIC, missing.getMessageSource());
        assertFalse(missing.isIngressNodeSource());

        BoundaryEventConfig blank = BoundaryEventConfig.message("paymentReceived", "");
        blank.setId("boundary-message-blank");
        assertTrue(blank.validate().isValid());
    }

    @Test
    void messageBlankNameIsInvalid() {
        BoundaryEventConfig cfg = BoundaryEventConfig.message("  ", "ignored");
        cfg.setId("boundary-message-b");

        assertFalse(cfg.validate().isValid());
    }

    @Test
    void jacksonRoundTripsAllFiveTypes() throws Exception {
        assertRoundTrip(BoundaryEventConfig.timer("PT5M").withId("boundary-timer-a").withLabel("Timer"),
                "timerType", "duration", "timerValue", "PT5M");
        assertRoundTrip(BoundaryEventConfig.message("pay", "info-only").withId("boundary-message-a"),
                "messageName", "pay", "correlationKey", "info-only");
        assertRoundTrip(BoundaryEventConfig.signal("go").withId("boundary-signal-a"),
                "signalName", "go");
        assertRoundTrip(BoundaryEventConfig.escalation("LATE").withId("boundary-escalation-a"),
                "escalationCode", "LATE");
        assertRoundTrip(BoundaryEventConfig.error("ERR_TIMEOUT").withId("boundary-error-a"),
                "errorCode", "ERR_TIMEOUT");
    }

    @Test
    void feShapedFlattenedPlusNestedConfigReadsViaAccessors() throws Exception {
        String feJson = """
                {
                  "id": "boundary-error-z",
                  "label": "Error Boundary Event",
                  "type": "ERROR",
                  "interrupting": true,
                  "errorCode": "ERR_TIMEOUT",
                  "errorMessage": "static label",
                  "config": { "errorCode": "ERR_TIMEOUT", "errorMessage": "static label" }
                }
                """;

        BoundaryEventConfig back = JSON.readValue(feJson, BoundaryEventConfig.class);

        assertEquals("boundary-error-z", back.getId());
        assertEquals(BoundaryEventConfig.BoundaryEventType.ERROR, back.getType());
        assertTrue(back.isInterrupting());
        assertEquals("ERR_TIMEOUT", back.getErrorCode());
        assertEquals("static label", back.getErrorMessage());
    }

    @Test
    void feShapedTimerFlattenedPlusNestedConfigReadsViaAccessors() throws Exception {
        String feJson = """
                {
                  "id": "boundary-timer-z",
                  "type": "TIMER",
                  "interrupting": false,
                  "timerType": "cycle",
                  "timerValue": "R3/PT10M",
                  "config": { "timerType": "cycle", "timerValue": "R3/PT10M" }
                }
                """;

        BoundaryEventConfig back = JSON.readValue(feJson, BoundaryEventConfig.class);

        assertEquals("cycle", back.getTimerType());
        assertEquals("R3/PT10M", back.getTimerValue());
        assertTrue(back.validate().isValid());
    }

    @Test
    void deployShapedNodeAndEdgeKeepConfigAndBoundaryEventId() throws Exception {
        TemplateDto template = new TemplateDto();
        template.setBoundaryEventConfigs(List.of(
                BoundaryEventConfig.error("E42").withId("be_err").withLabel("Fail"),
                BoundaryEventConfig.timer("PT5M").withId("be_timer").withLabel("Timeout")
        ));
        NodeDto node = new NodeDto();
        node.setId("n1");
        node.setData(template);

        EdgeDataDto edgeData = new EdgeDataDto();
        edgeData.setBoundaryEventId("be_err");
        EdgeDto edge = new EdgeDto();
        edge.setId("e1");
        edge.setSource("n1");
        edge.setTarget("handler");
        edge.setData(edgeData);

        JsonNode nodes = JSON.valueToTree(List.of(node));
        JsonNode edges = JSON.valueToTree(List.of(edge));

        JsonNode configs = nodes.get(0).get("data").get("boundaryEventConfigs");
        assertEquals("E42", configs.get(0).get("config").get("errorCode").asText());
        assertEquals("duration", configs.get(1).get("config").get("timerType").asText());
        assertEquals("PT5M", configs.get(1).get("config").get("timerValue").asText());
        assertEquals("be_err", edges.get(0).get("data").get("boundaryEventId").asText());
    }

    @Test
    void ingressNodeMessageBoundary_validatesTemplateAndWebhookKeyRef() {
        BoundaryEventConfig cfg = new BoundaryEventConfig();
        cfg.setId("boundary_hook_cancel");
        cfg.setType(BoundaryEventConfig.BoundaryEventType.MESSAGE);
        cfg.putConfig(BoundaryEventConfig.CONFIG_MESSAGE_SOURCE, BoundaryEventConfig.MESSAGE_SOURCE_INGRESS_NODE);

        assertTrue(cfg.isIngressNodeSource());
        assertFalse(cfg.validate().isValid(), "missing ingressNodeBinding must fail");

        Map<String, Object> binding = new LinkedHashMap<>();
        binding.put("templateId", "tpl-123");
        cfg.putConfig(BoundaryEventConfig.CONFIG_INGRESS_NODE_BINDING, binding);
        assertFalse(cfg.validate().isValid(), "missing $webhookKeyRef must fail");

        binding.put("configurationParameters", List.of(
                Map.of("shortCode", "operation", "value", "listen"),
                Map.of("shortCode", "webhook_key", "value", WebhookKeyRefHelper.placeholder("whk_0123456789abcdef01234567"))));
        assertTrue(cfg.validate().isValid(), "complete ingress binding must validate even without explicit messageName");
        assertNotNull(cfg.getIngressNodeBinding());
    }

    @Test
    void jsonDeserialization_preservesIngressNodeBinding() throws Exception {
        String json = """
                {
                  "id": "b_ing_1",
                  "label": "Inbound Cancel",
                  "type": "MESSAGE",
                  "interrupting": false,
                  "config": {
                    "correlationKey": "${customerId}",
                    "messageSource": "INGRESS_NODE",
                    "ingressNodeBinding": {
                      "templateId": "tpl-abc",
                      "listenParamShortCode": "operation",
                      "listenParamValue": "listen",
                      "configurationParameters": [
                        { "shortCode": "webhook_key", "value": { "$webhookKeyRef": "whk_aaaabbbbccccddddeeeeffff" } }
                      ]
                    }
                  }
                }
                """;
        BoundaryEventConfig cfg = JSON.readValue(json, BoundaryEventConfig.class);
        assertTrue(cfg.isIngressNodeSource());
        assertFalse(cfg.isInterrupting());
        assertEquals("tpl-abc", cfg.getIngressNodeBinding().get("templateId"));
        assertTrue(cfg.validate().isValid());
    }

    private static void assertRoundTrip(BoundaryEventConfig original, String... configPairs) throws Exception {
        BoundaryEventConfig back = JSON.readValue(JSON.writeValueAsString(original), BoundaryEventConfig.class);
        assertEquals(original.getId(), back.getId());
        assertEquals(original.getLabel(), back.getLabel());
        assertEquals(original.getType(), back.getType());
        assertEquals(original.isInterrupting(), back.isInterrupting());
        Map<String, Object> config = back.getConfig();
        for (int i = 0; i < configPairs.length; i += 2) {
            assertEquals(configPairs[i + 1], String.valueOf(config.get(configPairs[i])));
        }
    }
}
