package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Designer → → deploy contract for {@link BoundaryEventConfig}: blank errorCode is a catch-all,
 * message correlationKey is optional, and type-specific keys survive Jackson whether they arrive
 * nested under {@code config} or flattened beside id/type.
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
