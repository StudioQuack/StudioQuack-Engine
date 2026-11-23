package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The legacy {@code portValue:<port>:::result:<value>} stdout fallback parser. */
class StdoutMarkerParserTest {

    @Test
    void parses_markers_and_ignores_plain_lines() {
        String stdout = "some log line\n"
                + "portValue:out:::result:{\"a\":1}\n"
                + "portValue:status:::result:ok\n"
                + "another log\n";
        Map<String, String> out = StdoutMarkerParser.parse(stdout);
        assertEquals("{\"a\":1}", out.get("out"));
        assertEquals("ok", out.get("status"));
        assertEquals(2, out.size());
    }

    @Test
    void last_write_wins_per_port() {
        Map<String, String> out = StdoutMarkerParser.parse(
                "portValue:p:::result:first\nportValue:p:::result:second\n");
        assertEquals("second", out.get("p"));
    }

    @Test
    void tolerates_crlf_and_empty() {
        assertTrue(StdoutMarkerParser.parse("").isEmpty());
        assertTrue(StdoutMarkerParser.parse(null).isEmpty());
        Map<String, String> out = StdoutMarkerParser.parse("portValue:p:::result:v\r\n");
        assertEquals("v", out.get("p"));
    }

    @Test
    void parses_log_metric_and_message_events_in_order() {
        String stdout = "sqEvent:::{\"kind\":\"log\",\"level\":\"info\",\"message\":\"started\"}\n"
                + "sqEvent:::{\"kind\":\"metric\",\"name\":\"tokens\",\"value\":42}\n"
                + "sqEvent:::{\"kind\":\"log\",\"message\":\"quote: \\\"hi\\\"\\nnext: line\"}\n";

        List<Map<String, Object>> events = StdoutMarkerParser.events(stdout);

        assertEquals(3, events.size());
        assertEquals("log", events.get(0).get("kind"));
        assertEquals("started", events.get(0).get("message"));
        assertEquals("metric", events.get(1).get("kind"));
        assertEquals("tokens", events.get(1).get("name"));
        assertEquals(42, events.get(1).get("value"));
        assertEquals("log", events.get(2).get("kind"));
        assertEquals("quote: \"hi\"\nnext: line", events.get(2).get("message"));
    }

    @Test
    void skips_malformed_events_and_interleaves_with_port_markers() {
        String stdout = "ordinary output\n"
                + "portValue:first:::result:one\n"
                + "sqEvent:::{\"kind\":\"log\",\"message\":\"before: port\"}\n"
                + "sqEvent:::{not json}\n"
                + "sqEvent:::null\n"
                + "portValue:second:::result:two:with:colons\n"
                + "sqEvent:::{\"kind\":\"metric\",\"name\":\"done\",\"value\":1}\n";

        Map<String, String> ports = StdoutMarkerParser.parse(stdout);
        List<Map<String, Object>> events = StdoutMarkerParser.events(stdout);

        assertEquals("one", ports.get("first"));
        assertEquals("two:with:colons", ports.get("second"));
        assertEquals(2, ports.size());
        assertEquals(2, events.size());
        assertEquals("before: port", events.get(0).get("message"));
        assertEquals("done", events.get(1).get("name"));
    }

    @Test
    void events_tolerates_crlf_and_empty() {
        assertTrue(StdoutMarkerParser.events("").isEmpty());
        assertTrue(StdoutMarkerParser.events(null).isEmpty());

        List<Map<String, Object>> events = StdoutMarkerParser.events(
                "sqEvent:::{\"kind\":\"log\",\"message\":\"ok\"}\r\n");

        assertEquals(1, events.size());
        assertEquals("ok", events.get(0).get("message"));
    }
}
