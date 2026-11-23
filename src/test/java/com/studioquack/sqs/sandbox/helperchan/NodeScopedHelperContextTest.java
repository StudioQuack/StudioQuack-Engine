package com.studioquack.sqs.sandbox.helperchan;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeScopedHelperContextTest {

    @Test
    void drops_other_nodes_secret_handles_keeps_current_node() {
        Map<String, Object> current = new LinkedHashMap<>();
        current.put("id", "nodeA");
        current.put("db", "value-A-plaintext");

        Map<String, Object> nodeB = new LinkedHashMap<>();
        nodeB.put("id", "nodeB");
        nodeB.put("language", "python");
        nodeB.put("$secretRef", "sec_0000000000000000000000bb:v1");

        Map<String, Object> ec = new LinkedHashMap<>();
        ec.put("currentNodeId", "nodeA");
        ec.put("currentNode", current);
        ec.put("nodes", List.of(Map.of("id", "nodeA"), nodeB));

        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) NodeScopedHelperContext.project(ec);
        assertEquals("value-A-plaintext", ((Map<?, ?>) out.get("currentNode")).get("db"));
        String jsonish = out.toString();
        assertFalse(jsonish.contains("sec_0000000000000000000000bb"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) out.get("nodes");
        assertEquals("nodeB", nodes.get(1).get("id"));
        assertEquals("python", nodes.get(1).get("language"));
        assertFalse(nodes.get(1).containsKey("$secretRef"));
        assertTrue(ec.toString().contains("sec_0000000000000000000000bb"),
                "source map must stay intact for execute()");
    }
}
