package com.studioquack.sqs.sandbox.warmpool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A warm target persisted against a retired Node default (end-of-life / unpatched line) must come
 * back on the language's current default, while an operator-chosen image is left alone.
 */
class WarmTargetStoreRetiredDefaultTest {

    @TempDir
    Path tempDir;

    @Test
    void retired_node_defaults_move_to_the_current_default() throws Exception {
        List<WarmTargetEntry> loaded = load("""
                {"version":2,"entries":[
                  {"imageRef":"node:20-slim","language":"javascript","target":3,"caps":null},
                  {"imageRef":"node:22-slim","language":"typescript","target":2,"caps":null},
                  {"imageRef":"node:20-slim","language":null,"target":1,"caps":null}
                ]}""");

        assertEquals(SandboxLanguage.JAVASCRIPT.defaultImage(), loaded.get(0).imageRef());
        assertEquals(3, loaded.get(0).target());
        assertEquals(SandboxLanguage.TYPESCRIPT.defaultImage(), loaded.get(1).imageRef());
        assertEquals(SandboxLanguage.JAVASCRIPT.defaultImage(), loaded.get(2).imageRef());
    }

    @Test
    void operator_images_are_not_rewritten() throws Exception {
        List<WarmTargetEntry> loaded = load("""
                {"version":2,"entries":[
                  {"imageRef":"node:20-slim","language":"python","target":1,"caps":null},
                  {"imageRef":"registry.example/acme/node:20-slim","language":"javascript","target":1,"caps":null}
                ]}""");

        assertEquals("node:20-slim", loaded.get(0).imageRef());
        assertEquals("registry.example/acme/node:20-slim", loaded.get(1).imageRef());
    }

    private List<WarmTargetEntry> load(String json) throws Exception {
        Path file = tempDir.resolve("warm-targets.json");
        Files.writeString(file, json);
        return new WarmTargetStore(file.toString(), new ObjectMapper()).load();
    }
}
