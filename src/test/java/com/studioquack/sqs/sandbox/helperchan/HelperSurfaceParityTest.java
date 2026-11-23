package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Python and JavaScript / TypeScript helpers expose the same method surface. Each runtime loads
 * its real helper resource and lists what {@code studioquack} and {@code studioquack.llm} actually
 * define, so a method added to one helper and forgotten in the other fails here.
 */
class HelperSurfaceParityTest {

    static final List<String> SHARED = List.of(
            "getExecutionContext", "setExecutionContext", "updateExecutionContext", "setExecutionContextValue",
            "updateInput", "setOutput", "returnExecutionContext", "getWorkflowExecutionId", "getCurrentNode",
            "getCurrentNodeId", "getEdgeTransformListForCurrentNode", "getInput", "getInputValuesByPortValue",
            "setDefaultInputValuesByPortValue", "getOutput", "getSpecListForNodeOutput", "getTemplate",
            "getPortByValue", "getPortValueByName", "getPortSchemaByName", "getParameters",
            "getParameterByShortCode", "getParameterValueByShortCode", "getConnections",
            "findConnectionByPortValue", "getConnectionDetailListByPortValue",
            "findFirstConnectionNodeIdByPortValue", "getDucktapeConfigurations", "getConnectionDetailParameters",
            "getConnectionDetailParameterByShortCode", "getSecret", "fullContext", "currentNode",
            "currentNodeId", "workflowExecutionId", "parameters", "connections", "inputs", "input",
            "execute", "connect", "engineIP", "llmLocal", "returnResult", "fail", "escalate", "sendSignal",
            "sendMessage", "metric", "log", "isCancelled", "onCancel", "handleCancel", "isCancelPath",
            "setRequestContext");

    static final List<String> LLM = List.of(
            "completion", "chat", "local", "models", "load", "unload", "download", "downloadStatus");

    @Test
    void pythonHelper_definesTheSharedSurface(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(available("python3", "-c", "print(1)"), "python3 required");
        String out = run(tmp, "surface.py", List.of("python3"), loadHelper("python.py") + """

                import json as _j
                print("SURFACE " + _j.dumps({
                    "helper": [n for n in dir(studioquack) if not n.startswith("_")],
                    "llm": [n for n in dir(studioquack.llm) if not n.startswith("_")],
                }))
                """);
        assertCovers(out, "python", List.of("execute", "emit_partial"));
    }

    @Test
    void nodeHelper_definesTheSharedSurface(@TempDir Path tmp) throws Exception {
        Assumptions.assumeTrue(available("node", "-e", "process.exit(0)"), "node required");
        String out = run(tmp, "surface.js", List.of("node"), loadHelper("node.js") + """

                console.log("SURFACE " + JSON.stringify({
                  helper: Object.keys(studioquack),
                  llm: Object.keys(studioquack.llm),
                }));
                """);
        assertCovers(out, "node", List.of("executeStream", "emitPartial"));
    }

    private static void assertCovers(String out, String lang, List<String> idiom) throws IOException {
        int at = out.indexOf("SURFACE ");
        assertTrue(at >= 0, lang + " helper printed no surface: " + out);
        JsonNode surface = new ObjectMapper().readTree(out.substring(at + "SURFACE ".length()).trim());
        Set<String> helper = names(surface.get("helper"));
        Set<String> llm = names(surface.get("llm"));
        Set<String> missing = new TreeSet<>();
        List<String> expected = new ArrayList<>(SHARED);
        expected.addAll(idiom);
        for (String name : expected) {
            if (!helper.contains(name)) {
                missing.add(name);
            }
        }
        for (String name : LLM) {
            if (!llm.contains(name)) {
                missing.add("llm." + name);
            }
        }
        assertTrue(missing.isEmpty(), lang + " helper is missing " + missing);
    }

    private static Set<String> names(JsonNode array) {
        Set<String> out = new TreeSet<>();
        if (array != null) {
            array.forEach(n -> out.add(n.asText()));
        }
        return out;
    }

    private static String run(Path tmp, String file, List<String> cmd, String script) throws Exception {
        Path path = tmp.resolve(file);
        Files.writeString(path, script.replace("__SQ_CONTEXT__", ""), StandardCharsets.UTF_8);
        List<String> argv = new ArrayList<>(cmd);
        argv.add(path.toString());
        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.environment().remove("SQ_HELPER_SOCKET");
        pb.environment().remove("SQVM_HELPER_URL");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(30, TimeUnit.SECONDS), "helper surface probe timed out: " + out);
        return out;
    }

    private static String loadHelper(String name) throws IOException {
        try (InputStream in = HelperSurfaceParityTest.class.getResourceAsStream("/sandbox/helpers/" + name)) {
            if (in == null) {
                throw new IOException("missing classpath resource /sandbox/helpers/" + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean available(String... argv) {
        try {
            Process p = new ProcessBuilder(argv).start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
