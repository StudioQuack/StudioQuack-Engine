package com.studioquack.sqs.execution.transform;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Restricted transform helper denials + missing-path hard-fail. Prefers in-process {@code python3}
 * execution of the template; falls back to source-contains assertions when python is unavailable.
 */
class TransformHelperSecurityTest {

    @Test
    void template_contains_denial_surfaces() throws Exception {
        String tmpl = loadTemplate();
        assertTrue(tmpl.contains("denied: getSecret"));
        assertTrue(tmpl.contains("denied: studioquack.llm"));
        assertTrue(tmpl.contains("denied: studioquack.execute"));
        assertTrue(tmpl.contains("denied: studioquack.connect"));
        assertTrue(tmpl.contains("TransformMissingPathError"));
        assertTrue(tmpl.contains("SQ_EDGE_TRANSFORM_RESULT:"));
        assertTrue(tmpl.contains("__sq_edge_transform__"));
        // No full node file-materialise / secret enumeration surface.
        assertFalse(tmpl.contains("SandboxFileMaterializer"));
        assertFalse(tmpl.contains("listSecrets"));
        assertFalse(tmpl.contains("get_secret_value"));
    }

    @Test
    void python_denies_secret_llm_execute_connect() throws Exception {
        assumeTrue(pythonAvailable(), "python3 required for in-process denial check");
        assertDenied("getSecret('x')");
        assertDenied("studioquack.getSecret('x')");
        assertDenied("studioquack.llm.chat('hi')");
        assertDenied("studioquack.execute('other')");
        assertDenied("studioquack.connect('db', 5432)");
    }

    @Test
    void python_missing_path_raises_TransformMissingPathError() throws Exception {
        assumeTrue(pythonAvailable(), "python3 required for missing-path check");
        String out = runUserScript("target.x = source.missing.path", "{\"ok\": true}");
        assertTrue(out.contains("TransformMissingPathError") || out.contains("missing path"),
                "expected missing-path failure, got: " + out);
    }

    @Test
    void python_happy_path_emits_stdout_marker() throws Exception {
        assumeTrue(pythonAvailable(), "python3 required for happy-path check");
        String out = runUserScript("target.fullName = source.name", "{\"name\": \"Ada\"}");
        assertTrue(out.contains("SQ_EDGE_TRANSFORM_RESULT:"), out);
        assertTrue(out.contains("Ada"), out);
        assertFalse(out.contains("Traceback"), out);
    }

    private void assertDenied(String snippet) throws Exception {
        String out = runUserScript("target.x = 1\n" + snippet, "{\"a\": 1}");
        assertTrue(out.contains("TransformDeniedError") || out.contains("denied:"),
                "expected denial for " + snippet + ", got: " + out);
    }

    private String runUserScript(String userScript, String inboundJson) throws Exception {
        String tmpl = loadTemplate();
        String b64 = Base64.getEncoder().encodeToString(inboundJson.getBytes(StandardCharsets.UTF_8));
        String source = tmpl
                .replace("__SQ_TRANSFORM_INPUT__", b64)
                .replace("__SQ_TRANSFORM_USER_SCRIPT__", userScript);
        Path tmp = Files.createTempFile("sq-edge-transform-", ".py");
        Files.writeString(tmp, source, StandardCharsets.UTF_8);
        try {
            ProcessBuilder pb = new ProcessBuilder("python3", tmp.toAbsolutePath().toString());
            pb.redirectErrorStream(true);
            Process p = pb.start();
            boolean finished = p.waitFor(15, TimeUnit.SECONDS);
            String out = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))
                    .lines().collect(Collectors.joining("\n"));
            if (!finished) {
                p.destroyForcibly();
                return "TIMEOUT\n" + out;
            }
            return out + "\nexit=" + p.exitValue();
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static String loadTemplate() throws Exception {
        try (var in = TransformHelperSecurityTest.class.getResourceAsStream(
                "/sandbox/helpers/transform_python.py")) {
            assertTrue(in != null, "transform_python.py missing on test classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static boolean pythonAvailable() {
        try {
            Process p = new ProcessBuilder("python3", "-c", "print(1)").start();
            return p.waitFor(5, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
