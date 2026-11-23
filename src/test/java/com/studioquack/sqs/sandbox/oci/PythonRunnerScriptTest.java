package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real {@code sq_python_runner.py} + client with the host's {@code python3}: a warm-slot job
 * must see the same files and secret values as the cold bootstrap gives it.
 */
@DisabledOnOs(OS.WINDOWS)
class PythonRunnerScriptTest {

    @TempDir
    Path tmp;

    private Process runner;

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.destroyForcibly();
        }
    }

    @Test
    void jobGetsItsFilesAndDecodedSecrets() throws Exception {
        Assumptions.assumeTrue(pythonAvailable(), "python3 required for the Python runner smoke test");
        Path runnerPy = copyResource("sq_python_runner.py");
        Path clientPy = copyResource("sq_python_runner_client.py");
        Path workdir = Files.createDirectories(tmp.resolve("sandbox"));
        Path sock = tmp.resolve("r.sock");
        Path ready = tmp.resolve("r.ready");
        ProcessBuilder pb = new ProcessBuilder("python3", "-u", runnerPy.toString()).redirectErrorStream(true);
        pb.environment().put("SQ_RUNNER_SOCKET", sock.toString());
        pb.environment().put("SQ_RUNNER_READY", ready.toString());
        pb.environment().put("SQ_RUNNER_WORKDIR", workdir.toString());
        runner = pb.start();
        for (int i = 0; i < 100 && !Files.exists(ready); i++) {
            Thread.sleep(50);
        }
        assertTrue(Files.exists(ready), "python runner never became ready");

        String script = String.join("\n",
                "import os",
                "print('secret=' + os.environ['API_KEY'])",
                "print('multi=' + repr(os.environ['MULTI']))",
                "print('file=' + open('files/input.txt').read())");
        byte[] payload = ContainerBootstrap.stdinPayload(script, "API_KEY='s3cr'\\''et'\nMULTI='a\nb'\n",
                List.of(new ContainerBootstrap.SandboxFile("input.txt", "hello file".getBytes(StandardCharsets.UTF_8))));
        Process client = new ProcessBuilder("python3", clientPy.toString(), sock.toString(), "main.py").start();
        client.getOutputStream().write(payload);
        client.getOutputStream().close();
        String out = new String(client.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String err = new String(client.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(client.waitFor(20, TimeUnit.SECONDS));

        assertEquals(0, client.exitValue(), out + err);
        assertTrue(out.contains("secret=s3cr'et\n"), "quotes decoded like `. .env`: " + out + err);
        assertTrue(out.contains("multi='a\\nb'"), "multi-line secret kept whole: " + out + err);
        assertTrue(out.contains("file=hello file"), "attached file materialised: " + out + err);
    }

    private Path copyResource(String name) throws Exception {
        Path target = tmp.resolve(name);
        try (InputStream in = getClass().getResourceAsStream("/sandbox/helpers/" + name)) {
            Files.copy(in, target);
        }
        return target;
    }

    private static boolean pythonAvailable() {
        try {
            Process p = new ProcessBuilder("python3", "-c", "import sys").start();
            return p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
