package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.SandboxLanguage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real {@code sq_node_runner.js} + client through {@link NodeRunner#startScript} with the
 * host's {@code node} — the same process layout a warm container gets, minus Podman. Skipped where
 * no {@code node} is installed (and on Windows, which has no POSIX sh/FIFOs).
 */
@DisabledOnOs(OS.WINDOWS)
class NodeRunnerScriptTest {

    @TempDir
    Path tmp;

    private Path base;
    private Path workdir;
    private Process runner;

    @BeforeEach
    void setUp() throws IOException {
        Assumptions.assumeTrue(nodeAvailable(), "node required for the Node runner smoke test");
        base = tmp.resolve("rt/sq-node-runner");
        workdir = tmp.resolve("sandbox");
        Files.createDirectories(base.getParent());
        Files.createDirectories(workdir);
        // A dependency-cache package that counts how often it is loaded and prints while loading.
        Path pkg = tmp.resolve("deps/node_modules/sq-fake-dep");
        Files.createDirectories(pkg);
        Files.writeString(pkg.resolve("index.js"),
                "globalThis.__sqFakeDepLoads = (globalThis.__sqFakeDepLoads || 0) + 1;\n"
                        + "console.log('banner from sq-fake-dep');\n"
                        + "module.exports = { loadedAt: process.hrtime.bigint().toString() };\n");
    }

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.destroyForcibly();
        }
    }

    @Test
    void preloadedModuleIsReusedAndTheJobSeesItsEnvSecretsAndFiles() throws Exception {
        startRunner(SandboxLanguage.JAVASCRIPT, "sq-fake-dep");
        String script = String.join("\n",
                "const dep = require('sq-fake-dep');",
                "const fs = require('fs');",
                "console.log('loads=' + globalThis.__sqFakeDepLoads);",
                "console.log('env=' + process.env.SQ_JOB_VAR + ' secret=' + process.env.API_KEY);",
                "console.log('multi=' + JSON.stringify(process.env.MULTI));",
                "console.log('file=' + fs.readFileSync('files/input.txt', 'utf8'));",
                "console.log('main=' + (require.main === module) + ' cwd=' + process.cwd());",
                "console.error('to-stderr');",
                "process.exitCode = 3;");
        String envFile = "API_KEY='s3cr'\\''et'\nMULTI='line1\nline2'\n";
        Result r = runJob(script, envFile,
                List.of(new ContainerBootstrap.SandboxFile("input.txt", "hello file".getBytes(StandardCharsets.UTF_8))),
                "main.js", Map.of("SQ_JOB_VAR", "from-dispatch"));

        assertEquals(3, r.exitCode, r.toString());
        assertTrue(r.stdout.contains("loads=1"), "preloaded once, reused by the job: " + r);
        assertTrue(r.stdout.contains("env=from-dispatch secret=s3cr'et"), "env + decoded secret: " + r);
        assertTrue(r.stdout.contains("multi=\"line1\\nline2\""), "multi-line secret: " + r);
        assertTrue(r.stdout.contains("file=hello file"), "materialised file: " + r);
        assertTrue(r.stdout.contains("main=true cwd=" + workdir.toRealPath()), "runs as the main module: " + r);
        assertTrue(!r.stdout.contains("banner from sq-fake-dep"), "preload output stays out of the job: " + r);
        assertEquals("to-stderr\n", r.stderr, "stderr kept separate: " + r);
        assertTrue(Files.readString(Path.of(base + ".log")).contains("preload.ok sq-fake-dep"));
    }

    @Test
    void typescriptRunsWithTheEntrypointsTypeStripping() throws Exception {
        startRunner(SandboxLanguage.TYPESCRIPT, "");
        Result r = runJob("const n: number = 41;\nconsole.log('ts=' + (n + 1));\n", "", List.of(),
                "main.ts", Map.of());
        assertEquals(0, r.exitCode, r.toString());
        assertEquals("ts=42\n", r.stdout);
    }

    @Test
    void uncaughtErrorAndSignalReportTheColdPathStatus() throws Exception {
        startRunner(SandboxLanguage.JAVASCRIPT, "");
        Result r = runJob("throw new Error('boom');\n", "", List.of(), "main.js", Map.of());
        assertEquals(1, r.exitCode, r.toString());
        assertTrue(r.stderr.contains("Error: boom"), r.toString());

        runner.waitFor(5, TimeUnit.SECONDS);
        startRunner(SandboxLanguage.JAVASCRIPT, "");
        Result killed = runJob("process.kill(process.pid, 'SIGKILL');\n", "", List.of(), "main.js", Map.of());
        assertEquals(137, killed.exitCode, killed.toString());
    }

    @Test
    void refusedFileNameFailsTheJobLikeTheBootstrap() throws Exception {
        startRunner(SandboxLanguage.JAVASCRIPT, "");
        Result r = runJob("console.log('never');\n", "",
                List.of(new ContainerBootstrap.SandboxFile("..", new byte[0])), "main.js", Map.of());
        assertEquals(1, r.exitCode, r.toString());
        assertTrue(r.stderr.contains("refused slashed name"), r.toString());
    }

    private void startRunner(SandboxLanguage lang, String preloads) throws Exception {
        Files.deleteIfExists(Path.of(base + ".ready"));
        String script = NodeRunner.startScript(lang, preloads, tmp.resolve("deps").toString(),
                base.toString(), workdir.toString());
        runner = new ProcessBuilder("sh", "-c", script).redirectErrorStream(true).start();
        Path ready = Path.of(base + ".ready");
        for (int i = 0; i < 100 && !Files.exists(ready); i++) {
            Thread.sleep(50);
        }
        assertTrue(Files.exists(ready), "runner never became ready: "
                + (Files.exists(Path.of(base + ".log")) ? Files.readString(Path.of(base + ".log")) : "no log"));
    }

    private Result runJob(String script, String envFile, List<ContainerBootstrap.SandboxFile> files,
                          String scriptName, Map<String, String> env) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("node", base + "-client.js", base.toString(), scriptName);
        pb.environment().putAll(env);
        Process client = pb.start();
        client.getOutputStream().write(ContainerBootstrap.stdinPayload(script, envFile, files));
        client.getOutputStream().close();
        String out = new String(client.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String err = new String(client.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(client.waitFor(20, TimeUnit.SECONDS), "client did not finish");
        return new Result(client.exitValue(), out, err);
    }

    private record Result(int exitCode, String stdout, String stderr) {
    }

    private static boolean nodeAvailable() {
        try {
            Process p = new ProcessBuilder("node", "-e", "process.exit(0)").start();
            return p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
