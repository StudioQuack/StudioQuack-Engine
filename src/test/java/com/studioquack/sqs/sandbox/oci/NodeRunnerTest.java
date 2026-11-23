package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.deps.DependencyEntry;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Preload selection, the fresh-process rule and the start script of the Node runner. */
class NodeRunnerTest {

    @Test
    void preloadsFollowTheDependencyBucket() {
        DependencyManifestHolder holder = new DependencyManifestHolder();
        holder.apply(List.of(
                entry("javascript", "openai", true, null),
                entry("typescript", "zod", true, List.of("zod", "zod/v4")),
                entry("node", "openai", true, null),
                entry("javascript", "lodash", false, null),
                entry("python", "httpx", true, null)));

        // JavaScript and TypeScript share one node_modules, so both load every Node row (deduped).
        assertEquals("openai,zod,zod/v4", PreloadModules.csvFor(holder, SandboxLanguage.JAVASCRIPT));
        assertEquals("openai,zod,zod/v4", PreloadModules.csvFor(holder, SandboxLanguage.TYPESCRIPT));
        assertEquals("httpx", PreloadModules.csvFor(holder, SandboxLanguage.PYTHON));
        assertEquals("", PreloadModules.csvFor(holder, SandboxLanguage.BASH));
        assertEquals("", PreloadModules.csvFor(null, SandboxLanguage.PYTHON));
    }

    @Test
    void startupOnlyNodeVariablesNeedAFreshProcess() {
        assertFalse(NodeRunner.needsFreshProcess(Map.of("API_BASE", "x"), true));
        assertTrue(NodeRunner.needsFreshProcess(Map.of("NODE_OPTIONS", "--max-old-space-size=512"), false));
        assertTrue(NodeRunner.needsFreshProcess(Map.of("NODE_EXTRA_CA_CERTS", "/x.pem"), false));
        // NODE_ENV only matters once packages were loaded before the job's env existed.
        assertTrue(NodeRunner.needsFreshProcess(Map.of("NODE_ENV", "production"), true));
        assertFalse(NodeRunner.needsFreshProcess(Map.of("NODE_ENV", "production"), false));
        assertFalse(NodeRunner.needsFreshProcess(null, true));
    }

    @Test
    void startScriptUsesTheEntrypointFlagsAndSingleQuotesOnly() {
        String ts = NodeRunner.startScript(SandboxLanguage.TYPESCRIPT, "openai,zod", "/opt/sq/deps/node");
        assertTrue(ts.contains("node --no-warnings --no-deprecation --experimental-strip-types '"
                + NodeRunner.RUNNER_PATH + "'"), ts);
        assertTrue(ts.contains("NODE_PATH='/opt/sq/deps/node/node_modules'"), ts);
        assertTrue(ts.contains("SQ_RUNNER_PRELOADS='openai,zod'"), ts);
        assertTrue(ts.contains("mkfifo '" + NodeRunner.BASE + ".out' '" + NodeRunner.BASE + ".err'"), ts);
        // One `sh -c` argument on a Windows host: a double quote would split it there.
        assertFalse(ts.contains("\""), "no double quotes in the start script");

        String js = NodeRunner.startScript(SandboxLanguage.JAVASCRIPT, "", null);
        assertFalse(js.contains("--experimental-strip-types"), js);
        assertFalse(js.contains("NODE_PATH="), "no dep mount, no NODE_PATH: " + js);
        assertEquals(List.of("node", NodeRunner.CLIENT_PATH, NodeRunner.BASE, "main.ts"),
                NodeRunner.clientCommand(SandboxLanguage.TYPESCRIPT));
        assertTrue(NodeRunner.supports(SandboxLanguage.JAVASCRIPT));
        assertFalse(NodeRunner.supports(SandboxLanguage.PYTHON));
    }

    private static DependencyEntry entry(String language, String name, boolean preload, List<String> modules) {
        return new DependencyEntry(language, name, "", "installed", preload, modules, null, null, null);
    }
}
