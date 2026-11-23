package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Proxy env injection into the sandbox. Two properties matter beyond "the vars are set": a job must
 * not be able to unset them, and the helper channel must bypass the proxy.
 */
class SandboxEgressProxyEnvTest {

    private static final String PROXY = "http://10.20.0.9:3128";

    private static PodmanSandboxExecution execution(Environment env) {
        return new PodmanSandboxExecution(
                // A real runtime, not a mock: inline-mocking ContainerRuntime twice in one
                // surefire fork fails JVM retransformation (NestHost attribute) on JDK 25.
                new ContainerRuntime(new com.fasterxml.jackson.databind.ObjectMapper(), "auto", false),
                mock(SecretResolver.class),
                mock(Redactor.class),
                mock(HelperRequestRouter.class),
                mock(WarmPool.class),
                env,
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry());
    }

    private static MockEnvironment envWithProxy(String url, String noProxy) {
        MockEnvironment env = new MockEnvironment();
        if (url != null) {
            env.setProperty("sqs.sandbox.egress-proxy.url", url);
        }
        if (noProxy != null) {
            env.setProperty("sqs.sandbox.egress-proxy.no-proxy", noProxy);
        }
        return env;
    }

    /** Invokes the private env assembler — the single source shared by the cold and warm paths. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> containerEnv(PodmanSandboxExecution exec, SandboxJobRequest job) {
        // Added a trailing WarmSlot param (the broker's socket path source); null exercises the
        // cold path, which is what every case in this file is testing.
        return (Map<String, String>) ReflectionTestUtils.invokeMethod(
                exec, "containerEnv", job, null, SandboxLanguage.PYTHON, null);
    }

    private static SandboxJobRequest job(Map<String, String> jobEnv) {
        return SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .env(jobEnv)
                .build();
    }

    @Test
    @DisplayName("no proxy configured: nothing is injected")
    void noProxyConfigured_injectsNothing() {
        Map<String, String> env = containerEnv(execution(envWithProxy(null, null)), job(Map.of()));

        assertFalse(env.containsKey("HTTPS_PROXY"));
        assertFalse(env.containsKey("https_proxy"));
        assertFalse(env.containsKey("NO_PROXY"));
    }

    @Test
    @DisplayName("proxy configured: both upper- and lower-case variants are injected")
    void proxyConfigured_injectsBothCases() {
        Map<String, String> env = containerEnv(execution(envWithProxy(PROXY, null)), job(Map.of()));

        // Lower-case for curl/pip/requests, upper-case for SDKs and JVM/Go toolchains.
        assertEquals(PROXY, env.get("HTTP_PROXY"));
        assertEquals(PROXY, env.get("http_proxy"));
        assertEquals(PROXY, env.get("HTTPS_PROXY"));
        assertEquals(PROXY, env.get("https_proxy"));
    }

    @Test
    @DisplayName("loopback always bypasses the proxy")
    void noProxyList_alwaysBypassesLoopback() {
        Map<String, String> env = containerEnv(execution(envWithProxy(PROXY, null)), job(Map.of()));

        String noProxy = env.get("NO_PROXY");
        assertEquals(noProxy, env.get("no_proxy"), "both cases must carry the same list");
        assertTrue(noProxy.contains("127.0.0.1"));
        assertTrue(noProxy.contains("localhost"));
        assertFalse(noProxy.contains("host.containers.internal"),
                "retired helper alias must not appear in NO_PROXY");
    }

    @Test
    @DisplayName("operator-configured bypass hosts are appended to the built-ins")
    void noProxyList_appendsConfiguredHosts() {
        Map<String, String> env = containerEnv(
                execution(envWithProxy(PROXY, " registry.internal , 10.7.0.0/16 ,")), job(Map.of()));

        String noProxy = env.get("NO_PROXY");
        assertTrue(noProxy.contains("localhost"), "built-ins survive");
        assertTrue(noProxy.contains("registry.internal"), "entries are trimmed, not taken verbatim");
        assertTrue(noProxy.contains("10.7.0.0/16"));
        assertFalse(noProxy.contains(",,"), "the trailing empty entry must be dropped");
    }

    @Test
    @DisplayName("a job cannot unset or redirect the proxy via its own env vars")
    void jobSuppliedEnv_cannotOverrideProxy() {
        Map<String, String> hostile = Map.of(
                "HTTPS_PROXY", "",
                "https_proxy", "http://attacker.example:8080",
                "NO_PROXY", "*");

        Map<String, String> env = containerEnv(execution(envWithProxy(PROXY, null)), job(hostile));

        // The proxy is a platform control, so injection runs AFTER the job's env is merged.
        assertEquals(PROXY, env.get("HTTPS_PROXY"));
        assertEquals(PROXY, env.get("https_proxy"));
        assertFalse("*".equals(env.get("NO_PROXY")), "a job must not be able to widen the bypass list");
    }
}
