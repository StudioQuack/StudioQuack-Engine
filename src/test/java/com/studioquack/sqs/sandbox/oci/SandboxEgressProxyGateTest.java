package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.egress.EgressProxyProbe;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * The probe gate on proxy injection ({@code applyEgressProxyEnv}). The decided degrade direction: a
 * configured-but-DOWN proxy skips injection (the run goes direct) and the skip is accounted on
 * {@code sqs.sandbox.egress.proxy.unreachable} — injecting a dead proxy would break the run, not
 * degrade it.
 */
class SandboxEgressProxyGateTest {

    private static final String PROXY = "http://10.20.0.9:3128";

    private static PodmanSandboxExecution execution(Environment env, EgressProxyProbe probe) {
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
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry(),
                null, // SandboxEgressManager — the readiness gate is the sibling test's concern
                probe);
    }

    private static MockEnvironment envWithProxy(String url) {
        MockEnvironment env = new MockEnvironment();
        if (url != null) {
            env.setProperty("sqs.sandbox.egress-proxy.url", url);
        }
        return env;
    }

    /** Invokes the private env assembler — the single source shared by the cold and warm paths. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> containerEnv(PodmanSandboxExecution exec, SandboxJobRequest job) {
        return (Map<String, String>) ReflectionTestUtils.invokeMethod(
                exec, "containerEnv", job, null, SandboxLanguage.PYTHON, null);
    }

    private static SandboxJobRequest job(Map<String, String> jobEnv) {
        return SandboxJobRequest.builder("job-1", SandboxLanguage.PYTHON, "print(1)")
                .env(jobEnv)
                .build();
    }

    private static void assertNoProxyVars(Map<String, String> env) {
        for (String key : new String[]{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy",
                "NO_PROXY", "no_proxy"}) {
            assertFalse(env.containsKey(key), key + " must not be injected while the proxy is DOWN");
        }
    }

    @Test
    @DisplayName("proxy DOWN: no proxy env is injected and the skip is counted, per dispatch")
    void proxyDown_skipsInjectionAndCountsPerDispatch() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> "Connection refused", registry);
        probe.refreshNow(); // first check lands: DOWN
        PodmanSandboxExecution exec = execution(envWithProxy(PROXY), probe);

        Map<String, String> env = containerEnv(exec, job(Map.of()));
        assertNoProxyVars(env);
        assertEquals(1.0, registry.get(EgressProxyProbe.UNREACHABLE_METRIC).counter().count());

        containerEnv(exec, job(Map.of()));
        assertEquals(2.0, registry.get(EgressProxyProbe.UNREACHABLE_METRIC).counter().count(),
                "each dispatch that went direct must be accounted — that count is the blast radius");
    }

    @Test
    @DisplayName("proxy UP: injection is byte-identical to the pre-gate behaviour")
    void proxyUp_injectsExactlyAsToday() {
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> null, new SimpleMeterRegistry());
        probe.refreshNow(); // UP

        Map<String, String> env = containerEnv(execution(envWithProxy(PROXY), probe), job(Map.of()));

        assertEquals(PROXY, env.get("HTTP_PROXY"));
        assertEquals(PROXY, env.get("http_proxy"));
        assertEquals(PROXY, env.get("HTTPS_PROXY"));
        assertEquals(PROXY, env.get("https_proxy"));
        assertEquals(env.get("NO_PROXY"), env.get("no_proxy"));
    }

    @Test
    @DisplayName("a job cannot influence the probe or force injection while DOWN")
    void proxyDown_jobEnvCannotForceInjectionOrSwayTheProbe() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> "Connection refused", registry);
        probe.refreshNow(); // DOWN
        PodmanSandboxExecution exec = execution(envWithProxy(PROXY), probe);
        // The hostile job claims platform config keys and probe-flavoured overrides. None of it
        // reaches the probe: the verdict is a host-side TCP connect from daemon config only.
        Map<String, String> hostile = Map.of(
                "SQS_EGRESS_PROXY_URL", "http://attacker.example:8080",
                "HTTPS_PROXY", "http://attacker.example:8080",
                "sqs.sandbox.egress-proxy.url", "http://attacker.example:8080");

        Map<String, String> env = containerEnv(exec, job(hostile));

        assertEquals(EgressProxyProbe.State.DOWN, probe.snapshot().state(),
                "the probe verdict must be unmoved by anything the job supplied");
        // The job's own vars pass through untouched — identical to running with no platform proxy
        // at all (advisory: env injection never forced anything). What the job can NOT do is
        // resurrect the PLATFORM proxy: it was skipped, and the skip was counted.
        assertEquals("http://attacker.example:8080", env.get("HTTPS_PROXY"));
        assertEquals(1.0, registry.get(EgressProxyProbe.UNREACHABLE_METRIC).counter().count());
    }

    @Test
    @DisplayName("not configured: no env, no counter — exactly the pre-proxy daemon")
    void notConfigured_noEnvNoCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EgressProxyProbe probe = EgressProxyProbe.forTest("",
                (host, port, timeout) -> null, registry);

        Map<String, String> env = containerEnv(execution(envWithProxy(null), probe), job(Map.of()));

        assertNoProxyVars(env);
        assertNull(registry.find(EgressProxyProbe.UNREACHABLE_METRIC).counter(),
                "an unconfigured proxy must not even register sightings of the outage counter");
    }

    @Test
    @DisplayName("DOWN→UP: injection resumes on the next dispatch and counting stops")
    void recovery_resumesInjection() {
        AtomicReference<String> failure = new AtomicReference<>("Connection refused");
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EgressProxyProbe probe = EgressProxyProbe.forTest(PROXY,
                (host, port, timeout) -> failure.get(), registry);
        PodmanSandboxExecution exec = execution(envWithProxy(PROXY), probe);

        probe.refreshNow(); // DOWN
        assertNoProxyVars(containerEnv(exec, job(Map.of())));
        assertEquals(1.0, registry.get(EgressProxyProbe.UNREACHABLE_METRIC).counter().count());

        failure.set(null);  // proxy is back
        probe.refreshNow(); // UP
        Map<String, String> env = containerEnv(exec, job(Map.of()));

        assertEquals(PROXY, env.get("HTTPS_PROXY"), "injection resumes immediately on recovery");
        assertEquals(1.0, registry.get(EgressProxyProbe.UNREACHABLE_METRIC).counter().count(),
                "healthy dispatches must not keep the outage counter moving");
    }
}
