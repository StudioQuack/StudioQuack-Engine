package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Guards the transport used by the emit-only SDKs. {@code node.js} and {@code bash.sh} carry no
 * transport code at all — their images have no Python, so the in-container broker cannot run there,
 * and neither SDK ever makes a call that needs a response. They print {@code sqEvent:::} markers
 * instead, and this drain is the only thing that turns those markers into job logs and metrics.
 */
class SandboxStdoutEventDrainTest {

    private HelperRequestRouter helper;
    private PodmanSandboxExecution sandbox;
    private String token;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        helper = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(new AiModelProviderHolder(), mapper), null);
        sandbox = new PodmanSandboxExecution(
                // A real runtime, not a mock: inline-mocking ContainerRuntime twice in one
                // surefire fork fails JVM retransformation (NestHost attribute) on JDK 25.
                new ContainerRuntime(mapper, "auto", false),
                mock(SecretResolver.class),
                mock(Redactor.class),
                helper,
                mock(WarmPool.class),
                new MockEnvironment(),
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new com.studioquack.sqs.runtime.DispatchCancellationRegistry());
        token = helper.register(HelperJob.builder("job-drain", "bash").build());
    }

    private HelperJob job() {
        return helper.lookup(token).orElseThrow();
    }

    @Test
    void logAndMetricMarkersLandInTheSameSinksTheSocketWritesTo() {
        sandbox.drainStdoutEvents(token, String.join("\n",
                "ordinary user output nobody should parse",
                "sqEvent:::{\"kind\":\"log\",\"level\":\"warn\",\"message\":\"disk nearly full\"}",
                "portValue:out:::result:done",
                "sqEvent:::{\"kind\":\"metric\",\"name\":\"latency_ms\",\"value\":12.5}",
                ""));

        assertEquals(1, job().logs().size(), "the log marker must reach the job's log sink");
        Map<String, Object> logged = job().logs().get(0);
        assertEquals("disk nearly full", logged.get("message"));
        assertEquals("warn", logged.get("level"));
        assertTrue(logged.containsKey("at"), "a timestamp is stamped on, as the socket path does");
        assertTrue(!logged.containsKey("kind"), "the marker discriminator is not part of the sink shape");

        assertEquals(1, job().metrics().size(), "the metric marker must reach the job's metric sink");
        assertEquals("latency_ms", job().metrics().get(0).get("name"));
        assertEquals(12.5, job().metrics().get(0).get("value"));
    }

    @Test
    void aLogMarkerWithoutALevelDefaultsTheSameWayTheSocketPathDoes() {
        sandbox.drainStdoutEvents(token, "sqEvent:::{\"kind\":\"log\",\"message\":\"plain\"}\n");

        assertEquals("info", job().logs().get(0).get("level"));
    }

    @Test
    void unknownAndMalformedMarkersAreIgnoredWithoutLosingTheGoodOnes() {
        sandbox.drainStdoutEvents(token, String.join("\n",
                "sqEvent:::{not json at all",
                // `partial` has no daemon-side sink yet; inventing one here would diverge from the
                // socket transport this drain must mirror, so it is dropped rather than guessed at.
                "sqEvent:::{\"kind\":\"partial\",\"chunk\":\"abc\"}",
                "sqEvent:::{\"kind\":\"log\",\"message\":\"survived\"}",
                ""));

        assertEquals(List.of("survived"), job().logs().stream().map(l -> l.get("message")).toList());
        assertTrue(job().metrics().isEmpty());
    }

    @Test
    void aFailMarkerRecordsTheSameTypedFailureTheSocketFailEndpointDoes() {
        // bash: sq_fail Error_BadInput "a and b must be numbers" → BpmnError(Error_BadInput)
        sandbox.drainStdoutEvents(token, String.join("\n",
                "sqEvent:::{\"kind\":\"fail\",\"errorCode\":\"Error_BadInput\",\"message\":\"a and b must be numbers\"}",
                "sqEvent:::{\"kind\":\"log\",\"message\":\"still logged\"}",
                ""));

        assertTrue(job().failure() != null && job().failure().hasCode(), "the marker must become the job's typed failure");
        assertEquals("Error_BadInput", job().failure().errorCode());
        assertEquals("a and b must be numbers", job().failure().message());
        assertEquals(List.of("still logged"), job().logs().stream().map(l -> l.get("message")).toList(),
                "a throw marker is consumed by the fail handler, never mis-filed as a log");
    }

    @Test
    void aFailMarkerWithReservedOrBlankCodeIsRefusedLikeTheSocketPath() {
        sandbox.drainStdoutEvents(token, String.join("\n",
                "sqEvent:::{\"kind\":\"fail\",\"errorCode\":\"SQ_SKIP\",\"message\":\"nope\"}",
                "sqEvent:::{\"kind\":\"fail\",\"errorCode\":\"\",\"message\":\"nope\"}",
                ""));

        assertTrue(job().failure() == null, "reserved / blank codes never become a typed failure");
        assertTrue(job().logs().isEmpty(), "a refused throw marker is not demoted to a log row either");
    }

    @Test
    void throwMarkersWithoutAnEngineAreConsumedNotMisfiledAsLogs() {
        // The router built above has no HelperBpmnEventService (no engine in this slice), so the
        // escalate/signal/message handlers answer 503 — the markers must still be swallowed by the
        // throw path rather than landing in the log/metric sinks.
        sandbox.drainStdoutEvents(token, String.join("\n",
                "sqEvent:::{\"kind\":\"escalate\",\"escalationCode\":\"SLA_BREACH\",\"message\":\"late\"}",
                "sqEvent:::{\"kind\":\"signal\",\"signalName\":\"Go\",\"scope\":\"global\"}",
                "sqEvent:::{\"kind\":\"message\",\"messageName\":\"Approved\",\"correlationKey\":\"\"}",
                ""));

        assertTrue(job().logs().isEmpty());
        assertTrue(job().metrics().isEmpty());
    }

    @Test
    void aRevokedOrUnknownTokenIsAQuietNoOpRatherThanACrash() {
        sandbox.drainStdoutEvents("h_not_a_real_token",
                "sqEvent:::{\"kind\":\"log\",\"message\":\"orphan\"}\n");
        sandbox.drainStdoutEvents(null, "sqEvent:::{\"kind\":\"log\",\"message\":\"orphan\"}\n");

        assertTrue(job().logs().isEmpty(), "events for an unknown job must not leak into another job");
    }
}
