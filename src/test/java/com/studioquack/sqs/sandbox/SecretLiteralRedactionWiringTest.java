package com.studioquack.sqs.sandbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.aimodel.AiModelProviderProxy;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.sandbox.helperchan.HelperFailure;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.oci.PodmanSandboxExecution;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulLauncher;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import com.studioquack.sqs.sandbox.stateful.StatefulSandboxExecution;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmSlot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Redaction-wiring regression: the just-in-time-resolved secret literals carried on {@link
 * SandboxJobRequest#secretLiterals} must be scrubbed from every stream and diagnostic BEFORE it can
 * be logged or persisted, on every dispatch path. The wiring under test (traced from {@code
 * ScriptDispatchDelegate})
 */
class SecretLiteralRedactionWiringTest {

    /** Distinctive plaintext that no default redaction pattern matches (only the literal set catches it). */
    private static final String SECRET = "value-A-plaintext-x9q7";
    /** Control marker that must pass through untouched (not in the literal set). */
    private static final String CONTROL = "control-plain-marker";

    private final ObjectMapper mapper = new ObjectMapper();
    private HelperRequestRouter helper;

    @BeforeEach
    void setUp() {
        helper = new HelperRequestRouter(mapper,
                new AiModelProviderProxy(new AiModelProviderHolder(), mapper),
                null);
    }

    private PodmanSandboxExecution ephemeralSandbox(SecretResolver secretResolver) {
        return new PodmanSandboxExecution(
                // A real runtime, not a mock: inline-mocking ContainerRuntime fails JVM
                // retransformation (NestHost attribute) on JDK 25 — see SandboxStdoutEventDrainTest.
                new ContainerRuntime(mapper, "auto", false),
                secretResolver,
                new Redactor(), // the REAL redactor — it is part of the wiring under test
                helper,
                mock(WarmPool.class),
                new MockEnvironment(),
                mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(),
                new DispatchCancellationRegistry());
    }

    private static SandboxJobRequest jobWithLiterals(String jobId) {
        return SandboxJobRequest.builder(jobId, SandboxLanguage.PYTHON, "print(1)\n")
                .envId("env-1")
                .currentNodeId("nodeA")
                .workflowExecutionId("wfe-1")
                .secretLiterals(Set.of(SECRET))
                .build();
    }

    /**
     * Invokes the private response-builder the warm, cold AND entrypoint paths all return through
     * (single call site in {@code PodmanSandboxExecution.execute}). Reflection on the exact
     * signature is deliberate: if the method moves or changes shape, this fails loudly — which is
     * what a wiring guard is for.
     */
    private static SandboxJobResponse invokeEphemeralToResponse(PodmanSandboxExecution sandbox,
                                                                SandboxJobRequest job,
                                                                CliRunner.Result result,
                                                                String helperToken) throws Exception {
        Method m = PodmanSandboxExecution.class.getDeclaredMethod("toResponse",
                SandboxJobRequest.class, CliRunner.Result.class, String.class, WarmSlot.class, long.class);
        m.setAccessible(true);
        return (SandboxJobResponse) m.invoke(sandbox, job, result, helperToken, null, 12L);
    }

    @Test
    void ephemeral_success_streams_are_scrubbed_before_they_can_be_logged() throws Exception {
        PodmanSandboxExecution sandbox = ephemeralSandbox(mock(SecretResolver.class));
        CliRunner.Result containerOut = new CliRunner.Result(0,
                "connecting with password " + SECRET + "\nmarker " + CONTROL + "\n",
                "warn: token " + SECRET + " rejected; keeping " + CONTROL + "\n",
                false, 12L);

        SandboxJobResponse resp = invokeEphemeralToResponse(
                sandbox, jobWithLiterals("job-ep-ok"), containerOut, null);

        assertTrue(resp.ok(), "exit 0 maps to a success response");
        assertFalse(resp.stdout().contains(SECRET), "stdout must not carry the resolved literal");
        assertFalse(resp.stderr().contains(SECRET), "stderr must not carry the resolved literal");
        assertTrue(resp.stdout().contains("[redacted-secret]"), "the literal is scrubbed, not dropped silently");
        assertTrue(resp.stderr().contains("[redacted-secret]"), "the literal is scrubbed, not dropped silently");
        assertTrue(resp.stdout().contains(CONTROL), "a control value NOT in the literal set passes through");
        assertTrue(resp.stderr().contains(CONTROL), "a control value NOT in the literal set passes through");
    }

    @Test
    void ephemeral_typed_failure_message_is_scrubbed() throws Exception {
        PodmanSandboxExecution sandbox = ephemeralSandbox(mock(SecretResolver.class));
        // A script that raised studioquack.fail(code, msg) with the resolved secret inside the message.
        String token = helper.register(HelperJob.builder("job-ep-fail", "python").build());
        helper.lookup(token).orElseThrow().setFailure(
                new HelperFailure("BUSINESS_AUTH", "auth failed for " + SECRET, null));

        SandboxJobResponse resp = invokeEphemeralToResponse(
                sandbox, jobWithLiterals("job-ep-fail"),
                new CliRunner.Result(0, "", "", false, 3L), token);

        assertTrue(resp.failed(), "a typed SDK failure maps to a failure response");
        assertEquals("BUSINESS_AUTH", resp.errorCode());
        assertFalse(resp.errorMessage().contains(SECRET),
                "the typed-failure message must not carry the resolved literal");
        assertTrue(resp.errorMessage().contains("[redacted-secret]"));
    }

    @Test
    void ephemeral_setup_failure_diagnostics_are_scrubbed() {
        // A dispatch that fails BEFORE any container runs (here: the secret-resolution step itself
        // throws a message that happens to embed plaintext) must still redact its diagnostics with
        // the job's literal set — the catch path is a log surface too.
        SecretResolver throwing = mock(SecretResolver.class);
        when(throwing.resolveToEnv(any()))
                .thenThrow(new RuntimeException("setup boom " + CONTROL + " " + SECRET));
        // A runtime that reports ready so execute() reaches the secret-resolution step
        // deterministically (no host Podman needed; hand-written subclass, NOT a Mockito mock —
        // see the NestHost note above).
        ContainerRuntime readyRuntime = new ContainerRuntime(mapper, "auto", false) {
            @Override
            public boolean available() {
                return true;
            }

            @Override
            public Optional<String> isolationShortfall(IsolationTier jobTier) {
                return Optional.empty();
            }
        };
        PodmanSandboxExecution sandbox = new PodmanSandboxExecution(
                readyRuntime, throwing, new Redactor(), helper, mock(WarmPool.class),
                new MockEnvironment(), mock(DependencyManifestHolder.class),
                SandboxOwnership.unowned(), new DispatchCancellationRegistry());

        SandboxJobRequest job = SandboxJobRequest.builder("job-ep-setup", SandboxLanguage.PYTHON, "print(1)\n")
                .envId("env-1").currentNodeId("nodeA").workflowExecutionId("wfe-1")
                .secretsTree(Map.of("$secretRef", "sec_0000000000000000000000aa:v1"))
                .secretLiterals(Set.of(SECRET))
                .build();

        // Capture the daemon log for this class: the failure WARN is a persist surface and must
        // carry the same scrubbed diagnostics as the response (the raw exception text may embed a
        // just-resolved literal).
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(PodmanSandboxExecution.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        final SandboxJobResponse resp;
        try {
            resp = sandbox.execute(job);
        } finally {
            logger.detachAppender(appender);
        }

        assertTrue(resp.failed(), "a setup failure is a failure response, never a throw");
        assertFalse(resp.stderr().contains(SECRET), "setup diagnostics must not carry the literal");
        assertFalse(resp.errorMessage().contains(SECRET), "the classified message must not carry the literal");
        assertTrue(resp.stderr().contains("[redacted-secret]"), "the literal was scrubbed from diagnostics");
        assertTrue(resp.stderr().contains(CONTROL), "control text survives redaction");
        for (ch.qos.logback.classic.spi.ILoggingEvent event : appender.list) {
            assertFalse(event.getFormattedMessage().contains(SECRET),
                    "the daemon log must never carry the raw literal: " + event.getFormattedMessage());
        }
        assertTrue(appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains(CONTROL)),
                "the scrubbed diagnostics still reach the log (shape, not the value)");
    }

    @Test
    void stateful_streams_are_scrubbed_end_to_end() {
        // The in-instance path, driven through the public execute with a fake OCI CLI — the app's
        // captured stdout/stderr carry the resolved literal and must come back scrubbed.
        FakeOciCli cli = new FakeOciCli();
        StatefulInstanceManager instances = mock(StatefulInstanceManager.class);
        when(instances.brokerAlive(any())).thenReturn(false);
        when(instances.beginDispatch(any())).thenReturn(true);
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        StatefulSandboxExecution exec = new StatefulSandboxExecution(
                helper, instances, new StatefulLauncher(cli, props), new Redactor(), mapper, props,
                org.mockito.Mockito.mock(com.studioquack.sqs.runtime.SubProcessCompletionRegistry.class));
        StatefulInstance instance = new StatefulInstance("inst1", "env-1", "myenv/pg:1",
                "sq-stateful-inst1", "sq-stateful-vol-inst1", StatefulCaps.defaults(), true);
        instance.setContainerId("cid-live-1");
        instance.setStatus(StatefulStatus.RUNNING);
        cli.responder = argv -> new OciCli.Result(0,
                "app echo: " + SECRET + " keep " + CONTROL,
                "app err: " + SECRET,
                false);

        SandboxJobRequest job = SandboxJobRequest.builder("job-st", SandboxLanguage.PYTHON, "print(1)\n")
                .envId("env-1").currentNodeId("n1").workflowExecutionId("wfe-1")
                .ociImageRef("myenv/pg:1").stateful(true)
                .secretLiterals(Set.of(SECRET))
                .build();
        SandboxJobResponse resp = exec.execute(job, instance);

        assertFalse(resp.stdout().contains(SECRET), "stateful stdout must not carry the literal");
        assertFalse(resp.stderr().contains(SECRET), "stateful stderr must not carry the literal");
        assertTrue(resp.stdout().contains("[redacted-secret]"), "the literal is scrubbed on the stateful path");
        assertTrue(resp.stdout().contains(CONTROL), "control text survives on the stateful path");
    }

    @Test
    void stateful_dispatch_failure_diagnostics_are_scrubbed() {
        // The stateful catch path (the launcher call itself throws — e.g. the exec transport dies
        // mid-request): the exception text may embed plaintext, so both the response stderr AND
        // the daemon log must carry only the scrubbed form.
        FakeOciCli cli = new FakeOciCli();
        StatefulInstanceManager instances = mock(StatefulInstanceManager.class);
        when(instances.brokerAlive(any())).thenReturn(false);
        when(instances.beginDispatch(any())).thenReturn(true);
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        StatefulSandboxExecution exec = new StatefulSandboxExecution(
                helper, instances, new StatefulLauncher(cli, props), new Redactor(), mapper, props,
                org.mockito.Mockito.mock(com.studioquack.sqs.runtime.SubProcessCompletionRegistry.class));
        StatefulInstance instance = new StatefulInstance("inst1", "env-1", "myenv/pg:1",
                "sq-stateful-inst1", "sq-stateful-vol-inst1", StatefulCaps.defaults(), true);
        instance.setContainerId("cid-live-1");
        instance.setStatus(StatefulStatus.RUNNING);
        cli.responder = argv -> {
            throw new RuntimeException("dial boom " + CONTROL + " " + SECRET);
        };

        SandboxJobRequest job = SandboxJobRequest.builder("job-st-fail", SandboxLanguage.PYTHON, "print(1)\n")
                .envId("env-1").currentNodeId("n1").workflowExecutionId("wfe-1")
                .ociImageRef("myenv/pg:1").stateful(true)
                .secretLiterals(Set.of(SECRET))
                .build();

        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(StatefulSandboxExecution.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        final SandboxJobResponse resp;
        try {
            resp = exec.execute(job, instance);
        } finally {
            logger.detachAppender(appender);
        }

        assertTrue(resp.failed(), "a launcher failure is a failure response, never a throw");
        assertFalse(resp.stderr().contains(SECRET), "stateful diagnostics must not carry the literal");
        assertTrue(resp.stderr().contains("[redacted-secret]"), "the literal was scrubbed from diagnostics");
        assertTrue(resp.stderr().contains(CONTROL), "control text survives redaction");
        for (ch.qos.logback.classic.spi.ILoggingEvent event : appender.list) {
            assertFalse(event.getFormattedMessage().contains(SECRET),
                    "the daemon log must never carry the raw literal: " + event.getFormattedMessage());
        }
    }

    @Test
    void redactor_literal_contract_including_short_resolved_secrets() {
        Redactor redactor = new Redactor();

        // A resolved literal in the set is scrubbed verbatim; the control passes through.
        assertEquals("pw=[redacted-secret]; keep " + CONTROL,
                redactor.redact("pw=" + SECRET + "; keep " + CONTROL, Set.of(SECRET)));

        // A resolved literal is scrubbed regardless of length (PIN / "ab"). Blank is still skipped.
        assertEquals("code [redacted-secret] stays", redactor.redact("code abc stays", Set.of("abc")));
        assertEquals("x=[redacted-secret]", redactor.redact("x=z", Set.of("z")));
        assertEquals("keep", redactor.redact("keep", Set.of("")));

        // Null/empty literal sets degrade to pattern-only redaction (never null, never a throw).
        assertEquals("plain " + CONTROL, redactor.redact("plain " + CONTROL, Set.of()));
        assertEquals("plain " + CONTROL, redactor.redact("plain " + CONTROL, null));
    }
}
