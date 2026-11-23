package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobStore;
import com.studioquack.sqs.sandbox.stateful.OciImageDigestReporter;
import com.studioquack.sqs.sandbox.redactor.Redactor;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulRunArgs;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import com.studioquack.sqs.sandbox.stateful.shell.StatefulShellSessionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Proves the four stateful lifecycle handlers end-to-end on the daemon side against a deterministic
 * {@link FakeOciCli}: each parses the FIXED payload, drives the manager, and ships the FIXED result
 * JSON; a malformed payload becomes a {@code failed} outcome (never a throw), exactly like the
 * existing {@code ReloadConfigCommandHandlerTest} contract.
 */
class StatefulInstanceCommandHandlersTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private StatefulInstanceManager manager;
    private OciImageDigestReporter digestReporter;
    private LoadOciImageCommandHandler load;
    private PullOciImageCommandHandler pull;
    private DeleteOciImageCommandHandler delete;
    private StartStatefulInstanceCommandHandler start;
    private StopStatefulInstanceCommandHandler stop;
    private StatusStatefulInstanceCommandHandler status;
    private StatefulRuntimeProperties props;
    private StatefulShellSessionManager shellSessions;

    @TempDir
    Path blobDir;

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        cli.responder = StatefulInstanceCommandHandlersTest::respond;
        props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10);
        digestReporter = mock(OciImageDigestReporter.class);
        when(digestReporter.report(any(), any(), any(), any())).thenReturn(true);
        shellSessions = new StatefulShellSessionManager(new Redactor(), (e, s, f) -> { },
                argv -> { throw new UnsupportedOperationException("shell open not used in lifecycle tests"); });
        load = new LoadOciImageCommandHandler(mapper, manager);
        pull = new PullOciImageCommandHandler(mapper, manager, digestReporter);
        delete = new DeleteOciImageCommandHandler(mapper, manager);
        start = new StartStatefulInstanceCommandHandler(mapper, manager);
        stop = new StopStatefulInstanceCommandHandler(mapper, manager, shellSessions);
        status = new StatusStatefulInstanceCommandHandler(mapper, manager);
    }

    @AfterEach
    void tearDown() {
        if (shellSessions != null) {
            shellSessions.shutdown();
        }
    }

    private static OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        return switch (sub) {
            case "run" -> new OciCli.Result(0, "cid-777\n", "", false);
            case "load" -> new OciCli.Result(0, "Loaded image: r sha256:abcabcabcabc", "", false);
            case "pull" -> new OciCli.Result(0, "Trying to pull … sha256:abcabcabcabc\n", "", false);
            case "rmi" -> new OciCli.Result(0, "Untagged: r\n", "", false);
            case "volume" -> argv.contains("exists")
                    ? new OciCli.Result(1, "", "", false) : new OciCli.Result(0, "", "", false);
            case "inspect" -> new OciCli.Result(0,
                    "[{\"Id\":\"cid-777\",\"State\":{\"Status\":\"running\"}}]", "", false);
            default -> new OciCli.Result(0, "", "", false);
        };
    }

    private static DaemonCommand cmd(String type, String payload) {
        return new DaemonCommand("c-" + type, type, "2026-07-13T00:00:00Z", payload);
    }

    @Test
    void types_match_the_fixed_wire_contract() {
        assertEquals("load-oci-image", load.type());
        assertEquals("pull-oci-image", pull.type());
        assertEquals("delete-oci-image", delete.type());
        assertEquals("start-stateful-instance", start.type());
        assertEquals("stop-stateful-instance", stop.type());
        assertEquals("status-stateful-instance", status.type());
    }

    @Test
    void pull_reports_active_with_digest_and_calls_digest_reporter() throws Exception {
        DaemonCommandOutcome outcome = pull.handle(cmd("pull-oci-image",
                "{\"envId\":\"env-1\",\"ref\":\"library/postgres:16\",\"displayName\":\"Postgres\"}"));

        assertTrue(outcome.succeeded(), () -> "pull error: " + outcome.error());
        JsonNode out = mapper.readTree(outcome.output());
        assertEquals("ACTIVE", out.get("status").asText());
        assertEquals("sha256:abcabcabcabc", out.get("digest").asText());
        // The digest is reported back to the remote registry (DOWNLOADING → ACTIVE).
        verify(digestReporter).report(eq("env-1"), eq("library/postgres:16"), eq("sha256:abcabcabcabc"), eq("ACTIVE"));
    }

    @Test
    void delete_removes_image_and_reports_stopped() throws Exception {
        DaemonCommandOutcome outcome = delete.handle(cmd("delete-oci-image",
                "{\"envId\":\"env-1\",\"ref\":\"library/postgres:16\"}"));

        assertTrue(outcome.succeeded(), () -> "delete error: " + outcome.error());
        JsonNode out = mapper.readTree(outcome.output());
        assertEquals("STOPPED", out.get("status").asText());
        assertEquals("library/postgres:16", out.get("ref").asText());
    }

    @Test
    void start_returns_running_with_instance_and_container_ids_and_starts_uncapped() throws Exception {
        DaemonCommandOutcome outcome = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/pg:1\",\"stateful\":true,"
                        + "\"caps\":{\"cpu\":2,\"ramMb\":1024,\"diskMb\":4096,\"gpu\":1}}"));

        assertTrue(outcome.succeeded(), () -> "start error: " + outcome.error());
        JsonNode out = mapper.readTree(outcome.output());
        assertEquals("RUNNING", out.get("status").asText());
        assertEquals("cid-777", out.get("containerId").asText());
        assertNotNull(out.get("instanceId").asText());

        List<String> run = cli.firstCallWith("run");
        assertNotNull(run);
        for (String flag : List.of("--memory", "--memory-swap", "--cpus", "--storage-opt", "--gpus")) {
            assertFalse(run.contains(flag), flag + " must not be emitted on stateful instances");
        }
    }

    @Test
    void start_ignores_a_legacy_reverse_object() throws Exception {
        // Per-node network posture was removed: a stale payload that still names a reverse port
        // starts the plain instance. An out-of-range port used to fail the command; it is no
        // longer read at all.
        DaemonCommandOutcome valid = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/pg:1\",\"stateful\":true,\"caps\":{},"
                        + "\"reverse\":{\"port\":5432}}"));
        DaemonCommandOutcome outOfRange = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/redis:7\",\"stateful\":true,\"caps\":{},"
                        + "\"reverse\":{\"port\":70000}}"));

        for (DaemonCommandOutcome outcome : List.of(valid, outOfRange)) {
            assertTrue(outcome.succeeded(), () -> "start error: " + outcome.error());
            assertEquals("RUNNING", mapper.readTree(outcome.output()).get("status").asText());
        }
        assertTrue(cli.calls.stream().flatMap(List::stream)
                        .noneMatch(a -> a.startsWith(StatefulRunArgs.REVERSE_PORT_LABEL)),
                "no reverse port is stamped on either container");
    }

    @Test
    void start_joins_a_live_instance_on_divergence() throws Exception {
        // One live instance per (env, imageRef). The server refuses second starts on its own
        // RUNNING rows (HTTP 409), so a start command that still meets a live container means the
        // remote row and the daemon registry diverged: the daemon joins the live container (same
        // ids, RUNNING) so the row converges, instead of refusing and stranding it.
        DaemonCommandOutcome first = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/pg:1\",\"stateful\":true,\"caps\":{}}"));
        assertTrue(first.succeeded());
        String firstInstance = mapper.readTree(first.output()).get("instanceId").asText();

        DaemonCommandOutcome second = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/pg:1\",\"stateful\":true,\"caps\":{}}"));

        assertTrue(second.succeeded(), () -> "join start error: " + second.error());
        JsonNode out = mapper.readTree(second.output());
        assertEquals("RUNNING", out.get("status").asText());
        assertEquals(firstInstance, out.get("instanceId").asText(), "join returns the live identity");
        assertFalse(cli.sawCallContaining("stop"), "join never bounces the live container");
    }

    @Test
    void stop_then_status_report_terminal_states() throws Exception {
        DaemonCommandOutcome started = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/pg:1\",\"stateful\":true,\"caps\":{}}"));
        String instanceId = mapper.readTree(started.output()).get("instanceId").asText();

        DaemonCommandOutcome stopped = stop.handle(cmd("stop-stateful-instance",
                "{\"envId\":\"env-1\",\"instanceId\":\"" + instanceId + "\"}"));
        assertTrue(stopped.succeeded());
        assertEquals("STOPPED", mapper.readTree(stopped.output()).get("status").asText());
    }

    @Test
    void status_reports_running_and_container_id() throws Exception {
        DaemonCommandOutcome started = start.handle(cmd("start-stateful-instance",
                "{\"envId\":\"env-1\",\"imageRef\":\"myenv/pg:1\",\"stateful\":true,\"caps\":{}}"));
        String instanceId = mapper.readTree(started.output()).get("instanceId").asText();

        DaemonCommandOutcome st = status.handle(cmd("status-stateful-instance",
                "{\"envId\":\"env-1\",\"instanceId\":\"" + instanceId + "\"}"));
        assertTrue(st.succeeded());
        JsonNode out = mapper.readTree(st.output());
        assertEquals("RUNNING", out.get("status").asText());
        assertEquals("cid-777", out.get("containerId").asText());
    }

    @Test
    void load_reports_loaded_with_digest() throws Exception {
        Files.writeString(new OciImageBlobStore(props).pathFor("myenv/pg:1"), "tar");
        DaemonCommandOutcome outcome = load.handle(cmd("load-oci-image",
                "{\"envId\":\"env-1\",\"ref\":\"myenv/pg:1\",\"displayName\":\"PG\"}"));

        assertTrue(outcome.succeeded(), () -> "load error: " + outcome.error());
        JsonNode out = mapper.readTree(outcome.output());
        assertEquals("LOADED", out.get("status").asText());
        assertEquals("sha256:abcabcabcabc", out.get("digest").asText());
    }

    @Test
    void malformed_payloads_return_failed_never_throw() {
        assertFalse(start.handle(cmd("start-stateful-instance", "{ not json")).succeeded());
        assertFalse(stop.handle(cmd("stop-stateful-instance", "{ not json")).succeeded());
        assertFalse(status.handle(cmd("status-stateful-instance", "{ not json")).succeeded());
        assertFalse(load.handle(cmd("load-oci-image", "{ not json")).succeeded());
        assertFalse(pull.handle(cmd("pull-oci-image", "{ not json")).succeeded());
        assertFalse(delete.handle(cmd("delete-oci-image", "{ not json")).succeeded());
    }

    @Test
    void underspecified_payloads_return_failed() {
        assertFalse(start.handle(cmd("start-stateful-instance", "{\"envId\":\"env-1\"}")).succeeded(),
                "start needs imageRef");
        assertFalse(stop.handle(cmd("stop-stateful-instance", "{\"envId\":\"env-1\"}")).succeeded(),
                "stop needs instanceId");
        assertFalse(load.handle(cmd("load-oci-image", "{\"envId\":\"env-1\"}")).succeeded(),
                "load needs ref");
        assertFalse(pull.handle(cmd("pull-oci-image", "{\"envId\":\"env-1\"}")).succeeded(),
                "pull needs ref");
        assertFalse(delete.handle(cmd("delete-oci-image", "{\"envId\":\"env-1\"}")).succeeded(),
                "delete needs ref");
    }
}
