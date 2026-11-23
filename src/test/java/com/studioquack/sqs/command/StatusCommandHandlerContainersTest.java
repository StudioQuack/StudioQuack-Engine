package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.oci.ContainerTelemetryService;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobStore;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulRuntimeProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused test for the {@code status} {@code containers} inventory merge: proves a registered
 * {@link StatefulInstanceManager} instance shows up in the SAME {@code items} list the Podman
 * telemetry reports, carrying its actual image and a {@code STATEFUL_INSTANCE} kind, and that
 * ephemeral sandbox/warm-pool rows are tagged with the right generic kind from their role label.
 * Byte-identical across daemon tiers.
 */
class StatusCommandHandlerContainersTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path blobDir;

    private static DaemonCommand statusCmd() {
        return new DaemonCommand("s1", "status", "2026-07-31T00:00:00Z", null);
    }

    /** A manager whose runtime starts every container and reports it running. */
    private StatefulInstanceManager managerWithRunningContainers() {
        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                case "run" -> new OciCli.Result(0, "container-abc123def456\n", "", false);
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                case "inspect" -> new OciCli.Result(0,
                        "[{\"Id\":\"container-abc123def456\",\"State\":{\"Status\":\"running\"}}]", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        return new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props), null, false, 10);
    }

    @Test
    void containers_leaveOutAStoppedInstance() throws Exception {
        StatefulInstanceManager manager = managerWithRunningContainers();
        String instanceId = manager.start("env-1", "myorg/postgres-app:1.4", true,
                new StatefulCaps(1, 512, 1024, null)).instanceId();
        manager.stop("env-1", instanceId);
        StatusCommandHandler handler = new StatusCommandHandler(mapper, null, "base", null, null, manager);

        Map<?, ?> body = mapper.readValue(handler.handle(statusCmd()).output(), Map.class);

        List<?> items = (List<?>) ((Map<?, ?>) body.get("containers")).get("items");
        assertTrue(items.isEmpty(), "a stopped instance has no container to report: " + items);
    }

    @Test
    void containers_mergesRegisteredStatefulInstance_withActualImageAndKind() throws Exception {
        StatefulInstanceManager manager = managerWithRunningContainers();
        manager.start("env-1", "myorg/postgres-app:1.4", true, new StatefulCaps(1, 512, 1024, null));

        // No ContainerTelemetryService/WarmPool wired — isolates the merge seam itself.
        StatusCommandHandler handler = new StatusCommandHandler(mapper, null, "base", null, null, manager);

        DaemonCommandOutcome outcome = handler.handle(statusCmd());

        assertTrue(outcome.succeeded());
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        Map<?, ?> containers = (Map<?, ?>) body.get("containers");
        List<?> items = (List<?>) containers.get("items");
        assertEquals(1, items.size(), "the registered stateful instance is merged into the list");
        Map<?, ?> row = (Map<?, ?>) items.get(0);
        assertEquals("myorg/postgres-app:1.4", row.get("image"), "actual image ref, not a container name");
        assertEquals("STATEFUL_INSTANCE", row.get("kind"));
        assertEquals("RUNNING", row.get("status"));
        assertEquals("container-abc123def456", row.get("id"));
        // No command/secret metadata leaks into the row.
        assertFalse(containers.toString().contains("caps"));
        assertFalse(containers.toString().contains("health"));
    }

    @Test
    void containers_reportsActualImageAndKind_forPodmanTelemetryRows() throws Exception {
        ContainerTelemetryService telemetry = new ContainerTelemetryService(mapper, null,
                new ContainerTelemetryService.RuntimeCommandRunner() {
                    @Override
                    public com.studioquack.sqs.sandbox.runtime.CliRunner.Result run(List<String> argv,
                                                                                     java.time.Duration timeout) {
                        String joined = String.join(" ", argv);
                        if (joined.endsWith("ps -a --filter label=studioquack.sqs=1 --format {{.ID}}")) {
                            return new com.studioquack.sqs.sandbox.runtime.CliRunner.Result(0, "warm1\n", "", false, 1);
                        }
                        if (joined.endsWith("inspect warm1")) {
                            return new com.studioquack.sqs.sandbox.runtime.CliRunner.Result(0, """
                                    [{
                                      "Id": "warm1-full-id",
                                      "Name": "/sqs-warm-python",
                                      "Config": {
                                        "Image": "ghcr.io/studioquack/sqs-python:latest",
                                        "Labels": { "studioquack.sqs.role": "warm" }
                                      },
                                      "State": { "Status": "running" }
                                    }]
                                    """, "", false, 1);
                        }
                        return new com.studioquack.sqs.sandbox.runtime.CliRunner.Result(0, "", "", false, 1);
                    }

                    @Override
                    public String lookPath(String name) {
                        return "podman".equals(name) ? "/usr/bin/podman" : null;
                    }
                });
        StatusCommandHandler handler = new StatusCommandHandler(mapper, null, "base",
                telemetry, null, null);

        DaemonCommandOutcome outcome = handler.handle(statusCmd());

        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        Map<?, ?> containers = (Map<?, ?>) body.get("containers");
        List<?> items = (List<?>) containers.get("items");
        assertEquals(1, items.size());
        Map<?, ?> row = (Map<?, ?>) items.get(0);
        assertEquals("ghcr.io/studioquack/sqs-python:latest", row.get("image"));
        assertEquals("WARM_POOL", row.get("kind"));
    }

    @Test
    void containers_dedupesStatefulRow_whenTelemetryHasFullIdAndRegistryHasShortId() throws Exception {
        // The registry stores the short id (podman run / ps output); podman inspect — the
        // telemetry row — carries the full 64-char id. The FE truncates both to 12 chars, so an
        // exact-string dedup rendered the SAME container twice (seen on sq-stateful-docling).
        String shortId = "3cd064ad2452";
        String fullId = shortId + "f".repeat(52);

        FakeOciCli cli = new FakeOciCli();
        cli.responder = argv -> {
            String sub = argv.size() > 1 ? argv.get(1) : "";
            return switch (sub) {
                case "run" -> new OciCli.Result(0, shortId + "\n", "", false);
                case "volume" -> argv.contains("exists")
                        ? new OciCli.Result(1, "", "no such volume", false)
                        : new OciCli.Result(0, "", "", false);
                case "inspect" -> new OciCli.Result(0,
                        "[{\"Id\":\"" + shortId + "\",\"State\":{\"Status\":\"running\"}}]", "", false);
                default -> new OciCli.Result(0, "", "", false);
            };
        };
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        StatefulInstanceManager manager = new StatefulInstanceManager(cli, mapper, null, props,
                new OciImageBlobStore(props), null, false, 10);
        manager.start("env-1", "quay.io/docling-project/docling-serve-cpu:latest", true,
                new StatefulCaps(1, 512, 1024, null));

        ContainerTelemetryService telemetry = new ContainerTelemetryService(mapper, null,
                new ContainerTelemetryService.RuntimeCommandRunner() {
                    @Override
                    public com.studioquack.sqs.sandbox.runtime.CliRunner.Result run(List<String> argv,
                                                                                     java.time.Duration timeout) {
                        String joined = String.join(" ", argv);
                        if (joined.endsWith("ps -a --filter label=studioquack.sqs.stateful=1 --format {{.ID}}")) {
                            return new com.studioquack.sqs.sandbox.runtime.CliRunner.Result(
                                    0, shortId + "\n", "", false, 1);
                        }
                        if (joined.endsWith("inspect " + shortId)) {
                            return new com.studioquack.sqs.sandbox.runtime.CliRunner.Result(0, """
                                    [{
                                      "Id": "%s",
                                      "Name": "/sq-stateful-docling",
                                      "Config": {
                                        "Image": "quay.io/docling-project/docling-serve-cpu:latest",
                                        "Labels": { "studioquack.sqs.stateful": "1" }
                                      },
                                      "State": { "Status": "running" },
                                      "Created": "2026-08-27T02:24:10.914980006Z"
                                    }]
                                    """.formatted(fullId), "", false, 1);
                        }
                        return new com.studioquack.sqs.sandbox.runtime.CliRunner.Result(0, "", "", false, 1);
                    }

                    @Override
                    public String lookPath(String name) {
                        return "podman".equals(name) ? "/usr/bin/podman" : null;
                    }
                });
        StatusCommandHandler handler = new StatusCommandHandler(mapper, null, "base", telemetry, null, manager);

        DaemonCommandOutcome outcome = handler.handle(statusCmd());

        assertTrue(outcome.succeeded());
        Map<?, ?> body = mapper.readValue(outcome.output(), Map.class);
        List<?> items = (List<?>) ((Map<?, ?>) body.get("containers")).get("items");
        assertEquals(1, items.size(), "short registry id + full inspect id are the same container");
        Map<?, ?> row = (Map<?, ?>) items.get(0);
        assertEquals(fullId, row.get("id"), "the live inspect row wins the merge");
        assertEquals("STATEFUL_INSTANCE", row.get("kind"));
    }
}
