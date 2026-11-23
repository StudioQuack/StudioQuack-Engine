package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContainerTelemetryServiceTest {

    @Test
    void listsLabeledContainersFromFakeRuntimeRunner() {
        FakeRunner runner = new FakeRunner();
        runner.paths.put("podman", "/usr/bin/podman");
        runner.paths.put("docker", "/usr/bin/docker");
        runner.outputs.put("/usr/bin/podman ps -a --filter label=studioquack.sqs=1 --format {{.ID}}", ok("abc123\n"));
        runner.outputs.put("/usr/bin/podman ps -a --filter label=studioquack.sqs.stateful=1 --format {{.ID}}", ok(""));
        runner.outputs.put("/usr/bin/podman inspect abc123", ok("""
                [{
                  "Id": "abc123456789",
                  "Name": "/sqs-warm-python",
                  "Created": "2026-07-14T10:00:00Z",
                  "Config": {
                    "Image": "ghcr.io/studioquack/sqs-python:latest",
                    "Labels": {
                      "studioquack.sqs": "1",
                      "studioquack.sqs.lang": "python",
                      "studioquack.sqs.role": "warm"
                    }
                  },
                  "State": { "Status": "running" }
                }]
                """));
        runner.outputs.put("/usr/bin/docker ps -a --filter label=studioquack.sqs=1 --format {{.ID}}", ok(""));

        ContainerTelemetryService.ContainerTelemetry telemetry =
                new ContainerTelemetryService(new ObjectMapper(), null, runner).snapshot();

        // Podman-only by design (OciRuntimeKind.preferenceOrder() == [PODMAN]); docker is on PATH in
        // this fake but never queried, so only podman appears in queriedRuntimes.
        assertEquals(List.of("podman"), telemetry.queriedRuntimes());
        assertEquals(ContainerTelemetryService.LABEL_SELECTOR, telemetry.labelSelector());
        assertEquals(1, telemetry.items().size());
        ContainerTelemetryService.ContainerInfo row = telemetry.items().get(0);
        assertEquals("abc123456789", row.id());
        assertEquals("sqs-warm-python", row.name());
        assertEquals("running", row.status());
        assertEquals("ghcr.io/studioquack/sqs-python:latest", row.image());
        assertEquals(ContainerTelemetryService.ContainerKind.WARM_POOL, row.kind());
        assertEquals("python", row.language());
        assertEquals("podman", row.runtime());
    }

    @Test
    void includesStatefulContainersWithTheirActualImageReference() {
        FakeRunner runner = new FakeRunner();
        runner.paths.put("podman", "/usr/bin/podman");
        runner.outputs.put("/usr/bin/podman ps -a --filter label=studioquack.sqs=1 --format {{.ID}}", ok(""));
        runner.outputs.put("/usr/bin/podman ps -a --filter label=studioquack.sqs.stateful=1 --format {{.ID}}", ok("stateful-1\n"));
        runner.outputs.put("/usr/bin/podman inspect stateful-1", ok("""
                [{
                  "Id": "stateful-123456",
                  "Name": "/sq-stateful-postgres",
                  "Created": "2026-07-14T10:00:00Z",
                  "Config": {
                    "Image": "postgres:16",
                    "Labels": { "studioquack.sqs.stateful": "1" }
                  },
                  "State": { "Status": "running" }
                }]
                """));

        ContainerTelemetryService.ContainerInfo row =
                new ContainerTelemetryService(new ObjectMapper(), null, runner).snapshot().items().getFirst();

        assertEquals("postgres:16", row.image());
        assertEquals(ContainerTelemetryService.ContainerKind.STATEFUL_INSTANCE, row.kind());
    }

    @Test
    void recordsRuntimeErrorsWithoutDroppingOtherRuntimeRows() {
        FakeRunner runner = new FakeRunner();
        runner.paths.put("podman", "/usr/bin/podman");
        runner.paths.put("docker", "/usr/bin/docker");
        runner.outputs.put("/usr/bin/podman ps -a --filter label=studioquack.sqs=1 --format {{.ID}}",
                new CliRunner.Result(1, "", "podman unavailable", false, 2));
        runner.outputs.put("/usr/bin/docker ps -a --filter label=studioquack.sqs=1 --format {{.ID}}", ok(""));

        ContainerTelemetryService.ContainerTelemetry telemetry =
                new ContainerTelemetryService(new ObjectMapper(), null, runner).snapshot();

        assertTrue(telemetry.items().isEmpty());
        assertEquals("podman unavailable", telemetry.errors().get("podman"));
    }

    private static CliRunner.Result ok(String stdout) {
        return new CliRunner.Result(0, stdout, "", false, 1);
    }

    private static final class FakeRunner implements ContainerTelemetryService.RuntimeCommandRunner {
        final Map<String, String> paths = new HashMap<>();
        final Map<String, CliRunner.Result> outputs = new HashMap<>();

        @Override
        public CliRunner.Result run(List<String> argv, Duration timeout) {
            String key = String.join(" ", argv);
            return outputs.getOrDefault(key, new CliRunner.Result(127, "", "missing fake: " + key, false, 1));
        }

        @Override
        public String lookPath(String name) {
            return paths.get(name);
        }
    }
}
