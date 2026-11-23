package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciBuildLogStreamer;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService.BuildRequest;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciImageBuildProgressReporter;
import com.studioquack.sqs.sandbox.stateful.OciImageDigestReporter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** {@code build-oci-image} must ack immediately and never wait on podman build. */
class BuildOciImageCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @TempDir
    Path tempDir;

    private ExecutorService executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    void handleAcksImmediatelyWhileBuildStillWaiting() throws Exception {
        String buildId = "build-ack-1";
        String ref = "sq-local/demo:1";
        OciBuildContextStore store = new OciBuildContextStore(tempDir.toString());
        store.writeFile(buildId, "Dockerfile", "FROM scratch\n".getBytes(StandardCharsets.UTF_8));

        CountDownLatch buildEntered = new CountDownLatch(1);
        CountDownLatch holdBuild = new CountDownLatch(1);
        AtomicBoolean buildFinished = new AtomicBoolean(false);

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "oci-build-test-ack");
            t.setDaemon(true);
            return t;
        });
        FakeOciCli cli = new FakeOciCli();
        OciImageBuildProgressReporter progress = mock(OciImageBuildProgressReporter.class);
        OciImageDigestReporter digest = mock(OciImageDigestReporter.class);
        OciImageBuildService service = new OciImageBuildService(
                cli, store, progress, digest, executor,
                (argv, wall, sink) -> {
                    buildEntered.countDown();
                    try {
                        assertTrue(holdBuild.await(5, TimeUnit.SECONDS), "test release latch");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    buildFinished.set(true);
                    return new OciBuildLogStreamer.Outcome(0, false, null);
                });

        BuildOciImageCommandHandler handler =
                new BuildOciImageCommandHandler(mapper, store, service);

        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"envId\":\"env-1\",\"buildId\":\"" + buildId + "\",\"ref\":\"" + ref + "\"}"));

        // Handle returned accepted=true WHILE the build latch is still waiting.
        assertTrue(outcome.succeeded());
        JsonNode body = mapper.readTree(outcome.output());
        assertTrue(body.get("accepted").asBoolean());
        assertEquals(buildId, body.get("buildId").asText());
        assertEquals(ref, body.get("ref").asText());
        assertTrue(buildEntered.await(2, TimeUnit.SECONDS), "async build should have started");
        assertFalse(buildFinished.get(), "build must still be parked behind hold latch");
        assertEquals(1L, holdBuild.getCount(), "build latch still waiting at ack time");

        holdBuild.countDown();
        // Drain the parked worker so @AfterEach shutdown is clean.
        assertTrue(awaitTrue(buildFinished, 2, TimeUnit.SECONDS));
    }

    @Test
    void missingContextFailsWithoutSubmit() throws Exception {
        OciBuildContextStore store = new OciBuildContextStore(tempDir.toString());
        OciImageBuildService service = mock(OciImageBuildService.class);
        BuildOciImageCommandHandler handler =
                new BuildOciImageCommandHandler(mapper, store, service);

        DaemonCommandOutcome outcome = handler.handle(cmd(
                "{\"envId\":\"env-1\",\"buildId\":\"missing-id\",\"ref\":\"sq-local/x:1\"}"));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("build context missing"));
        verify(service, never()).submit(any(BuildRequest.class));
    }

    @Test
    void missingBuildIdOrRefFails() throws Exception {
        OciBuildContextStore store = mock(OciBuildContextStore.class);
        when(store.contextExists(any())).thenReturn(true);
        OciImageBuildService service = mock(OciImageBuildService.class);
        BuildOciImageCommandHandler handler =
                new BuildOciImageCommandHandler(mapper, store, service);

        assertFalse(handler.handle(cmd("{\"ref\":\"sq-local/x:1\"}")).succeeded());
        assertFalse(handler.handle(cmd("{\"buildId\":\"b1\"}")).succeeded());
        verify(service, never()).submit(any(BuildRequest.class));
    }

    private DaemonCommand cmd(String payload) {
        return new DaemonCommand("cmd-1", BuildOciImageCommandHandler.TYPE, null, payload);
    }

    private static boolean awaitTrue(AtomicBoolean flag, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (flag.get()) {
                return true;
            }
            Thread.sleep(20);
        }
        return flag.get();
    }
}
