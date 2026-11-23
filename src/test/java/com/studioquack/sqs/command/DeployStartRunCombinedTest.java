package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.sqs.channel.ChannelKemIkm;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandRegistry;
import com.studioquack.sqs.channel.command.DaemonCommandResult;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.status.RunStatusCallback;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The full channel→engine path driven through the {@link DaemonCommandRegistry} (exactly how the
 * {@code HeartbeatClient} dispatches pushed commands): a {@code deploy} command followed by a
 * {@code start-run} command for a {@code start → python(sandbox) → end} workflow.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:sqs-combined-cmd-test;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.flyway.enabled=false",
        "otel.sdk.disabled=true",
        "sqs.sandbox.wal-path=${java.io.tmpdir}/sqs-combined-jobs.wal",
        "sqs.sandbox.resume-poll-ms=8000",
        // Keep the test lean: don't pre-warm the default per-language pool on boot (that only spins
        // real containers when Podman is present, but there's no need to during this focused test).
        "sqs.sandbox.warmpool.enabled=false",
        // Focus on the dispatch pipeline, not overlay-egress enforcement (no overlay in this test).
        "sqs.sandbox.overlay-egress=off",
        // Isolate container ownership from other cached SpringBootTest contexts sharing ./data.
        "SQS_DATA_DIR=${java.io.tmpdir}/sqs-combined-data"
})
@ActiveProfiles("base")
@Import(DeployStartRunCombinedTest.RecordingConfig.class)
class DeployStartRunCombinedTest {

    @Autowired
    private DaemonCommandRegistry registry;
    @Autowired
    private ChannelKemIkm channelKemIkm;
    @Autowired
    private SqsEngine sqsEngine;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private RecordingRunStatusCallback callback;
    @Autowired
    private ContainerRuntime runtime;

    @Test
    void deploy_then_start_run_through_registry_reports_running_and_advances() throws Exception {
        byte[] testIkm = new byte[32];
        java.util.Arrays.fill(testIkm, (byte) 0x42);
        channelKemIkm.install("env-combined", testIkm);
        String issuedAt = java.time.Instant.now().toString();

        String workflowId = "wfCombined";
        String pythonNodeId = "pythonNode";
        String wfeId = UUID.randomUUID().toString();

        List<NodeDto> nodes = List.of(
                node("startN", "start.event", null, "start", null),
                node(pythonNodeId, "custom.sandbox", "custom-script-executor", "serviceTask",
                        "studioquack.return_result([{\"port\": \"out\", \"value\": \"combined-ok\"}])\n"
                                + "print('combined-node-ran')\n"),
                node("endN", "end.event", null, "end", null));
        List<EdgeDto> edges = List.of(edge("startN", pythonNodeId), edge(pythonNodeId, "endN"));

        // 1) deploy command through the registry (the dispatch path).
        String deployCmdId = "c-combined-deploy-" + wfeId;
        DeployCommandPayload deployPayload = new DeployCommandPayload(
                workflowId, "1", "Combined Flow", nodes, edges, null, null, false, false);
        String deployJson = mapper.writeValueAsString(deployPayload);
        String deployMac = EnvelopeCrypto.macCommand(testIkm, "env-combined",
                deployCmdId, "deploy", issuedAt, deployJson);
        Optional<DaemonCommandResult> deployResult = registry.dispatch(
                new DaemonCommand(deployCmdId, "deploy", issuedAt, deployJson, deployMac));
        assertTrue(deployResult.isPresent(), "deploy dispatched");
        assertEquals("ok", deployResult.get().status(), () -> "deploy failed: " + deployResult.get().error());

        // 2) start-run command through the registry, carrying the REF seed for the python node.
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowExecutionId(wfeId);
        seed.setWorkflowId(workflowId);
        seed.setEnvId("env-combined");
        seed.setCurrentNodeId(pythonNodeId);
        seed.setNodes(new ArrayList<>(nodes));
        seed.setEdges(new ArrayList<>(edges));

        String startCmdId = "c-combined-start-" + wfeId;
        StartRunCommandPayload startPayload = new StartRunCommandPayload(
                wfeId, "Process_" + workflowId, null, null, seed);
        String startJson = mapper.writeValueAsString(startPayload);
        String startMac = EnvelopeCrypto.macCommand(testIkm, "env-combined",
                startCmdId, "start-run", issuedAt, startJson);
        Optional<DaemonCommandResult> startResult = registry.dispatch(
                new DaemonCommand(startCmdId, "start-run", issuedAt, startJson, startMac));
        assertTrue(startResult.isPresent(), "start-run dispatched");
        assertEquals("ok", startResult.get().status(), () -> "start-run failed: " + startResult.get().error());
        String processInstanceId = (String) mapper.readValue(startResult.get().output(), java.util.Map.class)
                .get("processInstanceId");
        assertNotNull(processInstanceId);

        // 3) RUNNING callback received — the required assertion (independent of the sandbox).
        assertTrue(callback.received(wfeId, RunStatusCallback.Status.RUNNING),
                "RUNNING reported at start-run; events=" + callback.events());

        // 4) With the sandbox present, the python node runs and advances the PI to completion, and
        // the engine event bridge reports COMPLETED.
        boolean sandboxReady = runtime.available()
                && CliRunner.run(List.of(runtime.cli(), "info"), Duration.ofSeconds(15)).ok();
        if (sandboxReady) {
            assertTrue(await(Duration.ofSeconds(90),
                            () -> sqsEngine.processInstances().processInstanceBusinessKey(wfeId).count() == 0),
                    "the sandbox advanced the workflow past the ReceiveTask to completion");
            assertTrue(await(Duration.ofSeconds(10),
                            () -> callback.received(wfeId, RunStatusCallback.Status.COMPLETED)),
                    "COMPLETED reported via the engine event bridge; events=" + callback.events());
        }
    }

    // ---- fixtures (mirror SandboxDispatchEndToEndTest) ------------------

    private NodeDto node(String id, String bpmnType, String jobType, String nodeType, String script) {
        TemplateDto template = new TemplateDto();
        template.setId("tpl-" + id);
        template.setBpmnType(bpmnType);
        template.setJobType(jobType);
        template.setNodeType(nodeType);
        template.setLabel(id);
        template.setName(id);
        if (script != null) {
            template.setTemplateScript(new TemplateScriptDto(id + "-s", "tpl-" + id, "python", script));
            PortDto out = new PortDto();
            out.setId("out");
            out.setName("out");
            out.setValue("out");
            out.setType(PortType.OUTPUT);
            template.setPorts(new ArrayList<>(List.of(out)));
        }
        NodeDto node = new NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }

    private EdgeDto edge(String source, String target) {
        EdgeDto edge = new EdgeDto();
        edge.setId("e_" + source + "_" + target);
        edge.setSource(source);
        edge.setTarget(target);
        return edge;
    }

    private boolean await(Duration timeout, java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(200);
        }
        return cond.getAsBoolean();
    }

    @TestConfiguration
    static class RecordingConfig {
        @Bean
        @Primary
        RecordingRunStatusCallback recordingRunStatusCallback() {
            return new RecordingRunStatusCallback();
        }
    }
}
