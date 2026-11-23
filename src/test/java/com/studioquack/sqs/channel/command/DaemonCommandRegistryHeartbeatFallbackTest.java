package com.studioquack.sqs.channel.command;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the heartbeat TCP fallback admission gate on {@link DaemonCommandRegistry}:
 * read-only log pulls and zero-PHI ephemeral none-start/none-end drafts pass, while script-bearing
 * or non-ephemeral deploys and payload-carrying start-runs remain strictly refused with
 * {@link DaemonCommandRegistry#ERROR_EMERGENCY_ONLY_CHANNEL}.
 */
class DaemonCommandRegistryHeartbeatFallbackTest {

    private DaemonCommandRegistry registryWithEmergencyGate() {
        DaemonCommandHandler okDeploy = stubHandler("deploy");
        DaemonCommandHandler okStart = stubHandler("start-run");
        DaemonCommandHandler okRunLogs = stubHandler("run-logs");
        DaemonCommandHandler okEngineLogs = stubHandler("engine-logs");
        return new DaemonCommandRegistry(List.of(okDeploy, okStart, okRunLogs, okEngineLogs), 60_000L, true);
    }

    private static DaemonCommandHandler stubHandler(String type) {
        return new DaemonCommandHandler() {
            @Override public String type() { return type; }
            @Override public DaemonCommandOutcome handle(DaemonCommand command) { return DaemonCommandOutcome.ok("{}"); }
        };
    }

    @Test
    void admitsReadOnlyLogPullsOnHeartbeat() {
        DaemonCommandRegistry reg = registryWithEmergencyGate();
        var runLogs = reg.dispatch(new DaemonCommand("c1", "run-logs", "2026-11-22T10:00:00Z", "{\"workflowExecutionId\":\"r1\"}"),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        var engineLogs = reg.dispatch(new DaemonCommand("c2", "engine-logs", "2026-11-22T10:00:00Z", "{}"),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        assertEquals("ok", runLogs.status());
        assertEquals("ok", engineLogs.status());
    }

    @Test
    void admitsZeroPhiEphemeralNoneStartEndDraftOnHeartbeat() {
        DaemonCommandRegistry reg = registryWithEmergencyGate();
        String deployPayload = """
                {"workflowId":"wf-1","version":1,"name":"temp","ephemeral":true,"draftToken":"tok123",
                 "nodes":[
                   {"id":"s1","data":{"bpmnType":"start.event","label":"Start"}},
                   {"id":"e1","data":{"bpmnType":"end.event","label":"End"}}
                 ],
                 "edges":[{"id":"ed1","source":"s1","target":"e1"}],
                 "nodeScripts":[],"nodeImages":[]}
                """;
        String startPayload = """
                {"workflowExecutionId":"run-1","processKey":"Process_wf-1__draft_tok123",
                 "seedVars":{},"seedContext":{"workflowExecutionId":"run-1","workflowId":"wf-1","inDesign":true},
                 "logMode":"STREAM"}
                """;
        var d = reg.dispatch(new DaemonCommand("c3", "deploy", "2026-11-22T10:00:00Z", deployPayload),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        var s = reg.dispatch(new DaemonCommand("c4", "start-run", "2026-11-22T10:00:00Z", startPayload),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        assertEquals("ok", d.status());
        assertEquals("ok", s.status());
    }

    @Test
    void refusesScriptDeployAndPhiStartRunOnHeartbeat() {
        DaemonCommandRegistry reg = registryWithEmergencyGate();
        String scriptDeploy = """
                {"workflowId":"wf-2","version":1,"name":"real","ephemeral":true,"draftToken":"tok123",
                 "nodes":[{"id":"n1","data":{"bpmnType":"service.task","jobType":"custom-script-executor"}}],
                 "nodeScripts":[{"nodeId":"n1","language":"python","script":"print(1)"}]}
                """;
        String publishedDeploy = """
                {"workflowId":"wf-3","version":1,"name":"pub","ephemeral":false,
                 "nodes":[{"id":"s1","data":{"bpmnType":"start.event"}}]}
                """;
        String phiStart = """
                {"workflowExecutionId":"run-2","processKey":"Process_wf-2__draft_tok123",
                 "seedVars":{},"seedContext":{"workflowExecutionId":"run-2","inputData":{"ssn":"123"}}}
                """;
        var d1 = reg.dispatch(new DaemonCommand("c5", "deploy", "2026-11-22T10:00:00Z", scriptDeploy),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        var d2 = reg.dispatch(new DaemonCommand("c6", "deploy", "2026-11-22T10:00:00Z", publishedDeploy),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        var s1 = reg.dispatch(new DaemonCommand("c7", "start-run", "2026-11-22T10:00:00Z", phiStart),
                DaemonCommandRegistry.INBOUND_HEARTBEAT).orElseThrow();
        assertEquals("failed", d1.status());
        assertTrue(d1.error().contains(DaemonCommandRegistry.ERROR_EMERGENCY_ONLY_CHANNEL));
        assertEquals("failed", d2.status());
        assertTrue(d2.error().contains(DaemonCommandRegistry.ERROR_EMERGENCY_ONLY_CHANNEL));
        assertEquals("failed", s1.status());
        assertTrue(s1.error().contains(DaemonCommandRegistry.ERROR_EMERGENCY_ONLY_CHANNEL));
    }
}
