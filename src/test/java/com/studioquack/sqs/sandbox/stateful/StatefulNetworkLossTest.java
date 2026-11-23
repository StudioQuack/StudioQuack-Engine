package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Detection of a gVisor stateful sandbox left loopback-only by an in-place restart. */
class StatefulNetworkLossTest {

    private static final String HEADER = """
            Inter-|   Receive                                                |  Transmit
             face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
            """;
    private static final String LO_ONLY = HEADER + "    lo:    1234      10    0    0    0     0          0         0     1234      10    0    0    0     0       0          0\n";
    private static final String LO_AND_ETH0 = LO_ONLY
            + "  eth0:  987654     321    0    0    0     0          0         0    12345     123    0    0    0     0       0          0\n";

    @TempDir
    Path blobDir;

    @Test
    void onlyLoopbackParsesProcNetDev() {
        assertTrue(StatefulInstanceManager.onlyLoopback(LO_ONLY));
        assertFalse(StatefulInstanceManager.onlyLoopback(LO_AND_ETH0));
        assertFalse(StatefulInstanceManager.onlyLoopback(HEADER));
        assertFalse(StatefulInstanceManager.onlyLoopback(""));
    }

    @Test
    void runscOnContainerNetworkWithLoopbackOnlyIsLost() {
        List<List<String>> calls = new ArrayList<>();
        StatefulInstanceManager m = manager(calls, argv -> argv.contains("inspect")
                ? ok("runsc sq-stateful-net-1\n") : ok(LO_ONLY));

        assertTrue(m.sandboxNetworkLost(running()));
        assertEquals(List.of("podman", "exec", "cid-1", "cat", "/proc/net/dev"), calls.get(1));
    }

    @Test
    void healthyRunscKeepsItsNetwork() {
        StatefulInstanceManager m = manager(new ArrayList<>(), argv -> argv.contains("inspect")
                ? ok("runsc sq-stateful-net-1") : ok(LO_AND_ETH0));

        assertFalse(m.sandboxNetworkLost(running()));
    }

    @Test
    void runcAndNoNetworkAreNeverExecuted() {
        for (String inspect : List.of("crun sq-stateful-net-1", "runsc none", "runsc host",
                "runsc container:abc")) {
            List<List<String>> calls = new ArrayList<>();
            StatefulInstanceManager m = manager(calls, argv -> ok(inspect));

            assertFalse(m.sandboxNetworkLost(running()), inspect);
            assertEquals(1, calls.size(), inspect);
        }
    }

    @Test
    void unknownIsNeverLost() {
        StatefulInstanceManager inspectFails = manager(new ArrayList<>(), argv -> fail());
        StatefulInstanceManager execFails = manager(new ArrayList<>(), argv -> argv.contains("inspect")
                ? ok("runsc sq-stateful-net-1") : fail());
        StatefulInstance stopped = running();
        stopped.setStatus(StatefulStatus.STOPPED);

        assertFalse(inspectFails.sandboxNetworkLost(running()));
        assertFalse(execFails.sandboxNetworkLost(running()));
        assertFalse(execFails.sandboxNetworkLost(stopped));
    }

    private StatefulInstanceManager manager(List<List<String>> calls,
                                            Function<List<String>, OciCli.Result> responder) {
        OciCli cli = new OciCli() {
            @Override public boolean available() { return true; }
            @Override public String cliPath() { return "podman"; }
            @Override public Result run(List<String> argv, byte[] stdin, Duration timeout) {
                calls.add(argv);
                return responder.apply(argv);
            }
        };
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        return new StatefulInstanceManager(cli, new ObjectMapper(), null, props,
                new OciImageBlobStore(props), null, false, 10);
    }

    private static StatefulInstance running() {
        StatefulInstance inst = new StatefulInstance("inst-1", "env-1", "app:1", "sq-stateful-inst-1",
                "sq-stateful-inst-1-data", StatefulCaps.uncapped(), true);
        inst.setContainerId("cid-1");
        inst.setStatus(StatefulStatus.RUNNING);
        return inst;
    }

    private static OciCli.Result ok(String out) {
        return new OciCli.Result(0, out, "", false);
    }

    private static OciCli.Result fail() {
        return new OciCli.Result(125, "", "Error: no such container", false);
    }
}
