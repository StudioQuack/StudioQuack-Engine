package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link StatefulInstanceManager#reconcilePlatformSingleton}: the built-in node library is ONE
 * container per host whichever of its names (canonical, image-replace {@code -b}, network-drain
 * {@code -drain}) it currently carries — a boot survivor is adopted instead of duplicated, extra
 * family containers are removed, and a routed container that died is dropped so the next start
 * recreates it.
 */
class StatefulPlatformSingletonReconcileTest {

    private static final String ENV = "env-1";
    private static final String REF = "sq-local/sq-builtin-node-library:1.0.0";
    private static final String ID = StatefulInstance.BUILTIN_NODE_LIBRARY_INSTANCE_ID;
    private static final String BASE = "sq-stateful-" + ID;

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private StatefulInstanceManager manager;

    /** {@code podman ps} rows: id → (name, state). */
    private final Map<String, String[]> host = new java.util.LinkedHashMap<>();
    /** Inspect state by container id or name; absent → running. */
    private final Map<String, String> inspectState = new HashMap<>();

    @TempDir
    Path blobDir;

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        StatefulRuntimeProperties props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        cli.responder = this::respond;
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10);
    }

    private OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        return switch (sub) {
            case "ps" -> {
                StringBuilder out = new StringBuilder();
                host.forEach((id, row) -> out.append(id).append('\t').append(row[0]).append('\t')
                        .append("localhost/").append(REF).append('\t').append(row[1]).append('\n'));
                yield new OciCli.Result(0, out.toString(), "", false);
            }
            case "inspect" -> {
                String ref = argv.get(argv.size() - 1);
                String state = inspectState.getOrDefault(ref, "running");
                String name = host.containsKey(ref) ? host.get(ref)[0] : BASE;
                yield new OciCli.Result(0, inspect(ref, name, state), "", false);
            }
            case "run" -> new OciCli.Result(0, "cid-fresh\n", "", false);
            default -> new OciCli.Result(0, "", "", false);
        };
    }

    private static String inspect(String id, String name, String state) {
        return "[{\"Id\":\"" + id + "\",\"Name\":\"" + name + "\","
                + "\"State\":{\"Status\":\"" + state + "\",\"Running\":" + "running".equals(state) + "},"
                + "\"Config\":{\"Labels\":{\"studioquack.sqs.stateful\":\"1\",\"studioquack.sqs.env\":\"" + ENV
                + "\",\"studioquack.sqs.ref\":\"" + REF + "\",\"studioquack.sqs.instance\":\"" + ID + "\"}}}]";
    }

    @Test
    void bootSurvivorUnderAlternateName_isAdopted_andTheStartJoinsIt() {
        host.put("cid-b", new String[]{BASE + "-b", "running"});
        host.put("cid-old", new String[]{BASE, "exited"});
        inspectState.put("cid-old", "exited");

        int removed = manager.reconcilePlatformSingleton(ENV, REF, ID);

        assertEquals(1, removed, "only the exited canonical residue goes");
        assertTrue(cli.sawCallContaining("rm", "-f", BASE));
        assertFalse(cli.sawCallContaining("rm", "-f", BASE + "-b"), "the live survivor is kept");
        var start = manager.start(ENV, REF, true, StatefulCaps.uncapped(), ID, null, null, (Map<String, String>) null);
        assertEquals(StatefulStatus.RUNNING, start.status());
        assertEquals("cid-b", start.containerId(), "start joins the survivor instead of running a second one");
        assertFalse(cli.sawCallContaining("run"), "no second library container is created");
    }

    @Test
    void routedInstanceStays_everyOtherFamilyContainerIsRemoved() {
        var first = manager.start(ENV, REF, true, StatefulCaps.uncapped(), ID, null, null, (Map<String, String>) null);
        assertEquals("cid-fresh", first.containerId());
        host.put("cid-fresh", new String[]{BASE, "running"});
        host.put("cid-drain", new String[]{BASE + "-drain", "exited"});
        host.put("cid-b", new String[]{BASE + "-b", "running"});
        host.put("cid-user", new String[]{"sq-stateful-inst_user", "exited"});

        int removed = manager.reconcilePlatformSingleton(ENV, REF, ID);

        assertEquals(2, removed);
        assertTrue(cli.sawCallContaining("rm", "-f", BASE + "-drain"));
        assertTrue(cli.sawCallContaining("rm", "-f", BASE + "-b"));
        assertFalse(cli.sawCallContaining("rm", "-f", "sq-stateful-inst_user"), "user instances are not the family");
        assertEquals("cid-fresh", manager.resolveRunning(ENV, REF).orElseThrow().containerId());
    }

    @Test
    void routedContainerGoneOnTheHost_isDropped_soTheNextStartRecreatesIt() {
        manager.start(ENV, REF, true, StatefulCaps.uncapped(), ID, null, null, (Map<String, String>) null);
        inspectState.put(BASE, "exited");

        manager.reconcilePlatformSingleton(ENV, REF, ID);

        assertFalse(manager.hasLiveInstance(ENV, REF), "a dead container must not stay routed");
    }
}
