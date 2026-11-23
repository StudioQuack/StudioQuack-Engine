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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link StatefulInstanceManager#reconcileWithHost}. */
class StatefulInstanceReconcileTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private FakeOciCli cli;
    private StatefulRuntimeProperties props;
    private StatefulInstanceManager manager;

    /** Container ids {@code podman ps -a} returns. */
    private List<String> psIds = List.of();
    /** Per-id inspect JSON overrides; absent → the default running/labeled row. */
    private final Map<String, String> inspectById = new HashMap<>();

    @TempDir
    Path blobDir;

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        psIds = List.of();
        inspectById.clear();
        cli.responder = this::respond;
        // Fresh manager = empty in-memory registry (the daemon just restarted).
        manager = new StatefulInstanceManager(cli, mapper, null, props, new OciImageBlobStore(props),
                null, false, 10);
    }

    private OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        switch (sub) {
            case "ps" -> {
                return new OciCli.Result(0, String.join("\n", psIds), "", false);
            }
            case "inspect" -> {
                String id = argv.get(argv.size() - 1);
                String json = inspectById.get(id);
                if (json == null) {
                    // Fresh containers (post-adopt starts) answer as a plain running row.
                    json = inspectJson(id, "running", Map.of(), null);
                }
                return new OciCli.Result(0, json, "", false);
            }
            case "run" -> {
                return new OciCli.Result(0, "newcidfreshstart\n", "", false);
            }
            case "volume", "rm", "stop" -> {
                return new OciCli.Result(0, "", "", false);
            }
            default -> {
                return new OciCli.Result(0, "", "", false);
            }
        }
    }

    /**
     * One inspect row. {@code labels} carries the provenance triple; {@code created} pins ordering
     * (the elder container wins a duplicate). {@code null} created → no Created field.
     */
    private static String inspectJson(String id, String state, Map<String, String> labels, String created) {
        StringBuilder lbl = new StringBuilder();
        for (Map.Entry<String, String> e : labels.entrySet()) {
            lbl.append(",\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\"");
        }
        String running = "running".equals(state) ? "true" : "false";
        String name = labels.getOrDefault("studioquack.sqs.instance", "inst_unknown");
        return "[{\"Id\":\"" + id + "\",\"Name\":\"sq-stateful-" + name + "\","
                + (created == null ? "" : "\"Created\":\"" + created + "\",")
                + "\"State\":{\"Status\":\"" + state + "\",\"Running\":" + running + "},"
                + "\"Config\":{\"Labels\":{\"studioquack.sqs.stateful\":\"1\"" + lbl + "}},"
                + "\"NetworkSettings\":{\"Networks\":{\"sqs-stateful\":{\"IPAddress\":\"10.89.1.7\"}}}}]";
    }

    private static Map<String, String> provenance(String env, String ref, String instanceId) {
        return Map.of(
                "studioquack.sqs.env", env,
                "studioquack.sqs.ref", ref,
                "studioquack.sqs.instance", instanceId);
    }

    @Test
    void adoptsRunningSurvivor_andSecondStartIsRefused() {
        psIds = List.of("ctr-alive");
        inspectById.put("ctr-alive", inspectJson("ctr-alive", "running",
                provenance("env-1", "myenv/pg:1", "inst_surviving"), "2026-08-10T10:00:00Z"));

        StatefulReconcileReport report = manager.reconcileWithHost();

        assertEquals(1, report.adopted());
        assertEquals(0, report.reaped());
        assertTrue(manager.hasLiveInstance("env-1", "myenv/pg:1"),
                "adopted container must restore the live-instance guard");
        assertEquals("ctr-alive",
                manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerId());
        // The adopted instance joins the status inventory (remote list merge sees it).
        assertTrue(manager.inventory().stream().anyMatch(i -> "inst_surviving".equals(i.instanceId())));
        // Never touched with a removal.
        assertFalse(cli.sawCallContaining("rm", "ctr-alive"));

        // After reconcile: a second start against the adopted key joins the survivor (same ids, no
        // new container) — the control plane only sends a start when it believes nothing is live,
        // and joining heals that divergence without bouncing the adopted container.
        StatefulInstanceManager.StartResult second =
                manager.start("env-1", "myenv/pg:1", true, StatefulCaps.defaults());
        assertEquals(StatefulStatus.RUNNING, second.status());
        assertEquals("inst_surviving", second.instanceId(), "join returns the adopted identity");
        assertEquals("ctr-alive", second.containerId(), "join returns the adopted container");
        assertFalse(cli.sawCallContaining("stop", "sq-stateful-inst_surviving"),
                "join never bounces the adopted container");
        assertFalse(cli.sawCallContaining("rm", "-f", "sq-stateful-inst_surviving"),
                "join never removes the adopted container");
    }

    @Test
    void reapsNonRunningResidue_andUnblocksAFreshStart() {
        // Restart DURING a start: the container exists in `created`, the row says STARTING — the
        // residue is reaped so the key is free again.
        psIds = List.of("ctr-created");
        inspectById.put("ctr-created", inspectJson("ctr-created", "created",
                provenance("env-1", "myenv/pg:1", "inst_interrupted"), "2026-08-10T10:00:00Z"));

        StatefulReconcileReport report = manager.reconcileWithHost();

        assertEquals(1, report.reaped());
        assertEquals(0, report.adopted());
        assertTrue(cli.sawCallContaining("rm", "-f", "ctr-created"), "residue must be removed");
        assertFalse(manager.hasLiveInstance("env-1", "myenv/pg:1"));

        // …and a fresh start proceeds (row stops blocking once the server consumes the same report).
        StatefulInstanceManager.StartResult started =
                manager.start("env-1", "myenv/pg:1", true, StatefulCaps.defaults());
        assertEquals(StatefulStatus.RUNNING, started.status());
    }

    @Test
    void reapsYoungerDuplicate_keepsElderContainer() {
        psIds = List.of("ctr-young", "ctr-elder");
        inspectById.put("ctr-elder", inspectJson("ctr-elder", "running",
                provenance("env-1", "myenv/pg:1", "inst_elder"), "2026-08-10T10:00:00Z"));
        inspectById.put("ctr-young", inspectJson("ctr-young", "running",
                provenance("env-1", "myenv/pg:1", "inst_young"), "2026-08-10T10:05:00Z"));

        StatefulReconcileReport report = manager.reconcileWithHost();

        assertEquals(1, report.adopted(), "the elder container is adopted");
        assertEquals(1, report.reapedDuplicate(), "the younger duplicate is reaped");
        assertTrue(cli.sawCallContaining("rm", "-f", "ctr-young"));
        assertFalse(cli.sawCallContaining("rm", "-f", "ctr-elder"));
        assertEquals("ctr-elder",
                manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow().containerId(),
                "exactly one live instance per (env, imageRef) survives");
    }

    @Test
    void ignoresRunningContainerWithoutProvenance_neverReapsIt() {
        psIds = List.of("ctr-foreign");
        inspectById.put("ctr-foreign", inspectJson("ctr-foreign", "running", Map.of(),
                "2026-08-10T10:00:00Z"));

        StatefulReconcileReport report = manager.reconcileWithHost();

        assertEquals(1, report.ignored());
        assertEquals(0, report.adopted());
        assertFalse(cli.sawCallContaining("rm", "-f", "ctr-foreign"),
                "a running container we cannot identify is never reaped on a guess");
        assertTrue(manager.inventory().isEmpty());
    }

    @Test
    void pausedSurvivorIsIgnored_notReaped() {
        psIds = List.of("ctr-paused");
        inspectById.put("ctr-paused", inspectJson("ctr-paused", "paused",
                provenance("env-1", "myenv/pg:1", "inst_paused"), "2026-08-10T10:00:00Z"));

        StatefulReconcileReport report = manager.reconcileWithHost();

        assertEquals(1, report.ignored());
        assertFalse(cli.sawCallContaining("rm", "-f", "ctr-paused"),
                "a paused workload is suspended, not dead — left untouched");
    }

    @Test
    void noRuntimeProducesAnEmptyReport() {
        cli.available = false;

        StatefulReconcileReport report = manager.reconcileWithHost();

        assertFalse(report.runtimeAvailable());
        assertEquals(0, report.adopted());
        assertTrue(report.entries().isEmpty());
        assertNull(cli.firstCallWith("ps"), "no runtime → no enumeration attempted");
    }

    @Test
    void reportIsRetainedForTheStatusPayload() {
        psIds = List.of("ctr-alive");
        inspectById.put("ctr-alive", inspectJson("ctr-alive", "running",
                provenance("env-1", "myenv/pg:1", "inst_surviving"), "2026-08-10T10:00:00Z"));

        assertNull(manager.lastReconcile(), "no report before the first boot pass");
        manager.reconcileWithHost();
        StatefulReconcileReport retained = manager.lastReconcile();
        assertNotNull(retained);
        assertEquals(1, retained.adopted());
        assertEquals("inst_surviving", retained.entries().get(0).instanceId());
    }

    @Test
    void adoptsDispatchCapableFromContainerLabel() {
        psIds = List.of("ctr-lib");
        Map<String, String> labels = new java.util.LinkedHashMap<>(
                provenance("env-1", "sq-local/sq_built-in_node_library:1", "inst_lib"));
        labels.put(StatefulRunArgs.DISPATCH_LABEL, "1");
        inspectById.put("ctr-lib", inspectJson("ctr-lib", "running", labels, "2026-08-10T10:00:00Z"));

        manager.reconcileWithHost();

        StatefulInstance adopted = manager.resolveRunning("env-1", "sq-local/sq_built-in_node_library:1")
                .orElseThrow();
        assertTrue(adopted.dispatchCapable(),
                "D109 must rebuild dispatchCapable from the container label");
    }

    @Test
    void adoptedUnlabeledContainerIsNotDispatchCapable() {
        psIds = List.of("ctr-alive");
        inspectById.put("ctr-alive", inspectJson("ctr-alive", "running",
                provenance("env-1", "myenv/pg:1", "inst_surviving"), "2026-08-10T10:00:00Z"));

        manager.reconcileWithHost();

        StatefulInstance adopted = manager.resolveRunning("env-1", "myenv/pg:1").orElseThrow();
        assertFalse(adopted.dispatchCapable(), "unlabeled survivor stays FIFO");
    }

    /**
     * Re-installed engine, new env id: the library container from the previous enrolment survives
     * labelled with the OLD env. The current env's start joins it by instance id — and from then on
     * the container must answer under the current (env, image) key, or every library node dispatch
     * misses it and fails "image cold start failed" while the library shows RUNNING (Oct 2026).
     */
    @Test
    void joiningASurvivorFromAPreviousEnrolmentRebindsItToTheCurrentEnv() {
        String ref = "sq-local/sq-builtin-node-library:1.0.0";
        psIds = List.of("ctr-lib");
        inspectById.put("ctr-lib", inspectJson("ctr-lib", "running",
                provenance("env-old", ref, "sq-builtin-node-library"), "2026-10-13T00:00:00Z"));
        manager.reconcileWithHost();

        StatefulInstanceManager.StartResult joined = manager.start("env-new", ref, true,
                StatefulCaps.uncapped(), "sq-builtin-node-library", null, null, null);

        assertEquals(StatefulStatus.RUNNING, joined.status());
        assertEquals("ctr-lib", joined.containerId(), "joins the survivor, no second container");
        assertEquals("ctr-lib", manager.resolveRunning("env-new", ref).orElseThrow().containerId(),
                "the dispatch key of the current env must reach the joined container");
        assertTrue(manager.resolveRunning("env-old", ref).isEmpty(), "the stale key is retired");
        assertFalse(cli.sawCallContaining("run"), "never starts a second library container");
    }

    /** A reconcile after the rebind (overlay bounce) must not adopt the same container a second time. */
    @Test
    void reconcileAfterARebindKeepsTheRebound() {
        String ref = "sq-local/sq-builtin-node-library:1.0.0";
        psIds = List.of("ctr-lib");
        inspectById.put("ctr-lib", inspectJson("ctr-lib", "running",
                provenance("env-old", ref, "sq-builtin-node-library"), "2026-10-13T00:00:00Z"));
        manager.reconcileWithHost();
        manager.start("env-new", ref, true, StatefulCaps.uncapped(), "sq-builtin-node-library", null, null, null);

        manager.reconcileWithHost();

        assertEquals("ctr-lib", manager.resolveRunning("env-new", ref).orElseThrow().containerId());
        assertTrue(manager.resolveRunning("env-old", ref).isEmpty(), "label env must not re-key it back");
        assertFalse(cli.sawCallContaining("rm", "ctr-lib"), "never reaped as a duplicate of itself");
    }
}
