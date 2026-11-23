package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The temporary container a Create Node trial job runs in: started for one job, invisible to
 * routing, inventory and the host reconcile, and removed with its volume afterwards.
 */
class TemporaryContainerTest {

    private static final String ENV = "env-1";
    private static final String IMAGE = "localhost/sq-local/egress-ip:6";

    @TempDir
    Path blobDir;

    private FakeOciCli cli;
    private StatefulRuntimeProperties props;
    private StatefulInstanceManager manager;
    /** Names {@code podman ps --filter label=studioquack.sqs.temporary=1} reports. */
    private String temporaryContainersOnHost = "";

    @BeforeEach
    void setUp() {
        cli = new FakeOciCli();
        props = new StatefulRuntimeProperties();
        props.setBlobStore(blobDir.toString());
        cli.responder = this::respond;
        manager = new StatefulInstanceManager(cli, new ObjectMapper(), null, props,
                new OciImageBlobStore(props), null, false, 10);
    }

    private OciCli.Result respond(List<String> argv) {
        String sub = argv.size() > 1 ? argv.get(1) : "";
        return switch (sub) {
            case "run" -> new OciCli.Result(0, "tmp-container-1\n", "", false);
            case "ps" -> new OciCli.Result(0, temporaryContainersOnHost, "", false);
            case "volume" -> argv.contains("exists") ? new OciCli.Result(1, "", "no such volume", false)
                    : new OciCli.Result(0, "", "", false);
            case "inspect" -> new OciCli.Result(0,
                    "[{\"Id\":\"tmp-container-1\",\"State\":{\"Status\":\"running\"}}]", "", false);
            default -> new OciCli.Result(0, "", "", false);
        };
    }

    private static String containerNameOf(String instanceId) {
        return StatefulInstanceManager.TEMPORARY_CONTAINER_PREFIX
                + instanceId.substring(StatefulInstanceManager.TEMPORARY_INSTANCE_PREFIX.length());
    }

    @Test
    void a_temporary_container_carries_its_own_label_and_stays_out_of_routing_and_inventory() {
        StatefulInstanceManager.StartResult started = manager.startTemporary(ENV, IMAGE);

        assertEquals(StatefulStatus.RUNNING, started.status(), String.valueOf(started.message()));
        assertTrue(started.instanceId().startsWith(StatefulInstanceManager.TEMPORARY_INSTANCE_PREFIX));
        List<String> run = cli.firstCallWith("run");
        assertTrue(run.contains(containerNameOf(started.instanceId())), run.toString());
        assertTrue(run.contains(StatefulRunArgs.TEMPORARY_LABEL + "=1"), run.toString());
        assertFalse(run.contains("studioquack.sqs.stateful=1"), "the stateful table and reconcile must not see it");
        assertFalse(run.contains("--restart"), "a crash ends the job; nothing restarts the container");
        assertTrue(manager.resolveRunning(ENV, IMAGE).isEmpty(), "no (env, imageRef) dispatch may land on it");
        assertTrue(manager.inventory().isEmpty());
        assertTrue(manager.byInstanceId(started.instanceId()).isEmpty());
        assertTrue(manager.temporary(started.instanceId())
                .filter(StatefulInstance::isRunning).filter(StatefulInstance::isTemporary).isPresent());
    }

    @Test
    void removing_it_kills_the_container_and_drops_its_volume_once() {
        StatefulInstanceManager.StartResult started = manager.startTemporary(ENV, IMAGE);
        String name = containerNameOf(started.instanceId());
        long removalsBefore = removalsOf(name);

        manager.removeTemporary(started.instanceId());
        manager.removeTemporary(started.instanceId());

        assertEquals(removalsBefore + 1, removalsOf(name), "a second remove is a no-op");
        assertTrue(cli.sawCallContaining("volume", "rm", "-f", props.getVolumeNamePrefix() + started.instanceId()));
        assertTrue(manager.temporary(started.instanceId()).isEmpty());
    }

    /** {@code podman rm} calls naming the container (the start's stale-name cleanup included). */
    private long removalsOf(String containerName) {
        return cli.calls.stream()
                .filter(argv -> argv.size() > 1 && "rm".equals(argv.get(1)) && argv.contains(containerName))
                .count();
    }

    @Test
    void every_start_gets_a_container_of_its_own() {
        String first = manager.startTemporary(ENV, IMAGE).instanceId();
        String second = manager.startTemporary(ENV, IMAGE).instanceId();

        assertNotEquals(first, second);
        assertEquals(2, cli.calls.stream().filter(argv -> argv.size() > 1 && "run".equals(argv.get(1))).count());
    }

    @Test
    void the_boot_sweep_removes_temporary_containers_an_earlier_process_left_behind() {
        temporaryContainersOnHost = "sq-tmp-old1\nsq-tmp-old2\n";

        assertEquals(2, manager.reapLeftoverTemporaries());
        assertTrue(cli.sawCallContaining("ps", "--filter", "label=" + StatefulRunArgs.TEMPORARY_LABEL + "=1"));
        assertTrue(cli.sawCallContaining("rm", "-f", "sq-tmp-old1"));
        assertTrue(cli.sawCallContaining("volume", "rm", "-f", props.getVolumeNamePrefix() + "tmp_old2"));
    }

    @Test
    void the_boot_sweep_spares_a_container_this_process_is_running_a_job_in() {
        String live = manager.startTemporary(ENV, IMAGE).instanceId();
        temporaryContainersOnHost = containerNameOf(live) + "\n";

        assertEquals(0, manager.reapLeftoverTemporaries());
        assertTrue(manager.temporary(live).isPresent());
    }

    @Test
    void an_operator_instance_never_carries_the_temporary_label() {
        manager.start(ENV, IMAGE, true, StatefulCaps.defaults());

        List<String> run = cli.firstCallWith("run");
        assertTrue(run.contains("studioquack.sqs.stateful=1"), run.toString());
        assertFalse(run.contains(StatefulRunArgs.TEMPORARY_LABEL + "=1"));
    }

    @Test
    void staged_files_land_in_the_temporary_containers_bespoke_folder() throws Exception {
        String instanceId = manager.startTemporary(ENV, IMAGE).instanceId();
        Path staged = stagedWorkspace();
        Files.createDirectories(staged.resolve("nested"));
        Files.writeString(staged.resolve("nested").resolve("not-a-bespoke-file.txt"), "x");

        manager.installTemporaryFiles(instanceId, "tmpl-1", staged);

        List<Integer> writes = bespokeWrites();
        assertEquals(1, writes.size(), "only top-level files are bespoke files: " + cli.calls);
        List<String> argv = cli.calls.get(writes.get(0));
        assertTrue(argv.containsAll(List.of("exec", "-i", containerNameOf(instanceId))), argv.toString());
        assertTrue(argv.get(argv.size() - 1).contains("/data/sq-bespoke/tmpl-1/workspace.zip"), argv.toString());
        assertEquals("zip-bytes", new String(Base64.getDecoder().decode(cli.stdins.get(writes.get(0)))));
    }

    @Test
    void staged_files_never_go_into_an_operator_instance() throws Exception {
        manager.start(ENV, IMAGE, true, StatefulCaps.defaults());
        String operatorInstance = manager.resolveRunning(ENV, IMAGE).orElseThrow().instanceId();
        Path staged = stagedWorkspace();

        assertThrows(IOException.class, () -> manager.installTemporaryFiles(operatorInstance, "tmpl-1", staged));
        assertTrue(bespokeWrites().isEmpty());
    }

    @Test
    void a_failed_write_reports_the_containers_error() throws Exception {
        String instanceId = manager.startTemporary(ENV, IMAGE).instanceId();
        Path staged = stagedWorkspace();
        cli.responder = argv -> argv.contains("exec")
                ? new OciCli.Result(1, "", "sh: base64: not found", false) : respond(argv);

        IOException e = assertThrows(IOException.class,
                () -> manager.installTemporaryFiles(instanceId, "tmpl-1", staged));
        assertTrue(e.getMessage().contains("base64: not found"), e.getMessage());
    }

    private Path stagedWorkspace() throws IOException {
        Path staged = Files.createDirectories(blobDir.resolve("staged"));
        Files.writeString(staged.resolve("workspace.zip"), "zip-bytes");
        return staged;
    }

    /** Indexes of the CLI calls that decode a file into a container. */
    private List<Integer> bespokeWrites() {
        List<Integer> out = new ArrayList<>();
        for (int i = 0; i < cli.calls.size(); i++) {
            List<String> argv = cli.calls.get(i);
            if (!argv.isEmpty() && argv.get(argv.size() - 1).contains("base64 -d")) {
                out.add(i);
            }
        }
        return out;
    }
}
