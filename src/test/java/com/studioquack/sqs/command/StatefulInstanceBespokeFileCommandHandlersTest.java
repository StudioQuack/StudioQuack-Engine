package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deny-path + list happy-path unit tests for {@link StatefulInstanceDeleteFileCommandHandler} and
 * {@link StatefulInstanceListFilesCommandHandler}.
 */
class StatefulInstanceBespokeFileCommandHandlersTest {

    private static final String ENV = "env-1";
    private static final String INSTANCE_ID = "inst_abc";
    private static final String TEMPLATE_ID = "tmpl_hello";
    private static final String FILE_NAME = "handler.py";

    private final ObjectMapper mapper = new ObjectMapper();
    private StatefulInstanceManager manager;
    private FakeOciCli cli;
    private StatefulInstanceDeleteFileCommandHandler deleteHandler;
    private StatefulInstanceListFilesCommandHandler listHandler;

    @BeforeEach
    void setUp() {
        manager = mock(StatefulInstanceManager.class);
        cli = new FakeOciCli();
        deleteHandler = new StatefulInstanceDeleteFileCommandHandler(mapper, manager, cli);
        listHandler = new StatefulInstanceListFilesCommandHandler(mapper, manager, cli);
    }

    // --- delete deny paths -------------------------------------------------

    @Test
    void delete_denies_traversal_name_dotdot() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        DaemonCommandOutcome out = deleteHandler.handle(deleteCmd("../x", TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("basename") || out.error().contains("name"),
                () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void delete_denies_traversal_name_slash() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        DaemonCommandOutcome out = deleteHandler.handle(deleteCmd("a/b", TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("basename") || out.error().contains("name"),
                () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void delete_denies_bad_templateId() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        DaemonCommandOutcome out = deleteHandler.handle(deleteCmd(FILE_NAME, "../../etc"));
        assertFailed(out);
        assertTrue(out.error().toLowerCase().contains("template"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void delete_denies_non_running() throws Exception {
        StatefulInstance stopped = runningUserInstance();
        stopped.setStatus(StatefulStatus.STOPPED);
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(stopped));

        DaemonCommandOutcome out = deleteHandler.handle(deleteCmd(FILE_NAME, TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("not running"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void delete_denies_platform_managed() throws Exception {
        StatefulInstance docling = new StatefulInstance(
                "docling", ENV, "quay.io/other/app:1", "c-docling", "v-docling",
                StatefulCaps.defaults(), true);
        docling.setStatus(StatefulStatus.RUNNING);
        docling.setContainerId("cid-docling");
        when(manager.byInstanceId("docling")).thenReturn(Optional.of(docling));

        ObjectNode payload = deletePayload(FILE_NAME, TEMPLATE_ID);
        payload.put("instanceId", "docling");
        DaemonCommandOutcome out = deleteHandler.handle(cmd(
                StatefulInstanceDeleteFileCommandHandler.TYPE, payload));
        assertFailed(out);
        assertTrue(out.error().contains("platform-managed"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void delete_denies_unknown_instance() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.empty());
        DaemonCommandOutcome out = deleteHandler.handle(deleteCmd(FILE_NAME, TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("unknown instanceId"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void delete_happy_path_uses_argv_rm() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        cli.responder = argv -> new OciCli.Result(0, "", "", false);

        DaemonCommandOutcome out = deleteHandler.handle(deleteCmd(FILE_NAME, TEMPLATE_ID));
        assertEquals(DaemonCommandOutcome.STATUS_OK, out.status(), () -> out.error());
        assertTrue(cli.sawCallContaining("exec", "rm", "-f",
                BespokeFilePaths.containerPath(TEMPLATE_ID, FILE_NAME)));
        List<String> call = cli.firstCallWith("rm");
        // Path must be its own argv element (no shell wrapper).
        assertTrue(call.contains("rm") && call.contains("-f")
                        && call.contains(BespokeFilePaths.containerPath(TEMPLATE_ID, FILE_NAME))
                        && !call.contains("sh"),
                () -> "expected argv rm -f <path>, got: " + call);
    }

    // --- list deny paths ---------------------------------------------------

    @Test
    void list_denies_bad_templateId() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        DaemonCommandOutcome out = listHandler.handle(listCmd("../../etc"));
        assertFailed(out);
        assertTrue(out.error().toLowerCase().contains("template"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void list_denies_non_running() throws Exception {
        StatefulInstance stopped = runningUserInstance();
        stopped.setStatus(StatefulStatus.STOPPED);
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(stopped));

        DaemonCommandOutcome out = listHandler.handle(listCmd(TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("not running"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void list_denies_platform_managed() throws Exception {
        StatefulInstance inst = new StatefulInstance(
                INSTANCE_ID, ENV, "quay.io/docling-project/docling-serve-cpu:latest",
                "c-1", "v-1", StatefulCaps.defaults(), true);
        inst.setStatus(StatefulStatus.RUNNING);
        inst.setContainerId("cid-1");
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(inst));

        DaemonCommandOutcome out = listHandler.handle(listCmd(TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("platform-managed"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void list_denies_unknown_instance() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.empty());
        DaemonCommandOutcome out = listHandler.handle(listCmd(TEMPLATE_ID));
        assertFailed(out);
        assertTrue(out.error().contains("unknown instanceId"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    // --- list happy path ---------------------------------------------------

    @Test
    void list_happy_path_parses_tsv_and_skips_bad_names() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        String canned = "handler.py\t128\t1700000000\n"
                + "../evil\t9\t1\n"
                + "ok.txt\t3\t1700000001\n"
                + "a/b\t1\t2\n";
        cli.responder = argv -> new OciCli.Result(0, canned, "", false);

        DaemonCommandOutcome out = listHandler.handle(listCmd(TEMPLATE_ID));
        assertEquals(DaemonCommandOutcome.STATUS_OK, out.status(), () -> out.error());

        JsonNode root = mapper.readTree(out.output());
        JsonNode files = root.get("files");
        assertEquals(2, files.size(), () -> out.output());
        assertEquals("handler.py", files.get(0).get("name").asText());
        assertEquals(128L, files.get(0).get("sizeBytes").asLong());
        assertEquals(1700000000L, files.get(0).get("mtime").asLong());
        assertEquals("ok.txt", files.get(1).get("name").asText());
        assertEquals(3L, files.get(1).get("sizeBytes").asLong());
        assertEquals(1700000001L, files.get(1).get("mtime").asLong());

        // The listing script rides stdin (sh -s): its double quotes would be shattered by the
        // Win32 command-line round-trip on Windows hosts (see ShExec).
        assertTrue(cli.sawCallContaining("exec", "sh", "-s"));
        String script = new String(cli.lastStdin, java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(script.contains(BespokeFilePaths.containerDir(TEMPLATE_ID)),
                () -> "script must scope to template dir: " + script);
    }

    @Test
    void list_empty_dir_returns_empty_files_array() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        cli.responder = argv -> new OciCli.Result(0, "", "", false);

        DaemonCommandOutcome out = listHandler.handle(listCmd(TEMPLATE_ID));
        assertEquals(DaemonCommandOutcome.STATUS_OK, out.status(), () -> out.error());
        assertEquals("{\"files\":[]}", out.output().replace(" ", ""));
    }

    // --- helpers -----------------------------------------------------------

    private StatefulInstance runningUserInstance() {
        StatefulInstance inst = new StatefulInstance(
                INSTANCE_ID, ENV, "myenv/app:1", "c-user", "v-user",
                StatefulCaps.defaults(), true);
        inst.setStatus(StatefulStatus.RUNNING);
        inst.setContainerId("cid-user");
        return inst;
    }

    private ObjectNode deletePayload(String name, String templateId) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("envId", ENV);
        payload.put("imageRef", "myenv/app:1");
        payload.put("instanceId", INSTANCE_ID);
        payload.put("templateId", templateId);
        payload.put("name", name);
        return payload;
    }

    private ObjectNode listPayload(String templateId) {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("envId", ENV);
        payload.put("imageRef", "myenv/app:1");
        payload.put("instanceId", INSTANCE_ID);
        payload.put("templateId", templateId);
        return payload;
    }

    private DaemonCommand deleteCmd(String name, String templateId) throws Exception {
        return cmd(StatefulInstanceDeleteFileCommandHandler.TYPE, deletePayload(name, templateId));
    }

    private DaemonCommand listCmd(String templateId) throws Exception {
        return cmd(StatefulInstanceListFilesCommandHandler.TYPE, listPayload(templateId));
    }

    private DaemonCommand cmd(String type, ObjectNode payload) throws Exception {
        return new DaemonCommand("cmd-bespoke", type, "2026-08-06T00:00:00Z",
                mapper.writeValueAsString(payload));
    }

    private static void assertFailed(DaemonCommandOutcome out) {
        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status(),
                () -> "expected failed, got ok: " + out.output());
    }
}
