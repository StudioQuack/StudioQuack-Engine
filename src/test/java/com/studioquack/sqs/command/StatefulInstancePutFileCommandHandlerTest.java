package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.common.security.envelope.Direction;
import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.common.security.envelope.SecureEnvelope;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.sandbox.stateful.FakeOciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deny-path unit tests for {@link StatefulInstancePutFileCommandHandler}. Envelope sealing matches
 * {@link SecretCommandHandlersTest} (bearer IKM, to the agent).
 */
class StatefulInstancePutFileCommandHandlerTest {

    private static final String ENV = "env-1";
    private static final String BEARER = "sqvm_test_bearer";
    private static final String INSTANCE_ID = "inst_abc";
    private static final String TEMPLATE_ID = "tmpl_hello";
    private static final String FILE_NAME = "handler.py";

    private final ObjectMapper mapper = new ObjectMapper();
    private SqsChannelProperties channel;
    private StatefulInstanceManager manager;
    private FakeOciCli cli;
    private StatefulInstancePutFileCommandHandler handler;

    @BeforeEach
    void setUp() {
        channel = new SqsChannelProperties();
        channel.setEnvId(ENV);
        channel.setBearer(BEARER);
        manager = mock(StatefulInstanceManager.class);
        cli = new FakeOciCli();
        handler = new StatefulInstancePutFileCommandHandler(mapper, manager, cli, channel);
    }

    @Test
    void denies_traversal_name_dotdot() throws Exception {
        assertBasenameDenied("../x");
    }

    @Test
    void denies_traversal_name_slash() throws Exception {
        assertBasenameDenied("a/b");
    }

    @Test
    void denies_traversal_name_backslash() throws Exception {
        assertBasenameDenied("a\\b");
    }

    @Test
    void denies_bad_templateId_path() throws Exception {
        assertTemplateIdDenied("../../etc");
    }

    @Test
    void denies_bad_templateId_empty() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        ObjectNode payload = validPayload(FILE_NAME, TEMPLATE_ID, "hi".getBytes(StandardCharsets.UTF_8));
        payload.put("templateId", "");
        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().toLowerCase().contains("template"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void denies_bad_templateId_too_long() throws Exception {
        assertTemplateIdDenied(
                "this-template-id-is-way-too-long-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    }

    @Test
    void denies_bad_templateId_illegal_chars() throws Exception {
        assertTemplateIdDenied("bad template!");
    }

    @Test
    void denies_non_running_instance() throws Exception {
        StatefulInstance stopped = runningUserInstance();
        stopped.setStatus(StatefulStatus.STOPPED);
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(stopped));

        DaemonCommandOutcome out = handler.handle(cmd(
                validPayload(FILE_NAME, TEMPLATE_ID, "hi".getBytes(StandardCharsets.UTF_8))));
        assertFailed(out);
        assertTrue(out.error().contains("not running"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void denies_platform_managed_by_instanceId_docling() throws Exception {
        StatefulInstance docling = new StatefulInstance(
                "docling", ENV, "quay.io/other/app:1", "c-docling", "v-docling",
                StatefulCaps.defaults(), true);
        docling.setStatus(StatefulStatus.RUNNING);
        docling.setContainerId("cid-docling");
        when(manager.byInstanceId("docling")).thenReturn(Optional.of(docling));

        ObjectNode payload = validPayload(FILE_NAME, TEMPLATE_ID, "hi".getBytes(StandardCharsets.UTF_8));
        payload.put("instanceId", "docling");
        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("platform-managed"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void denies_platform_managed_by_docling_serve_image() throws Exception {
        StatefulInstance inst = new StatefulInstance(
                INSTANCE_ID, ENV, "quay.io/docling-project/docling-serve-cpu:latest",
                "c-1", "v-1", StatefulCaps.defaults(), true);
        inst.setStatus(StatefulStatus.RUNNING);
        inst.setContainerId("cid-1");
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(inst));

        DaemonCommandOutcome out = handler.handle(cmd(
                validPayload(FILE_NAME, TEMPLATE_ID, "hi".getBytes(StandardCharsets.UTF_8))));
        assertFailed(out);
        assertTrue(out.error().contains("platform-managed"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void denies_sha256_mismatch_after_unseal() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        byte[] raw = "hello-bespoke".getBytes(StandardCharsets.UTF_8);
        ObjectNode payload = validPayload(FILE_NAME, TEMPLATE_ID, raw);
        payload.put("sha256", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef");

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("sha256"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void denies_sizeBytes_mismatch_after_unseal() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        byte[] raw = "hello-bespoke".getBytes(StandardCharsets.UTF_8);
        ObjectNode payload = validPayload(FILE_NAME, TEMPLATE_ID, raw);
        payload.put("sizeBytes", raw.length + 5);

        DaemonCommandOutcome out = handler.handle(cmd(payload));
        assertFailed(out);
        assertTrue(out.error().contains("size"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    @Test
    void denies_unknown_instanceId() throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.empty());

        DaemonCommandOutcome out = handler.handle(cmd(
                validPayload(FILE_NAME, TEMPLATE_ID, "hi".getBytes(StandardCharsets.UTF_8))));
        assertFailed(out);
        assertTrue(out.error().contains("unknown instanceId"), () -> out.error());
        assertTrue(cli.calls.isEmpty());
    }

    // --- helpers -----------------------------------------------------------

    private void assertBasenameDenied(String badName) throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        DaemonCommandOutcome out = handler.handle(cmd(
                validPayload(badName, TEMPLATE_ID, "hi".getBytes(StandardCharsets.UTF_8))));
        assertFailed(out);
        assertTrue(out.error().contains("basename") || out.error().contains("name"),
                () -> "expected basename refusal, got: " + out.error());
        assertTrue(cli.calls.isEmpty(), "cli must not run on deny");
    }

    private void assertTemplateIdDenied(String badTid) throws Exception {
        when(manager.byInstanceId(INSTANCE_ID)).thenReturn(Optional.of(runningUserInstance()));
        DaemonCommandOutcome out = handler.handle(cmd(
                validPayload(FILE_NAME, badTid, "hi".getBytes(StandardCharsets.UTF_8))));
        assertFailed(out);
        assertTrue(out.error().toLowerCase().contains("template"),
                () -> "expected templateId refusal, got: " + out.error());
        assertTrue(cli.calls.isEmpty());
    }

    private StatefulInstance runningUserInstance() {
        StatefulInstance inst = new StatefulInstance(
                INSTANCE_ID, ENV, "myenv/app:1", "c-user", "v-user",
                StatefulCaps.defaults(), true);
        inst.setStatus(StatefulStatus.RUNNING);
        inst.setContainerId("cid-user");
        return inst;
    }

    private ObjectNode validPayload(String name, String templateId, byte[] raw) throws Exception {
        SecureEnvelope sealed = EnvelopeCrypto.seal(
                EnvelopeCrypto.ikmFromBearer(BEARER),
                Direction.BE_TO_AGENT,
                ENV,
                StatefulInstancePutFileCommandHandler.AAD_BESPOKE_PUT,
                raw);
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw));
        ObjectNode payload = mapper.createObjectNode();
        payload.put("envId", ENV);
        payload.put("imageRef", "myenv/app:1");
        payload.put("instanceId", INSTANCE_ID);
        payload.put("templateId", templateId);
        payload.put("name", name);
        payload.put("sizeBytes", raw.length);
        payload.put("sha256", sha);
        payload.set("content", mapper.valueToTree(sealed));
        return payload;
    }

    private DaemonCommand cmd(ObjectNode payload) throws Exception {
        return new DaemonCommand("cmd-put", StatefulInstancePutFileCommandHandler.TYPE,
                "2026-08-06T00:00:00Z", mapper.writeValueAsString(payload));
    }

    private static void assertFailed(DaemonCommandOutcome out) {
        assertEquals(DaemonCommandOutcome.STATUS_FAILED, out.status(),
                () -> "expected failed, got ok: " + out.output());
    }
}
