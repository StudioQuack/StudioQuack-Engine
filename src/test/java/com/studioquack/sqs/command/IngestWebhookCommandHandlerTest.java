package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.webhook.EngineApiKeyScope;
import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.sqs.channel.SecurityTelemetryAccumulator;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.command.IngestWebhookCommandHandler.IngestWebhookRequest;
import com.studioquack.sqs.command.IngestWebhookCommandHandler.IngestWebhookResponse;
import com.studioquack.sqs.command.MessageCorrelationService.SqEventType;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventRequest;
import com.studioquack.sqs.sandbox.helperchan.WebhookIngressOutcome;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.webhook.DaemonWebhookKeyEntity;
import com.studioquack.sqs.webhook.DaemonWebhookKeyRepository;
import com.studioquack.sqs.webhook.DaemonWebhookKeyStore;
import com.studioquack.sqs.webhook.DeployedWebhookBindingEntity;
import com.studioquack.sqs.webhook.DeployedWebhookBindingStore;
import com.studioquack.sqs.webhook.WebhookIdempotencyStore;
import com.studioquack.sqs.webhook.WebhookNodeInvoker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IngestWebhookCommandHandlerTest {

    private static final String REF_ID = "whk_0123456789abcdef01234567";
    private static final String KEY_ID = "AbCd1234";

    private ObjectMapper mapper;
    private SqsChannelProperties chProps;
    private OsSecretManager osSecrets;
    private SecretStoreNamespace namespace;
    private DaemonWebhookKeyStore keyStore;
    private DeployedWebhookBindingStore bindingStore;
    private WebhookNodeInvoker nodeInvoker;
    private MessageCorrelationService correlationService;
    private SecurityTelemetryAccumulator telemetry;
    private AtomicReference<Instant> nowRef;
    private WebhookIdempotencyStore idempotencyStore;
    private IngestWebhookCommandHandler handler;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper();
        keyStore = mock(DaemonWebhookKeyStore.class);
        bindingStore = mock(DeployedWebhookBindingStore.class);
        nodeInvoker = mock(WebhookNodeInvoker.class);
        correlationService = mock(MessageCorrelationService.class);
        telemetry = new SecurityTelemetryAccumulator();
        nowRef = new AtomicReference<>(Instant.parse("2026-11-28T00:00:00Z"));
        Clock clock = new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return nowRef.get(); }
        };
        idempotencyStore = new WebhookIdempotencyStore(Duration.ofHours(24), 1000, clock);

        chProps = new SqsChannelProperties();
        osSecrets = mock(OsSecretManager.class);
        namespace = new SecretStoreNamespace(new com.studioquack.sqs.secrets.os.OsSecretManagerProperties(), chProps);
        handler = new IngestWebhookCommandHandler(
                mapper,
                chProps,
                osSecrets,
                namespace,
                keyStore,
                bindingStore,
                nodeInvoker,
                correlationService,
                idempotencyStore,
                Runnable::run); // synchronous deterministic executor for unit tests
        handler.setSecurityTelemetry(telemetry);
    }

    @Test
    void invalidSecret_returns401_andRecordsSecurityTelemetry() {
        when(keyStore.verify(REF_ID, KEY_ID, "URL_KEY", "bad", EngineApiKeyScope.NODE_SCOPED))
                .thenReturn(DaemonWebhookKeyStore.VerificationResult.denied("invalid_api_secret"));

        IngestWebhookResponse resp = handler.process(request("POST", "bad", "{\"hi\":1}", "application/json"));

        assertFalse(resp.secretValid());
        assertFalse(resp.accepted());
        assertEquals(401, resp.httpStatus());
        assertEquals(1, telemetry.drain().get("webhook_key_auth"));
        verify(nodeInvoker, never()).invoke(any());
    }

    @Test
    void engineWideKey_rejectedWith401OnNodeScopedWebhookIngest() {
        Map<String, DaemonWebhookKeyEntity> db = new HashMap<>();
        DaemonWebhookKeyRepository repo = mock(DaemonWebhookKeyRepository.class);
        when(repo.existsById(anyString())).thenAnswer(inv -> db.containsKey(inv.getArgument(0)));
        when(repo.findById(anyString())).thenAnswer(inv -> Optional.ofNullable(db.get(inv.getArgument(0))));
        when(repo.findByKeyId(anyString())).thenAnswer(inv -> {
            String keyId = inv.getArgument(0);
            return db.values().stream().filter(e -> keyId.equals(e.getKeyId())).findFirst();
        });
        when(repo.save(any(DaemonWebhookKeyEntity.class))).thenAnswer(inv -> {
            DaemonWebhookKeyEntity e = inv.getArgument(0);
            db.put(e.getRefId(), e);
            return e;
        });

        DaemonWebhookKeyStore realStore = new DaemonWebhookKeyStore(repo);
        var engineKey = realStore.create(REF_ID, "Engine Wide Trigger", EngineApiKeyScope.ENGINE_WIDE, false);
        var parsed = WebhookKeyFormat.parse(engineKey.rawKey()).orElseThrow();

        IngestWebhookCommandHandler handlerWithRealStore = new IngestWebhookCommandHandler(
                mapper, chProps, osSecrets, namespace, realStore, bindingStore,
                nodeInvoker, correlationService, idempotencyStore, Runnable::run);
        handlerWithRealStore.setSecurityTelemetry(telemetry);

        String b64 = Base64.getEncoder().encodeToString("{\"hi\":1}".getBytes(StandardCharsets.UTF_8));
        IngestWebhookRequest req = new IngestWebhookRequest(
                "env1", engineKey.refId(), engineKey.keyId(), DaemonWebhookKeyStore.AUTH_MODE_URL_KEY,
                parsed.secret(), "POST", Map.of(), Map.of("content-type", "application/json"),
                b64, "application/json", "0123456789abcdef");

        IngestWebhookResponse resp = handlerWithRealStore.process(req);

        assertFalse(resp.secretValid());
        assertFalse(resp.accepted());
        assertEquals(401, resp.httpStatus());
        assertEquals("key_scope_mismatch", resp.rejectionReason());
        assertEquals(1, telemetry.drain().get("webhook_key_auth"));
        verify(bindingStore, never()).findByWebhookKeyRef(any());
        verify(nodeInvoker, never()).invoke(any());
    }

    @Test
    void handshakeChallenge_shortCircuitsWith200AndZeroMatches() throws Exception {
        when(keyStore.verify(REF_ID, KEY_ID, "URL_KEY", "good", EngineApiKeyScope.NODE_SCOPED))
                .thenReturn(DaemonWebhookKeyStore.VerificationResult.ok(
                        REF_ID, KEY_ID, EngineApiKeyScope.NODE_SCOPED, false));
        when(bindingStore.findByWebhookKeyRef(REF_ID))
                .thenReturn(List.of(binding("b1", "wf1", "n1", DeployedWebhookBindingStore.KIND_MESSAGE_START,
                        "__sq_hook_wf1_n1", "tpl-1", "hash-1")));
        when(nodeInvoker.invoke(any())).thenReturn(WebhookNodeInvoker.WebhookInvocationResult.success(
                new WebhookIngressOutcome(
                        new WebhookIngressOutcome.WebhookChallenge(200, "text/plain", "challenge-xyz"),
                        null, List.of())));

        IngestWebhookResponse resp = handler.process(new IngestWebhookRequest(
                "env1", REF_ID, KEY_ID, "URL_KEY", "good", "GET",
                Map.of("hub.mode", List.of("subscribe"), "hub.challenge", List.of("challenge-xyz")),
                Map.of(), "", null, "abcdef0123456789"));

        assertTrue(resp.secretValid());
        assertTrue(resp.isHandshake());
        assertTrue(resp.accepted());
        assertEquals(200, resp.httpStatus());
        assertEquals("text/plain", resp.contentType());
        assertEquals("challenge-xyz", resp.responseBody());
        assertEquals(0, resp.matchedCount());
        verify(correlationService, never()).correlateWebhook(any());
    }

    @Test
    void badProviderSignature_returns401WithProviderSignatureValidFalse() throws Exception {
        when(keyStore.verify(REF_ID, KEY_ID, "URL_KEY", "good", EngineApiKeyScope.NODE_SCOPED))
                .thenReturn(DaemonWebhookKeyStore.VerificationResult.ok(
                        REF_ID, KEY_ID, EngineApiKeyScope.NODE_SCOPED, false));
        when(bindingStore.findByWebhookKeyRef(REF_ID))
                .thenReturn(List.of(binding("b1", "wf1", "n1", DeployedWebhookBindingStore.KIND_MESSAGE_START,
                        "__sq_hook_wf1_n1", "tpl-1", "hash-1")));
        when(nodeInvoker.invoke(any())).thenReturn(WebhookNodeInvoker.WebhookInvocationResult.success(
                new WebhookIngressOutcome(
                        null,
                        new WebhookIngressOutcome.WebhookReject(401, "invalid_signature"),
                        List.of())));

        IngestWebhookResponse resp = handler.process(request("POST", "good", "{\"ev\":1}", "application/json"));

        assertTrue(resp.secretValid());
        assertFalse(resp.isHandshake());
        assertFalse(resp.providerSignatureValid());
        assertFalse(resp.accepted());
        assertEquals(401, resp.httpStatus());
        assertEquals("invalid_signature", resp.rejectionReason());
    }

    @Test
    void priorityRouting_boundaryBeatsCatchBeatsStart_andDeduplicatesIdenticalParsersAndIdempotency() throws Exception {
        when(keyStore.verify(REF_ID, KEY_ID, "URL_KEY", "good", EngineApiKeyScope.NODE_SCOPED))
                .thenReturn(DaemonWebhookKeyStore.VerificationResult.ok(
                        REF_ID, KEY_ID, EngineApiKeyScope.NODE_SCOPED, false));

        // All 3 bindings share the same (templateId, paramsHash) → parser invokes ONCE!
        var bBoundary = binding("b_bnd", "wf1", "n_host", DeployedWebhookBindingStore.KIND_BOUNDARY_MESSAGE,
                "__sq_hook_wf1_n_host_b1", "tpl-wa", "same-hash");
        var bCatch = binding("b_cat", "wf2", "n_sw", DeployedWebhookBindingStore.KIND_SEND_AND_WAIT,
                "__sq_hook_wf2_n_sw", "tpl-wa", "same-hash");
        var bStart = binding("b_str", "wf3", "n_listen", DeployedWebhookBindingStore.KIND_MESSAGE_START,
                "__sq_hook_wf3_n_listen", "tpl-wa", "same-hash");
        when(bindingStore.findByWebhookKeyRef(REF_ID)).thenReturn(List.of(bStart, bCatch, bBoundary));

        // Event 1 matches BOUNDARY → catch and start must NOT fire.
        when(nodeInvoker.invoke(any())).thenReturn(WebhookNodeInvoker.WebhookInvocationResult.success(
                new WebhookIngressOutcome(null, null, List.of(
                        new WebhookIngressOutcome.WebhookEmittedEvent("idem-1", "wa-user-1", Map.of("text", "cancel"))))));
        when(correlationService.correlateWebhook(any())).thenAnswer(inv -> {
            WebhookEventRequest r = inv.getArgument(0);
            if (r == null) {
                return null;
            }
            if (r.eventType() == SqEventType.BOUNDARY_MESSAGE) {
                return WebhookEventOutcome.correlated("pi-bnd", SqEventType.BOUNDARY_MESSAGE);
            }
            return WebhookEventOutcome.notCorrelated(r.eventType());
        });

        IngestWebhookResponse r1 = handler.process(request("POST", "good", "{\"text\":\"cancel\"}", "application/json"));
        assertTrue(r1.accepted());
        assertEquals(1, r1.matchedCount());
        assertEquals("idem-1", r1.idempotencyKey());
        verify(nodeInvoker, times(1)).invoke(any()); // deduplicated across all 3 bindings!

        ArgumentCaptor<WebhookEventRequest> cap1 = ArgumentCaptor.forClass(WebhookEventRequest.class);
        verify(correlationService, times(1)).correlateWebhook(cap1.capture());
        assertEquals(SqEventType.BOUNDARY_MESSAGE, cap1.getValue().eventType());

        // Duplicate idempotencyKey within 24h returns 200 accepted with matchedCount=0 and no correlation!
        IngestWebhookResponse dup = handler.process(request("POST", "good", "{\"text\":\"cancel\"}", "application/json"));
        assertTrue(dup.accepted());
        assertEquals(0, dup.matchedCount());
        verify(correlationService, times(1)).correlateWebhook(any());

        // Event 2 misses BOUNDARY, matches SEND_AND_WAIT/CATCH → start must NOT fire.
        when(nodeInvoker.invoke(any())).thenReturn(WebhookNodeInvoker.WebhookInvocationResult.success(
                new WebhookIngressOutcome(null, null, List.of(
                        new WebhookIngressOutcome.WebhookEmittedEvent("idem-2", "wa-user-2", Map.of("text", "reply"))))));
        when(correlationService.correlateWebhook(any())).thenAnswer(inv -> {
            WebhookEventRequest r = inv.getArgument(0);
            if (r == null) {
                return null;
            }
            if (r.eventType() == SqEventType.MESSAGE_CATCH) {
                return WebhookEventOutcome.correlated("pi-catch", SqEventType.MESSAGE_CATCH);
            }
            return WebhookEventOutcome.notCorrelated(r.eventType());
        });

        IngestWebhookResponse r2 = handler.process(request("POST", "good", "{\"text\":\"reply\"}", "application/json"));
        assertEquals(1, r2.matchedCount());
        verify(correlationService, never()).correlateWebhook(org.mockito.ArgumentMatchers.argThat(
                r -> r != null && r.eventType() == SqEventType.MESSAGE_START));

        // Event 3 misses BOUNDARY and CATCH → falls through to MESSAGE_START!
        when(nodeInvoker.invoke(any())).thenReturn(WebhookNodeInvoker.WebhookInvocationResult.success(
                new WebhookIngressOutcome(null, null, List.of(
                        new WebhookIngressOutcome.WebhookEmittedEvent("idem-3", "wa-user-3", Map.of("text", "hello"))))));
        when(correlationService.correlateWebhook(any())).thenAnswer(inv -> {
            WebhookEventRequest r = inv.getArgument(0);
            if (r == null) {
                return null;
            }
            if (r.eventType() == SqEventType.MESSAGE_START) {
                return WebhookEventOutcome.correlated("pi-new", SqEventType.MESSAGE_START);
            }
            return WebhookEventOutcome.notCorrelated(r.eventType());
        });

        IngestWebhookResponse r3 = handler.process(request("POST", "good", "{\"text\":\"hello\"}", "application/json"));
        assertEquals(1, r3.matchedCount());
        verify(correlationService, times(1)).correlateWebhook(org.mockito.ArgumentMatchers.argThat(
                r -> r != null && r.eventType() == SqEventType.MESSAGE_START));
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildWebhookRequestMap_parsesFormUrlEncodedAndPreservesRawBase64() {
        IngestWebhookRequest req = request("POST", "good", "From=%2B15551234&Body=Hello+World",
                "application/x-www-form-urlencoded");
        Map<String, Object> built = handler.buildWebhookRequestMap(req);
        assertEquals("POST", built.get("method"));
        assertEquals("utf8", built.get("rawBodyEncoding"));
        assertEquals("From=%2B15551234&Body=Hello+World", built.get("rawBody"));
        Map<String, Object> body = (Map<String, Object>) built.get("body");
        assertEquals("+15551234", body.get("From"));
        assertEquals("Hello World", body.get("Body"));
    }

    private IngestWebhookRequest request(String method, String secret, String body, String contentType) {
        String b64 = Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        return new IngestWebhookRequest(
                "env1", REF_ID, KEY_ID, "URL_KEY", secret, method,
                Map.of(), Map.of("content-type", contentType), b64, contentType, "0123456789abcdef");
    }

    private DeployedWebhookBindingEntity binding(String id, String wf, String nodeId, String kind,
                                                 String msgName, String tplId, String hash) throws Exception {
        TemplateDto tpl = new TemplateDto();
        tpl.setId(tplId);
        return DeployedWebhookBindingEntity.builder()
                .bindingId(id)
                .webhookKeyRef(REF_ID)
                .workflowId(wf)
                .namespaceKey("")
                .nodeId(nodeId)
                .bindingKind(kind)
                .messageName(msgName)
                .templateId(tplId)
                .paramsHash(hash)
                .targetNodeJson(mapper.writeValueAsString(tpl))
                .build();
    }
}
