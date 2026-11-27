package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.webhook.WebhookKeyFormat;
import com.studioquack.sqs.channel.EnvelopeClient;
import com.studioquack.sqs.channel.SecurityTelemetryAccumulator;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.command.MessageCorrelationService.SqEventType;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventOutcome;
import com.studioquack.sqs.command.MessageCorrelationService.WebhookEventRequest;
import com.studioquack.sqs.sandbox.helperchan.WebhookIngressOutcome;
import com.studioquack.sqs.secrets.SecretStoreNamespace;
import com.studioquack.sqs.secrets.os.OsSecretManager;
import com.studioquack.sqs.webhook.DaemonWebhookKeyStore;
import com.studioquack.sqs.webhook.DeployedWebhookBindingEntity;
import com.studioquack.sqs.webhook.DeployedWebhookBindingStore;
import com.studioquack.sqs.webhook.WebhookIdempotencyStore;
import com.studioquack.sqs.webhook.WebhookNodeInvoker;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Handles control-plane-relayed {@code ingest-webhook-key-event} commands.
 *
 * <p>100% parameter-driven and provider-agnostic:
 * <ol>
 *   <li>Opens the FR-200 {@code SecureEnvelope} ({@code Direction.BE_TO_AGENT}, AAD
 *       {@code "webhook-key-ingest"}).</li>
 *   <li>Verifies {@code presentedSecret} in constant time against {@link DaemonWebhookKeyStore}
 *       (or permits {@code KEY_ID_ONLY} iff {@code allowKeyIdRoute == true}).</li>
 *   <li>Resolves active bindings from {@link DeployedWebhookBindingStore} and deduplicates
 *       identical {@code (templateId, paramsHash)} parser invocations across shared-key bindings.</li>
 *   <li>Executes each bound node's own code in {@code invocationMode = "ingress"} via
 *       {@link WebhookNodeInvoker} with resolved {@code $secretRef} parameters.</li>
 *   <li>Short-circuits on verification handshakes ({@code isHandshake = true}) or provider
 *       signature rejections ({@code providerSignatureValid = false}).</li>
 *   <li>Deduplicates emitted events via {@link WebhookIdempotencyStore} (24h TTL, cap 100k) and
 *       correlates REF-only via {@link MessageCorrelationService} in strict priority order:
 *       <b>{@code BOUNDARY_MESSAGE} &gt; {@code SEND_AND_WAIT}/{@code MESSAGE_CATCH} &gt;
 *       {@code MESSAGE_START}</b> (starts dispatched asynchronously on virtual threads so HTTP 200
 *       returns within provider timeout budgets).</li>
 * </ol>
 */
@Slf4j
@Component
public class IngestWebhookCommandHandler implements DaemonCommandHandler {

    public static final String TYPE = "ingest-webhook-key-event";
    public static final String AAD_TAG = "webhook-key-ingest";
    public static final String TELEMETRY_WEBHOOK_KEY_AUTH = "webhook_key_auth";

    private final ObjectMapper mapper;
    private final SqsChannelProperties channel;
    private final OsSecretManager osSecrets;
    private final SecretStoreNamespace namespace;
    private final DaemonWebhookKeyStore webhookKeyStore;
    private final DeployedWebhookBindingStore bindingStore;
    private final WebhookNodeInvoker nodeInvoker;
    private final MessageCorrelationService correlationService;
    private final WebhookIdempotencyStore idempotencyStore;
    private final Executor asyncStartExecutor;

    private SecurityTelemetryAccumulator securityTelemetry;

    @Autowired
    public IngestWebhookCommandHandler(ObjectMapper mapper,
                                       SqsChannelProperties channel,
                                       OsSecretManager osSecrets,
                                       SecretStoreNamespace namespace,
                                       DaemonWebhookKeyStore webhookKeyStore,
                                       DeployedWebhookBindingStore bindingStore,
                                       WebhookNodeInvoker nodeInvoker,
                                       MessageCorrelationService correlationService,
                                       WebhookIdempotencyStore idempotencyStore) {
        this(mapper, channel, osSecrets, namespace, webhookKeyStore, bindingStore, nodeInvoker,
                correlationService, idempotencyStore, Executors.newVirtualThreadPerTaskExecutor());
    }

    /** Test-friendly constructor accepting a deterministic executor for async MESSAGE_START dispatch. */
    public IngestWebhookCommandHandler(ObjectMapper mapper,
                                       SqsChannelProperties channel,
                                       OsSecretManager osSecrets,
                                       SecretStoreNamespace namespace,
                                       DaemonWebhookKeyStore webhookKeyStore,
                                       DeployedWebhookBindingStore bindingStore,
                                       WebhookNodeInvoker nodeInvoker,
                                       MessageCorrelationService correlationService,
                                       WebhookIdempotencyStore idempotencyStore,
                                       Executor asyncStartExecutor) {
        this.mapper = mapper;
        this.channel = channel;
        this.osSecrets = osSecrets;
        this.namespace = namespace;
        this.webhookKeyStore = webhookKeyStore;
        this.bindingStore = bindingStore;
        this.nodeInvoker = nodeInvoker;
        this.correlationService = correlationService;
        this.idempotencyStore = idempotencyStore;
        this.asyncStartExecutor = asyncStartExecutor != null
                ? asyncStartExecutor : Executors.newVirtualThreadPerTaskExecutor();
    }

    @Autowired(required = false)
    public void setSecurityTelemetry(SecurityTelemetryAccumulator securityTelemetry) {
        this.securityTelemetry = securityTelemetry;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) throws Exception {
        EnvelopeClient envelopeClient = WebhookKeyCommandHandlers.buildEnvelopeClient(
                channel, osSecrets, namespace, mapper);
        IngestWebhookRequest req = WebhookKeyCommandHandlers.open(
                command, AAD_TAG, IngestWebhookRequest.class, mapper, envelopeClient);
        IngestWebhookResponse response = process(req);
        return DaemonCommandOutcome.ok(mapper.writeValueAsString(response));
    }

    /** Core webhook ingest logic — exposed package-private for direct unit tests without envelope ceremony. */
    IngestWebhookResponse process(IngestWebhookRequest req) {
        if (req == null) {
            return IngestWebhookResponse.rejected(false, true, 400, "malformed_request");
        }

        // 1) Local constant-time secret verification against DaemonWebhookKeyStore.
        DaemonWebhookKeyStore.VerificationResult verification = webhookKeyStore.verify(
                req.refId(), req.keyId(), req.authMode(), req.presentedSecret());
        if (!verification.valid()) {
            if (securityTelemetry != null) {
                securityTelemetry.record(TELEMETRY_WEBHOOK_KEY_AUTH);
            }
            log.warn("webhook-ingest.auth-failed refId={} keyPrefix={} authMode={} reason={}",
                    req.refId(), WebhookKeyFormat.safeKeyIdPrefix(req.keyId()),
                    req.authMode(), verification.failureReason());
            return IngestWebhookResponse.rejected(false, true, 401, verification.failureReason());
        }

        String verifiedRefId = verification.refId();

        // 2) Lookup active deployed bindings for this webhook key.
        List<DeployedWebhookBindingEntity> bindings = bindingStore.findByWebhookKeyRef(verifiedRefId);
        if (bindings == null || bindings.isEmpty()) {
            log.info("webhook-ingest.no-bindings refId={} keyPrefix={}",
                    verifiedRefId, WebhookKeyFormat.safeKeyIdPrefix(verification.keyId()));
            return IngestWebhookResponse.rejected(true, true, 404, "no_active_bindings");
        }

        // 3) Build the unified webhookRequest envelope (raw bytes preserved for HMAC verification).
        Map<String, Object> webhookRequest = buildWebhookRequestMap(req);

        // 4) Group bindings by (templateId, paramsHash) so identical parser configs execute ONCE.
        Map<String, List<DeployedWebhookBindingEntity>> groups = new LinkedHashMap<>();
        for (DeployedWebhookBindingEntity binding : bindings) {
            String dedupKey = StringUtils.defaultString(binding.getTemplateId())
                    + "::" + StringUtils.defaultString(binding.getParamsHash());
            groups.computeIfAbsent(dedupKey, k -> new ArrayList<>()).add(binding);
        }

        int totalMatched = 0;
        String firstIdempotencyKey = null;
        boolean anyEventsAccepted = false;
        WebhookIngressOutcome.WebhookReject lastRejection = null;

        for (List<DeployedWebhookBindingEntity> group : groups.values()) {
            DeployedWebhookBindingEntity representative = group.get(0);
            TemplateDto template = parseTemplate(representative.getTargetNodeJson());
            if (template == null) {
                log.warn("webhook-ingest: missing template snapshot for binding={}", representative.getBindingId());
                continue;
            }
            String activeRole = activeSelectRoleForBindingKind(representative.getBindingKind());
            WebhookNodeInvoker.WebhookInvocationResult invResult = nodeInvoker.invoke(
                    new WebhookNodeInvoker.WebhookInvocationInput(
                            req.envId(),
                            representative.getWorkflowId(),
                            representative.getNodeId(),
                            activeRole,
                            template,
                            webhookRequest));

            if (invResult == null || !invResult.ok()) {
                log.warn("webhook-ingest.invoke-failed refId={} binding={} code={}",
                        verifiedRefId, representative.getBindingId(),
                        invResult == null ? "null" : invResult.errorCode());
                continue;
            }
            WebhookIngressOutcome outcome = invResult.outcome();
            if (outcome == null || outcome.isEmpty()) {
                continue;
            }

            // Short-circuit 1: Verification handshake (GET challenge / URL verification).
            if (outcome.challenge() != null) {
                WebhookIngressOutcome.WebhookChallenge ch = outcome.challenge();
                log.info("webhook-ingest.handshake refId={} status={}", verifiedRefId, ch.status());
                return new IngestWebhookResponse(
                        true, true, true, true,
                        ch.status(), ch.contentType(), ch.body(),
                        null, 0, null);
            }

            // Short-circuit 2: Provider signature rejection (e.g. bad HMAC).
            if (outcome.reject() != null) {
                lastRejection = outcome.reject();
                log.warn("webhook-ingest.provider-rejected refId={} binding={} status={} reason={}",
                        verifiedRefId, representative.getBindingId(),
                        lastRejection.status(), lastRejection.reason());
                continue;
            }

            // 5) Correlate emitted events with 24h TTL idempotency dedup and priority routing:
            //    BOUNDARY_MESSAGE > SEND_AND_WAIT / MESSAGE_CATCH > MESSAGE_START.
            List<WebhookIngressOutcome.WebhookEmittedEvent> events = outcome.events();
            if (events == null || events.isEmpty()) {
                continue;
            }
            anyEventsAccepted = true;
            for (WebhookIngressOutcome.WebhookEmittedEvent event : events) {
                if (event == null) {
                    continue;
                }
                if (firstIdempotencyKey == null && StringUtils.isNotBlank(event.idempotencyKey())) {
                    firstIdempotencyKey = event.idempotencyKey().trim();
                }
                if (!idempotencyStore.recordIfNew(verifiedRefId, event.idempotencyKey())) {
                    log.info("webhook-ingest.duplicate refId={} idempotencyKey={}",
                            verifiedRefId, event.idempotencyKey());
                    continue;
                }
                totalMatched += routeEventByPriority(group, event);
            }
        }

        if (!anyEventsAccepted && lastRejection != null) {
            return IngestWebhookResponse.rejected(
                    true, false, lastRejection.status(), lastRejection.reason());
        }

        return new IngestWebhookResponse(
                true, false, true, true,
                200, "application/json", "{\"accepted\":true}",
                firstIdempotencyKey, totalMatched, null);
    }

    /**
     * Routes one emitted event across the group's bindings in strict priority order:
     * <ol>
     *   <li>Waiting {@code BOUNDARY_MESSAGE} bindings matching {@code (messageName, correlationKey)}</li>
     *   <li>Waiting {@code SEND_AND_WAIT} and {@code MESSAGE_CATCH} bindings matching {@code (messageName, correlationKey)}</li>
     *   <li>{@code MESSAGE_START} bindings (dispatched asynchronously on virtual threads)</li>
     * </ol>
     */
    private int routeEventByPriority(List<DeployedWebhookBindingEntity> group,
                                     WebhookIngressOutcome.WebhookEmittedEvent event) {
        List<DeployedWebhookBindingEntity> boundaries = new ArrayList<>();
        List<DeployedWebhookBindingEntity> catches = new ArrayList<>();
        List<DeployedWebhookBindingEntity> starts = new ArrayList<>();
        for (DeployedWebhookBindingEntity b : group) {
            String kind = StringUtils.trimToEmpty(b.getBindingKind());
            if (DeployedWebhookBindingStore.KIND_BOUNDARY_MESSAGE.equals(kind)) {
                boundaries.add(b);
            } else if (DeployedWebhookBindingStore.KIND_SEND_AND_WAIT.equals(kind)
                    || DeployedWebhookBindingStore.KIND_MESSAGE_CATCH.equals(kind)) {
                catches.add(b);
            } else if (DeployedWebhookBindingStore.KIND_MESSAGE_START.equals(kind)) {
                starts.add(b);
            }
        }

        String correlationKey = StringUtils.trimToNull(event.correlationKey());
        Map<String, Object> payload = event.payload() == null ? Map.of() : event.payload();

        // Priority 1: Waiting BOUNDARY_MESSAGE bindings.
        if (correlationKey != null && !boundaries.isEmpty()) {
            int matchedBoundaries = 0;
            for (DeployedWebhookBindingEntity b : boundaries) {
                WebhookEventOutcome out = safeCorrelate(new WebhookEventRequest(
                        SqEventType.BOUNDARY_MESSAGE,
                        b.getWorkflowId(),
                        b.getNodeId(),
                        correlationKey,
                        b.getMessageName(),
                        payload));
                if (out != null && out.correlated()) {
                    matchedBoundaries++;
                }
            }
            if (matchedBoundaries > 0) {
                return matchedBoundaries;
            }
        }

        // Priority 2: Waiting SEND_AND_WAIT / MESSAGE_CATCH bindings.
        if (correlationKey != null && !catches.isEmpty()) {
            int matchedCatches = 0;
            for (DeployedWebhookBindingEntity b : catches) {
                WebhookEventOutcome out = safeCorrelate(new WebhookEventRequest(
                        SqEventType.MESSAGE_CATCH,
                        b.getWorkflowId(),
                        b.getNodeId(),
                        correlationKey,
                        b.getMessageName(),
                        payload));
                if (out != null && out.correlated()) {
                    matchedCatches++;
                }
            }
            if (matchedCatches > 0) {
                return matchedCatches;
            }
        }

        // Priority 3: MESSAGE_START bindings (asynchronous dispatch so HTTP 200 returns fast).
        int dispatchedStarts = 0;
        for (DeployedWebhookBindingEntity b : starts) {
            WebhookEventRequest startReq = new WebhookEventRequest(
                    SqEventType.MESSAGE_START,
                    b.getWorkflowId(),
                    b.getNodeId(),
                    correlationKey,
                    b.getMessageName(),
                    payload);
            asyncStartExecutor.execute(() -> {
                try {
                    WebhookEventOutcome out = correlationService.correlateWebhook(startReq);
                    log.info("webhook-ingest.start-dispatched workflowId={} node={} correlated={} pi={}",
                            b.getWorkflowId(), b.getNodeId(),
                            out != null && out.correlated(),
                            out == null ? null : out.processInstanceId());
                } catch (Exception ex) {
                    log.warn("webhook-ingest.start-failed workflowId={} node={}: {}",
                            b.getWorkflowId(), b.getNodeId(), ex.getMessage());
                }
            });
            dispatchedStarts++;
        }
        return dispatchedStarts;
    }

    private WebhookEventOutcome safeCorrelate(WebhookEventRequest req) {
        try {
            return correlationService.correlateWebhook(req);
        } catch (Exception ex) {
            log.warn("webhook-ingest.correlate-failed type={} workflowId={} node={}: {}",
                    req.eventType(), req.workflowId(), req.nodeId(), ex.getMessage());
            return null;
        }
    }

    Map<String, Object> buildWebhookRequestMap(IngestWebhookRequest req) {
        Map<String, Object> map = new LinkedHashMap<>();
        String method = StringUtils.isBlank(req.method()) ? "POST" : req.method().trim().toUpperCase(Locale.ROOT);
        String envId = StringUtils.defaultIfBlank(req.envId(), "unknown");
        map.put("method", method);
        map.put("path", "/be/v1/environments/" + envId + "/runtime/message-events/<REDACTED>");

        Map<String, List<String>> queryMulti = new LinkedHashMap<>();
        Map<String, String> queryFlat = new LinkedHashMap<>();
        if (req.queryParams() != null) {
            for (Map.Entry<String, List<String>> e : req.queryParams().entrySet()) {
                if (e.getKey() == null) {
                    continue;
                }
                List<String> vals = e.getValue() == null ? List.of() : List.copyOf(e.getValue());
                queryMulti.put(e.getKey(), vals);
                queryFlat.put(e.getKey(), vals.isEmpty() ? "" : StringUtils.defaultString(vals.get(0)));
            }
        }
        map.put("query", queryFlat);
        map.put("queryMulti", queryMulti);

        Map<String, String> headers = new LinkedHashMap<>();
        if (req.headers() != null) {
            for (Map.Entry<String, String> e : req.headers().entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    headers.put(e.getKey().trim().toLowerCase(Locale.ROOT), e.getValue());
                }
            }
        }
        map.put("headers", headers);

        String rawB64 = StringUtils.defaultString(req.rawBodyBase64());
        byte[] rawBytes;
        try {
            rawBytes = rawB64.isEmpty() ? new byte[0] : Base64.getDecoder().decode(rawB64);
        } catch (IllegalArgumentException badB64) {
            rawBytes = new byte[0];
        }
        String decodedUtf8 = decodeStrictUtf8OrNull(rawBytes);
        boolean isUtf8 = decodedUtf8 != null;
        map.put("rawBody", isUtf8 ? decodedUtf8 : rawB64);
        map.put("rawBodyBase64", rawB64);
        map.put("rawBodyEncoding", isUtf8 ? "utf8" : "base64");

        String contentType = StringUtils.defaultIfBlank(req.contentType(), headers.get("content-type"));
        map.put("body", parseStructuredBody(decodedUtf8, contentType));
        return map;
    }

    private Object parseStructuredBody(String utf8Body, String contentType) {
        if (utf8Body == null || utf8Body.isBlank()) {
            return null;
        }
        String ct = contentType == null ? "" : contentType.toLowerCase(Locale.ROOT);
        String trimmed = utf8Body.trim();
        if (ct.contains("application/x-www-form-urlencoded")) {
            return parseFormUrlEncoded(utf8Body);
        }
        if (ct.contains("json") || trimmed.startsWith("{") || trimmed.startsWith("[")) {
            try {
                return mapper.readValue(trimmed, Object.class);
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseFormUrlEncoded(String raw) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return out;
        }
        for (String pair : raw.split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String k = eq >= 0 ? urlDecode(pair.substring(0, eq)) : urlDecode(pair);
            String v = eq >= 0 ? urlDecode(pair.substring(eq + 1)) : "";
            Object existing = out.get(k);
            if (existing == null) {
                out.put(k, v);
            } else if (existing instanceof List<?> list) {
                ((List<String>) list).add(v);
            } else {
                List<String> multi = new ArrayList<>();
                multi.add(String.valueOf(existing));
                multi.add(v);
                out.put(k, multi);
            }
        }
        return out;
    }

    private static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            return s;
        }
    }

    private static String decodeStrictUtf8OrNull(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException ex) {
            return null;
        }
    }

    private TemplateDto parseTemplate(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return mapper.readValue(json, TemplateDto.class);
        } catch (Exception ex) {
            log.warn("webhook-ingest: failed deserializing bound template: {}", ex.getMessage());
            return null;
        }
    }

    private static String activeSelectRoleForBindingKind(String bindingKind) {
        if (DeployedWebhookBindingStore.KIND_MESSAGE_CATCH.equals(bindingKind)) {
            return "message_catch";
        }
        if (DeployedWebhookBindingStore.KIND_SEND_AND_WAIT.equals(bindingKind)) {
            return "send_and_wait";
        }
        return "listen";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record IngestWebhookRequest(
            String envId,
            String refId,
            String keyId,
            String authMode,
            String presentedSecret,
            String method,
            Map<String, List<String>> queryParams,
            Map<String, String> headers,
            String rawBodyBase64,
            String contentType,
            String remoteIpHash) {
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record IngestWebhookResponse(
            boolean secretValid,
            boolean isHandshake,
            boolean providerSignatureValid,
            boolean accepted,
            int httpStatus,
            String contentType,
            String responseBody,
            String idempotencyKey,
            int matchedCount,
            String rejectionReason) {

        static IngestWebhookResponse rejected(boolean secretValid, boolean providerSigValid,
                                              int httpStatus, String reason) {
            String safeReason = StringUtils.defaultIfBlank(reason, "rejected");
            return new IngestWebhookResponse(
                    secretValid, false, providerSigValid, false,
                    httpStatus, "application/json",
                    "{\"error\":\"" + safeReason.replace("\"", "") + "\"}",
                    null, 0, safeReason);
        }
    }
}
