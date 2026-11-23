package com.studioquack.sqs.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/**
 * {@link ConversationStreamCallback} that POSTs live Duckbot token frames to the control plane
 * over the secure channel so the open chat can render them. Active only when
 * {@code sqs.channel.enabled=true}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelConversationStreamCallback implements ConversationStreamCallback {

    /** Stream frames are small; keep the helper-thread handoff off a long BE hang. */
    private static final Duration POST_TIMEOUT = Duration.ofSeconds(10);

    /** Dispatcher rejects a blank nodeId; stream frames may omit one. */
    private static final String BLANK_NODE_LANE = "_";

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder;
    private final AsyncSandboxLogDispatcher dispatcher;
    private final ConversationStreamCoalescer coalescer;
    private final ConversationStreamUploader uploader;
    /**
     * Lazy: same cycle reason as {@link ChannelSandboxLogCallback}. A function-port Duckbot-end
     * child runs under a fresh id the BE never registered — POSTing frames under that id 404s
     * ({@code Error_RunNotFound}) and the thinking bubble never paints.
     */
    @Autowired(required = false)
    private ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry;

    public ChannelConversationStreamCallback(SqsChannelProperties props,
                                             @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                             ChannelHttpClients httpClients,
                                             ObjectMapper mapper,
                                             com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder,
                                             AsyncSandboxLogDispatcher dispatcher) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.outboundBaseHolder = outboundBaseHolder;
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
        // The coalescer's batch rides the same dispatcher lane the single-frame POST used,
        // so per-lane FIFO ordering (and the FE's seq guard) is preserved end to end.
        this.coalescer = new ConversationStreamCoalescer((runId, lane, merged, onComplete) ->
                dispatcher.enqueue(runId, lane, () -> {
                    try {
                        postFrame(merged);
                    } finally {
                        onComplete.run();
                    }
                }));
        // Primary path: one long-lived NDJSON upload per (run, lane) — every frame is
        // flushed the moment it is produced, never batched behind a round-trip or a log
        // POST. The coalescer remains as the automatic fallback for a control plane that
        // predates /live (404) and for broken connections.
        this.uploader = new ConversationStreamUploader(
                runId -> URI.create(outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                        + "/runs/" + URLEncoder.encode(runId, StandardCharsets.UTF_8)
                        + "/conversation-stream/live"),
                props::getBearer,
                httpClients::sslContext,
                httpClients.hostnameVerifier(),
                mapper,
                coalescer::offer);
    }

    /** Test seam: true once no frame is buffered, in flight, or queued anywhere downstream. */
    boolean awaitStreamIdle(java.time.Duration timeout) {
        return uploader.awaitIdle(timeout) && coalescer.awaitIdle(timeout) && dispatcher.awaitIdle(timeout);
    }

    /** Test seam: force every frame onto the coalescing per-batch POST path. */
    void disableLiveUploadForTests() {
        uploader.disableForTests();
    }

    /** Test hook: true once a 404 (or the disable seam) has parked the live path. */
    boolean liveUploadDisabled() {
        return uploader.isLiveDisabled();
    }

    @jakarta.annotation.PreDestroy
    void shutdownCoalescer() {
        uploader.shutdown();
        coalescer.shutdown();
    }

    /** Test / Spring-free seam for sub-PI → top-level run re-key. */
    public void wireSubProcessRegistryForTests(ObjectProvider<SubProcessCompletionRegistry> registry) {
        this.subProcessRegistry = registry;
    }

    @Override
    public void emit(ConversationStreamEvent event) {
        try {
            if (event == null || event.event() == null
                    || isBlank(event.conversationId()) || isBlank(event.workflowExecutionId())) {
                return;
            }
            if (isBlank(backendBaseUrl)) {
                log.debug("duckbot-stream[channel]: no sqs.backend.base-url — skipping push for {}",
                        event.conversationId());
                return;
            }
            ConversationStreamEvent posted = withReportableRunId(event);
            String lane = isBlank(posted.nodeId()) ? BLANK_NODE_LANE : posted.nodeId();
            uploader.offer(lane, posted);
        } catch (Exception ex) {
            log.warn("duckbot-stream[channel]: emit failed conversationId={} event={} seq={}: {}",
                    event == null ? null : event.conversationId(),
                    event == null ? null : event.event(),
                    event == null ? 0 : event.seq(),
                    ex.toString());
        }
    }

    /** Enrolment-bearer JSON POST. Never throws; never logs delta or the bearer. */
    private void postFrame(ConversationStreamEvent event) {
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(event.workflowExecutionId(), StandardCharsets.UTF_8)
                    + "/conversation-stream";
            String body = mapper.writeValueAsString(new Body(
                    event.conversationId(), event.agentMessageKey(), event.nodeId(),
                    event.event(), event.channel(), event.delta(), event.seq(), event.msgOrder(),
                    event.nodeLabel()));
            HttpResponse<String> resp = ChannelHttp.postJson(
                    httpClients.get(), url, props.getBearer(), body, POST_TIMEOUT);
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("duckbot-stream[channel]: BE {} for runId={} conversationId={} event={} seq={}",
                        resp.statusCode(), event.workflowExecutionId(), event.conversationId(),
                        event.event(), event.seq());
            }
        } catch (Exception ex) {
            log.warn("duckbot-stream[channel]: push failed conversationId={} event={} seq={}: {}",
                    event.conversationId(), event.event(), event.seq(), ex.toString());
        }
    }

    /**
     * Maps a function-port sub-PI child run id to the top-level run the BE registered, so the
     * frame lands on a run that actually exists. Falls back to the raw id for top-level runs or
     * when the registry is unavailable.
     */
    private ConversationStreamEvent withReportableRunId(ConversationStreamEvent event) {
        String runId = resolveReportRunId(event.workflowExecutionId());
        if (runId.equals(event.workflowExecutionId())) {
            return event;
        }
        return new ConversationStreamEvent(runId, event.conversationId(), event.agentMessageKey(),
                event.nodeId(), event.event(), event.channel(), event.delta(), event.seq(),
                event.msgOrder(), event.nodeLabel());
    }

    private String resolveReportRunId(String workflowExecutionId) {
        try {
            ObjectProvider<SubProcessCompletionRegistry> provider = subProcessRegistry;
            SubProcessCompletionRegistry registry = provider == null ? null : provider.getIfAvailable();
            if (registry != null) {
                String resolved = registry.resolveReportableRunId(workflowExecutionId);
                if (resolved != null && !resolved.isBlank()) {
                    return resolved;
                }
            }
        } catch (RuntimeException ignored) {
            // fall through to the raw id
        }
        return workflowExecutionId;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Ingestion body (camelCase, nulls omitted). Field names match the BE
     * {@code ClientConversationStreamController.ConversationStreamBody} record.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Body(String conversationId, String agentMessageKey, String nodeId, String event,
                String channel, String delta, int seq, Integer msgOrder, String nodeLabel) {
    }
}
