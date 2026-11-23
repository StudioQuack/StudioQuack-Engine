package com.studioquack.sqs.status;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
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

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@link ConversationOutputCallback} that POSTs a Duckbot end node's final answer to the control
 * plane over the secure channel so it is stored in the conversation + streamed to the Duckbot
 * screen. Active only when {@code sqs.channel.enabled=true}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelConversationOutputCallback implements ConversationOutputCallback {

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder;
    /** Lazy: same cycle reason as {@link ChannelSandboxLogCallback}. */
    @Autowired(required = false)
    private ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry;

    public ChannelConversationOutputCallback(SqsChannelProperties props,
                                             @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                             ChannelHttpClients httpClients,
                                             ObjectMapper mapper,
                                             com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.outboundBaseHolder = outboundBaseHolder;
    }

    @Override
    public List<ConversationFileRefDto> report(ConversationOutput output) {
        if (output == null || output.workflowExecutionId() == null || output.workflowExecutionId().isBlank()
                || output.conversationId() == null || output.conversationId().isBlank()) {
            return List.of();
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.debug("duckbot-output[channel]: no sqs.backend.base-url — skipping push for {}",
                    output.conversationId());
            return List.of();
        }
        try {
            String runId = resolveReportRunId(output.workflowExecutionId());
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(runId, StandardCharsets.UTF_8)
                    + "/conversation-output";
            String body = mapper.writeValueAsString(new Body(
                    output.conversationId(), output.agentMessageKey(), output.appPageId(),
                    output.role(), output.message(), output.msgOrder(), outputFiles(output),
                    outputVoices(output), output.nodeId(), output.thinkingSections()));
            // Files / voices make this push multi-megabyte, so it gets the deploy-class timeout
            // rather than the text-only one — a truncated answer file is worse than a slower callback.
            Duration timeout = hasFiles(output) || hasVoices(output)
                    ? Duration.ofMinutes(2) : Duration.ofSeconds(15);
            HttpResponse<String> resp = ChannelHttp.postJson(
                    httpClients.get(), url, props.getBearer(), body, timeout);
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("duckbot-output[channel]: BE {} for runId={} conversationId={} body={}",
                        resp.statusCode(), runId, output.conversationId(), ChannelHttp.trim(resp.body()));
                return List.of();
            }
            log.debug("duckbot-output[channel] conversationId={} -> BE ok", output.conversationId());
            return parseDeliveredAttachments(resp.body());
        } catch (Exception ex) {
            log.warn("duckbot-output[channel]: push failed for conversationId={}: {}",
                    output.conversationId(), ex.toString());
            return List.of();
        }
    }

    private List<ConversationFileRefDto> parseDeliveredAttachments(String responseBody) {
        if (responseBody == null || responseBody.isBlank()) {
            return List.of();
        }
        try {
            ResponseBody parsed = mapper.readValue(responseBody, ResponseBody.class);
            if (parsed == null || parsed.attachments() == null || parsed.attachments().isEmpty()) {
                return List.of();
            }
            List<ConversationFileRefDto> refs = new ArrayList<>();
            for (AttachmentBody a : parsed.attachments()) {
                if (a == null || a.fileId() == null || a.fileId().isBlank()) {
                    continue;
                }
                ConversationFileRefDto ref = new ConversationFileRefDto();
                ref.setFileId(a.fileId());
                ref.setName(a.name());
                ref.setMimeType(a.mimeType());
                ref.setSize(a.size());
                ref.setKind(a.kind());
                ref.setUploadedAt(a.uploadedAt());
                refs.add(ref);
            }
            return refs.isEmpty() ? List.of() : Collections.unmodifiableList(refs);
        } catch (Exception e) {
            log.debug("duckbot-output[channel]: could not parse attachment refs from BE response: {}",
                    e.toString());
            return List.of();
        }
    }

    /**
     * Maps a function-port sub-PI child run id to the top-level run the BE registered.
     * Falls back to the raw id for top-level runs or when the registry is unavailable.
     */
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

    private static boolean hasFiles(ConversationOutput output) {
        return output.files() != null && !output.files().isEmpty();
    }

    private static boolean hasVoices(ConversationOutput output) {
        return output.voices() != null && !output.voices().isEmpty();
    }

    /** Wire form of the answer's files; null (omitted) when the answer carried none. */
    private static List<FileBody> outputFiles(ConversationOutput output) {
        return toFileBodies(output.files());
    }

    /** Wire form of the answer's voices; null (omitted) when the answer carried none. */
    private static List<FileBody> outputVoices(ConversationOutput output) {
        return toFileBodies(output.voices());
    }

    private static List<FileBody> toFileBodies(List<ConversationOutputCallback.OutputFile> items) {
        if (items == null || items.isEmpty()) {
            return null;
        }
        List<FileBody> bodies = new ArrayList<>();
        for (ConversationOutputCallback.OutputFile file : items) {
            if (file == null || file.contentBase64() == null || file.contentBase64().isBlank()) {
                continue;
            }
            bodies.add(new FileBody(file.name(), file.mimeType(), file.kind(), file.contentBase64()));
        }
        return bodies.isEmpty() ? null : bodies;
    }

    /** Ingestion body (camelCase, nulls omitted). Field names match the BE ClientConversationOutputController record. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Body(String conversationId, String agentMessageKey, String appPageId, String role,
                String message, int msgOrder, List<FileBody> files, List<FileBody> voices,
                String nodeId, List<ConversationOutputCallback.ThinkingSection> thinkingSections) {
    }

    /**
     * One answer file with its bytes — the backend stores them in stored files and hands the chat a
     * ref.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record FileBody(String name, String mimeType, String kind, String contentBase64) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record ResponseBody(Boolean ok, Boolean delivered, List<AttachmentBody> attachments) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record AttachmentBody(String fileId, String name, String mimeType, long size, String kind,
                          String uploadedAt) {
    }
}
