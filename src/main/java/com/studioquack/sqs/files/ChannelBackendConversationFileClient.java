package com.studioquack.sqs.files;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;

/**
 * Secure-channel implementation of {@link BackendConversationFileClient}. Active only when
 * {@code sqs.channel.enabled=true}; a daemon with no channel has no way to reach the backend and
 * so has no fetch bean (an omitted attachment then stays a contentless reference).
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelBackendConversationFileClient implements BackendConversationFileClient {

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final OutboundBaseHolder outboundBaseHolder;

    public ChannelBackendConversationFileClient(SqsChannelProperties props,
                                                @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                                ChannelHttpClients httpClients,
                                                ObjectMapper mapper,
                                                OutboundBaseHolder outboundBaseHolder) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.outboundBaseHolder = outboundBaseHolder;
    }

    @Override
    public Optional<FetchedFile> fetch(String workflowExecutionId, String conversationId, String fileId) {
        if (backendBaseUrl == null || backendBaseUrl.isBlank()
                || workflowExecutionId == null || workflowExecutionId.isBlank()
                || conversationId == null || conversationId.isBlank()
                || fileId == null || fileId.isBlank()) {
            return Optional.empty();
        }
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(workflowExecutionId, StandardCharsets.UTF_8)
                    + "/conversation-files/" + URLEncoder.encode(fileId, StandardCharsets.UTF_8)
                    + "?conversationId=" + URLEncoder.encode(conversationId, StandardCharsets.UTF_8);
            HttpResponse<String> resp = ChannelHttp.getJson(
                    httpClients.get(), url, props.getBearer(), props.getInstanceId(), Duration.ofSeconds(30));
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("conversation-file fetch: BE {} for run={} file={}",
                        resp.statusCode(), workflowExecutionId, fileId);
                return Optional.empty();
            }
            JsonNode body = mapper.readTree(resp.body());
            String base64 = body.path("contentBase64").asText(null);
            if (base64 == null || base64.isBlank()) {
                return Optional.empty();
            }
            byte[] content = Base64.getDecoder().decode(base64);
            String name = body.path("name").asText(null);
            String mimeType = body.path("mimeType").asText(null);
            return Optional.of(new FetchedFile(name, mimeType, content));
        } catch (Exception ex) {
            // Best-effort: a failed fetch leaves the reference contentless, never breaks the run.
            log.warn("conversation-file fetch failed for run={} file={}: {}",
                    workflowExecutionId, fileId, ex.toString());
            return Optional.empty();
        }
    }
}
