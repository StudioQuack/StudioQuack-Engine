package com.studioquack.sqs.usertask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;


@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelUserTaskApprovalSink implements UserTaskApprovalSink {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final OutboundBaseHolder outboundBaseHolder;

    public ChannelUserTaskApprovalSink(SqsChannelProperties props,
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
    public Outcome open(Map<String, Object> report) {
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.debug("user-task.approval[channel]: no sqs.backend.base-url — nothing to announce");
            return Outcome.ACCEPTED;
        }
        Object taskId = report.get("taskId");
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/user-tasks";
            HttpResponse<String> resp = ChannelHttp.postJson(httpClients.get(), url, props.getBearer(),
                    mapper.writeValueAsString(report), TIMEOUT);
            int status = resp.statusCode();
            if (ChannelHttp.isOk(status)) {
                return Outcome.ACCEPTED;
            }
            // 401 is the backend refusing the credential, not the report: an edge or bearer problem
            // the backend side can fix while the task stays parked, so it must not settle the task.
            if (status == 401 || status == 408 || status == 429 || status >= 500) {
                log.warn("user-task.approval[channel]: BE {} for task={} — will retry", status, taskId);
                return Outcome.RETRY;
            }
            // Other 4xx: unknown workflow, not admitted on this env, malformed — retrying cannot fix it.
            log.warn("user-task.approval[channel]: BE refused task={} status={} body={}",
                    taskId, status, ChannelHttp.trim(resp.body()));
            return Outcome.REFUSED;
        } catch (Exception ex) {
            log.warn("user-task.approval[channel]: announce failed task={}: {}", taskId, ex.toString());
            return Outcome.RETRY;
        }
    }
}
