package com.studioquack.sqs.channel;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Pattern;


@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class HostReplacedReporter {

    private static final int ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1_000L;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    /**
     * The installer passes the environment it is installing; it only enriches the audit record.
     */
    private static final Pattern ENV_ID = Pattern.compile("^[A-Za-z0-9-]{1,64}$");

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;

    public HostReplacedReporter(SqsChannelProperties props,
                                @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                ChannelHttpClients httpClients) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
    }

    /**
     * @param replacedByEnvironmentId environment being installed in this engine's place, or null
     * @return true once the backend recorded the replacement (including an earlier report)
     */
    public boolean reportReplaced(String replacedByEnvironmentId) {
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.warn("host-replace: no sqs.backend.base-url — the replacement cannot be reported");
            return false;
        }
        String credential = props.getBearer();
        if (credential == null || credential.isBlank()) {
            log.warn("host-replace: this daemon holds no channel credential — the replacement cannot be reported");
            return false;
        }
        String url = props.environmentBase(backendBaseUrl) + "/displaced";
        String body = requestBody(replacedByEnvironmentId);
        for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
            try {
                HttpResponse<String> resp = ChannelHttp.postJson(httpClients.get(), url, credential, body, TIMEOUT);
                if (ChannelHttp.isOk(resp.statusCode())) {
                    log.info("host-replace: the backend recorded that env={} was replaced on this host (attempt {})",
                            props.getEnvId(), attempt);
                    return true;
                }
                log.warn("host-replace: backend refused the report with {} on attempt {}: {}",
                        resp.statusCode(), attempt, ChannelHttp.trim(resp.body()));
                // 401/404/409 are answers about this environment, not transient faults.
                if (resp.statusCode() == 401 || resp.statusCode() == 404 || resp.statusCode() == 409) {
                    return false;
                }
            } catch (Exception ex) {
                log.warn("host-replace: report attempt {}/{} failed: {}", attempt, ATTEMPTS, ex.toString());
            }
            if (attempt < ATTEMPTS && !sleep()) {
                break;
            }
        }
        return false;
    }

    static String requestBody(String replacedByEnvironmentId) {
        String replacedBy = replacedByEnvironmentId == null ? "" : replacedByEnvironmentId.trim();
        if (!ENV_ID.matcher(replacedBy).matches()) {
            return "{}";
        }
        // Id-shaped by the pattern above, so it needs no JSON escaping.
        return "{\"replacedByEnvironmentId\":\"" + replacedBy + "\"}";
    }

    private static boolean sleep() {
        try {
            Thread.sleep(RETRY_DELAY_MS);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
