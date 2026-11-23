package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.sandbox.stateful.shell.ShellOutputCallback;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ShellOutputCallback} that POSTs redacted shell frames to the control plane over the
 * enrolled secure channel. Active only when {@code sqs.channel.enabled=true}.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelShellOutputCallback implements ShellOutputCallback {

    /** Short timeout: shell chunks are small and the reader must not stall long. */
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /** Transient (5xx / IO) attempts per frame, including the first try. */
    private static final int TRANSIENT_ATTEMPTS = 3;

    /** Consecutive 4xx responses before we stop posting {@code output} for a session. */
    static final int MAX_CONSECUTIVE_4XX = 3;

    private static final long TRANSIENT_BACKOFF_MS = 100L;

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final OutboundBaseHolder outboundBaseHolder;

    /** Per-session consecutive HTTP 4xx count. */
    private final ConcurrentHashMap<String, AtomicInteger> consecutive4xx = new ConcurrentHashMap<>();
    /** Sessions for which we gave up on further {@code output} frames after repeated 4xx. */
    private final Set<String> abandonedOutput = ConcurrentHashMap.newKeySet();
    /** Sessions already warned about abandonment (log once). */
    private final Set<String> abandonWarned = ConcurrentHashMap.newKeySet();

    public ChannelShellOutputCallback(SqsChannelProperties props,
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
    public void postOutput(String envId, String sessionId, Map<String, Object> frame) {
        if (sessionId == null || sessionId.isBlank() || frame == null || frame.isEmpty()) {
            return;
        }
        String event = stringField(frame, "event");
        boolean closed = "closed".equals(event);
        if (!closed && abandonedOutput.contains(sessionId)) {
            return;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.debug("shell-output[channel]: no sqs.backend.base-url — skipping sessionId={}", sessionId);
            return;
        }

        Object seqObj = frame.get("seq");
        int dataChars = dataCharCount(frame);
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/shell-sessions/" + URLEncoder.encode(sessionId, StandardCharsets.UTF_8)
                    + "/output";
            // Ensure path sessionId wins if the map omitted it (manager always sets it).
            if (!frame.containsKey("sessionId") || frame.get("sessionId") == null) {
                frame.put("sessionId", sessionId);
            }
            String body = mapper.writeValueAsString(frame);

            for (int attempt = 1; attempt <= TRANSIENT_ATTEMPTS; attempt++) {
                HttpResponse<String> resp;
                try {
                    resp = ChannelHttp.postJson(
                            httpClients.get(), url, props.getBearer(), body, TIMEOUT);
                } catch (Exception transport) {
                    if (attempt < TRANSIENT_ATTEMPTS && ChannelHttp.isConnectionFailure(transport)) {
                        log.debug("shell-output[channel]: transport attempt {}/{} sessionId={} seq={} dataChars={}: {}",
                                attempt, TRANSIENT_ATTEMPTS, sessionId, seqObj, dataChars, transport.toString());
                        sleepQuietly(TRANSIENT_BACKOFF_MS * attempt);
                        continue;
                    }
                    log.warn("shell-output[channel]: push failed sessionId={} event={} seq={} dataChars={}: {}",
                            sessionId, event, seqObj, dataChars, transport.toString());
                    return;
                }

                int status = resp.statusCode();
                if (ChannelHttp.isOk(status)) {
                    // 2xx = delivered; do NOT retry. 200 is not proof the session is still live on BE.
                    consecutive4xx.remove(sessionId);
                    if (closed) {
                        clearSessionState(sessionId);
                    }
                    log.debug("shell-output[channel]: ok status={} sessionId={} event={} seq={} dataChars={}",
                            status, sessionId, event, seqObj, dataChars);
                    return;
                }

                if (status >= 400 && status < 500) {
                    int n = consecutive4xx
                            .computeIfAbsent(sessionId, k -> new AtomicInteger())
                            .incrementAndGet();
                    if (n >= MAX_CONSECUTIVE_4XX) {
                        abandonedOutput.add(sessionId);
                        if (abandonWarned.add(sessionId)) {
                            log.warn("shell-output[channel]: giving up output posts after {} consecutive 4xx "
                                            + "(status={}) sessionId={} event={} seq={} dataChars={}",
                                    n, status, sessionId, event, seqObj, dataChars);
                        }
                    } else {
                        log.warn("shell-output[channel]: BE {} sessionId={} event={} seq={} dataChars={} "
                                        + "(4xx streak={}/{})",
                                status, sessionId, event, seqObj, dataChars, n, MAX_CONSECUTIVE_4XX);
                    }
                    if (closed) {
                        clearSessionState(sessionId);
                    }
                    return; // never retry 4xx for the same frame
                }

                // 5xx / other non-2xx — transient retry
                if (attempt < TRANSIENT_ATTEMPTS) {
                    log.debug("shell-output[channel]: BE {} attempt {}/{} sessionId={} seq={} dataChars={}",
                            status, attempt, TRANSIENT_ATTEMPTS, sessionId, seqObj, dataChars);
                    sleepQuietly(TRANSIENT_BACKOFF_MS * attempt);
                    continue;
                }
                log.warn("shell-output[channel]: BE {} after {} attempts sessionId={} event={} seq={} dataChars={}",
                        status, TRANSIENT_ATTEMPTS, sessionId, event, seqObj, dataChars);
                return;
            }
        } catch (Exception ex) {
            // Best-effort mirror: never let a callback break the shell reader.
            log.warn("shell-output[channel]: unexpected failure sessionId={} event={} seq={} dataChars={}: {}",
                    sessionId, event, seqObj, dataChars, ex.toString());
        }
    }

    private void clearSessionState(String sessionId) {
        consecutive4xx.remove(sessionId);
        abandonedOutput.remove(sessionId);
        abandonWarned.remove(sessionId);
    }

    private static String stringField(Map<String, Object> frame, String key) {
        Object v = frame.get(key);
        return v == null ? null : Objects.toString(v, null);
    }

    private static int dataCharCount(Map<String, Object> frame) {
        Object data = frame.get("data");
        if (data instanceof CharSequence cs) {
            return cs.length();
        }
        return data == null ? 0 : Objects.toString(data).length();
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
