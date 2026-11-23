package com.studioquack.sqs.release;

import com.studioquack.sqs.channel.config.SqsChannelProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;

/**
 * Anonymous HTTPS client for {@code /releases/client/**} artifacts on the control-plane origin (
 * {@code sqs.backend.base-url}). Trust is the JDK/OS store; no enrolled client certificate is
 * offered — channel mTLS is not release identity (see {@code ChannelTls}).
 */
@Component
public class ReleaseHttpClient {


    public static final String ENV_ID_HEADER = "X-SQ-Env-Id";

    private final HttpClient client;
    private final SqsChannelProperties channelProps;

    @Autowired
    public ReleaseHttpClient(SqsChannelProperties channelProps) {
        this(channelProps, defaultClient());
    }

    /** Test hook: supply a client (e.g. plain HTTP to a loopback fake backend). */
    private ReleaseHttpClient(SqsChannelProperties channelProps, HttpClient client) {
        this.channelProps = channelProps;
        this.client = client;
    }

    /** Test hook without channel properties (no bearer). */
    public static ReleaseHttpClient forTests(HttpClient client) {
        return forTests(new SqsChannelProperties(), client);
    }

    /** Test hook with explicit channel properties. */
    public static ReleaseHttpClient forTests(SqsChannelProperties channelProps, HttpClient client) {
        return new ReleaseHttpClient(channelProps, client);
    }

    public HttpClient client() {
        return client;
    }

    public HttpRequest.Builder get(URI uri, Duration timeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(timeout).GET();
        if (requiresProBearer(uri)) {
            String bearer = channelProps.getBearer();
            if (bearer == null || bearer.isBlank()) {
                throw new IllegalStateException("sqs-pro release fetch requires a channel bearer");
            }
            builder.header("Authorization", "Bearer " + bearer);
            String envId = channelProps.getEnvId();
            if (envId != null && !envId.isBlank()) {
                builder.header(ENV_ID_HEADER, envId.trim());
            }
        }
        return builder;
    }

    static boolean requiresProBearer(URI uri) {
        if (uri == null) {
            return false;
        }
        String path = uri.getPath();
        if (path == null || path.isBlank()) {
            return false;
        }
        return path.replace('\\', '/').contains("/sqs-pro/");
    }

    private static HttpClient defaultClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }
}
