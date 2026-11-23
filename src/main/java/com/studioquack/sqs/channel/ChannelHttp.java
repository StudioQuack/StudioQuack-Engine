package com.studioquack.sqs.channel;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import com.studioquack.sqs.transport.TransferByteMeter;


public final class ChannelHttp {

    /**
     * Header the BE validates the enrolment bearer from on every daemon call.
     */
    public static final String AUTHORIZATION = "Authorization";
    public static final String BEARER_PREFIX = "Bearer ";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String ACCEPT = "Accept";
    public static final String APPLICATION_JSON = "application/json";
    public static final String TEXT_EVENT_STREAM = "text/event-stream";
    /**
     * Instance correlation header echoed on heartbeat + exec-channel.
     */
    public static final String INSTANCE_HEADER = "X-SQVM-Instance-Id";
    /**
     * Per-dispatch delivery token header on exec-results.
     */
    public static final String DELIVERY_TOKEN_HEADER = "X-SQ-Delivery-Token";

    /**
     * Heartbeat/command JSON can carry a deploy graph; 16 MiB is the DoS ceiling, not a typical size.
     */
    public static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;

    private ChannelHttp() {
    }

    static HttpResponse.BodyHandler<String> boundedUtf8() {
        return boundedUtf8(MAX_RESPONSE_BYTES);
    }

    static HttpResponse.BodyHandler<String> boundedUtf8(int maxBytes) {
        return responseInfo -> HttpResponse.BodySubscribers.mapping(
                HttpResponse.BodySubscribers.ofInputStream(),
                in -> {
                    try (in) {
                        byte[] buf = in.readNBytes(maxBytes + 1);
                        if (buf.length > maxBytes) {
                            throw new java.io.IOException(
                                    "channel HTTP response exceeded " + maxBytes + " bytes");
                        }
                        return new String(buf, StandardCharsets.UTF_8);
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
    }

    /**
     * Bearer-authenticated JSON POST; returns the raw string response.
     */
    public static HttpResponse<String> postJson(HttpClient client, String url, String bearer,
                                                String jsonBody, Duration timeout) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8));
        if (bearer != null && !bearer.isBlank()) {
            b.header(AUTHORIZATION, BEARER_PREFIX + bearer);
        }
        if (jsonBody != null) {
            TransferByteMeter.add(jsonBody.getBytes(StandardCharsets.UTF_8).length);
        }
        HttpResponse<String> resp = client.send(b.build(), boundedUtf8());
        String body = resp.body();
        if (body != null) {
            TransferByteMeter.add(body.getBytes(StandardCharsets.UTF_8).length);
        }
        return resp;
    }

    /**
     * Bearer-authenticated JSON GET; returns the raw string response.
     */
    public static HttpResponse<String> getJson(HttpClient client, String url, String bearer,
                                               String instanceId, Duration timeout) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .header(ACCEPT, APPLICATION_JSON)
                .GET();
        if (bearer != null && !bearer.isBlank()) {
            b.header(AUTHORIZATION, BEARER_PREFIX + bearer);
        }
        if (instanceId != null && !instanceId.isBlank()) {
            b.header(INSTANCE_HEADER, instanceId);
        }
        HttpResponse<String> resp = client.send(b.build(), boundedUtf8());
        String body = resp.body();
        if (body != null) {
            TransferByteMeter.add(body.getBytes(StandardCharsets.UTF_8).length);
        }
        return resp;
    }

    /**
     * True for a 2xx status.
     */
    public static boolean isOk(int status) {
        return status >= 200 && status < 300;
    }

    /**
     * Classifies a throwable from an HTTP {@code send()} as a TRANSPORT failure (could not connect / timed out /
     * reset), as opposed to a completed round-trip, which {@link BackendBaseResolver#reportResponse(int)}
     * classifies by status (only gateway errors count against the base). Used by the outbound clients to decide
     * when to report an overlay failure to the {@link BackendBaseResolver} so the control plane can fall back to
     * public.
     */
    public static boolean isConnectionFailure(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof InterruptedException) {
                return false;
            }
            if (t instanceof java.io.IOException
                    || t instanceof java.nio.channels.UnresolvedAddressException) {
                return true;
            }
        }
        return false;
    }

    /**
     * One log line from a response body: whitespace runs (an nginx/Cloudflare HTML error page's newlines) fold to
     * one space, then the result is capped at 256 chars.
     */
    public static String trim(String body) {
        if (body == null) {
            return "";
        }
        String line = body.replaceAll("\\s+", " ").strip();
        return line.length() > 256 ? line.substring(0, 256) + "…" : line;
    }
}
