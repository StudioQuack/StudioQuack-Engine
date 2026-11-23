package com.studioquack.sqs.channel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A tiny mock HTTP server built on the JDK's {@link com.sun.net.httpserver.HttpServer} (no new test
 * dependency). It records every request (method, path, query, headers, body) so tests can assert
 * the EXACT paths + camelCase JSON the daemon clients emit, and serves canned responses (including
 * a programmable responder for hybrid-KEM / WG handshakes that must compute a reply from the
 * request).
 */
public class MockBackend implements AutoCloseable {

    /** A captured inbound request. */
    public record Recorded(String method, String path, String rawQuery,
                           Map<String, String> headers, String body) {
    }

    /** A canned or computed response. */
    public record Response(int status, String body, String contentType,
                           Map<String, String> extraHeaders) {
        public Response {
            extraHeaders = extraHeaders == null ? Map.of() : Map.copyOf(extraHeaders);
        }

        public static Response json(int status, String body) {
            return new Response(status, body, "application/json", Map.of());
        }

        /** JSON plus extra response headers (e.g. {@code Retry-After} on 503). */
        public static Response json(int status, String body, Map<String, String> extraHeaders) {
            return new Response(status, body, "application/json", extraHeaders);
        }

        public static Response sse(String body) {
            return new Response(200, body, "text/event-stream", Map.of());
        }
    }

    /** Computes a response from the recorded request (for handshakes). */
    public interface Responder {
        Response respond(Recorded request) throws Exception;
    }

    private final HttpServer server;
    private final int port;
    private final List<Recorded> recorded = new CopyOnWriteArrayList<>();
    private final Map<String, Responder> responders = new ConcurrentHashMap<>();

    public MockBackend() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/", this::handle);
        server.start();
    }

    /** Base URL to hand to {@code SqsChannelProperties.environmentBase(...)}. */
    public String baseUrl() {
        return "http://127.0.0.1:" + port;
    }

    /** Registers a fixed JSON response for an exact request path. */
    public void stubJson(String path, int status, String body) {
        responders.put(path, req -> Response.json(status, body));
    }

    /** Registers a computed responder for an exact request path. */
    public void stub(String path, Responder responder) {
        responders.put(path, responder);
    }

    /** All requests captured so far, in arrival order. */
    public List<Recorded> requests() {
        return recorded;
    }

    /** The first captured request whose path ends with {@code suffix}, or null. */
    public Recorded firstMatching(String suffix) {
        for (Recorded r : recorded) {
            if (r.path().endsWith(suffix)) {
                return r;
            }
        }
        return null;
    }

    /** Count of captured requests whose path ends with {@code suffix}. */
    public long countMatching(String suffix) {
        return recorded.stream().filter(r -> r.path().endsWith(suffix)).count();
    }

    private void handle(HttpExchange exchange) throws IOException {
        String body;
        try (InputStream in = exchange.getRequestBody()) {
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        Map<String, String> headers = new HashMap<>();
        exchange.getRequestHeaders().forEach((k, v) -> {
            if (!v.isEmpty()) {
                headers.put(k.toLowerCase(), v.get(0));
            }
        });
        String path = exchange.getRequestURI().getPath();
        Recorded rec = new Recorded(exchange.getRequestMethod(), path,
                exchange.getRequestURI().getRawQuery(), headers, body);
        recorded.add(rec);

        Responder responder = responders.get(path);
        Response resp;
        try {
            resp = responder == null
                    ? Response.json(404, "{\"errorCode\":\"Error_NoStub\"}")
                    : responder.respond(rec);
        } catch (Exception ex) {
            resp = Response.json(500, "{\"error\":\"" + ex.getMessage() + "\"}");
        }

        byte[] out = resp.body() == null ? new byte[0] : resp.body().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", resp.contentType());
        resp.extraHeaders().forEach((k, v) -> exchange.getResponseHeaders().add(k, v));
        exchange.sendResponseHeaders(resp.status(), out.length == 0 ? -1 : out.length);
        exchange.getResponseBody().write(out);
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
