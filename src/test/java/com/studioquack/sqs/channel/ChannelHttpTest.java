package com.studioquack.sqs.channel;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChannelHttpTest {

    @Test
    void trim_foldsAnHtmlErrorPageIntoOneLine() {
        String nginx502 = "<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body>\r\n"
                + "<center><h1>502 Bad Gateway</h1></center>\r\n<hr><center>nginx</center>\r\n</body>\r\n</html>\r\n";
        String line = ChannelHttp.trim(nginx502);
        assertTrue(line.indexOf('\n') < 0 && line.indexOf('\r') < 0, line);
        assertTrue(line.startsWith("<html> <head><title>502 Bad Gateway</title>"), line);
        assertEquals(257, ChannelHttp.trim("x".repeat(300)).length());
    }

    @Test
    void boundedUtf8_rejectsOversizedBody() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/big", ex -> {
            byte[] body = "abcdefghij".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/big"))
                    .GET().timeout(Duration.ofSeconds(2)).build();
            Exception ex = assertThrows(Exception.class,
                    () -> client.send(req, ChannelHttp.boundedUtf8(4)));
            assertTrue(rootMessage(ex).contains("exceeded 4 bytes"), rootMessage(ex));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void boundedUtf8_acceptsBodyAtLimit() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/ok", ex -> {
            byte[] body = "abcd".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
            HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/ok"))
                    .GET().timeout(Duration.ofSeconds(2)).build();
            HttpResponse<String> resp = client.send(req, ChannelHttp.boundedUtf8(4));
            assertEquals(200, resp.statusCode());
            assertEquals("abcd", resp.body());
        } finally {
            server.stop(0);
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return String.valueOf(c.getMessage());
    }
}
