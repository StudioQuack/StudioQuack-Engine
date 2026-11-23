package com.studioquack.sqs.sandbox.helperchan;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Locale;

/**
 * A Unix-domain HTTP front end that delegates to a REAL {@link HelperRequestRouter}, standing in
 * for the in-container broker. {@link UnixHttpStub} answers every request with one canned body,
 * which is enough for a single-shot call but not for a conversation.
 */
final class UnixRouterBridge implements AutoCloseable {

    private final ServerSocketChannel server;
    private final HelperRequestRouter router;
    private volatile boolean closed;

    private UnixRouterBridge(Path socketPath, HelperRequestRouter router) throws IOException {
        this.router = router;
        this.server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        this.server.bind(UnixDomainSocketAddress.of(socketPath));
        Thread acceptor = new Thread(this::acceptLoop, "unix-router-bridge");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    static UnixRouterBridge over(Path socketPath, HelperRequestRouter router) throws IOException {
        return new UnixRouterBridge(socketPath, router);
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                SocketChannel client = server.accept();
                Thread worker = new Thread(() -> serve(client), "unix-router-bridge-conn");
                worker.setDaemon(true);
                worker.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(SocketChannel client) {
        try (SocketChannel c = client) {
            String raw = readRequest(c);
            int headerEnd = raw.indexOf("\r\n\r\n");
            String head = headerEnd < 0 ? raw : raw.substring(0, headerEnd);
            String body = headerEnd < 0 ? "" : raw.substring(headerEnd + 4);
            String[] requestLine = head.split("\r\n")[0].split(" ");
            String method = requestLine.length > 0 ? requestLine[0] : "GET";
            String path = requestLine.length > 1 ? requestLine[1] : "/";
            String sub = path.startsWith("/v1/helper/") ? path.substring("/v1/helper/".length()) : path;

            HelperRequestRouter.RoutedResponse resp = router.route(
                    method, sub, bearer(head), body.getBytes(StandardCharsets.UTF_8));
            write(c, resp);
        } catch (IOException ignore) {
            // a client that hung up mid-request must not take the bridge down
        }
    }

    private static String bearer(String head) {
        for (String line : head.split("\r\n")) {
            if (line.toLowerCase(Locale.ROOT).startsWith("authorization:")) {
                String v = line.substring(line.indexOf(':') + 1).trim();
                return v.startsWith("Bearer ") ? v.substring("Bearer ".length()) : v;
            }
        }
        return "";
    }

    private static void write(SocketChannel c, HelperRequestRouter.RoutedResponse resp) throws IOException {
        byte[] payload = resp.body() == null ? new byte[0] : resp.body();
        String headers = "HTTP/1.1 " + resp.status() + " Status\r\n"
                + "Content-Type: " + resp.contentType() + "\r\n"
                + "Content-Length: " + payload.length + "\r\n"
                + "Connection: close\r\n\r\n";
        c.write(ByteBuffer.wrap(headers.getBytes(StandardCharsets.UTF_8)));
        if (payload.length > 0) {
            c.write(ByteBuffer.wrap(payload));
        }
    }

    /** Reads headers, then exactly {@code Content-Length} body bytes (never to EOF — see stub doc). */
    private static String readRequest(SocketChannel client) throws IOException {
        StringBuilder seen = new StringBuilder();
        ByteBuffer buf = ByteBuffer.allocate(16384);
        int headerEnd = -1;
        while (headerEnd < 0) {
            buf.clear();
            if (client.read(buf) < 0) {
                return seen.toString();
            }
            buf.flip();
            seen.append(StandardCharsets.UTF_8.decode(buf));
            headerEnd = seen.indexOf("\r\n\r\n");
        }
        int declared = contentLength(seen.substring(0, headerEnd));
        int have = seen.length() - (headerEnd + 4);
        while (have < declared) {
            buf.clear();
            int read = client.read(buf);
            if (read < 0) {
                break;
            }
            buf.flip();
            seen.append(StandardCharsets.UTF_8.decode(buf));
            have += read;
        }
        return seen.toString();
    }

    private static int contentLength(String headers) {
        for (String line : headers.split("\r\n")) {
            if (line.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
                try {
                    return Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                } catch (NumberFormatException ignored) {
                    return 0;
                }
            }
        }
        return 0;
    }

    /** Convenience for assertions on tunnelled payloads. */
    static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws IOException {
        closed = true;
        server.close();
    }
}
