package com.studioquack.sqs.sandbox.helperchan;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A minimal HTTP responder on a Unix domain socket, for tests that exercise the in-sandbox SDKs.
 * The SDKs reach the daemon over a UDS rather than TCP, and {@code
 * com.sun.net.httpserver.HttpServer} cannot bind {@code AF_UNIX} — so a hand-rolled responder is
 * the only way to stand in for the in-container broker.
 */
final class UnixHttpStub implements AutoCloseable {

    private final ServerSocketChannel server;
    private final String response;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    private UnixHttpStub(Path socketPath, String response) throws IOException {
        this.server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        this.server.bind(UnixDomainSocketAddress.of(socketPath));
        this.response = response;
        Thread acceptor = new Thread(this::serve, "unix-http-stub");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    /** Binds {@code socketPath} and answers every request with {@code status} + {@code body}. */
    static UnixHttpStub serving(Path socketPath, int status, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        String response = "HTTP/1.1 " + status + " " + reason(status) + "\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: " + payload.length + "\r\n"
                + "Connection: close\r\n\r\n"
                + body;
        return new UnixHttpStub(socketPath, response);
    }

    /**
     * Binds {@code socketPath} and answers every request with an SSE body.
     * No Content-Length: the client must read line-by-line until the socket closes.
     */
    static UnixHttpStub servingSse(Path socketPath, String sseBody) throws IOException {
        String response = "HTTP/1.1 200 OK\r\n"
                + "Content-Type: text/event-stream\r\n"
                + "Cache-Control: no-cache\r\n"
                + "Connection: close\r\n\r\n"
                + sseBody;
        return new UnixHttpStub(socketPath, response);
    }

    /** Every request received so far, headers and body, in arrival order. */
    List<String> requests() {
        return requests;
    }

    private void serve() {
        while (!closed) {
            try (SocketChannel client = server.accept()) {
                requests.add(readRequest(client));
                client.write(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
            } catch (IOException e) {
                if (closed) {
                    return;
                }
                // One bad connection must not take the stub down mid-test.
            }
        }
    }

    /** Reads headers, then exactly {@code Content-Length} bytes of body. */
    private static String readRequest(SocketChannel client) throws IOException {
        StringBuilder seen = new StringBuilder();
        ByteBuffer buf = ByteBuffer.allocate(8192);
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
            if (line.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:")) {
                try {
                    return Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                } catch (NumberFormatException ignored) {
                    return 0;
                }
            }
        }
        return 0;
    }

    private static String reason(int status) {
        return switch (status) {
            case 200 -> "OK";
            case 401 -> "Unauthorized";
            case 500 -> "Internal Server Error";
            default -> "Status";
        };
    }

    @Override
    public void close() throws IOException {
        closed = true;
        server.close();
    }
}
