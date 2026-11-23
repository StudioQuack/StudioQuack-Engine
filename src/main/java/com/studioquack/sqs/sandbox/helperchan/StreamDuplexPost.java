package com.studioquack.sqs.sandbox.helperchan;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;

/**
 * Full-duplex HTTP/1.1 {@code POST} for {@code application/x-ndjson} in + SSE out. {@code
 * java.net.http.HttpClient} on HTTP/1.1 does not read the response until the request body
 * completes, so live {@code partial} events would stall until {@code close}.
 */
final class StreamDuplexPost implements Closeable {

    private final Socket socket;
    private final OutputStream out;
    private final InputStream in;
    private final Object writeLock = new Object();
    private volatile boolean requestEnded;
    private volatile boolean chunkedResponse;
    private volatile boolean closed;

    private StreamDuplexPost(Socket socket) throws IOException {
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.in = socket.getInputStream();
    }

    /**
     * Connects, writes request headers (chunked body follows), and returns. Does not
     * wait for the response — the caller starts the SSE reader on another thread.
     */
    static StreamDuplexPost connect(URI uri, Duration connectTimeout, Duration soTimeout)
            throws IOException {
        if (uri == null || uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IOException("stream POST missing host");
        }
        int port = uri.getPort();
        if (port < 0) {
            port = "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
        }
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);
        int connectMs = (int) Math.min(Integer.MAX_VALUE, Math.max(1L, connectTimeout.toMillis()));
        socket.connect(new InetSocketAddress(uri.getHost(), port), connectMs);
        int soMs = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, soTimeout.toMillis()));
        socket.setSoTimeout(soMs);
        StreamDuplexPost post = new StreamDuplexPost(socket);
        post.writeRequestHeaders(uri, port);
        return post;
    }

    private void writeRequestHeaders(URI uri, int port) throws IOException {
        String path = uri.getRawPath();
        if (path == null || path.isBlank()) {
            path = "/";
        }
        if (uri.getRawQuery() != null && !uri.getRawQuery().isBlank()) {
            path = path + "?" + uri.getRawQuery();
        }
        String hostHeader = uri.getHost();
        if (port > 0 && port != 80 && port != 443) {
            hostHeader = hostHeader + ":" + port;
        }
        String headers = "POST " + path + " HTTP/1.1\r\n"
                + "Host: " + hostHeader + "\r\n"
                + "Content-Type: application/x-ndjson\r\n"
                + "Accept: text/event-stream\r\n"
                + "Transfer-Encoding: chunked\r\n"
                + "Connection: close\r\n"
                + "\r\n";
        synchronized (writeLock) {
            out.write(headers.getBytes(StandardCharsets.US_ASCII));
            out.flush();
        }
    }

    /** One NDJSON line (already including the trailing newline) as one HTTP chunk. */
    void writeNdjson(byte[] lineWithNewline) throws IOException {
        if (lineWithNewline == null || lineWithNewline.length == 0) {
            return;
        }
        writeChunk(lineWithNewline);
    }

    /** Ends the request body (zero-length chunk). Safe to call more than once. */
    void endRequest() throws IOException {
        synchronized (writeLock) {
            if (requestEnded || closed) {
                return;
            }
            requestEnded = true;
            out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
        }
    }

    /**
     * Blocks until the status line and response headers arrive. Must run on the SSE
     * reader thread before {@link #bodyReader()}.
     *
     * @return HTTP status code
     */
    int awaitResponse() throws IOException {
        String statusLine = readCrLfLine(in);
        if (statusLine == null || statusLine.isBlank()) {
            throw new IOException("empty HTTP response");
        }
        int status = parseStatus(statusLine);
        chunkedResponse = false;
        while (true) {
            String header = readCrLfLine(in);
            if (header == null || header.isEmpty()) {
                break;
            }
            int colon = header.indexOf(':');
            if (colon <= 0) {
                continue;
            }
            String name = header.substring(0, colon).trim().toLowerCase(Locale.ROOT);
            String value = header.substring(colon + 1).trim().toLowerCase(Locale.ROOT);
            if ("transfer-encoding".equals(name) && value.contains("chunked")) {
                chunkedResponse = true;
            }
        }
        return status;
    }

    /** SSE body; chunk-decoded when the server advertised {@code Transfer-Encoding: chunked}. */
    BufferedReader bodyReader() {
        InputStream body = chunkedResponse ? new ChunkedInputStream(in) : in;
        return new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        closed = true;
        try {
            endRequest();
        } catch (IOException ignored) {
            // best-effort
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }

    private void writeChunk(byte[] payload) throws IOException {
        synchronized (writeLock) {
            if (requestEnded || closed) {
                return;
            }
            out.write(Integer.toHexString(payload.length).getBytes(StandardCharsets.US_ASCII));
            out.write('\r');
            out.write('\n');
            out.write(payload);
            out.write('\r');
            out.write('\n');
            out.flush();
        }
    }

    private static int parseStatus(String statusLine) throws IOException {
        // HTTP/1.1 200 OK
        int first = statusLine.indexOf(' ');
        if (first < 0) {
            throw new IOException("malformed status line");
        }
        int second = statusLine.indexOf(' ', first + 1);
        String code = (second < 0 ? statusLine.substring(first + 1) : statusLine.substring(first + 1, second))
                .trim();
        try {
            return Integer.parseInt(code);
        } catch (NumberFormatException e) {
            throw new IOException("malformed status code");
        }
    }

    /** Reads one CRLF-terminated line without buffering past the delimiter (body must stay intact). */
    private static String readCrLfLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int prev = 0;
        int n;
        while ((n = in.read()) != -1) {
            if (n == '\n' && prev == '\r') {
                byte[] raw = buf.toByteArray();
                int len = Math.max(0, raw.length - 1);
                return new String(raw, 0, len, StandardCharsets.US_ASCII);
            }
            buf.write(n);
            prev = n;
            if (buf.size() > 8192) {
                throw new IOException("HTTP header line too long");
            }
        }
        return buf.size() == 0 ? null : buf.toString(StandardCharsets.US_ASCII);
    }

    /** HTTP/1.1 chunked body decoder. Does not log payload bytes. */
    private static final class ChunkedInputStream extends InputStream {
        private final InputStream in;
        private int remaining;
        private boolean done;

        private ChunkedInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n <= 0 ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (done) {
                return -1;
            }
            if (remaining <= 0) {
                if (!nextChunk()) {
                    done = true;
                    return -1;
                }
            }
            int n = in.read(b, off, Math.min(len, remaining));
            if (n < 0) {
                done = true;
                return -1;
            }
            remaining -= n;
            if (remaining == 0) {
                int c1 = in.read();
                int c2 = in.read();
                if (c1 != '\r' || c2 != '\n') {
                    // tolerate missing CRLF on a closed socket
                }
            }
            return n;
        }

        private boolean nextChunk() throws IOException {
            String sizeLine = readCrLfLine(in);
            if (sizeLine == null) {
                return false;
            }
            int semi = sizeLine.indexOf(';');
            String hex = (semi < 0 ? sizeLine : sizeLine.substring(0, semi)).trim();
            int size;
            try {
                size = Integer.parseInt(hex, 16);
            } catch (NumberFormatException e) {
                return false;
            }
            if (size == 0) {
                readCrLfLine(in);
                return false;
            }
            remaining = size;
            return true;
        }
    }
}
