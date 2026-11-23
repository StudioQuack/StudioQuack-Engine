package com.studioquack.sqs.channel;

import java.io.IOException;
import java.io.InputStream;

/**
 * Caps JDK {@code HttpExchange} request bodies. Overlay/public JSON routes share this ceiling; OCI blob ingest
 * stays on its own stream path ( residual).
 */
public final class BoundedExchangeBody {

    /**
     * Deploy graphs and peer execute payloads; not a typical size.
     */
    public static final int MAX_JSON_BYTES = 16 * 1024 * 1024;

    private BoundedExchangeBody() {
    }

    public static byte[] read(InputStream in) throws IOException {
        return read(in, MAX_JSON_BYTES);
    }

    public static byte[] read(InputStream in, int maxBytes) throws IOException {
        byte[] buf = in.readNBytes(maxBytes + 1);
        if (buf.length > maxBytes) {
            throw new TooLarge(maxBytes);
        }
        return buf;
    }

    public static final class TooLarge extends IOException {
        public TooLarge(int maxBytes) {
            super("request body exceeded " + maxBytes + " bytes");
        }
    }
}
