package com.studioquack.sqs.channel.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.stream.Stream;

/**
 * Answers reverse lookups of private-network (overlay) addresses at once with "no name"; every other lookup
 * goes to the platform resolver. JDK {@code HttpsServer} reverse-resolves each client address before its first
 * TLS byte ({@code sun.net.httpserver.SSLStreams}: {@code createSSLEngine(addr.getHostName(), port)}).
 */
public final class OverlayReverseLookupProvider extends InetAddressResolverProvider {

    private static volatile boolean active;

    @Override
    public InetAddressResolver get(Configuration configuration) {
        active = true;
        return resolver(configuration.builtinResolver());
    }

    static InetAddressResolver resolver(InetAddressResolver platform) {
        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy)
                    throws UnknownHostException {
                return platform.lookupByName(host, lookupPolicy);
            }

            @Override
            public String lookupByAddress(byte[] addr) throws UnknownHostException {
                if (isOverlay(addr)) {
                    throw new UnknownHostException("private-network address has no host name");
                }
                return platform.lookupByAddress(addr);
            }
        };
    }

    @Override
    public String name() {
        return "studioquack-overlay-reverse-lookup";
    }

    /**
     * Makes the first lookup in the JVM, which fixes the resolver for the life of the process.
     *
     * @return whether this provider is the one in use
     */
    public static boolean install() {
        try {
            InetAddress.getAllByName("localhost");
        } catch (UnknownHostException ignored) {
            // The resolver is chosen before the lookup runs; a missing localhost entry changes nothing.
        }
        return active;
    }

    /**
     * True once the JDK resolves through this provider.
     */
    public static boolean active() {
        return active;
    }

    /**
     * 100.64.0.0/10 (overlay IPv4) or fd7a:115c:a1e0::/48 (overlay IPv6).
     */
    static boolean isOverlay(byte[] addr) {
        if (addr == null) {
            return false;
        }
        if (addr.length == 4) {
            return (addr[0] & 0xff) == 100 && (addr[1] & 0xc0) == 0x40;
        }
        if (addr.length == 16) {
            return (addr[0] & 0xff) == 0xfd && (addr[1] & 0xff) == 0x7a
                    && (addr[2] & 0xff) == 0x11 && (addr[3] & 0xff) == 0x5c
                    && (addr[4] & 0xff) == 0xa1 && (addr[5] & 0xff) == 0xe0;
        }
        return false;
    }
}
