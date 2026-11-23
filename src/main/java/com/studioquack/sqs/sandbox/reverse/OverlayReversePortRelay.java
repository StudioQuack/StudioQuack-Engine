package com.studioquack.sqs.sandbox.reverse;

import com.studioquack.sqs.channel.net.OverlayAddressChangedEvent;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Publishes a container's port on the engine's overlay address and polices who may dial it, per
 * network. This is the component that makes a reverse channel reachable and the component that
 * keeps it narrow.
 */
@Slf4j
@Component
public class OverlayReversePortRelay {

    /** Concurrent connections per published port — a bound, not a tuning knob (DoS control). */
    static final int MAX_CONNECTIONS_PER_PORT = 64;
    private static final int BUFFER_BYTES = 16 * 1024;
    /** Network key for the legacy single-network call form (tests, pre-drain daemons). */
    public static final String DEFAULT_NETWORK = "default";

    private final NetworkRuleStore ruleStore;
    private final OverlayPeerDirectory peerDirectory;
    private final ReversePortRegistry registry;
    private final ExecutorService ioPool;

    /** One bind address per enrolled network (key = the network's interface name). */
    private final Map<String, String> overlayIps = new ConcurrentHashMap<>();
    private final Map<Integer, PublishedPort> published = new ConcurrentHashMap<>();

    /**
     * {@code @Autowired} is load-bearing: the test-seam constructor below makes this a
     * multi-constructor bean, and Spring then looks for a no-arg one and fails the whole context.
     */
    @Autowired
    public OverlayReversePortRelay(NetworkRuleStore ruleStore, OverlayPeerDirectory peerDirectory) {
        this(ruleStore, peerDirectory, new ReversePortRegistry(), defaultPool());
    }

    /** Test seam — injects the registry + pool. */
    OverlayReversePortRelay(NetworkRuleStore ruleStore, OverlayPeerDirectory peerDirectory,
                            ReversePortRegistry registry, ExecutorService ioPool) {
        this.ruleStore = ruleStore;
        this.peerDirectory = peerDirectory;
        this.registry = registry;
        this.ioPool = ioPool;
    }

    private static ExecutorService defaultPool() {
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "sq-reverse-io-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        return Executors.newCachedThreadPool(tf);
    }

    /** Legacy single-network form — the default network's address changed. */
    public synchronized void overlayUp(String overlayCidr) {
        overlayUp(DEFAULT_NETWORK, overlayCidr);
    }

    /**
     * Agent overlay IP changed: unbind the stale address and rebind published ports on the
     * new one. Refuses {@code 0.0.0.0} / wildcard — same never-wildcard rule as the HTTP listeners.
     */
    @EventListener
    public void onOverlayAddressChanged(OverlayAddressChangedEvent event) {
        if (event == null || event.currentIp() == null || event.currentIp().isBlank()) {
            return;
        }
        try {
            overlayUp(event.currentIp());
        } catch (RuntimeException ex) {
            log.warn("reverse-relay: overlay rebind to {} failed: {}", event.currentIp(), ex.toString());
        }
    }

    /** One network's engine address came up (handshake / watchdog recovery / drain begin). */
    public synchronized void overlayUp(String networkKey, String overlayCidr) {
        String ip = hostOfCidr(overlayCidr);
        if (ip.isEmpty() || networkKey == null || networkKey.isBlank()) {
            return;
        }
        assertOverlayBindAddress(ip);
        String previous = overlayIps.put(networkKey, ip);
        if (!ip.equals(previous)) {
            // The address this network's published sockets are bound to is gone — rebind them on the new one.
            // Claims survive: port space is per engine, and the engine is still itself.
            for (Map.Entry<Integer, PublishedPort> e : published.entrySet()) {
                if (networkKey.equals(e.getValue().networkKey)) {
                    rebind(e.getKey(), e.getValue(), ip);
                }
            }
        }
    }

    /** The tunnel went down: close every listener on every network. Claims survive a reconnect. */
    public synchronized void overlayDown(String reason) {
        overlayIps.clear();
        for (PublishedPort p : published.values()) {
            p.closeListener();
        }
        log.info("reverse-relay: overlay down ({}) — {} published port(s) suspended", reason, published.size());
    }

    /** ONE network went away (drain finalize): close its listeners, forget its address. */
    public synchronized void overlayDown(String networkKey, String reason) {
        overlayIps.remove(networkKey);
        int closed = 0;
        for (PublishedPort p : published.values()) {
            if (networkKey.equals(p.networkKey)) {
                p.closeListener();
                closed++;
            }
        }
        log.info("reverse-relay: network {} down ({}) — {} published port(s) on it suspended",
                networkKey, reason, closed);
    }

    /** Legacy single-network form — publishes on the default network. */
    public synchronized void publish(int port, String targetHost, int targetPort, String claimant) {
        publish(port, targetHost, targetPort, claimant, DEFAULT_NETWORK);
    }

    /**
     * Publishes {@code targetHost:targetPort} as {@code overlayIp:port} ON THE GIVEN NETWORK.
     *
     * @throws ReversePortRegistry.ReversePortException When the port is reserved or already held.
     * @throws IllegalStateException When that network's overlay is down or the bind fails — the s.
     */
    public synchronized void publish(int port, String targetHost, int targetPort, String claimant,
                                     String networkKey) {
        // A blank key means "the engine's current primary network" (a pre-drain instance record,
        // or a start that raced the overlay): the peer directory knows which network that is.
        String key = (networkKey == null || networkKey.isBlank())
                ? peerDirectory.primaryKey() : networkKey;
        registry.claim(port, claimant);
        // Same-claimant re-publish (a stateful instance restarted on a new bridge IP): close the
        // stale listener before binding the new target, so the port never points at a dead container.
        PublishedPort stale = published.remove(port);
        if (stale != null) {
            stale.close();
        }
        try {
            PublishedPort p = new PublishedPort(port, targetHost, targetPort, claimant, key);
            p.bind(overlayIps.get(key));
            published.put(port, p);
            log.info("reverse-relay: published overlay:{} -> {}:{} for {} on network {}",
                    port, targetHost, targetPort, claimant, key);
        } catch (IOException e) {
            registry.release(port, claimant);
            throw new IllegalStateException("reverse port " + port + " could not be published on network "
                    + key + ": " + e.getMessage(), e);
        }
    }

    /** Stops publishing {@code port} (closes live connections) and releases the claim. */
    public synchronized void unpublish(int port, String claimant) {
        PublishedPort p = published.remove(port);
        if (p != null) {
            p.close();
        }
        registry.release(port, claimant);
    }

    /**
     * The drain quiesce: stops accepting NEW connections on {@code port} (the listener closes) and
     * waits, up to {@code drainWaitMs}, for the live ones to finish — the "quiesces, drains" half of
     * the stateful-instance replacement. The port stays published and the claim stays held, so the
     * successor re-publishes the same port on the new network as the same claimant.
     */
    public synchronized boolean quiescePort(int port, long drainWaitMs) {
        PublishedPort p = published.get(port);
        if (p == null) {
            return true;
        }
        p.closeListener();
        long deadline = System.nanoTime() + Math.max(0L, drainWaitMs) * 1_000_000L;
        while (!p.live.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        boolean drained = p.live.isEmpty();
        log.info("reverse-relay: quiesced port {} on network {} (drained={} remaining={})",
                port, p.networkKey, drained, p.live.size());
        return drained;
    }

    /**
     * @return whether {@code sourceIp} may dial {@code port} under the projected rules, resolved
     *         on the network the listener belongs to — a source address only ever means anything
     *         on the network it arrived from.
     */
    boolean isAllowed(int port, String sourceIp, String networkKey) {
        if (sourceIp == null || sourceIp.isBlank()) {
            return false;
        }
        Set<String> sources = ruleStore.allowedSourceEngineIds(port);
        for (String engineId : sources) {
            // AddressOn is network-scoped; addressOf is the daemon-dialing view. Both resolve through
            // PeerAddressResolver when the agent map is live.
            if (peerDirectory.addressOn(networkKey, engineId).map(sourceIp::equals).orElse(false)
                    || peerDirectory.addressOf(engineId).map(sourceIp::equals).orElse(false)) {
                return true;
            }
        }
        return false;
    }

    /** @return the default network's bind address (diagnostics; {@code null} when it is down). */
    public String overlayIp() {
        return overlayIps.get(DEFAULT_NETWORK);
    }

    /** @return one network's bind address ({@code null} when that network is down). */
    public String overlayIp(String networkKey) {
        return overlayIps.get(networkKey);
    }

    /** @return true when at least one network has a bind address (republish gates). */
    public boolean anyOverlayUp() {
        return !overlayIps.isEmpty();
    }

    /** @return published ports (diagnostics / status surface). */
    public Set<Integer> publishedPorts() {
        return Set.copyOf(published.keySet());
    }

    private void rebind(int port, PublishedPort p, String ip) {
        try {
            p.bind(ip);
            log.info("reverse-relay: re-published overlay:{} on new address {} (network {})",
                    port, ip, p.networkKey);
        } catch (IOException e) {
            // Keep the claim; the next overlayUp retries. Loud because the port is unreachable.
            log.error("reverse-relay: rebind of port {} failed on {}: {}", port, ip, e.toString());
        }
    }

    static void assertOverlayBindAddress(String host) {
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("reverse-relay: no overlay bind address — refusing");
        }
        InetAddress addr;
        try {
            addr = InetAddress.getByName(host.trim());
        } catch (java.net.UnknownHostException e) {
            throw new IllegalStateException("reverse-relay: unresolvable overlay bind host " + host, e);
        }
        if (addr.isAnyLocalAddress()) {
            throw new IllegalStateException("reverse-relay: wildcard bind address " + host
                    + " refused — published ports bind the overlay IP only");
        }
    }

    private static String hostOfCidr(String cidr) {
        if (cidr == null || cidr.isBlank()) {
            return "";
        }
        int slash = cidr.indexOf('/');
        return (slash < 0 ? cidr : cidr.substring(0, slash)).trim();
    }

    /** One published port: the overlay listener, its acceptor thread, and its live connections. */
    private final class PublishedPort {
        private final int port;
        private final String targetHost;
        private final int targetPort;
        private final String networkKey;
        private final List<Socket> live = new CopyOnWriteArrayList<>();
        private volatile ServerSocket listener;

        PublishedPort(int port, String targetHost, int targetPort, String claimant, String networkKey) {
            this.port = port;
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.networkKey = networkKey;
            // claimant is tracked by the registry (it owns conflict reporting); the port
            // itself only needs the dial target.
        }

        void bind(String bindIp) throws IOException {
            closeListener();
            if (bindIp == null || bindIp.isBlank()) {
                throw new IOException("overlay network " + networkKey
                        + " is down — no engine address to publish on");
            }
            assertOverlayBindAddress(bindIp);
            ServerSocket server = new ServerSocket(port, 50, InetAddress.getByName(bindIp));
            this.listener = server;
            Thread t = new Thread(this::acceptLoop, "sq-reverse-accept-" + port);
            t.setDaemon(true);
            t.start();
        }

        private void acceptLoop() {
            ServerSocket server = listener;
            while (server != null && !server.isClosed()) {
                final Socket inbound;
                try {
                    inbound = server.accept();
                } catch (IOException closed) {
                    return;
                }
                String source = inbound.getInetAddress() == null
                        ? "" : inbound.getInetAddress().getHostAddress();
                if (live.size() >= MAX_CONNECTIONS_PER_PORT) {
                    log.warn("reverse-relay: port {} at connection cap — refusing {}", port, source);
                    closeQuietly(inbound);
                    continue;
                }
                if (!isAllowed(port, source, networkKey)) {
                    // Default DENY: no matching Network Access Control rule. Debug level — a port scan must not be
                    // able to fill the daemon log.
                    log.debug("reverse-relay: refused {} on port {} (no rule)", source, port);
                    closeQuietly(inbound);
                    continue;
                }
                live.add(inbound);
                ioPool.submit(() -> serve(inbound, source));
            }
        }

        /** Relays one accepted connection to the container and back until either side closes. */
        private void serve(Socket inbound, String source) {
            Socket outbound = null;
            try {
                outbound = new Socket(targetHost, targetPort);
                Socket out = outbound;
                // One pump runs on this thread, the other on the pool; when either ends
                // (EOF / error / reset) both sockets close, which unblocks the other pump.
                ioPool.submit(() -> pump(out, inbound, source));
                pump(inbound, out, source);
            } catch (IOException e) {
                log.debug("reverse-relay: {} -> port {} target unreachable: {}", source, port, e.toString());
            } finally {
                closeQuietly(inbound);
                closeQuietly(outbound);
                live.remove(inbound);
            }
        }

        private void pump(Socket from, Socket to, String source) {
            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                byte[] buf = new byte[BUFFER_BYTES];
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // A reset/close ends the pump; the finally in serve() reaps both sockets.
            } finally {
                closeQuietly(from);
                closeQuietly(to);
            }
        }

        void closeListener() {
            ServerSocket server = listener;
            listener = null;
            if (server != null) {
                try {
                    server.close();
                } catch (IOException ignored) {
                    // closing anyway
                }
            }
        }

        void close() {
            closeListener();
            for (Socket s : live) {
                closeQuietly(s);
            }
            live.clear();
        }
    }

    private static void closeQuietly(Socket s) {
        if (s != null) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
    }
}
