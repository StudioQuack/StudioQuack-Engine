package com.studioquack.sqs.channel.net;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link NetAgentClient} for tests. No process, no socket.
 */
public final class FakeNetAgentClient implements NetAgentClient {

    private volatile String controlUrl;
    private volatile String hostname = "env-test";
    private volatile String backendState = "Stopped";
    private volatile String version = "1.102.0-sq-net";
    private volatile String publicKey = "nodekey:test";
    private volatile String interfaceName = "sqnet0";
    private final List<String> selfIps = new CopyOnWriteArrayList<>();
    private final List<NetAgentStatus.Peer> peers = new CopyOnWriteArrayList<>();
    private final AtomicInteger upCalls = new AtomicInteger();
    private final AtomicInteger downCalls = new AtomicInteger();
    private final AtomicInteger statusCalls = new AtomicInteger();
    private volatile String lastAuthKey;
    private volatile RuntimeException nextStatusError;

    @Override
    public void up(String controlUrl, String authKey, String hostname) {
        this.controlUrl = controlUrl;
        this.lastAuthKey = authKey;
        if (hostname != null && !hostname.isBlank()) {
            this.hostname = hostname;
        }
        this.backendState = "Running";
        if (selfIps.isEmpty()) {
            selfIps.add("100.64.0.10");
        }
        upCalls.incrementAndGet();
    }

    @Override
    public void down() {
        backendState = "Stopped";
        downCalls.incrementAndGet();
    }

    @Override
    public void logout() {
        backendState = "NeedsLogin";
        controlUrl = null;
        lastAuthKey = null;
    }

    @Override
    public String version() {
        return version;
    }

    @Override
    public NetAgentStatus status() {
        if (nextStatusError != null) {
            RuntimeException ex = nextStatusError;
            nextStatusError = null;
            throw ex;
        }
        statusCalls.incrementAndGet();
        return new NetAgentStatus(backendState, hostname, publicKey,
                List.copyOf(selfIps), interfaceName, List.copyOf(peers));
    }

    @Override
    public String backend() {
        return "fake";
    }

    public FakeNetAgentClient withSelfIp(String ip) {
        selfIps.clear();
        if (ip != null) {
            selfIps.add(ip);
        }
        return this;
    }

    public FakeNetAgentClient withInterfaceName(String name) {
        this.interfaceName = name;
        return this;
    }

    public FakeNetAgentClient withBackendState(String state) {
        this.backendState = state;
        return this;
    }

    public FakeNetAgentClient withVersion(String version) {
        this.version = version;
        return this;
    }

    public FakeNetAgentClient withPublicKey(String publicKey) {
        this.publicKey = publicKey;
        return this;
    }

    public FakeNetAgentClient withHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public FakeNetAgentClient addPeer(String hostname, String ip, boolean online) {
        List<String> ips = ip == null ? List.of() : List.of(ip);
        peers.add(new NetAgentStatus.Peer(hostname, ips, online));
        return this;
    }

    public FakeNetAgentClient clearPeers() {
        peers.clear();
        return this;
    }

    public FakeNetAgentClient failNextStatus(RuntimeException ex) {
        this.nextStatusError = ex;
        return this;
    }

    public String lastControlUrl() {
        return controlUrl;
    }

    public String lastAuthKey() {
        return lastAuthKey;
    }

    public int upCalls() {
        return upCalls.get();
    }

    public int downCalls() {
        return downCalls.get();
    }

    public int statusCalls() {
        return statusCalls.get();
    }

    public List<NetAgentStatus.Peer> peersView() {
        return new ArrayList<>(peers);
    }
}
