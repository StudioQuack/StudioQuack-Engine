package com.studioquack.sqs.channel.wg;

import org.springframework.context.ApplicationEvent;

/**
 * Fired when a {@code set-mesh-peers} push changes the PRIMARY network's granted-peer {@code envId → address}
 * mapping between handshakes. Why it exists: the directory and the WireGuard data plane are refreshed on
 * different schedules — the directory instantly on this push, the interface's {@code allowed-ips} and host
 * routes only at handshake / PSK rotation.
 */
public class MeshPeerSetChangedEvent extends ApplicationEvent {

    /**
     * The network (interface name) whose granted view changed — always the primary today.
     */
    private final String networkKey;

    public MeshPeerSetChangedEvent(Object source, String networkKey) {
        super(source);
        this.networkKey = networkKey;
    }

    public String networkKey() {
        return networkKey;
    }
}
