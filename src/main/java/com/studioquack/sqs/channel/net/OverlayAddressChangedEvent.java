package com.studioquack.sqs.channel.net;

import org.springframework.context.ApplicationEvent;

/**
 * Published by {@link NetStatusWatcher} when the agent's own overlay IP changes. Listeners (
 * {@code OverlayPeerListener}, {@code OverlayReverseListener}, {@code OverlayReversePortRelay}) rebind — they
 * never stay on a stale address.
 */
public class OverlayAddressChangedEvent extends ApplicationEvent {

    private final String previousIp;
    private final String currentIp;

    public OverlayAddressChangedEvent(Object source, String previousIp, String currentIp) {
        super(source);
        this.previousIp = previousIp;
        this.currentIp = currentIp;
    }

    public String previousIp() {
        return previousIp;
    }

    public String currentIp() {
        return currentIp;
    }
}
