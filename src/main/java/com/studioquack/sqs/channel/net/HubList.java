package com.studioquack.sqs.channel.net;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ordered hub-proxy list from handshake v2 / the set-hubs command. The health probe walks in order;
 * {@link #inUse()} is the last reachable entry.
 */
@Component
public final class HubList {

    private final AtomicReference<List<HubAddress>> hubs = new AtomicReference<>(List.of());
    private final AtomicReference<HubAddress> inUse = new AtomicReference<>();

    public void replace(List<HubAddress> next) {
        List<HubAddress> copy = new ArrayList<>();
        if (next != null) {
            for (HubAddress h : next) {
                if (h != null && h.usable()) {
                    copy.add(h);
                }
            }
        }
        hubs.set(List.copyOf(copy));
        HubAddress current = inUse.get();
        if (current != null && copy.stream().noneMatch(h -> same(h, current))) {
            inUse.set(null);
        }
    }

    public List<HubAddress> snapshot() {
        return hubs.get();
    }

    public HubAddress inUse() {
        return inUse.get();
    }

    public void markInUse(HubAddress hub) {
        inUse.set(hub);
    }

    public boolean currentGone() {
        HubAddress current = inUse.get();
        if (current == null) {
            return !hubs.get().isEmpty();
        }
        return hubs.get().stream().noneMatch(h -> same(h, current));
    }

    private static boolean same(HubAddress a, HubAddress b) {
        if (a == null || b == null) {
            return false;
        }
        return a.port() == b.port()
                && a.address() != null && a.address().equals(b.address());
    }
}
