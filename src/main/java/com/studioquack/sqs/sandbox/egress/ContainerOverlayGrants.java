package com.studioquack.sqs.sandbox.egress;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Network Access Control, caller side — pure argv builder for one sandbox container's overlay
 * grants in {@link OverlayEgressRules#CHAIN_PERBOX}. The overlay is default-deny in {@code
 * SQS_SBX_FWD}; these ACCEPTs are the only way a container's packets reach it.
 */
public final class ContainerOverlayGrants {

    private ContainerOverlayGrants() {
    }

    /**
     * One authorized outbound dial, already resolved: the GRANTED target engine's current
     * overlay address plus the granted port. Resolution happens in the caller (sandbox egress
     * manager) through the same-owner peer directory <b>on the container's network</b>; an
     * engine id that does not resolve there never becomes a grant, so this builder can only
     * ever narrow, never widen.
     */
    public record OverlayGrant(String destinationIp, int port) {
        public OverlayGrant {
            Objects.requireNonNull(destinationIp, "destinationIp");
            if (destinationIp.isBlank()) {
                throw new IllegalArgumentException("destinationIp must not be blank");
            }
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("port out of range: " + port);
            }
        }
    }

    /**
     * The ACCEPTs opening {@code grants} for one container. Each is pinned to one destination {@code
     * /32} AND one TCP port AND the container's own network interface — least privilege per the
     * Network Access Control projection, and per network so a mid-drain container can never be handed
     * an exit its tunnel does not have.
     */
    public static List<List<String>> rulesFor(String containerIp, String wgIface, List<OverlayGrant> grants) {
        Objects.requireNonNull(containerIp, "containerIp");
        if (containerIp.isBlank()) {
            throw new IllegalArgumentException("containerIp must not be blank");
        }
        // Dedup + sort so the same projection always yields the same argv sequence — the
        // manager diffs by exact argv when a peer's address churns.
        List<OverlayGrant> unique = (grants == null ? List.<OverlayGrant>of() : grants).stream()
                .distinct()
                .sorted(Comparator.comparing(OverlayGrant::destinationIp)
                        .thenComparingInt(OverlayGrant::port))
                .toList();
        List<List<String>> out = new ArrayList<>(unique.size());
        for (OverlayGrant g : unique) {
            out.add(List.of("iptables", "-A", OverlayEgressRules.CHAIN_PERBOX,
                    "-s", containerIp,
                    "-d", g.destinationIp() + "/32",
                    "-p", "tcp", "--dport", Integer.toString(g.port()),
                    "-o", wgIface,
                    "-m", "comment", "--comment", "sq-overlay-grant",
                    "-j", "ACCEPT"));
        }
        return out;
    }
}
