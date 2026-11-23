package com.studioquack.sqs.sandbox.egress;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Pure, side-effect-free host-rule builder for sandbox egress. Internet leaves via the host
 * default route ({@code MASQUERADE} in {@code nat/POSTROUTING}).
 */
public final class OverlayEgressRules {

    /** filter/INPUT chain: packets from sandbox subnets addressed to this host. */
    public static final String CHAIN_IN = "SQS_SBX_IN";
    /** filter/FORWARD chain: packets the host would route onward. */
    public static final String CHAIN_FWD = "SQS_SBX_FWD";
    /**
     * filter chain jumped to from {@link #CHAIN_FWD} right below the daemon-listener DROPs,
     * holding the PER-CONTAINER overlay grants (Network Access Control, caller side): which
     * {@code (peer /32, port)} pairs each sandbox IP may dial. Created here but NEVER flushed by
     * a re-apply; only {@link #resetGrantChainCommands} empties it.
     */
    public static final String CHAIN_PERBOX = "SQS_SBX_PERBOX";
    /**
     * nat/POSTROUTING chain: RETURN local/overlay destinations, then MASQUERADE the rest
     * onto the host default egress.
     */
    public static final String CHAIN_NAT = "SQS_SBX_NAT";
    /**
     * Per-network daemon-surface denies (INPUT half): each enrolled network contributes
     * its {@code <overlayIp>:9876} DROP.
     */
    public static final String CHAIN_DMN = "SQS_SBX_DMN";
    /**
     * Per-network daemon-listener denies (FORWARD half): {@code :9876}/{@code :8443}
     * DROPs scoped to that network's overlay CIDR, ABOVE the PERBOX jump.
     */
    public static final String CHAIN_DMNF = "SQS_SBX_DMNF";
    /**
     * Hairpin deny: each enrolled overlay iface contributes {@code -o <iface> DROP}.
     * Internet that survives the local DROPs is ACCEPTed on {@link #CHAIN_FWD} after this
     * jump (host default egress), not via the mesh.
     */
    public static final String CHAIN_OUT = "SQS_SBX_OUT";

    /**
     * CGNAT overlay range. IPv6 overlay deny is {@code fd7a:115c:a1e0::/48} when {@code ip6tables} is
     * programmed.
     */
    public static final String OVERLAY_CIDR = "100.64.0.0/10";

    /** Kept for {@link NetworkPlan} table bookkeeping; does not install a default via overlay. */
    public static final int TABLE_ID = 88;
    /** Base preference for {@link NetworkPlan} bookkeeping. */
    public static final int STEER_PREF_BASE = 10_700;

    /** Overlay reverse-listener port denied from sandboxes (see OverlayReverseListener). */
    public static final int OVERLAY_API_PORT = 9876;

    /**
     * The daemon's overlay execute endpoint (mTLS) — denied from sandboxes on ANY overlay
     * address, mirroring {@link #OVERLAY_API_PORT}.
     */
    public static final int OVERLAY_EXECUTE_PORT = 8443;

    /**
     * Destinations that must never be MASQUERADE'd (and are DROP'd on FORWARD): overlay CGNAT,
     * RFC1918, link-local / metadata, loopback.
     */
    static final List<String> NAT_EXCLUDE_DESTS = List.of(
            OVERLAY_CIDR,
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "169.254.0.0/16",
            "127.0.0.0/8");

    private OverlayEgressRules() {
    }

    /** One discovered Podman sandbox network (name + addressing). */
    public record SandboxNet(String name, String subnetCidr, String gatewayIp) {
        public SandboxNet {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(subnetCidr, "subnetCidr");
            Objects.requireNonNull(gatewayIp, "gatewayIp");
        }
    }

    /**
     * One enrolled overlay network's rule inputs.
     *
     * @param key The manager's network key (the interface name) — diagnostics only.
     * @param iface The OS overlay interface (default {@code sqnet0}).
     * @param overlayIp The engine's own overlay address on this network (host part, no /32).
     */
    public record NetworkPlan(String key, String iface, String overlayIp, String overlayScope,
                              int tableId, int steeringPref) {
        public NetworkPlan {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(iface, "iface");
            Objects.requireNonNull(overlayIp, "overlayIp");
            Objects.requireNonNull(overlayScope, "overlayScope");
        }
    }

    /**
     * Full rebuild for the FIRST enrolled network: create-or-flush the shared chains, rebuild
     * the skeleton (host-egress MASQUERADE + local fail-closed), then add this network's
     * daemon-listener denies and overlay-iface hairpin DROP.
     */
    public static List<List<String>> reconcileCommands(NetworkPlan first, List<SandboxNet> nets) {
        Objects.requireNonNull(first, "first");
        List<List<String>> out = new ArrayList<>();

        createOrFlushFilter(out, CHAIN_IN);
        createOrFlushFilter(out, CHAIN_DMN);
        createOrFlushFilter(out, CHAIN_FWD);
        createOrFlushFilter(out, CHAIN_DMNF);
        createOrFlushFilter(out, CHAIN_OUT);
        createOrFlushNat(out, CHAIN_NAT);
        // PERBOX is create-if-absent, NEVER flushed: it holds live containers' grant rules.
        out.add(List.of("iptables", "-N", CHAIN_PERBOX));

        for (SandboxNet net : nets(nets)) {
            String subnet = net.subnetCidr();
            checkThenInsert(out, List.of("iptables", "-C", "INPUT", "-s", subnet, "-j", CHAIN_IN),
                    List.of("iptables", "-I", "INPUT", "-s", subnet, "-j", CHAIN_IN));
            checkThenInsert(out, List.of("iptables", "-C", "FORWARD", "-s", subnet, "-j", CHAIN_FWD),
                    List.of("iptables", "-I", "FORWARD", "-s", subnet, "-j", CHAIN_FWD));
        }

        // ---- SQS_SBX_IN: per-network daemon denies, then loopback DROP, then fail-closed ----
        out.add(List.of("iptables", "-A", CHAIN_IN, "-j", CHAIN_DMN));
        out.add(List.of("iptables", "-A", CHAIN_IN, "-d", "127.0.0.0/8", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_IN, "-j", "DROP"));

        // ---- SQS_SBX_FWD: established → daemon-listener DROPs → PERBOX → cross-sandbox
        //      → local/overlay DROPs → no-hairpin → host-egress ACCEPT → fail-closed ----
        out.add(List.of("iptables", "-A", CHAIN_FWD,
                "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-j", CHAIN_DMNF));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-j", CHAIN_PERBOX));
        List<SandboxNet> nets_ = nets(nets);
        for (int i = 0; i < nets_.size(); i++) {
            for (int j = 0; j < nets_.size(); j++) {
                if (i == j) {
                    continue;
                }
                out.add(List.of("iptables", "-A", CHAIN_FWD,
                        "-s", nets_.get(i).subnetCidr(), "-d", nets_.get(j).subnetCidr(), "-j", "DROP"));
            }
        }
        appendLocalDenies(out);
        // Hairpin deny (per-network -o overlay DROP) then host-egress ACCEPT.
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-j", CHAIN_OUT));
        out.add(List.of("iptables", "-A", CHAIN_FWD,
                "-m", "comment", "--comment", "sq-host-egress", "-j", "ACCEPT"));
        out.add(List.of("iptables", "-A", CHAIN_FWD,
                "-m", "comment", "--comment", "sq-sbx-fail-closed", "-j", "DROP"));

        // ---- NAT: exclude overlay/RFC1918/link-local/loopback, then MASQUERADE ----
        for (String dest : NAT_EXCLUDE_DESTS) {
            out.add(List.of("iptables", "-t", "nat", "-A", CHAIN_NAT, "-d", dest, "-j", "RETURN"));
        }
        out.add(List.of("iptables", "-t", "nat", "-A", CHAIN_NAT, "-j", "MASQUERADE"));
        for (SandboxNet n : nets_) {
            checkThenInsert(out,
                    List.of("iptables", "-t", "nat", "-C", "POSTROUTING", "-s", n.subnetCidr(),
                            "-j", CHAIN_NAT),
                    List.of("iptables", "-t", "nat", "-I", "POSTROUTING", "-s", n.subnetCidr(),
                            "-j", CHAIN_NAT));
        }

        out.addAll(addNetworkCommands(first, nets_));
        return List.copyOf(out);
    }

    /**
     * Additive content for one enrolled network: daemon-listener denies and the overlay-iface
     * hairpin DROP. No SNAT, no policy-routing default via the overlay iface.
     */
    public static List<List<String>> addNetworkCommands(NetworkPlan net, List<SandboxNet> nets) {
        Objects.requireNonNull(net, "net");
        List<List<String>> out = new ArrayList<>();

        out.add(List.of("iptables", "-A", CHAIN_DMN,
                "-d", net.overlayIp(), "-p", "tcp", "--dport", Integer.toString(OVERLAY_API_PORT),
                "-m", "comment", "--comment", "deny-sandbox-to-overlay-reverse-listener",
                "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_DMNF,
                "-d", net.overlayScope(), "-p", "tcp", "--dport", Integer.toString(OVERLAY_API_PORT),
                "-m", "comment", "--comment", "deny-sandbox-to-daemon-peer-listener",
                "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_DMNF,
                "-d", net.overlayScope(), "-p", "tcp", "--dport", Integer.toString(OVERLAY_EXECUTE_PORT),
                "-m", "comment", "--comment", "deny-sandbox-to-daemon-execute-endpoint",
                "-j", "DROP"));
        // Internet must not leave via the overlay iface (no hairpin).
        out.add(List.of("iptables", "-A", CHAIN_OUT,
                "-o", net.iface(),
                "-m", "comment", "--comment", "sq-no-overlay-hairpin",
                "-j", "DROP"));
        return List.copyOf(out);
    }

    /**
     * The per-subnet half of {@link #reconcileCommands} for sandbox networks created after it ran
     * (Podman creates {@code sqs-jobs} the first time a job needs internet): the INPUT / FORWARD /
     * POSTROUTING jumps and the cross-sandbox DROPs, all check-then-insert. No shared chain is
     * flushed, so sandboxes already running stay filtered throughout.
     */
    public static List<List<String>> addSubnetCommands(List<SandboxNet> added, List<SandboxNet> all) {
        List<List<String>> out = new ArrayList<>();
        for (SandboxNet net : nets(added)) {
            String subnet = net.subnetCidr();
            checkThenInsert(out, List.of("iptables", "-C", "INPUT", "-s", subnet, "-j", CHAIN_IN),
                    List.of("iptables", "-I", "INPUT", "-s", subnet, "-j", CHAIN_IN));
            checkThenInsert(out, List.of("iptables", "-C", "FORWARD", "-s", subnet, "-j", CHAIN_FWD),
                    List.of("iptables", "-I", "FORWARD", "-s", subnet, "-j", CHAIN_FWD));
            for (SandboxNet other : nets(all)) {
                String peer = other.subnetCidr();
                if (peer.equals(subnet)) {
                    continue;
                }
                checkThenInsert(out,
                        List.of("iptables", "-C", CHAIN_FWD, "-s", subnet, "-d", peer, "-j", "DROP"),
                        List.of("iptables", "-I", CHAIN_FWD, "1", "-s", subnet, "-d", peer, "-j", "DROP"));
                checkThenInsert(out,
                        List.of("iptables", "-C", CHAIN_FWD, "-s", peer, "-d", subnet, "-j", "DROP"),
                        List.of("iptables", "-I", CHAIN_FWD, "1", "-s", peer, "-d", subnet, "-j", "DROP"));
            }
            checkThenInsert(out,
                    List.of("iptables", "-t", "nat", "-C", "POSTROUTING", "-s", subnet, "-j", CHAIN_NAT),
                    List.of("iptables", "-t", "nat", "-I", "POSTROUTING", "-s", subnet, "-j", CHAIN_NAT));
        }
        return List.copyOf(out);
    }

    /**
     * Empties {@link #CHAIN_PERBOX}, creating it first so the flush cannot fail on a fresh host.
     * The {@code SQS_SBX_FWD} jump into it is untouched.
     */
    public static List<List<String>> resetGrantChainCommands() {
        return List.of(
                List.of("iptables", "-N", CHAIN_PERBOX),
                List.of("iptables", "-F", CHAIN_PERBOX));
    }

    /**
     * Exact removal of one network's content. Shared skeleton (including MASQUERADE) is untouched.
     */
    public static List<List<String>> removeNetworkCommands(NetworkPlan net, List<SandboxNet> nets) {
        List<List<String>> out = new ArrayList<>();
        for (List<String> rule : addNetworkCommands(net, nets)) {
            if (rule.contains("-C")) {
                continue;
            }
            int op = rule.indexOf("-A");
            if (op < 0) {
                op = rule.indexOf("-I");
            }
            if (op >= 0) {
                List<String> del = new ArrayList<>(rule);
                del.set(op, "-D");
                out.add(del);
            }
        }
        return List.copyOf(out);
    }

    private static void appendLocalDenies(List<List<String>> out) {
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "127.0.0.0/8", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "169.254.0.0/16", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "172.16.0.0/12", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "192.168.0.0/16", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "100.64.0.0/10", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "224.0.0.0/4", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "240.0.0.0/4", "-j", "DROP"));
        out.add(List.of("iptables", "-A", CHAIN_FWD, "-d", "10.0.0.0/8", "-j", "DROP"));
    }

    private static void createOrFlushFilter(List<List<String>> out, String chain) {
        out.add(List.of("iptables", "-N", chain));
        out.add(List.of("iptables", "-F", chain));
    }

    private static void createOrFlushNat(List<List<String>> out, String chain) {
        out.add(List.of("iptables", "-t", "nat", "-N", chain));
        out.add(List.of("iptables", "-t", "nat", "-F", chain));
    }

    /** Emit check argv then insert argv; manager skips insert when check succeeds. */
    private static void checkThenInsert(List<List<String>> out, List<String> check, List<String> insert) {
        out.add(check);
        out.add(insert);
    }

    private static List<SandboxNet> nets(List<SandboxNet> nets) {
        return nets == null ? List.of() : nets;
    }
}
