package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.NetworkPlan;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Attacker-perspective proofs against the generated iptables argv. A tiny in-test rule walker
 * interprets {@link OverlayEgressRules#applyCommands} output (chain jumps, {@code -s/-d} CIDR,
 * {@code -p/--dport}, {@code -o}, conntrack state) and decides ACCEPT/DROP for a packet sourced
 * from inside a job sandbox.
 */
class OverlayEgressAttackerTest {

    private static final String OVERLAY_IP = "100.64.1.7";
    private static final String IFACE = "sqnet0";
    private static final String HOST_IFACE = "eth0";
    private static final String JOB_SRC = "10.89.0.5";
    private static final String JOB_GW = "10.89.0.1";
    private static final String PEER = "100.64.1.9";

    private static RuleSet RULES;

    private static NetworkPlan plan() {
        return new NetworkPlan(IFACE, IFACE, OVERLAY_IP, OverlayEgressRules.OVERLAY_CIDR,
                OverlayEgressRules.TABLE_ID, OverlayEgressRules.STEER_PREF_BASE);
    }

    @BeforeAll
    static void buildRules() {
        RULES = RuleSet.from(OverlayEgressRules.reconcileCommands(plan(), List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"),
                new SandboxNet("sqs-stateful", "10.89.1.0/24", "10.89.1.1"),
                new SandboxNet("sqs-build", "10.89.2.0/24", "10.89.2.1"))));
    }

    @Test
    void attacker_verdict_table() {
        record Case(String name, Packet pkt, String expectFilter, String expectNat) {}
        List<Case> cases = List.of(
                new Case("(a) NEW tcp overlayIp:9876",
                        Packet.input(JOB_SRC, OVERLAY_IP, "tcp", 9876, "NEW"),
                        "DROP", null),
                new Case("(b) NEW tcp 127.0.0.1:9876",
                        Packet.input(JOB_SRC, "127.0.0.1", "tcp", 9876, "NEW"),
                        "DROP", null),
                new Case("(c) NEW tcp bridge gateway:9876",
                        Packet.input(JOB_SRC, JOB_GW, "tcp", 9876, "NEW"),
                        "DROP", null),
                new Case("(d) NEW udp gateway:53 DNS (T-17: no gateway-DNS ACCEPT remains)",
                        Packet.input(JOB_SRC, JOB_GW, "udp", 53, "NEW"),
                        "DROP", null),
                new Case("(d2) NEW tcp gateway:53 DNS-over-TCP (same refusal)",
                        Packet.input(JOB_SRC, JOB_GW, "tcp", 53, "NEW"),
                        "DROP", null),
                new Case("(e) NEW tcp stateful sibling:5432",
                        Packet.forward(JOB_SRC, "10.89.1.7", "tcp", 5432, "NEW", IFACE),
                        "DROP", null),
                new Case("(f) NEW tcp IMDS 169.254.169.254:80",
                        Packet.forward(JOB_SRC, "169.254.169.254", "tcp", 80, "NEW", IFACE),
                        "DROP", null),
                new Case("(g) NEW tcp host LAN 192.168.1.10:443",
                        Packet.forward(JOB_SRC, "192.168.1.10", "tcp", 443, "NEW", IFACE),
                        "DROP", null),
                new Case("(h) NEW tcp sibling engine 100.64.1.9:2222 (no grant)",
                        Packet.forward(JOB_SRC, PEER, "tcp", 2222, "NEW", IFACE),
                        "DROP", null),
                // The daemon listeners are unreachable from a container toward ANY overlay
                // address at the FORWARD layer — independent of any per-container grant.
                new Case("(h2) NEW tcp sibling daemon 100.64.1.9:9876",
                        Packet.forward(JOB_SRC, PEER, "tcp", 9876, "NEW", IFACE),
                        "DROP", null),
                new Case("(h3) NEW tcp sibling daemon 100.64.1.9:8443",
                        Packet.forward(JOB_SRC, PEER, "tcp", 8443, "NEW", IFACE),
                        "DROP", null),
                new Case("(i) NEW tcp 8.8.8.8:443 out overlay (hairpin denied)",
                        Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", IFACE),
                        "DROP", null),
                new Case("(j) NEW tcp 8.8.8.8:443 out eth0 + MASQUERADE",
                        Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", HOST_IFACE),
                        "ACCEPT", "MASQUERADE")
        );

        StringBuilder table = new StringBuilder();
        table.append('\n').append(String.format("%-55s %-8s %-8s%n", "case", "filter", "nat"));
        for (Case c : cases) {
            Verdict v = RULES.decide(c.pkt);
            table.append(String.format("%-55s %-8s %-8s%n", c.name, v.filter(),
                    v.snatTo() == null ? "-" : v.snatTo()));
            assertEquals(c.expectFilter, v.filter(), c.name);
            if (c.expectNat != null) {
                assertEquals(c.expectNat, v.snatTo(), c.name + " SNAT");
            }
        }
        System.out.println(table);
        assertTrue(cases.size() >= 10);
    }

    /**
     * The DNS-hardening acceptance line, attacker side: a workload that ignores the daemon-pinned
     * {@code --dns} resolvers (rewritten resolv.conf) and hardcodes the BRIDGE GATEWAY as its
     * resolver — the path into the host's internal DNS — is DROPped at the fail-closed end of
     * {@code SQS_SBX_IN}, udp AND tcp. Meanwhile the pinned PUBLIC resolver leaves via the host
     * default route (ACCEPT + MASQUERADE) — DNS keeps working for allowed nodes, internal names
     * never resolve.
     */
    @Test
    void gateway_resolver_is_unreachable_but_pinned_public_dns_uses_host_egress() {
        // The gateway-as-resolver bypass dies for every sandbox subnet's gateway, both protos.
        for (String gw : List.of("10.89.0.1", "10.89.1.1", "10.89.2.1")) {
            assertEquals("DROP", RULES.decide(Packet.input(JOB_SRC, gw, "udp", 53, "NEW")).filter(),
                    "udp/53 to gateway " + gw + " must hit the fail-closed DROP");
            assertEquals("DROP", RULES.decide(Packet.input(JOB_SRC, gw, "tcp", 53, "NEW")).filter(),
                    "tcp/53 to gateway " + gw + " must hit the fail-closed DROP");
        }
        Verdict dns = RULES.decide(Packet.forward(JOB_SRC, "9.9.9.9", "udp", 53, "NEW", HOST_IFACE));
        assertEquals("ACCEPT", dns.filter(), "pinned public resolver uses host egress");
        assertEquals("MASQUERADE", dns.snatTo(), "DNS egress is MASQUERADE'd on the host");
        assertEquals("DROP", RULES.decide(
                        Packet.forward(JOB_SRC, "9.9.9.9", "udp", 53, "NEW", IFACE)).filter(),
                "DNS out the overlay iface is the hairpin and must die");
        assertEquals("DROP", RULES.decide(Packet.input(JOB_SRC, OVERLAY_IP, "udp", 53, "NEW")).filter(),
                "even the engine's own overlay address answers no sandbox DNS");
    }

    /**
     * Network Access Control, caller side — the per-container grant chain: with a PERBOX
     * grant for the job's bridge IP the granted sibling-engine packet is ACCEPTed (never
     * MASQUERADE'd), only for that container, and its internet path is unchanged.
     */
    @Test
    void perbox_grant_verdicts() {
        RuleSet granted = RuleSet.from(OverlayEgressRules.reconcileCommands(plan(), List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"))));
        ContainerOverlayGrants.rulesFor(JOB_SRC, IFACE,
                        List.of(new ContainerOverlayGrants.OverlayGrant(PEER, 2222)))
                .forEach(granted::ingest);

        Verdict overlay = granted.decide(
                Packet.forward(JOB_SRC, PEER, "tcp", 2222, "NEW", IFACE));
        assertEquals("ACCEPT", overlay.filter(), "granted (engine, port) must be reachable");
        assertEquals(null, overlay.snatTo(), "overlay dests RETURN from NAT — no MASQUERADE");
        Verdict other = granted.decide(
                Packet.forward("10.89.0.6", PEER, "tcp", 2222, "NEW", IFACE));
        assertEquals("DROP", other.filter(), "a grant rule is per container, not per network");

        Verdict internet = granted.decide(
                Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", HOST_IFACE));
        assertEquals("ACCEPT", internet.filter(), "a grant never changes the internet path");
        assertEquals("MASQUERADE", internet.snatTo());
    }

    /**
     * The least-privilege property: a container reaches exactly the granted {@code (engine,
     * port)} pairs and nothing else on the overlay. Same engine on an ungranted port, an
     * ungranted engine, and the daemon listeners on ANY overlay address all stay DROPped; with
     * no grants at all the container reaches nothing.
     */
    @Test
    void perbox_least_privilege_verdicts() {
        RuleSet rules = RuleSet.from(OverlayEgressRules.reconcileCommands(plan(), List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"))));
        ContainerOverlayGrants.rulesFor(JOB_SRC, IFACE,
                        List.of(new ContainerOverlayGrants.OverlayGrant(PEER, 2222)))
                .forEach(rules::ingest);

        record Case(String name, Packet pkt, String expect) {}
        List<Case> cases = List.of(
                new Case("granted engine 100.64.1.9, granted port 2222",
                        Packet.forward(JOB_SRC, PEER, "tcp", 2222, "NEW", IFACE), "ACCEPT"),
                new Case("granted engine 100.64.1.9, UNGRANTED port 2223",
                        Packet.forward(JOB_SRC, PEER, "tcp", 2223, "NEW", IFACE), "DROP"),
                new Case("granted engine 100.64.1.9, granted port 2222 over UDP",
                        Packet.forward(JOB_SRC, PEER, "udp", 2222, "NEW", IFACE), "DROP"),
                new Case("UNGRANTED engine 100.64.1.11, port 2222",
                        Packet.forward(JOB_SRC, "100.64.1.11", "tcp", 2222, "NEW", IFACE), "DROP"),
                new Case("granted engine's daemon peer listener 100.64.1.9:9876",
                        Packet.forward(JOB_SRC, PEER, "tcp", 9876, "NEW", IFACE), "DROP"),
                new Case("granted engine's daemon execute endpoint 100.64.1.9:8443",
                        Packet.forward(JOB_SRC, PEER, "tcp", 8443, "NEW", IFACE), "DROP"),
                new Case("ANY overlay address :9876 (100.64.1.200)",
                        Packet.forward(JOB_SRC, "100.64.1.200", "tcp", 9876, "NEW", IFACE), "DROP"),
                new Case("ANY overlay address :8443 (100.64.1.200)",
                        Packet.forward(JOB_SRC, "100.64.1.200", "tcp", 8443, "NEW", IFACE), "DROP"));
        for (Case c : cases) {
            assertEquals(c.expect, rules.decide(c.pkt).filter(), c.name);
        }

        // NO rules granted: the container reaches nothing on the overlay.
        RuleSet noGrants = RuleSet.from(OverlayEgressRules.reconcileCommands(plan(), List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"))));
        ContainerOverlayGrants.rulesFor(JOB_SRC, IFACE, List.of()).forEach(noGrants::ingest);
        assertEquals("DROP", noGrants.decide(
                        Packet.forward(JOB_SRC, PEER, "tcp", 2222, "NEW", IFACE)).filter(),
                "without a Network Access Control rule the container must reach nothing");
    }

    /**
     * A second enrolled iface only adds daemon denies + its own hairpin DROP. Internet stays
     * host-egress MASQUERADE; neither iface SNATs to an overlay IP.
     */
    @Test
    void secondIface_addsDaemonDenies_internetStaysHostMasquerade() {
        NetworkPlan extra = new NetworkPlan("sqnet1", "sqnet1", "100.64.2.9",
                OverlayEgressRules.OVERLAY_CIDR, OverlayEgressRules.TABLE_ID + 1,
                OverlayEgressRules.STEER_PREF_BASE + 1);
        List<List<String>> cmds = new ArrayList<>(OverlayEgressRules.reconcileCommands(plan(), List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"))));
        cmds.addAll(OverlayEgressRules.addNetworkCommands(extra, List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"))));
        RuleSet twoIfaces = RuleSet.from(cmds);

        record Case(String name, Packet pkt, String expectFilter, String expectNat) {}
        List<Case> cases = List.of(
                new Case("internet via host NIC MASQUERADEs",
                        Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", HOST_IFACE),
                        "ACCEPT", "MASQUERADE"),
                new Case("internet via primary overlay iface is hairpin",
                        Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", IFACE),
                        "DROP", null),
                new Case("internet via second overlay iface is hairpin",
                        Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", "sqnet1"),
                        "DROP", null),
                new Case("sibling daemon :9876",
                        Packet.forward(JOB_SRC, PEER, "tcp", 9876, "NEW", IFACE),
                        "DROP", null),
                new Case("sibling daemon :8443",
                        Packet.forward(JOB_SRC, PEER, "tcp", 8443, "NEW", IFACE),
                        "DROP", null),
                new Case("own listener on the second address :9876",
                        Packet.input(JOB_SRC, "100.64.2.9", "tcp", 9876, "NEW"),
                        "DROP", null),
                new Case("ungranted overlay peer",
                        Packet.forward(JOB_SRC, "100.64.2.10", "tcp", 2222, "NEW", "sqnet1"),
                        "DROP", null));
        for (Case c : cases) {
            Verdict v = twoIfaces.decide(c.pkt);
            assertEquals(c.expectFilter, v.filter(), c.name);
            if (c.expectNat != null) {
                assertEquals(c.expectNat, v.snatTo(), c.name + " NAT");
            }
        }

        RuleSet afterRemoval = RuleSet.from(cmds);
        OverlayEgressRules.removeNetworkCommands(extra, List.of(
                new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1")))
                .forEach(afterRemoval::ingestRemoval);
        assertEquals("ACCEPT", afterRemoval.decide(
                Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", HOST_IFACE)).filter());
        assertEquals("MASQUERADE", afterRemoval.decide(
                Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", HOST_IFACE)).snatTo());
        assertEquals("ACCEPT", afterRemoval.decide(
                Packet.forward(JOB_SRC, "8.8.8.8", "tcp", 443, "NEW", "sqnet1")).filter(),
                "removing the iface also removes its hairpin DROP");
    }

    // ---- tiny iptables walker ------------------------------------------------

    record Packet(String hook, String src, String dst, String proto, int dport,
                  String ctstate, String outIface) {
        static Packet input(String src, String dst, String proto, int dport, String ct) {
            return new Packet("INPUT", src, dst, proto, dport, ct, null);
        }

        static Packet forward(String src, String dst, String proto, int dport, String ct, String oif) {
            return new Packet("FORWARD", src, dst, proto, dport, ct, oif);
        }
    }

    record Verdict(String filter, String snatTo) {}

    static final class RuleSet {
        private final Map<String, List<Rule>> chains = new HashMap<>();
        private final List<Rule> inputJumps = new ArrayList<>();
        private final List<Rule> forwardJumps = new ArrayList<>();
        private final List<Rule> natJumps = new ArrayList<>();

        static RuleSet from(List<List<String>> cmds) {
            RuleSet rs = new RuleSet();
            for (List<String> argv : cmds) {
                if (!argv.isEmpty() && "iptables".equals(argv.get(0))) {
                    rs.ingest(argv);
                }
            }
            return rs;
        }

        /** Package-visible so the grant cases can add per-container rules. */
        void ingest(List<String> argv) {
            boolean nat = argv.contains("-t") && argv.get(argv.indexOf("-t") + 1).equals("nat");
            String op = null;
            String chain = null;
            for (int i = 1; i < argv.size(); i++) {
                String t = argv.get(i);
                if ("-N".equals(t) || "-F".equals(t) || "-C".equals(t) || "-A".equals(t) || "-I".equals(t)) {
                    op = t;
                    if (i + 1 < argv.size()) {
                        chain = argv.get(i + 1);
                    }
                    break;
                }
            }
            if (op == null || chain == null) {
                return;
            }
            if ("-N".equals(op) || "-F".equals(op) || "-C".equals(op)) {
                if ("-N".equals(op)) {
                    chains.putIfAbsent(chain, new ArrayList<>());
                }
                if ("-F".equals(op)) {
                    chains.put(chain, new ArrayList<>());
                }
                return;
            }
            Rule rule = Rule.parse(argv);
            if ("INPUT".equals(chain) && !nat) {
                inputJumps.add(rule);
            } else if ("FORWARD".equals(chain) && !nat) {
                forwardJumps.add(rule);
            } else if ("POSTROUTING".equals(chain) && nat) {
                natJumps.add(rule);
            } else {
                chains.computeIfAbsent(chain, k -> new ArrayList<>()).add(rule);
            }
        }

        /**
         * Applies a {@code -D} removal: drops the matching rule from wherever it sits, so the
         * drain-teardown cases can prove one network's removal leaves the other's verdicts intact.
         * Matching is by the full rule shape (the same exact-argv contract the manager relies on).
         */
        void ingestRemoval(List<String> argv) {
            if (!argv.contains("-D")) {
                return;
            }
            Rule probe = Rule.parse(argv);
            String chain = argv.get(argv.indexOf("-D") + 1);
            if ("INPUT".equals(chain)) {
                inputJumps.removeIf(r -> sameRule(r, probe));
            } else if ("FORWARD".equals(chain)) {
                forwardJumps.removeIf(r -> sameRule(r, probe));
            } else if ("POSTROUTING".equals(chain)) {
                natJumps.removeIf(r -> sameRule(r, probe));
            } else {
                chains.computeIfAbsent(chain, k -> new ArrayList<>()).removeIf(r -> sameRule(r, probe));
            }
        }

        private static boolean sameRule(Rule a, Rule b) {
            return java.util.Objects.equals(a.srcCidr, b.srcCidr)
                    && java.util.Objects.equals(a.dstCidr, b.dstCidr)
                    && java.util.Objects.equals(a.proto, b.proto)
                    && java.util.Objects.equals(a.dport, b.dport)
                    && java.util.Objects.equals(a.outIface, b.outIface)
                    && java.util.Objects.equals(a.target, b.target)
                    && java.util.Objects.equals(a.toSource, b.toSource);
        }

        Verdict decide(Packet pkt) {
            String filter;
            if ("INPUT".equals(pkt.hook())) {
                filter = walkJumps(inputJumps, pkt);
            } else {
                filter = walkJumps(forwardJumps, pkt);
            }
            String snat = null;
            if ("ACCEPT".equals(filter) && "FORWARD".equals(pkt.hook())) {
                snat = walkNat(pkt);
            }
            return new Verdict(filter, snat);
        }

        private String walkJumps(List<Rule> jumps, Packet pkt) {
            for (Rule j : jumps) {
                if (j.matches(pkt)) {
                    if (j.target.startsWith("SQS_")) {
                        // User-chain semantics: a chain with no matching rule RETURNs to the
                        // calling chain (an empty SQS_SBX_PERBOX must not swallow the packet).
                        String sub = walkChain(j.target, pkt);
                        if (!"NO_MATCH".equals(sub)) {
                            return sub;
                        }
                        continue;
                    }
                    return j.target;
                }
            }
            return "NO_MATCH";
        }

        private String walkChain(String chain, Packet pkt) {
            List<Rule> rules = chains.getOrDefault(chain, List.of());
            for (Rule r : rules) {
                if (r.matches(pkt)) {
                    if (r.target.startsWith("SQS_")) {
                        // Nested user-chain jump (FWD → PERBOX): RETURN semantics on no match.
                        String sub = walkChain(r.target, pkt);
                        if (!"NO_MATCH".equals(sub)) {
                            return sub;
                        }
                        continue;
                    }
                    if ("RETURN".equals(r.target)) {
                        // RETURN hands the packet back to the calling chain: no verdict here.
                        return "NO_MATCH";
                    }
                    return r.target;
                }
            }
            return "NO_MATCH";
        }

        private String walkNat(Packet pkt) {
            for (Rule j : natJumps) {
                if (j.matches(pkt) && j.target.startsWith("SQS_")) {
                    for (Rule r : chains.getOrDefault(j.target, List.of())) {
                        if (!r.matches(pkt)) {
                            continue;
                        }
                        if ("RETURN".equals(r.target)) {
                            return null;
                        }
                        if ("MASQUERADE".equals(r.target)) {
                            return "MASQUERADE";
                        }
                        if ("SNAT".equals(r.target)) {
                            return r.toSource;
                        }
                    }
                }
            }
            return null;
        }
    }

    static final class Rule {
        String srcCidr;
        String dstCidr;
        boolean dstNegated;
        String proto;
        Integer dport;
        String outIface;
        boolean outIfaceNegated;
        String ctstate;
        String target;
        String toSource;
        boolean commentOnly;

        static Rule parse(List<String> argv) {
            Rule r = new Rule();
            for (int i = 0; i < argv.size(); i++) {
                String t = argv.get(i);
                switch (t) {
                    case "-s" -> r.srcCidr = argv.get(++i);
                    case "-d" -> r.dstCidr = argv.get(++i);
                    case "-p" -> r.proto = argv.get(++i).toLowerCase(Locale.ROOT);
                    case "--dport" -> r.dport = Integer.parseInt(argv.get(++i));
                    case "-o" -> r.outIface = argv.get(++i);
                    case "!" -> {
                        if (i + 1 < argv.size() && "-o".equals(argv.get(i + 1))) {
                            r.outIfaceNegated = true;
                        }
                        if (i + 1 < argv.size() && "-d".equals(argv.get(i + 1))) {
                            r.dstNegated = true;
                        }
                    }
                    case "--ctstate" -> r.ctstate = argv.get(++i);
                    case "-j" -> r.target = argv.get(++i);
                    case "--to-source" -> r.toSource = argv.get(++i);
                    default -> { /* ignore */ }
                }
            }
            return r;
        }

        boolean matches(Packet pkt) {
            if (srcCidr != null && !ipInCidr(pkt.src(), srcCidr)) {
                return false;
            }
            if (dstCidr != null) {
                boolean in = ipInCidr(pkt.dst(), dstCidr);
                if (dstNegated ? in : !in) {
                    return false;
                }
            }
            if (proto != null && !proto.equalsIgnoreCase(pkt.proto())) {
                return false;
            }
            if (dport != null && dport != pkt.dport()) {
                return false;
            }
            if (ctstate != null) {
                // ESTABLISHED,RELATED — only match when packet carries that state token.
                boolean ok = false;
                for (String s : ctstate.split(",")) {
                    if (s.equalsIgnoreCase(pkt.ctstate())) {
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    return false;
                }
            }
            if (outIface != null) {
                if (pkt.outIface() == null) {
                    return false;
                }
                boolean eq = outIface.equals(pkt.outIface());
                if (outIfaceNegated) {
                    if (eq) {
                        return false;
                    }
                } else if (!eq) {
                    return false;
                }
            }
            return target != null;
        }
    }

    /** Minimal IPv4 CIDR membership for the attacker cases (octet-aligned prefixes). */
    static boolean ipInCidr(String ip, String cidr) {
        if (ip == null || cidr == null) {
            return false;
        }
        if (!cidr.contains("/")) {
            return ip.equals(cidr);
        }
        String[] parts = cidr.split("/");
        int prefix = Integer.parseInt(parts[1]);
        long ipN = ipv4(ip);
        long netN = ipv4(parts[0]);
        long mask = prefix == 0 ? 0L : (0xFFFFFFFFL << (32 - prefix)) & 0xFFFFFFFFL;
        return (ipN & mask) == (netN & mask);
    }

    static long ipv4(String ip) {
        String[] o = ip.split("\\.");
        return ((Long.parseLong(o[0]) << 24)
                | (Long.parseLong(o[1]) << 16)
                | (Long.parseLong(o[2]) << 8)
                | Long.parseLong(o[3])) & 0xFFFFFFFFL;
    }
}
