package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.ExecResult;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.Platform;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Network Access Control, caller side: a container's overlay grants come from this engine's
 * outbound projection resolved through the peer directory (fail closed on an unresolvable or
 * non-overlay address), an engine without a resolvable grant programs nothing, a rule edit or an
 * address churn diffs each live container's rules by exact argv (insert new, delete stale and
 * kill its flow, never flush), and teardown removes exactly what is programmed.
 */
class SandboxEgressManagerGrantsTest {

    private static final List<SandboxNet> NETS = List.of(
            new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"));
    private static final String BOX_A = "10.89.0.5";
    private static final String BOX_B = "10.89.0.6";

    private OverlayPeerDirectory directory;
    private NetworkRuleStore store;
    private List<List<String>> ran;
    private SandboxEgressManager manager;

    @BeforeEach
    void setUp() {
        directory = new OverlayPeerDirectory();
        store = NetworkRuleStore.inMemory();
        ran = new ArrayList<>();
        manager = SandboxEgressManager.forTest("auto", true, true, "sqnet0", Platform.supported(),
                "/usr/bin/podman", recording(ran), cli -> NETS, directory, store);
        manager.overlayUp("100.64.1.7/32");
        assertEquals(EgressReadiness.State.ENFORCED, manager.readiness().state());
        ran.clear();
    }

    /** Records every argv; -C always misses (fresh host) so every insert actually runs. */
    private static SandboxEgressManager.CommandExec recording(List<List<String>> sink) {
        return (argv, timeout) -> {
            sink.add(List.copyOf(argv));
            if (argv.contains("-C")) {
                return new ExecResult(false, 1, "Bad rule");
            }
            return new ExecResult(true, 0, "");
        };
    }

    private static boolean isPerbox(List<String> argv, String op, String containerIp) {
        return argv.contains(op) && argv.contains(OverlayEgressRules.CHAIN_PERBOX)
                && argv.contains("-s") && argv.contains(containerIp);
    }

    private static List<List<String>> perbox(List<List<String>> argv, String op, String containerIp) {
        return argv.stream().filter(a -> isPerbox(a, op, containerIp)).toList();
    }

    private List<List<String>> conntrackDeletes() {
        return ran.stream().filter(a -> a.contains("conntrack") && a.contains("-D")).toList();
    }

    @Test
    void initializationFlushesTheGrantChainBeforeAnyGrantRule() {
        List<List<String>> all = new ArrayList<>();
        SandboxEgressManager fresh = SandboxEgressManager.forTest("auto", true, true, "sqnet0",
                Platform.supported(), "/usr/bin/podman", recording(all), cli -> NETS, directory, store);
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));

        fresh.overlayUp("100.64.1.7/32");
        assertTrue(fresh.applyContainerGrants(BOX_A));

        String perboxChain = OverlayEgressRules.CHAIN_PERBOX;
        int create = all.indexOf(List.of("iptables", "-N", perboxChain));
        int flush = all.indexOf(List.of("iptables", "-F", perboxChain));
        int firstGrant = all.indexOf(perbox(all, "-A", BOX_A).get(0));
        assertTrue(create >= 0 && flush > create, "created, then flushed: " + all);
        assertTrue(firstGrant > flush, "no grant rule may land before the stale chain is emptied");
        assertTrue(all.contains(List.of("iptables", "-A", OverlayEgressRules.CHAIN_FWD, "-j", perboxChain)),
                "the FWD jump into the chain stays");
    }

    @Test
    void runtimeRebuildKeepsLiveGrants() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));
        manager.applyContainerGrants(BOX_A);
        // The only network drops (a poisoned re-enrollment) and comes back: a full rebuild, but
        // the daemon — and the container holding the grant — never went away.
        manager.overlayUp("10.0.0.5/32");
        manager.overlayUp("100.64.1.7/32");
        assertEquals(EgressReadiness.State.ENFORCED, manager.readiness().state());

        assertFalse(ran.contains(List.of("iptables", "-F", OverlayEgressRules.CHAIN_PERBOX)),
                "only daemon start and a recreated namespace flush the grant chain");
        assertTrue(perbox(ran, "-D", BOX_A).isEmpty(), "the live grant survives the rebuild");

        ran.clear();
        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.22"));
        assertEquals(1, perbox(ran, "-A", BOX_A).size(), "and is still tracked for churn");
    }

    @Test
    void applyResolvesGrantsThroughThePeerDirectory_failClosedOnUnknown() {
        store.replaceAll(List.of(), List.of(
                new NetworkRuleStore.OutboundRule("engine-b", 2222),
                new NetworkRuleStore.OutboundRule("engine-c", 5432)));
        // engine-c is NOT in the directory (offline / not in the mesh): no rule may name it.
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));

        assertTrue(manager.holdsOutboundGrants());
        assertTrue(manager.applyContainerGrants(BOX_A));

        List<List<String>> adds = perbox(ran, "-A", BOX_A);
        assertEquals(1, adds.size(), "exactly one ACCEPT — the resolvable grant");
        assertTrue(adds.get(0).contains("100.64.1.9/32") && adds.get(0).contains("2222"));
        assertFalse(ran.stream().anyMatch(a -> a.contains("5432")),
                "an unresolvable granted engine gets NO rule — never a broader fallback");
    }

    @Test
    void engineWithoutAResolvableGrantProgramsNothing() {
        assertFalse(manager.holdsOutboundGrants(), "no outbound rule → nothing to grant");

        // A rule whose engine is not in the mesh is no grant either.
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-c", 5432)));
        assertFalse(manager.holdsOutboundGrants(), "an unresolvable rule grants nothing");
        assertTrue(manager.applyContainerGrants(BOX_A));

        assertTrue(ran.isEmpty(), "the no-grant path spawns no process at all: " + ran);
    }

    @Test
    void nonOverlayPeerAddressNeverBecomesAGrant() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        // A poisoned directory entry: a LAN address can never be a same-owner overlay peer.
        directory.replace("sqnet0", Map.of("engine-b", "192.168.1.9"));

        assertFalse(manager.holdsOutboundGrants());
        manager.applyContainerGrants(BOX_A);

        assertTrue(perbox(ran, "-A", BOX_A).isEmpty(), "D-04 — only an overlay /32 may be granted");
    }

    @Test
    void addressChurnReprogramsLiveContainers_ruleByRule() {
        store.replaceAll(List.of(), List.of(
                new NetworkRuleStore.OutboundRule("engine-b", 2222),
                new NetworkRuleStore.OutboundRule("engine-d", 8080)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.9", "engine-d", "100.64.0.30"));
        // The outbound view is per ENGINE: both live containers hold the same grant set.
        manager.applyContainerGrants(BOX_A);
        manager.applyContainerGrants(BOX_B);
        ran.clear();

        // engine-b reconnects on a new /32 (same key, new address); engine-d does not move.
        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.22", "engine-d", "100.64.0.30"));

        for (String box : List.of(BOX_A, BOX_B)) {
            List<List<String>> adds = perbox(ran, "-A", box);
            List<List<String>> dels = perbox(ran, "-D", box);
            assertEquals(1, adds.size(), box + ": gains the moved engine's new address");
            assertTrue(adds.get(0).contains("100.64.0.22/32") && adds.get(0).contains("2222"));
            assertEquals(1, dels.size(), box + ": the stale address's rule is removed, not orphaned");
            assertTrue(dels.get(0).contains("100.64.0.9/32") && dels.get(0).contains("2222"));
            assertTrue(ran.stream().noneMatch(a -> isPerbox(a, "-D", box) && a.contains("100.64.0.30/32")),
                    box + ": engine-d's grant never moved — its rule must not be retracted");
            assertTrue(ran.stream().noneMatch(a -> isPerbox(a, "-A", box) && a.contains("100.64.0.30/32")),
                    box + ": engine-d's grant never moved — its rule must not be blindly re-added");
        }
        assertFalse(ran.stream().anyMatch(a -> a.contains("-F")),
                "PERBOX must never be flushed mid-run");
    }

    @Test
    void peerLeavingTheMeshRemovesTheRule_failClosed() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));
        manager.applyContainerGrants(BOX_A);
        ran.clear();

        // The overlay drops (or the mesh shrank): the granted engine no longer resolves.
        directory.clear();

        List<List<String>> dels = perbox(ran, "-D", BOX_A);
        assertEquals(1, dels.size(), "a departed peer's rule must be retracted");
        assertTrue(dels.get(0).contains("100.64.1.9/32"));
        assertTrue(perbox(ran, "-A", BOX_A).isEmpty(), "and no fallback accept replaces it");
    }

    @Test
    void ruleRevokedWhileLive_retractsTheRuleAndKillsTheFlow() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));
        manager.applyContainerGrants(BOX_A);
        ran.clear();

        // The owner deletes the grant in Engine Settings while the container is live.
        store.replaceAll(List.of(), List.of());

        List<List<String>> dels = perbox(ran, "-D", BOX_A);
        assertEquals(1, dels.size(), "a deleted grant is retracted from live containers");
        assertTrue(dels.get(0).contains("100.64.1.9/32") && dels.get(0).contains("2222"));

        // The retracted ACCEPT is not enough — the ESTABLISHED,RELATED leg of the chain would
        // keep an open connection alive. The conntrack entry must be deleted for exactly the
        // revoked (container, destination, port) tuple, in the grant direction.
        List<List<String>> ct = conntrackDeletes();
        assertEquals(1, ct.size(), "the revoked grant's live flow must be killed");
        assertTrue(ct.get(0).contains(BOX_A) && ct.get(0).contains("100.64.1.9")
                        && ct.get(0).contains("2222") && ct.get(0).contains("tcp"),
                "conntrack delete targets the revoked (container, dst, port) tuple");
    }

    @Test
    void ruleAddedWhileLive_reachesTheLiveContainer() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9", "engine-d", "100.64.1.30"));
        manager.applyContainerGrants(BOX_A);
        ran.clear();

        store.replaceAll(List.of(), List.of(
                new NetworkRuleStore.OutboundRule("engine-b", 2222),
                new NetworkRuleStore.OutboundRule("engine-d", 8080)));

        List<List<String>> adds = perbox(ran, "-A", BOX_A);
        assertEquals(1, adds.size(), "only the new grant is inserted");
        assertTrue(adds.get(0).contains("100.64.1.30/32") && adds.get(0).contains("8080"));
        assertTrue(perbox(ran, "-D", BOX_A).isEmpty(), "the unchanged grant is left alone");
        assertTrue(conntrackDeletes().isEmpty(), "adding a grant kills no flow");
    }

    @Test
    void survivingGrantsKeepTheirLiveFlowsOnChurn() {
        store.replaceAll(List.of(), List.of(
                new NetworkRuleStore.OutboundRule("engine-b", 2222),
                new NetworkRuleStore.OutboundRule("engine-d", 8080)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.9", "engine-d", "100.64.0.30"));
        manager.applyContainerGrants(BOX_A);
        ran.clear();

        // engine-b churns to a new /32; engine-d's grant is untouched.
        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.22", "engine-d", "100.64.0.30"));

        List<List<String>> ct = conntrackDeletes();
        assertEquals(1, ct.size(), "only the churned grant's flow is killed");
        assertTrue(ct.get(0).contains("100.64.0.9") && ct.get(0).contains("2222"),
                "the OLD address's flow dies with its rule");
        assertTrue(ct.get(0).stream().noneMatch(x -> x.equals("100.64.0.30") || x.equals("8080")),
                "an unmoved grant's live flow is never touched");
    }

    @Test
    void teardownRemovesTheCurrentRule_notTheStaleOne() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));
        manager.applyContainerGrants(BOX_A);
        // Churn mid-run: the applied rule now names the NEW address.
        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.22"));
        ran.clear();

        manager.clearContainerGrants(BOX_A);

        List<List<String>> dels = perbox(ran, "-D", BOX_A);
        assertEquals(1, dels.size());
        assertTrue(dels.get(0).contains("100.64.0.22/32"),
                "teardown removes what is actually programmed (tracked argv), "
                        + "not a recompute that could chase a third address");
        assertFalse(dels.stream().anyMatch(a -> a.contains("100.64.1.9/32")),
                "the stale rule was already retracted at churn time");
    }

    @Test
    void releasedContainerIsNoLongerReprogrammed() {
        store.replaceAll(List.of(), List.of(new NetworkRuleStore.OutboundRule("engine-b", 2222)));
        directory.replace("sqnet0", Map.of("engine-b", "100.64.1.9"));
        manager.applyContainerGrants(BOX_A);
        manager.clearContainerGrants(BOX_A);
        ran.clear();

        directory.replace("sqnet0", Map.of("engine-b", "100.64.0.22"));

        assertTrue(ran.stream().noneMatch(a -> isPerbox(a, "-A", BOX_A) || isPerbox(a, "-D", BOX_A)),
                "a released slot's bridge IP must never be handed a grant by a later churn");
    }
}
