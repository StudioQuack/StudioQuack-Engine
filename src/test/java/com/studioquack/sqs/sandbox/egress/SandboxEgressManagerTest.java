package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.ExecResult;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager.Platform;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SandboxEgressManager state machine (no real iptables / podman). */
class SandboxEgressManagerTest {

    private static final List<SandboxNet> NETS = List.of(
            new SandboxNet("sqs-jobs", "10.89.0.0/24", "10.89.0.1"),
            new SandboxNet("sqs-stateful", "10.89.1.0/24", "10.89.1.1"),
            new SandboxNet("sqs-build", "10.89.2.0/24", "10.89.2.1"));

    @Test
    void off_always_allows() {
        SandboxEgressManager m = mgr("off", true, true, Platform.supported(), okExec());
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertTrue(r.internetAllowed());
        assertEquals("overlay-egress disabled", r.reason());
    }

    @Test
    void auto_unsupported_platform_degraded_refuses_networked_start() {
        // AUTO on a host that cannot enforce is DEGRADED but NOT permissive: a networked container
        // would ride the host default route with no rules, so it fails closed.
        SandboxEgressManager m = mgr("auto", true, true,
                Platform.unsupported("overlay-egress requires native Linux + iptables/iproute2 + podman"),
                okExec());
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertFalse(r.internetAllowed());
        assertFalse(r.operatorOptOut());
        assertTrue(r.reason().contains("iptables"));
        assertTrue(r.refusalMessage().contains("overlay-egress=off"));
    }

    @Test
    void off_is_the_only_degraded_that_allows() {
        SandboxEgressManager m = mgr("off", true, true,
                Platform.unsupported("macOS utun"), okExec());
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertTrue(r.operatorOptOut());
        assertTrue(r.internetAllowed());
        assertEquals("overlay-egress disabled", r.reason());
        assertTrue(m.degradedRunLogNote().contains("operator opt-out"));
    }

    @Test
    void enforced_unsupported_unavailable_refuses() {
        SandboxEgressManager m = mgr("enforced", true, true,
                Platform.unsupported("macOS utun"), okExec());
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertFalse(r.internetAllowed());
    }

    @Test
    void channel_disabled_auto_degraded_refuses() {
        SandboxEgressManager m = mgr("auto", false, true, Platform.supported(), okExec());
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertFalse(r.internetAllowed());
        assertEquals("overlay not configured", r.reason());
    }

    @Test
    void overlayUp_outside_cgnat_range_unavailable_no_rules() {
        // A poisoned assignment outside 100.64.0.0/10 never reaches iptables/ip.
        AtomicInteger calls = new AtomicInteger();
        SandboxEgressManager.CommandExec exec = (argv, timeout) -> {
            calls.incrementAndGet();
            return tolerate(argv);
        };
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), exec);
        m.overlayUp("10.0.0.7/32");
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertTrue(r.reason().contains("outside 100.64.0.0/10"));
        assertEquals(0, calls.get(), "no host command may run for a non-overlay address");
    }

    @Test
    void overlay_not_up_yet_unavailable() {
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), okExec());
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertFalse(r.internetAllowed());
        assertEquals("overlay-not-up-yet", r.reason());
    }

    @Test
    void overlayUp_success_enforced_with_overlayIp() {
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), okExec());
        m.overlayUp("100.64.1.7/32");
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.ENFORCED, r.state());
        assertTrue(r.internetAllowed());
        assertEquals("100.64.1.7", r.overlayIp());
    }

    @Test
    void apply_failure_unavailable_fail_closed() {
        AtomicInteger n = new AtomicInteger();
        SandboxEgressManager.CommandExec exec = (argv, timeout) -> {
            if (argv.contains("-A") && argv.contains("SQS_SBX_IN") && n.incrementAndGet() == 1) {
                return new ExecResult(false, 1, "iptables: No chain/target/match by that name");
            }
            return tolerate(argv);
        };
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), exec);
        m.overlayUp("100.64.1.7/32");
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertFalse(r.internetAllowed());
        assertTrue(r.reason().startsWith("overlay-egress rules failed:"));
    }

    @Test
    void overlayDown_unavailable() {
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), okExec());
        m.overlayUp("100.64.1.7/32");
        m.overlayDown("handshake-failed");
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertEquals("handshake-failed", r.reason());
        assertFalse(r.internetAllowed());
    }

    @Test
    void reassertBaseline_withoutOverlay_clearsOverlayDownLatch_onOptedOutHost() {
        SandboxEgressManager m = mgr("off", true, true, Platform.unsupported("windows"), okExec());
        m.overlayDown("handshake-failed");
        assertFalse(m.readiness().internetAllowed());
        m.reassertBaselineWithoutOverlay();
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertTrue(r.operatorOptOut());
        assertTrue(m.readiness().internetAllowed());
    }

    @Test
    void reassertBaseline_withoutOverlay_restoresUnsupportedDegraded_onAutoHost() {
        SandboxEgressManager m = mgr("auto", true, true, Platform.unsupported("no iptables"), okExec());
        m.overlayDown("handshake-failed");
        m.reassertBaselineWithoutOverlay();
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.DEGRADED, r.state());
        assertEquals("no iptables", r.reason());
        assertFalse(r.internetAllowed());
    }

    @Test
    void reassertBaseline_withoutOverlay_keepsFailClosed_onEnforcementCapableHost() {
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), okExec());
        m.overlayDown("handshake-failed");
        m.reassertBaselineWithoutOverlay();
        EgressReadiness r = m.readiness();
        assertEquals(EgressReadiness.State.UNAVAILABLE, r.state());
        assertEquals("overlay-not-up-yet", r.reason());
        assertFalse(r.internetAllowed());
    }

    @Test
    void reUp_applies_masquerade_never_to_source() {
        List<List<String>> applied = new ArrayList<>();
        SandboxEgressManager.CommandExec exec = (argv, timeout) -> {
            List<String> bare = argv;
            if (argv.size() >= 2 && "sudo".equals(argv.get(0))) {
                bare = argv.subList(2, argv.size());
            }
            applied.add(List.copyOf(bare));
            return tolerate(argv);
        };
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), exec);
        m.overlayUp("100.64.1.7/32");
        int afterFirst = applied.size();
        assertTrue(applied.stream().anyMatch(c -> c.contains("MASQUERADE")),
                "overlayUp must program host-egress MASQUERADE");
        assertFalse(applied.stream().anyMatch(c -> c.contains("--to-source")),
                "must not SNAT to the overlay IP");

        m.overlayUp("100.64.1.9/32");
        List<List<String>> second = applied.subList(afterFirst, applied.size());
        assertFalse(second.isEmpty(), "second overlayUp must re-apply");
        assertFalse(second.stream().anyMatch(c -> c.contains("--to-source")),
                "re-apply must not SNAT to the new /32");
        assertEquals("100.64.1.9", m.readiness().overlayIp());
        assertEquals(EgressReadiness.State.ENFORCED, m.readiness().state());
    }

    private static SandboxEgressManager mgr(String mode, boolean channel, boolean net,
                                            Platform platform,
                                            SandboxEgressManager.CommandExec exec) {
        return SandboxEgressManager.forTest(mode, channel, net, "sqnet0", platform,
                "/usr/bin/podman", exec, cli -> NETS);
    }

    @Test
    void degradedRunLogNote_degraded_names_reason() {
        SandboxEgressManager m = mgr("auto", true, true,
                Platform.unsupported("overlay-egress requires native Linux + iptables/iproute2 + podman (host os=macos)"),
                okExec());
        String note = m.degradedRunLogNote();
        assertTrue(note != null && note.contains("DEGRADED"), "DEGRADED host must produce a note");
        assertTrue(note.contains("host os=macos"), "note carries the readiness reason");
        assertTrue(note.contains("NOT applied"), "note says the displayed posture was not applied");
    }

    @Test
    void degradedRunLogNote_enforced_is_silent() {
        SandboxEgressManager m = mgr("auto", true, true, Platform.supported(), okExec());
        m.overlayUp("100.64.1.7/32");
        assertEquals(EgressReadiness.State.ENFORCED, m.readiness().state());
        assertNull(m.degradedRunLogNote(), "ENFORCED host keeps log rows clean");
    }

    @Test
    void degradedRunLogNote_unavailable_is_silent() {
        SandboxEgressManager m = mgr("enforced", true, true,
                Platform.unsupported("macOS utun"), okExec());
        assertEquals(EgressReadiness.State.UNAVAILABLE, m.readiness().state());
        assertNull(m.degradedRunLogNote());
    }

    private static SandboxEgressManager.CommandExec okExec() {
        return (argv, timeout) -> tolerate(argv);
    }

    private static ExecResult tolerate(List<String> argv) {
        if (argv.contains("-C")) {
            return new ExecResult(false, 1, "iptables: Bad rule (does a matching rule exist in that chain?).");
        }
        return new ExecResult(true, 0, "");
    }
}
