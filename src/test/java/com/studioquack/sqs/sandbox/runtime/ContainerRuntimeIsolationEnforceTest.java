package com.studioquack.sqs.sandbox.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.IsolationTier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The fail-closed isolation gate. The point of these tests is the negative case: with enforcement
 * on and no gVisor, dispatch must be REFUSED rather than silently degraded. {@link
 * ContainerRuntime} probes the real host in its constructor, so every test pins a synthetic {@link
 * CapabilityReport} afterwards — otherwise the assertions would depend on whether the machine
 * running them happens to have runsc registered.
 */
class ContainerRuntimeIsolationEnforceTest {

    private static ContainerRuntime runtime(String pin, boolean enforce, boolean gvisor) {
        ContainerRuntime rt = new ContainerRuntime(new ObjectMapper(), pin, enforce);
        pinReport(rt, gvisor);
        return rt;
    }

    /** Replaces the host-probed report with a deterministic one: CLI up, gVisor present or not. */
    @SuppressWarnings("unchecked")
    private static void pinReport(ContainerRuntime rt, boolean gvisor) {
        IsolationTier recommended = gvisor ? IsolationTier.BEST : IsolationTier.INTERMEDIATE;
        CapabilityReport pinned = new CapabilityReport(true, OciRuntimeKind.PODMAN, "/usr/bin/podman",
                "5.0.0", List.of(), gvisor, true, null, recommended, recommended,
                gvisor ? "runsc" : "", "");
        ((AtomicReference<CapabilityReport>) ReflectionTestUtils.getField(rt, "report")).set(pinned);
    }

    @Test
    @DisplayName("enforcement off: a host without gVisor still dispatches (degraded, as today)")
    void enforcementOff_allowsDegradedHost() {
        ContainerRuntime rt = runtime("auto", false, false);

        assertFalse(rt.isolationEnforced());
        assertNull(rt.enforcedFloor(), "no floor applies when enforcement is off");
        assertTrue(rt.isolationShortfall(null).isEmpty());
        assertTrue(rt.isolationShortfall(IsolationTier.BEST).isEmpty(),
                "without enforcement even a BEST request degrades silently — the pre-T-17 behaviour");
    }

    @Test
    @DisplayName("enforcement on + auto pin: floor is gVisor, so a host without runsc is refused")
    void enforcementOn_autoPin_refusesHostWithoutGvisor() {
        ContainerRuntime rt = runtime("auto", true, false);

        assertTrue(rt.isolationEnforced());
        assertEquals(IsolationTier.BEST, rt.enforcedFloor(),
                "auto must resolve to BEST, else enforcing against the recommended tier always passes");

        Optional<String> shortfall = rt.isolationShortfall(null);
        assertTrue(shortfall.isPresent(), "dispatch must be refused, not degraded");
        assertTrue(shortfall.get().contains("runsc"), "the reason must name the missing dependency");
    }

    @Test
    @DisplayName("enforcement on + auto pin: a host with gVisor dispatches normally")
    void enforcementOn_autoPin_allowsHostWithGvisor() {
        ContainerRuntime rt = runtime("auto", true, true);

        assertEquals(IsolationTier.BEST, rt.enforcedFloor());
        assertTrue(rt.isolationShortfall(null).isEmpty());
        assertTrue(rt.isolationShortfall(IsolationTier.BEST).isEmpty());
    }

    @Test
    @DisplayName("a job cannot talk its way under the floor by requesting a weaker tier")
    void enforcementOn_jobRequestingWeakerTier_isStillRefused() {
        ContainerRuntime rt = runtime("auto", true, true);

        assertTrue(rt.isolationShortfall(IsolationTier.TRUSTED).isPresent(),
                "a job asking for TRUSTED on an enforced host must be refused");
        assertTrue(rt.isolationShortfall(IsolationTier.INTERMEDIATE).isPresent());
    }

    @Test
    @DisplayName("an explicit weaker pin lowers the floor to exactly that tier")
    void enforcementOn_explicitPin_setsFloorToThatTier() {
        ContainerRuntime rt = runtime("intermediate", true, false);

        assertEquals(IsolationTier.INTERMEDIATE, rt.enforcedFloor());
        assertTrue(rt.isolationShortfall(null).isEmpty(),
                "the CLI is up, so the INTERMEDIATE floor is met even without gVisor");
    }

    @Test
    @DisplayName("tier strength comparison is not accidentally inverted")
    void tierStrengthOrdering() {
        assertTrue(IsolationTier.BEST.atLeastAsStrongAs(IsolationTier.INTERMEDIATE));
        assertTrue(IsolationTier.BEST.atLeastAsStrongAs(IsolationTier.BEST));
        assertFalse(IsolationTier.INTERMEDIATE.atLeastAsStrongAs(IsolationTier.BEST));
        assertFalse(IsolationTier.TRUSTED.atLeastAsStrongAs(IsolationTier.INTERMEDIATE));
        assertTrue(IsolationTier.TRUSTED.atLeastAsStrongAs(null), "a null floor means no floor");
    }
}
