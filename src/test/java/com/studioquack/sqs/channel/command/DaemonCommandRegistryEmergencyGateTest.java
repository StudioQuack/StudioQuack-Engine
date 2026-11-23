package com.studioquack.sqs.channel.command;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With the mesh data plane installed, the heartbeat lane may only carry the emergency command
 * set. Overlay delivery stays unrestricted. Activation is fixed at construction.
 */
class DaemonCommandRegistryEmergencyGateTest {

    /** Counts executions so a refusal that merely returns late is distinguishable from a real one. */
    private static final class CountingHandler implements DaemonCommandHandler {
        final AtomicInteger executions = new AtomicInteger();
        private final String type;

        CountingHandler(String type) {
            this.type = type;
        }

        @Override
        public String type() {
            return type;
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) {
            executions.incrementAndGet();
            return DaemonCommandOutcome.ok("done");
        }
    }

    private static DaemonCommand command(String id, String type) {
        return new DaemonCommand(id, type, "2026-08-03T00:00:00Z", "{}");
    }

    /** Gated registry: mesh data plane is installed. */
    private static DaemonCommandRegistry gatedRegistry(DaemonCommandHandler... handlers) {
        return new DaemonCommandRegistry(List.of(handlers), 60_000L, true);
    }

    /** Ungated registry: mesh data plane is not installed. */
    private static DaemonCommandRegistry ungatedRegistry(DaemonCommandHandler... handlers) {
        return new DaemonCommandRegistry(List.of(handlers), 60_000L, false);
    }

    @Test
    @DisplayName("Gated heartbeat + non-enumerated type is refused with Error_EmergencyOnlyChannel")
    void meshActiveHeartbeatRefusesNonEmergency() {
        CountingHandler deploy = new CountingHandler("deploy");
        CountingHandler retry = new CountingHandler("retry-incident");
        DaemonCommandRegistry registry = gatedRegistry(deploy, retry);

        Optional<DaemonCommandResult> deployResult =
                registry.dispatch(command("c-deploy", "deploy"), DaemonCommandRegistry.INBOUND_HEARTBEAT);
        Optional<DaemonCommandResult> retryResult =
                registry.dispatch(command("c-retry", "retry-incident"),
                        DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isZero();
        assertThat(retry.executions.get()).isZero();
        assertThat(deployResult).isPresent();
        assertThat(retryResult).isPresent();
        assertThat(deployResult.orElseThrow().status()).isNotEqualTo("ok");
        assertThat(retryResult.orElseThrow().status()).isNotEqualTo("ok");
        assertThat(deployResult.orElseThrow().error())
                .startsWith(DaemonCommandRegistry.ERROR_EMERGENCY_ONLY_CHANNEL);
        assertThat(retryResult.orElseThrow().error())
                .contains(DaemonCommandRegistry.ERROR_EMERGENCY_ONLY_CHANNEL);
    }

    @ParameterizedTest(name = "Gated heartbeat + emergency type ''{0}'' succeeds")
    @ValueSource(strings = {"restart", "status", "net-reconnect", "vm-reset"})
    void meshActiveHeartbeatAllowsEachEmergencyType(String type) {
        CountingHandler handler = new CountingHandler(type);
        DaemonCommandRegistry registry = gatedRegistry(handler);

        Optional<DaemonCommandResult> result =
                registry.dispatch(command("c-" + type, type), DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(handler.executions.get()).isEqualTo(1);
        assertThat(result).isPresent();
        assertThat(result.orElseThrow().status()).isEqualTo("ok");
    }

    @Test
    @DisplayName("no data plane + heartbeat + non-enumerated still succeeds (heartbeat is the only lane)")
    void meshInactiveHeartbeatAllowsGeneralCommands() {
        CountingHandler deploy = new CountingHandler("deploy");
        DaemonCommandRegistry registry = ungatedRegistry(deploy);

        Optional<DaemonCommandResult> result =
                registry.dispatch(command("c-base", "deploy"), DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isEqualTo(1);
        assertThat(result).isPresent();
        assertThat(result.orElseThrow().status()).isEqualTo("ok");
    }

    @Test
    @DisplayName("Gated overlay + non-enumerated succeeds (overlay unaffected)")
    void meshActiveOverlaySkipsEmergencyGate() {
        CountingHandler deploy = new CountingHandler("deploy");
        DaemonCommandRegistry registry = gatedRegistry(deploy);

        Optional<DaemonCommandResult> result =
                registry.dispatch(command("c-overlay", "deploy"), DaemonCommandRegistry.INBOUND_OVERLAY);

        assertThat(deploy.executions.get()).isEqualTo(1);
        assertThat(result).isPresent();
        assertThat(result.orElseThrow().status()).isEqualTo("ok");
    }

    @Test
    @DisplayName("a heartbeat refusal is not cached, so overlay can still run the same commandId")
    void refusalDoesNotClaimTheCommandId() {
        CountingHandler deploy = new CountingHandler("deploy");
        DaemonCommandRegistry registry = gatedRegistry(deploy);

        DaemonCommand cmd = command("c-retained", "deploy");
        registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);
        assertThat(deploy.executions.get()).isZero();

        Optional<DaemonCommandResult> viaOverlay =
                registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_OVERLAY);

        assertThat(deploy.executions.get())
                .as("caching the refusal would permanently lose the command against overlay delivery")
                .isEqualTo(1);
        assertThat(viaOverlay).isPresent();
        assertThat(viaOverlay.orElseThrow().status()).isEqualTo("ok");
    }

    @Test
    @DisplayName("Gated heartbeat twin of an already-executed command gets the cached result, not a refusal")
    void heartbeatTwinOfCompletedCommandGetsCachedResult() {
        CountingHandler deploy = new CountingHandler("deploy");
        DaemonCommandRegistry registry = gatedRegistry(deploy);

        DaemonCommand cmd = command("c-twin", "deploy");
        Optional<DaemonCommandResult> viaOverlay =
                registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_OVERLAY);
        assertThat(viaOverlay.orElseThrow().status()).isEqualTo("ok");
        assertThat(deploy.executions.get()).isEqualTo(1);

        Optional<DaemonCommandResult> twin =
                registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get())
                .as("the twin must never re-execute")
                .isEqualTo(1);
        assertThat(twin).isPresent();
        assertThat(twin.orElseThrow().status())
                .as("a finished command's outcome is served from the claim cache, not gated")
                .isEqualTo("ok");
    }

    @Test
    @DisplayName("Gated heartbeat twin of an IN-PROGRESS command is still refused (no lane parking)")
    void heartbeatTwinOfInProgressCommandStillRefused() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        DaemonCommandHandler blocking = new DaemonCommandHandler() {
            @Override
            public String type() {
                return "deploy";
            }

            @Override
            public DaemonCommandOutcome handle(DaemonCommand command) {
                entered.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return DaemonCommandOutcome.ok("done");
            }
        };
        DaemonCommandRegistry registry = gatedRegistry(blocking);

        DaemonCommand cmd = command("c-inflight", "deploy");
        java.util.concurrent.CompletableFuture<Optional<DaemonCommandResult>> overlayFuture =
                java.util.concurrent.CompletableFuture.supplyAsync(
                        () -> registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_OVERLAY));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        // The in-progress claim must NOT be awaited on the serial heartbeat lane: the gate
        // refuses immediately, exactly as before the completed-claim pass-through existed.
        Optional<DaemonCommandResult> twin =
                registry.dispatch(cmd, DaemonCommandRegistry.INBOUND_HEARTBEAT);
        assertThat(twin).isPresent();
        assertThat(twin.orElseThrow().error())
                .contains(DaemonCommandRegistry.ERROR_EMERGENCY_ONLY_CHANNEL);

        release.countDown();
        assertThat(overlayFuture.get(5, TimeUnit.SECONDS).orElseThrow().status()).isEqualTo("ok");
    }

    @Test
    @DisplayName("activation: on only when the mesh is enabled and its data plane is installed")
    void activationRule() {
        assertThat(DaemonCommandRegistry.emergencyOnlyHeartbeat(true, true)).isTrue();
        assertThat(DaemonCommandRegistry.emergencyOnlyHeartbeat(true, false)).isFalse();
        assertThat(DaemonCommandRegistry.emergencyOnlyHeartbeat(false, true)).isFalse();
        assertThat(DaemonCommandRegistry.emergencyOnlyHeartbeat(false, false)).isFalse();
    }
}
