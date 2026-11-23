package com.studioquack.sqs.channel.command;

import com.studioquack.common.security.envelope.EnvelopeCrypto;
import com.studioquack.sqs.channel.ChannelKemIkm;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * After hybrid enrolment the registry refuses unsigned or env-mismatched
 * command MACs so a heartbeat impersonator cannot forge deploy/start-run.
 */
class DaemonCommandRegistryCommandMacTest {

    private static final byte[] IKM = new byte[32];
    private static final String ENV = "env-mac-1";
    private static final String NOW = java.time.Instant.now().toString();

    static {
        IKM[0] = 0x42;
        IKM[31] = 0x11;
    }

    private static final class CountingHandler implements DaemonCommandHandler {
        final AtomicInteger executions = new AtomicInteger();

        @Override
        public String type() {
            return "deploy";
        }

        @Override
        public DaemonCommandOutcome handle(DaemonCommand command) {
            executions.incrementAndGet();
            return DaemonCommandOutcome.ok("done");
        }
    }

    private static DaemonCommandRegistry registryWithIkm(CountingHandler handler) {
        ChannelKemIkm holder = new ChannelKemIkm();
        holder.install(ENV, IKM);
        return new DaemonCommandRegistry(List.of(handler), 60_000L, false, holder);
    }

    @Test
    @DisplayName("hybrid daemon refuses unsigned deploy")
    void unsignedDeployIsRefused() {
        CountingHandler deploy = new CountingHandler();
        DaemonCommandRegistry registry = registryWithIkm(deploy);

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-unsigned", "deploy", NOW, "{}"),
                DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isZero();
        assertThat(result).isPresent();
        assertThat(result.orElseThrow().status()).isNotEqualTo("ok");
        assertThat(result.orElseThrow().error())
                .startsWith(DaemonCommandRegistry.ERROR_COMMAND_UNAUTHENTICATED);
    }

    @Test
    @DisplayName("valid MAC runs the handler")
    void validMacRuns() {
        CountingHandler deploy = new CountingHandler();
        DaemonCommandRegistry registry = registryWithIkm(deploy);
        String mac = EnvelopeCrypto.macCommand(
                IKM, ENV, "c-ok", "deploy", NOW, "{}");

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-ok", "deploy", NOW, "{}", mac),
                DaemonCommandRegistry.INBOUND_OVERLAY);

        assertThat(deploy.executions.get()).isEqualTo(1);
        assertThat(result).isPresent();
        assertThat(result.orElseThrow().status()).isEqualTo("ok");
    }

    @Test
    @DisplayName("MAC bound to another env is refused")
    void envBoundMacIsRefused() {
        CountingHandler deploy = new CountingHandler();
        DaemonCommandRegistry registry = registryWithIkm(deploy);
        String mac = EnvelopeCrypto.macCommand(
                IKM, "env-other", "c-swap", "deploy", NOW, "{}");

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-swap", "deploy", NOW, "{}", mac),
                DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isZero();
        assertThat(result.orElseThrow().error())
                .startsWith(DaemonCommandRegistry.ERROR_COMMAND_UNAUTHENTICATED);
    }

    @Test
    @DisplayName("signed command older than the dedup TTL is refused (post-restart replay)")
    void staleSignedCommandIsRefused() {
        CountingHandler deploy = new CountingHandler();
        DaemonCommandRegistry registry = registryWithIkm(deploy);
        String old = java.time.Instant.now().minusSeconds(120).toString();
        String mac = EnvelopeCrypto.macCommand(IKM, ENV, "c-stale", "deploy", old, "{}");

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-stale", "deploy", old, "{}", mac),
                DaemonCommandRegistry.INBOUND_OVERLAY);

        assertThat(deploy.executions.get()).isZero();
        assertThat(result.orElseThrow().error())
                .startsWith(DaemonCommandRegistry.ERROR_COMMAND_UNAUTHENTICATED);
    }

    @Test
    @DisplayName("signed command without issuedAt is refused")
    void unstampedSignedCommandIsRefused() {
        CountingHandler deploy = new CountingHandler();
        DaemonCommandRegistry registry = registryWithIkm(deploy);
        String mac = EnvelopeCrypto.macCommand(IKM, ENV, "c-nostamp", "deploy", null, "{}");

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-nostamp", "deploy", null, "{}", mac),
                DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isZero();
        assertThat(result.orElseThrow().error())
                .startsWith(DaemonCommandRegistry.ERROR_COMMAND_UNAUTHENTICATED);
    }

    @Test
    @DisplayName("holder present but IKM not installed yet refuses unsigned commands")
    void emptyHolderRefusesUnsigned() {
        CountingHandler deploy = new CountingHandler();
        ChannelKemIkm holder = new ChannelKemIkm();
        DaemonCommandRegistry registry =
                new DaemonCommandRegistry(List.of(deploy), 60_000L, false, holder);

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-boot", "deploy", NOW, "{}"),
                DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isZero();
        assertThat(result.orElseThrow().error())
                .startsWith(DaemonCommandRegistry.ERROR_COMMAND_UNAUTHENTICATED);
    }

    @Test
    @DisplayName("registry without a ChannelKemIkm holder (offline tests) still accepts unsigned commands")
    void preHybridUnsignedStillRuns() {
        CountingHandler deploy = new CountingHandler();
        DaemonCommandRegistry registry = new DaemonCommandRegistry(List.of(deploy), 60_000L);

        Optional<DaemonCommandResult> result = registry.dispatch(
                new DaemonCommand("c-legacy", "deploy", NOW, "{}"),
                DaemonCommandRegistry.INBOUND_HEARTBEAT);

        assertThat(deploy.executions.get()).isEqualTo(1);
        assertThat(result.orElseThrow().status()).isEqualTo("ok");
    }
}
