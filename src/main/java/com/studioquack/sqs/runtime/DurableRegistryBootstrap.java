package com.studioquack.sqs.runtime;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Runs the durable-registry boot recovery once the daemon is ready (items 2 & 3). On
 * {@link ApplicationReadyEvent} it:
 * <ul>
 * <li>{@link DispatchExecutionRegistry#hydrateOnBoot} — re-loads PENDING {@code dispatch_wait_state}
 * rows from a previous boot whose ReceiveTask is still parked (else DISCARDs the orphan);</li>
 * <li>{@link SubProcessCompletionRegistry#hydrateOnBoot} — recover-by-fail any PENDING
 * {@code sub_pi_pending} children from a previous boot (never resume an orphaned child).</li>
 * </ul>
 *
 * <p>Kept separate from the orphan run-index sweeper: this is the epoch-carry-over recovery of the
 * daemon's own wait-state registries, run exactly once at startup. It is guarded so a recovery
 * failure can never prevent the daemon from starting.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DurableRegistryBootstrap {

    private final DispatchExecutionRegistry dispatchRegistry;
    private final SubProcessCompletionRegistry subProcessRegistry;

    @EventListener(ApplicationReadyEvent.class)
    @Order(50) // run BEFORE the orphan sweeper's boot pass (@Order(100)) so epoch carry-over is handled first
    public void recoverOnBoot() {
        try {
            int hydrated = dispatchRegistry.hydrateOnBoot();
            int recovered = subProcessRegistry.hydrateOnBoot();
            log.info("durable-registry boot recovery: dispatch_wait_state hydrated={} sub_pi recovered-by-fail={}",
                    hydrated, recovered);
        } catch (RuntimeException e) {
            // Boot recovery must never block startup — the periodic sweeper is the backstop.
            log.warn("durable-registry boot recovery failed (continuing startup): {}", e.toString());
        }
    }
}
