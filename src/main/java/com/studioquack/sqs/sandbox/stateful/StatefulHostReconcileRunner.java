package com.studioquack.sqs.sandbox.stateful;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Runs the stateful host reconcile once per daemon boot, on a single background thread (podman
 * inspect/stop/rm calls must not block application startup; one pass per boot, no polling loop —
 * an idle timer here would violate the client idle-work budget).
 */
@Slf4j
@Component
public class StatefulHostReconcileRunner {

    private final StatefulInstanceManager statefulInstanceManager;
    private final StatefulReconcileReporter reporter;

    public StatefulHostReconcileRunner(StatefulInstanceManager statefulInstanceManager,
                                       StatefulReconcileReporter reporter) {
        this.statefulInstanceManager = statefulInstanceManager;
        this.reporter = reporter;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        Thread t = new Thread(this::runSafely, "stateful-host-reconcile");
        t.setDaemon(true);
        t.start();
    }

    private void runSafely() {
        try {
            int reaped = statefulInstanceManager.reapLeftoverTemporaries();
            if (reaped > 0) {
                log.info("[SQ-6] removed {} temporary container(s) a previous engine process left behind", reaped);
            }
        } catch (Exception e) {
            log.warn("[SQ-6] temporary container reap failed (retried next boot): {}", e.toString());
        }
        try {
            StatefulReconcileReport outcome = statefulInstanceManager.reconcileWithHost();
            reporter.report(null, outcome);
        } catch (Exception e) {
            // The reconcile must never take the daemon down with it; the next boot retries.
            log.warn("[SQ-6] stateful host reconcile failed (registry stays empty this boot): {}", e.toString());
        }
    }
}
