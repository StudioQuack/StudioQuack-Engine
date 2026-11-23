package com.studioquack.sqs.maintenance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The exit thread must keep the JVM alive until it calls {@code System.exit(code)}: a daemon thread
 * let the JVM end with status 0 once the context's last non-daemon thread stopped, and systemd's
 * {@code Restart=on-failure} then never brought the engine back after a restart.
 */
class MaintenanceConfigExitThreadTest {

    @Test
    void exit_thread_is_not_a_daemon_so_the_exit_code_survives() {
        Thread exit = MaintenanceConfig.exitThread(() -> { });

        assertFalse(exit.isDaemon());
        assertEquals("sqs-maintenance-exit", exit.getName());
    }
}
