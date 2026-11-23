package com.studioquack.sqs.maintenance;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration surface for the daemon's operator-driven log-tailing and lifecycle ops
 * ({@code logs}, {@code restart}, {@code uninstall}). Binds the {@code sqs.maintenance.*} tree.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "sqs.maintenance")
public class MaintenanceProperties {

    /**
     * Directory holding the daemon's file application logs, if the deployment
     * writes logs to a file (via {@code logging.file.name}/{@code logging.file.path}).
     * Blank → NO daemon-reachable log dir ({@code logs} returns a host-managed note).
     */
    private String logDir = "";

    /**
     * Explicit override for whether this daemon runs under a service manager that relaunches it after
     * a self-exit. Set {@code true}/{@code false} from the installer when detection is unreliable
     * (notably Windows services).
     */
    private Boolean serviceManaged = null;

    /** Grace (ms) between reporting the {@code restart} outcome and the self-exit, so the result is flushed on a heartbeat first. */
    private long restartGraceMillis = 3_000L;

    /** Grace (ms) between reporting the {@code uninstall} outcome and the self-exit / side-car teardown. */
    private long stopGraceMillis = 3_000L;
}
