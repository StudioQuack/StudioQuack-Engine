package com.studioquack.sqs.channel;

import java.util.Map;

/**
 * Supplies the {@code resources} snapshot the daemon reports on every heartbeat. Kept as an SPI (not a fixed
 * struct) so sandbox/warm-pool and engine can contribute live figures (cpuUsagePct, jobsRunning, warmPool*,
 * effective* caps, isolationBackend, productionReady, …) without the channel depending on them.
 */
@FunctionalInterface
public interface HeartbeatResourceProvider {

    /**
     * The current resource snapshot (camelCase keys matching the BE contract).
     */
    Map<String, Object> resources();
}
