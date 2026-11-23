package com.studioquack.sqs.channel.net;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Heartbeat / {@code status} {@code net} object: counters since process start plus the last observed agent
 * snapshot. Shape-only — no secrets, no peer addresses.
 */
@Component
public class NetTelemetry {

    private final AtomicLong tlsHandshakeErrors = new AtomicLong();
    private final AtomicLong meshTokenRejects = new AtomicLong();

    private volatile String agentVersion;
    private volatile String backendState;
    private volatile String overlayIp;
    private volatile String cellId;
    private volatile String hubInUse;
    private volatile int peersCount;
    public void noteTlsHandshakeError() {
        tlsHandshakeErrors.incrementAndGet();
    }

    public void noteMeshTokenReject() {
        meshTokenRejects.incrementAndGet();
    }

    public void updateAgent(String agentVersion, String backendState, String overlayIp, int peersCount) {
        if (agentVersion != null) {
            this.agentVersion = agentVersion;
        }
        this.backendState = backendState;
        this.overlayIp = overlayIp;
        this.peersCount = peersCount;
    }

    public void updateCell(String cellId) {
        this.cellId = cellId;
    }

    public void updateHubInUse(String hubInUse) {
        this.hubInUse = hubInUse;
    }

    /**
     * Snapshot for the heartbeat / status payload.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> net = new LinkedHashMap<>();
        net.put("agentVersion", agentVersion);
        net.put("backendState", backendState);
        net.put("overlayIp", overlayIp);
        net.put("cellId", cellId);
        net.put("hubInUse", hubInUse);
        net.put("peersCount", peersCount);
        net.put("tlsHandshakeErrors", tlsHandshakeErrors.get());
        net.put("meshTokenRejects", meshTokenRejects.get());
        // Non-loopback http control URLs are refused before join, so this stays false.
        net.put("controlUrlPlainHttp", false);
        return net;
    }
}
