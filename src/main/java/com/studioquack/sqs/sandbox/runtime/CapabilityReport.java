package com.studioquack.sqs.sandbox.runtime;

import com.studioquack.sqs.sandbox.IsolationTier;

import java.util.List;

/**
 * A boot-time / on-demand snapshot of the host's sandbox capabilities, mirroring the heartbeat
 * isolation view. Surfaced on the health endpoint and (later) pushed to the control plane so the
 * operator sees the effective vs requested tier and any degradation advisory.
 *
 * @param available A container CLI is on PATH AND its daemon answered the liveness probe.
 */
public record CapabilityReport(
        boolean available,
        OciRuntimeKind kind,
        String cliPath,
        String serverVersion,
        List<DetectedOciRuntime> runtimes,
        boolean gvisorAvailable,
        boolean containerCliAvailable,
        IsolationTier requestedTier,
        IsolationTier recommendedTier,
        IsolationTier effectiveTier,
        String runtimeFlag,
        String advisory) {
}
