package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.HeartbeatResourceProvider;
import com.studioquack.sqs.channel.SqsChannelBootstrap;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.oci.ContainerTelemetryService;
import com.studioquack.sqs.sandbox.oci.ContainerTelemetryService.ContainerInfo;
import com.studioquack.sqs.sandbox.oci.ContainerTelemetryService.ContainerKind;
import com.studioquack.sqs.sandbox.oci.ContainerTelemetryService.ContainerTelemetry;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code status}: reports the daemon's live resource-usage snapshot as JSON. Where the numbers come from.
 */
@Slf4j
@Component
public class StatusCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire type; must match the control plane.
     */
    public static final String TYPE = "status";

    private final ObjectMapper objectMapper;

    /**
     * Optional: {@code null} when the secure channel is disabled and no provider bean exists.
     * {@link HeartbeatResourceProvider} is a {@code @FunctionalInterface}, so this is trivially stubbable in
     * tests.
     */
    @Nullable
    private final HeartbeatResourceProvider resourceProvider;

    @Nullable
    private final ContainerTelemetryService containerTelemetryService;

    @Nullable
    private final WarmPool warmPool;

    /**
     * Optional: {@code null} when the stateful sandbox is not wired (e.g. a slice test). Its
     * {@link StatefulInstanceManager#inventory()} is merged into the {@code containers} snapshot so a registered
     * stateful instance shows up in the SAME inventory as ephemeral sandbox/warm containers, rather than only in
     * the stateful-instance-specific commands.
     */
    @Nullable
    private final StatefulInstanceManager statefulInstanceManager;

    /**
     * Optional channel bootstrap — supplies the WireGuard overlay's last recovery error so the FE's "prefer
     * WireGuard" retry can surface WHY the tunnel is down. {@code null} when the secure channel is disabled (slice
     * test).
     */
    @Nullable
    private final SqsChannelBootstrap channelBootstrap;

    /**
     * Optional tamper-event backlog. While the daemon beats, events ride the heartbeat body; once the cadence goes
     * OVERLAY_SILENT the beat stops and this reply becomes the only prompt path home for a security signal that
     * must not wait out a state change.
     */
    @Nullable
    private final com.studioquack.sqs.channel.TamperEventReporter tamperEventReporter;

    /**
     * Optional built-in node-library manager — supplies the {@code nodeLibrary} block (built sha256 + update
     * progress/failure) so Engine Settings → Stateful Instances can render the Update button's state. {@code null}
     * in slice tests; the block is omitted.
     */
    @Nullable
    private final com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager nodeLibraryManager;

    @Autowired(required = false)
    private com.studioquack.sqs.channel.net.NetTelemetry netTelemetry;

    /**
     * {@code acceptingWork} must ride the status reply too: on an engine polled over the private network this
     * reply overwrites the backend's resources snapshot every 15 s, and a snapshot without the flag reads as
     * "accepting" — undoing an update's closed intake or a disk-full stop.
     */
    @Autowired(required = false)
    private com.studioquack.sqs.engine.update.WorkIntakeGate workIntakeGate;

    /**
     * {@code executionHalt} rides the status reply for the same reason as {@code acceptingWork}: the backend
     * stores whichever snapshot lands last, so a status poll without it would flip the engine out of
     * limits-exceeded on the card and re-fire the alarm on every beat/poll alternation.
     */
    @Autowired(required = false)
    private com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    private final String tier;

    @Autowired
    public StatusCommandHandler(ObjectMapper objectMapper,
                                @Nullable HeartbeatResourceProvider resourceProvider,
                                @Value("${sqs.tier:base}") String tier,
                                @Nullable ContainerTelemetryService containerTelemetryService,
                                @Nullable WarmPool warmPool,
                                @Nullable StatefulInstanceManager statefulInstanceManager,
                                @Nullable @Lazy SqsChannelBootstrap channelBootstrap,
                                @Nullable com.studioquack.sqs.channel.TamperEventReporter tamperEventReporter,
                                @Nullable com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager nodeLibraryManager) {
        this.objectMapper = objectMapper;
        this.resourceProvider = resourceProvider;
        this.containerTelemetryService = containerTelemetryService;
        this.warmPool = warmPool;
        this.statefulInstanceManager = statefulInstanceManager;
        this.channelBootstrap = channelBootstrap;
        this.tamperEventReporter = tamperEventReporter;
        this.nodeLibraryManager = nodeLibraryManager;
        this.tier = (tier == null || tier.isBlank()) ? "base" : tier;
    }

    public StatusCommandHandler(ObjectMapper objectMapper,
                                @Nullable HeartbeatResourceProvider resourceProvider,
                                String tier,
                                @Nullable ContainerTelemetryService containerTelemetryService,
                                @Nullable WarmPool warmPool,
                                @Nullable StatefulInstanceManager statefulInstanceManager) {
        this(objectMapper, resourceProvider, tier, containerTelemetryService, warmPool,
                statefulInstanceManager, null, null, null);
    }

    public StatusCommandHandler(ObjectMapper objectMapper,
                                @Nullable HeartbeatResourceProvider resourceProvider,
                                String tier,
                                @Nullable ContainerTelemetryService containerTelemetryService,
                                @Nullable WarmPool warmPool) {
        this(objectMapper, resourceProvider, tier, containerTelemetryService, warmPool, null, null,
                null, null);
    }

    public StatusCommandHandler(ObjectMapper objectMapper,
                                @Nullable HeartbeatResourceProvider resourceProvider,
                                String tier) {
        this(objectMapper, resourceProvider, tier, null, null, null, null, null, null);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("tier", tier);
            body.put("daemonStatus", "UP");
            body.put("uptimeSec", ManagementFactory.getRuntimeMXBean().getUptime() / 1000L);
            body.put("reportedAt", Instant.now().toString());
            // Which daemon build is actually running, so the UI can compare it with the release manifest.
            body.put("daemonBuild", DaemonBuildIdentity.snapshot());
            Map<String, Object> resources = new LinkedHashMap<>(snapshot());
            // Which build runs and where its last in-place update stands — "Update daemon" keys off it.
            resources.put("daemonBuild", com.studioquack.sqs.engine.update.DaemonBuild.snapshot());
            // Disk-full stop: the backend shows the engine STOPPED and raises the alarm from this.
            resources.put("diskGuard", com.studioquack.sqs.maintenance.DiskFullGuard.snapshot());
            if (workIntakeGate != null) {
                resources.put(com.studioquack.sqs.engine.update.WorkIntakeGate.RESOURCE_KEY,
                        workIntakeGate.isAccepting());
            }
            if (executionHaltGuard != null && executionHaltGuard.isEnabled()) {
                resources.put(com.studioquack.sqs.engine.halt.ExecutionHaltGuard.RESOURCE_KEY,
                        executionHaltGuard.mode().name());
            }
            body.put("resources", resources);
            body.put("containers", containers());
            body.put("warmPool", warmPool());
            body.put("transferBytesTotal", com.studioquack.sqs.transport.TransferByteMeter.snapshotTotal());
            // The boot host-reconcile outcome rides every status result so the BE
            // (and the FE) can see adopted/reaped stateful containers even when the callback POST
            // could not be delivered. Absent until the first boot reconcile completes.
            if (statefulInstanceManager != null && statefulInstanceManager.lastReconcile() != null) {
                body.put("statefulReconcile", objectMapper.valueToTree(statefulInstanceManager.lastReconcile()));
            }
            // Built-in node library: built sha256 + on-demand update progress/failure for the
            // engine Settings → Stateful Instances Update button. Omitted when the manager is
            // unwired (slice tests) or the library is disabled (snapshot() returns null).
            if (nodeLibraryManager != null) {
                com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager.NodeLibrarySnapshot nl =
                        nodeLibraryManager.snapshot();
                if (nl != null) {
                    body.put("nodeLibrary", objectMapper.valueToTree(nl));
                }
            }
            // WireGuard overlay health for the FE transport tile: the watchdog's last recovery error
            // (null when the tunnel is up / never failed) lets the "prefer WireGuard" retry surface
            // the real reason a bring-up keeps failing instead of a generic "tunnel down".
            if (channelBootstrap != null) {
                String netError = channelBootstrap.netLastError();
                if (netError != null && !netError.isBlank()) {
                    body.put("netLastError", netError);
                }
            }
            if (netTelemetry != null) {
                body.put("net", netTelemetry.snapshot());
            }
            // Tamper events: the beat's payload while the daemon beats; this reply is their prompt
            // path home once the cadence goes silent on a confirmed overlay. Same send-or-lose
            // drain semantics as the beat — never re-queued.
            if (tamperEventReporter != null) {
                List<com.studioquack.sqs.channel.TamperEventReporter.TamperEvent> tamperEvents =
                        tamperEventReporter.drain();
                if (!tamperEvents.isEmpty()) {
                    body.put("tamperEvents", tamperEvents);
                }
            }
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(body));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("status failed: " + message(e));
        }
    }

    /**
     * The live resource snapshot. Prefers the shared {@link HeartbeatResourceProvider}; falls back to a minimal
     * JVM snapshot when none is wired (channel disabled) or when a provider misbehaves, so {@code status} always
     * returns SOMETHING actionable rather than an error.
     */
    private Map<String, Object> snapshot() {
        if (resourceProvider != null) {
            try {
                Map<String, Object> r = resourceProvider.resources();
                if (r != null && !r.isEmpty()) {
                    return r;
                }
            } catch (RuntimeException e) {
                log.warn("status: resource provider failed, using minimal JVM fallback: {}", e.toString());
            }
        }
        Map<String, Object> r = new LinkedHashMap<>();
        Runtime rt = Runtime.getRuntime();
        r.put("cpuCount", rt.availableProcessors());
        r.put("jvmHeapUsedBytes", rt.totalMemory() - rt.freeMemory());
        r.put("jvmHeapMaxBytes", rt.maxMemory());
        r.put("resourceProvider", "minimal-fallback");
        return r;
    }

    private Object containers() {
        ContainerTelemetry base = containerTelemetryService == null
                ? new ContainerTelemetry("", List.of(), ContainerTelemetryService.LABEL_SELECTOR,
                        Instant.now().toString(), List.of(),
                        Map.of("telemetry", "container telemetry service not wired"))
                : containerTelemetryService.snapshot();
        List<StatefulInstance> stateful = statefulInstanceManager == null
                ? List.of() : statefulInstanceManager.inventory();
        List<ContainerInfo> items = new ArrayList<>(base.items().stream()
                .map(this::withWarmPoolTarget)
                .toList());
        if (stateful.isEmpty()) {
            return new ContainerTelemetry(base.activeRuntime(), base.queriedRuntimes(), base.labelSelector(),
                    base.reportedAt(), items, base.errors());
        }
        java.util.Set<String> knownContainerIds = items.stream()
                .map(ContainerInfo::id)
                .filter(java.util.Objects::nonNull)
                .collect(java.util.stream.Collectors.toSet());
        for (StatefulInstance inst : stateful) {
            // A stopped instance has no container (stop removes it); its record stays registered
            // only to answer status queries for its id, so it is not part of the container inventory.
            if (inst.status() == StatefulStatus.STOPPED) {
                continue;
            }
            ContainerInfo item = toContainerInfo(inst);
            if (item.id() != null
                    && knownContainerIds.stream().anyMatch(known -> sameContainerId(known, item.id()))) {
                continue;
            }
            items.add(item);
            if (item.id() != null) {
                knownContainerIds.add(item.id());
            }
        }
        return new ContainerTelemetry(base.activeRuntime(), base.queriedRuntimes(), base.labelSelector(),
                base.reportedAt(), items, base.errors());
    }

    /**
     * Container IDs compare by prefix in EITHER direction: the telemetry row carries podman inspect's full 64-char
     * ID while the stateful registry may hold the short ID (e.g. after adoption via {@code podman ps}), and the FE
     * truncates to 12 chars for display — an exact-string dedup let the same container appear twice in the
     * overview.
     */
    private static boolean sameContainerId(String a, String b) {
        return a.equals(b) || a.startsWith(b) || b.startsWith(a);
    }

    /**
     * Maps a registered stateful instance into the SAME item shape the Podman telemetry reports, so the FE renders
     * one merged list. {@code image} is the instance's actual {@code imageRef} (never the container name); no
     * command/secret/health payload leaks into this row.
     */
    private static ContainerInfo toContainerInfo(StatefulInstance inst) {
        String id = (inst.containerId() != null && !inst.containerId().isBlank())
                ? inst.containerId() : inst.instanceId();
        return new ContainerInfo(
                id,
                inst.containerName(),
                inst.status(),
                inst.imageRef(),
                null,
                "podman",
                inst.startedAt() == null ? null : inst.startedAt().toString(),
                ContainerKind.STATEFUL_INSTANCE,
                null);
    }

    /**
     * Gives intended warm slots their configured target so their created/running state is contextual. Lookup is by
     * IMAGE; falls back to the language label only when the inspect row has no image ref.
     */
    private ContainerInfo withWarmPoolTarget(ContainerInfo item) {
        if (warmPool == null || item.kind() != ContainerKind.WARM_POOL) {
            return item;
        }
        String key = item.image() != null && !item.image().isBlank() ? item.image() : item.language();
        if (key == null) {
            return item;
        }
        return item.withWarmPoolTarget(warmPool.target(key));
    }

    /**
     * Warm-pool telemetry block. {@code languages[]} is now image-keyed rows ({@code imageRef} is the identity)
     * that RETAIN a {@code language} field for runner/broker eligibility display.
     */
    private Object warmPool() {
        if (warmPool == null) {
            return Map.of(
                    "warmerKind", "unavailable",
                    "languages", java.util.List.of(),
                    "persistence", "live-only: no warm-pool target store is wired");
        }
        return Map.of(
                "warmerKind", warmPool.warmerKind(),
                "languages", warmPool.status(),
                "persistence", "persisted: targets are restored when the daemon restarts");
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
