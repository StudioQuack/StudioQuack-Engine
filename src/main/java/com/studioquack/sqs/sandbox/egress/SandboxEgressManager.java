package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.channel.SecurityTelemetryAccumulator;
import com.studioquack.sqs.channel.wg.OverlayPeerDirectory;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.NetworkPlan;
import com.studioquack.sqs.sandbox.egress.OverlayEgressRules.SandboxNet;
import com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Host-egress readiness + iptables lifecycle. {@link #readiness()} is a pure volatile read for the
 * dispatch hot path.
 */
@Slf4j
@Component
public class SandboxEgressManager {

    public enum EgressMode {
        AUTO, ENFORCED, OFF;

        static EgressMode fromWire(String raw) {
            if (raw == null || raw.isBlank()) {
                return AUTO;
            }
            return switch (raw.trim().toLowerCase(Locale.ROOT)) {
                case "off" -> OFF;
                case "enforced" -> ENFORCED;
                default -> AUTO;
            };
        }
    }

    public record ExecResult(boolean ok, int exitCode, String stderr) {
        public ExecResult {
            stderr = stderr == null ? "" : stderr;
        }
    }

    @FunctionalInterface
    public interface CommandExec {
        ExecResult run(List<String> argv, Duration timeout);
    }

    /** Platform support for host iptables/iproute2 programming of sandbox bridges + the tunnels. */
    public record Platform(boolean ok, String gapReason) {
        public static Platform supported() {
            return new Platform(true, "");
        }

        public static Platform unsupported(String reason) {
            return new Platform(false, reason == null ? "unsupported platform" : reason);
        }
    }

    /**
     * One enrolled overlay network. {@code appliedRules} is the exact argv this network contributed
     * (the removal source of truth — a recompute could resolve a moved peer to its new address and
     * miss the stale rule, which is precisely the leak tracking exists to prevent).
     */
    private record NetworkState(String key, String iface, String overlayIp, String overlayScope,
                                int tableId, int steeringPref, boolean enforced,
                                List<List<String>> appliedRules) {
        NetworkPlan toPlan() {
            return new NetworkPlan(key, iface, overlayIp, overlayScope, tableId, steeringPref);
        }
    }

    /** Which network one live container rides (bookkeeping only under). */
    private record AssignedNetwork(String containerIp, String networkKey) {
    }

    /** Read-only view of one enrolled network, for the drain status surface. */
    public record NetworkView(String key, String iface, String overlayIp, String overlayScope,
                              int tableId, boolean enforced) {
    }

    private final EgressMode mode;
    private final boolean channelEnabled;
    private final boolean netEnabled;
    private final String netIface;
    private final Platform platform;
    private final String podmanCli;
    private final CommandExec exec;
    private final Function<String, List<SandboxNet>> networkDiscovery;
    private final boolean elevate;
    /** Null only in tests/stubs: the outbound rule projection + the resolver for its ids. */
    private final NetworkRuleStore networkRuleStore;
    private final OverlayPeerDirectory peerDirectory;
    /**
     * Finds the namespace the sandbox bridges live in when it is not the host's own: the Podman
     * machine (macOS / Windows) or rootless Podman's (a non-root Linux daemon). Null on a root
     * Linux daemon, where the rules go on the host itself.
     */
    private final Supplier<PodmanNetnsTarget> netnsLocator;
    /** Where the rules run; null until {@link #netnsLocator} finds it (host rules on root Linux). */
    private volatile PodmanNetnsTarget netns;
    /** Last overlay bring-up — re-driven when the namespace appears late or loses its rules. */
    private volatile NetworkRequest lastRequest;

    private record NetworkRequest(String key, String iface, String overlayCidr, String overlayScope,
                                  Integer pinnedTableId) {
    }

    private static final String VM_GAP_PREFIX = "overlay-egress needs a running Podman machine with iptables";
    private static final String NETNS_RULES_MISSING =
            "overlay-egress rules missing in the sandbox network namespace — re-applying";
    /** A failed bring-up is retried at this pace, not on every verify tick. */
    private static final long NETNS_RETRY_BACKOFF_NANOS = Duration.ofMinutes(2).toNanos();
    private volatile long lastNetnsRetryNanos = System.nanoTime() - NETNS_RETRY_BACKOFF_NANOS;

    /**
     * The readiness of the DEFAULT network (what a new container would get) — the single volatile
     * read the dispatch hot path makes. Per-network states live in {@link #networks}.
     */
    private volatile EgressReadiness current;

    /** Enrolled networks, keyed by interface name; insertion order = registration order. */
    private final Object networkLock = new Object();
    private final Map<String, NetworkState> networks = new LinkedHashMap<>();
    /** Monotonic table/pref allocator: table = 88 + offset, steering pref = 10700 + offset. */
    private int nextOffset;
    /** Residual — one-shot latch for the conntrack-unavailable WARN (never per-rule). */
    private final java.util.concurrent.atomic.AtomicBoolean conntrackWarned =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    /** The network NEW containers are assigned to. */
    private String defaultNetworkKey;
    /** The sandbox networks discovered on the most recent registration (removal input). */
    private List<SandboxNet> lastNets = List.of();

    /**
     * Live per-container overlay grants: bridge IP → the network they were resolved on and the EXACT
     * argv applied. Tracked so an address churn (a granted engine reconnecting on a new /32) or a rule
     * edit can be diffed rule-by-rule — {@code SQS_SBX_PERBOX} is never flushed while grants are
     * tracked, so stale ACCEPTs must be removed individually and new ones inserted, never rebuilt
     * around live neighbours.
     */
    private final Object grantLock = new Object();
    private final Map<String, LiveGrants> liveGrants = new LinkedHashMap<>();
    /**
     * Set while {@code SQS_SBX_PERBOX} may hold rules no tracked grant owns — from daemon start,
     * and again when the sandbox namespace is rebuilt — until the next full rebuild flushes it.
     */
    private volatile boolean grantChainStale = true;

    /** Live per-container network assignments (bridge IP → network + steering argv). */
    private final Map<String, AssignedNetwork> assignedNetworks = new LinkedHashMap<>();

    /** Nudged when a container's assignment is released (the drain manager's occupancy). */
    private volatile Consumer<String> containerReleaseListener = ip -> {
    };

    /**
     * E.3 — optional corroborating counter for DEGRADED posture observations. Wired after construction
     * (bean order); absence is fine (tests / channel-less boots).
     */
    private volatile SecurityTelemetryAccumulator securityTelemetry;

    /** The network one live container's grants resolve on, plus the applied argv (the removal source of truth). */
    private record LiveGrants(String networkKey, List<List<String>> appliedRules) {
    }

    /**
     * Spring production ctor — platform support = Linux + iptables/iproute2 + resolvable podman, or a
     * namespace the rules can be applied inside: the Podman machine (macOS / Windows) or rootless
     * Podman's (a non-root Linux daemon). Anything else is DEGRADED (AUTO) / UNAVAILABLE (ENFORCED);
     * never pretend.
     */
    @Autowired
    public SandboxEgressManager(
            @Value("${sqs.sandbox.overlay-egress:auto}") String modeWire,
            @Value("${sqs.channel.enabled:false}") boolean channelEnabled,
            @Value("${sqs.channel.net.enabled:true}") boolean netEnabled,
            @Value("${sqs.channel.net.interface-name:sqnet0}") String netIface,
            ObjectProvider<ContainerRuntime> runtimeProvider,
            ObjectProvider<OverlayPeerDirectory> peerDirectoryProvider,
            ObjectProvider<NetworkRuleStore> ruleStoreProvider) {
        String cli = resolveCli(runtimeProvider);
        String podman = cli == null || cli.isBlank() ? "podman" : cli;
        String osName = System.getProperty("os.name", "");
        boolean hasCli = cli != null && !cli.isBlank();
        boolean vmHost = !isLinux(osName) && hasCli;
        boolean nonRoot = !vmHost && needsElevate();
        this(EgressMode.fromWire(modeWire), channelEnabled, netEnabled,
                netIface == null || netIface.isBlank() ? "sqnet0" : netIface.trim(),
                vmHost ? vmGap(osName) : detectPlatform(cli),
                podman,
                defaultExec(),
                SandboxEgressManager::discoverNetworks,
                nonRoot,
                peerDirectoryProvider == null ? null : peerDirectoryProvider.getIfAvailable(),
                ruleStoreProvider == null ? null : ruleStoreProvider.getIfAvailable(),
                vmHost ? () -> PodmanNetnsTarget.detectMachine(podman, SandboxNetworks.JOBS,
                        PodmanNetnsTarget.defaultRunner())
                        : nonRoot && hasCli ? () -> PodmanNetnsTarget.detectLocalRootless(podman,
                        SandboxNetworks.JOBS, PodmanNetnsTarget.defaultRunner())
                        : null);
        if (this.mode == EgressMode.OFF) {
            log.warn("sandbox.egress.opt-out overlay-egress=off — enforcement is DISABLED by operator "
                    + "configuration; networked sandboxes will share this host's network (LAN, "
                    + "link-local, loopback reachable). Acceptable on a developer laptop only.");
        }
    }

    /** Subclass / stub seam (tests that override {@link #readiness()}). */
    protected SandboxEgressManager() {
        this(EgressMode.OFF, false, false, "sqnet0", Platform.supported(), "podman",
                defaultExec(), cli -> List.of(), false, null, null, null);
    }

    private SandboxEgressManager(EgressMode mode, boolean channelEnabled, boolean netEnabled,
                                 String netIface, Platform platform, String podmanCli,
                                 CommandExec exec, Function<String, List<SandboxNet>> networkDiscovery,
                                 boolean elevate,
                                 OverlayPeerDirectory peerDirectory, NetworkRuleStore networkRuleStore,
                                 Supplier<PodmanNetnsTarget> netnsLocator) {
        this.mode = mode == null ? EgressMode.AUTO : mode;
        this.channelEnabled = channelEnabled;
        this.netEnabled = netEnabled;
        this.netIface = netIface;
        this.platform = platform == null ? Platform.supported() : platform;
        this.podmanCli = podmanCli;
        this.exec = exec == null ? defaultExec() : exec;
        this.networkDiscovery = networkDiscovery == null
                ? SandboxEgressManager::discoverNetworks : networkDiscovery;
        this.elevate = elevate;
        this.peerDirectory = peerDirectory;
        this.networkRuleStore = networkRuleStore;
        this.netnsLocator = netnsLocator;
        this.current = initialReadiness();
        // Address churn and rule edits both reprogram live containers (never a chain flush).
        if (this.peerDirectory != null) {
            this.peerDirectory.setChangeListener(this::reprogramLiveGrants);
        }
        if (this.networkRuleStore != null) {
            this.networkRuleStore.setChangeListener(this::reprogramLiveGrants);
        }
        if (current.state() == EgressReadiness.State.DEGRADED
                && current.reason() != null
                && !current.reason().equals("overlay-egress disabled")) {
            log.warn("sandbox.egress: DEGRADED — {} (OPEN sandboxes allowed; traffic not overlay-sourced)",
                    current.reason());
            // Reason-class only — never the host reason string / overlay IP on the wire.
            recordSandboxEgressDegraded();
        }
    }

    /** E.3 — Spring / test seam; null clears. */
    public void setSecurityTelemetryAccumulator(SecurityTelemetryAccumulator accumulator) {
        this.securityTelemetry = accumulator;
    }

    /** Fail-open counter bump for observed DEGRADED posture (heartbeat corroboration only). */
    private void recordSandboxEgressDegraded() {
        try {
            SecurityTelemetryAccumulator acc = securityTelemetry;
            if (acc != null) {
                acc.record("sandbox_egress_degraded");
            }
        } catch (RuntimeException ignored) {
            // Telemetry must never affect egress readiness or sandbox spawn.
        }
    }

    /**
     * Test factory — injects platform + command exec + network discovery (no Spring, no real
     * iptables). No rule store / peer directory: no container ever resolves a grant.
     */
    public static SandboxEgressManager forTest(String modeWire, boolean channelEnabled,
                                               boolean netEnabled, String netIface,
                                               Platform platform, String podmanCli,
                                               CommandExec exec,
                                               Function<String, List<SandboxNet>> networkDiscovery) {
        return new SandboxEgressManager(EgressMode.fromWire(modeWire), channelEnabled,
                netEnabled, netIface, platform, podmanCli, exec, networkDiscovery, false,
                null, null, null);
    }

    /** Test factory for a macOS / Windows host: rules go through whatever {@code netnsLocator} finds. */
    public static SandboxEgressManager forNetnsTest(String modeWire, boolean channelEnabled,
                                                 boolean netEnabled, String netIface,
                                                 Function<String, List<SandboxNet>> networkDiscovery,
                                                 Supplier<PodmanNetnsTarget> netnsLocator) {
        return new SandboxEgressManager(EgressMode.fromWire(modeWire), channelEnabled,
                netEnabled, netIface, vmGap("Mac OS X"), "podman", defaultExec(), networkDiscovery,
                false, null, null, netnsLocator);
    }

    /** Test factory with the outbound projection + resolver wired (churn / reprogram tests). */
    public static SandboxEgressManager forTest(String modeWire, boolean channelEnabled,
                                               boolean netEnabled, String netIface,
                                               Platform platform, String podmanCli,
                                               CommandExec exec,
                                               Function<String, List<SandboxNet>> networkDiscovery,
                                               OverlayPeerDirectory peerDirectory,
                                               NetworkRuleStore networkRuleStore) {
        return new SandboxEgressManager(EgressMode.fromWire(modeWire), channelEnabled,
                netEnabled, netIface, platform, podmanCli, exec, networkDiscovery, false,
                peerDirectory, networkRuleStore, null);
    }

    /** Pure in-memory volatile read — dispatch hot path; reports the DEFAULT network. */
    public EgressReadiness readiness() {
        return current;
    }

    /**
     * (Item 0) — the operator-facing run-log sentence for a container that started while this host's
     * enforcement was {@link EgressReadiness.State#DEGRADED} (e.g. self-hosted macOS/Windows, where
     * host iptables cannot be programmed): the node ran without the host's local-network and overlay
     * denies, and the run log must say so next to the node's own output.
     */
    public String degradedRunLogNote() {
        EgressReadiness r = current;
        if (r == null || r.state() != EgressReadiness.State.DEGRADED) {
            return null;
        }
        return "Network isolation DEGRADED on this host (" + r.reason() + "): this node ran "
                + "without overlay egress enforcement — local-network and overlay denies were NOT applied"
                + (r.operatorOptOut() ? " (operator opt-out: overlay-egress=off)." : ".");
    }

    // ============================================================================================
    // Per-network lifecycle
    // ============================================================================================.

    /**
     * Mesh is up: apply host-egress rules (MASQUERADE on the host default route, local/overlay
     * fail-closed). The network key is the configured agent interface name ({@code sqnet0}).
     */
    public void overlayUp(String overlayCidr) {
        networkUp(netIface, netIface, overlayCidr, OverlayEgressRules.OVERLAY_CIDR, null);
    }

    /**
     * Enrolls (or re-enrolls) ONE overlay network: programs host-egress + daemon-listener denies. A
     * second iface is still additive (leftover drain callers) but never installs a policy-routing
     * default via the overlay.
     *
     * @param key The network key — the interface name, unique per network.
     */
    public void networkUp(String key, String iface, String overlayCidr, String overlayScope,
                          Integer pinnedTableId) {
        if (mode == EgressMode.OFF) {
            current = optedOutReadiness();
            return;
        }
        if (netnsLocator != null) {
            lastRequest = new NetworkRequest(key, iface, overlayCidr, overlayScope, pinnedTableId);
            locateNetns();
        }
        if (!platform().ok()) {
            current = unsupportedReadiness();
            return;
        }
        if (!channelEnabled || !netEnabled) {
            current = notConfiguredReadiness();
            return;
        }
        String overlayIp = hostOfCidr(overlayCidr);
        if (overlayIp.isEmpty()) {
            markNetworkUnavailable(key, "overlay-egress rules failed: blank overlay CIDR");
            return;
        }
        // The address and scope become iptables/ip argv on the host. Only the CGNAT overlay range is ever
        // programmed; anything else is a poisoned assignment.
        if (!OverlayAddresses.isOverlayAddress(overlayIp)) {
            markNetworkUnavailable(key, "overlay-egress rules failed: overlay IP outside "
                    + OverlayEgressRules.OVERLAY_CIDR);
            return;
        }
        if (overlayScope != null && !overlayScope.isBlank()
                && !OverlayAddresses.isOverlayCidr(overlayScope)) {
            markNetworkUnavailable(key, "overlay-egress rules failed: overlay scope outside "
                    + OverlayEgressRules.OVERLAY_CIDR);
            return;
        }
        String scope = (overlayScope == null || overlayScope.isBlank())
                ? OverlayEgressRules.OVERLAY_CIDR : overlayScope.trim();
        List<SandboxNet> nets = networkDiscovery.apply(podmanCli);
        if (nets == null) {
            nets = List.of();
        }
        synchronized (networkLock) {
            lastNets = nets;
            NetworkState previous = networks.get(key);
            boolean firstEver = networks.isEmpty();
            int tableId;
            int steeringPref;
            if (pinnedTableId != null) {
                tableId = pinnedTableId;
                steeringPref = OverlayEgressRules.STEER_PREF_BASE + (tableId - OverlayEgressRules.TABLE_ID);
                nextOffset = Math.max(nextOffset, tableId - OverlayEgressRules.TABLE_ID + 1);
            } else if (previous != null) {
                tableId = previous.tableId();
                steeringPref = previous.steeringPref();
            } else {
                tableId = OverlayEgressRules.TABLE_ID + nextOffset;
                steeringPref = OverlayEgressRules.STEER_PREF_BASE + nextOffset;
                nextOffset++;
            }
            NetworkState next = new NetworkState(key, iface, overlayIp, scope, tableId, steeringPref,
                    false, List.of());

            List<List<String>> commands = new ArrayList<>();
            boolean resetGrants = firstEver && grantChainStale;
            if (firstEver) {
                // Reconcile: flush + rebuild the shared skeleton (MASQUERADE + local denies).
                commands.addAll(OverlayEgressRules.reconcileCommands(next.toPlan(), nets));
                if (resetGrants) {
                    // Rules already in PERBOX at (re)initialization belong to a dead process's containers.
                    commands.addAll(OverlayEgressRules.resetGrantChainCommands());
                }
            } else {
                if (previous != null) {
                    // Address churn: exact-remove this network's daemon denies / hairpin DROP.
                    for (List<String> del : OverlayEgressRules.removeNetworkCommands(
                            previous.toPlan(), nets)) {
                        runTolerated(del);
                    }
                }
                commands.addAll(OverlayEgressRules.addNetworkCommands(next.toPlan(), nets));
            }
            PodmanNetnsTarget machine = netns;
            // A rootless machine's namespace must be held open first, or the rules die with it.
            String failure = machine == null ? null : machine.ensureAnchor();
            if (failure == null) {
                failure = applyAll(commands);
            }
            if (failure != null) {
                log.error("sandbox.egress: rule apply failed for network {} ({}). {}", key, failure,
                        machine != null
                                ? "Check that " + machine.describe() + " is running."
                                : "Ensure /etc/sudoers.d/studioquack-wg covers iptables and ip (sudo -n).");
                markNetworkUnavailable(key, "overlay-egress rules failed: " + failure);
                return;
            }
            if (resetGrants) {
                grantChainStale = false;
            }
            networks.put(key, new NetworkState(key, iface, overlayIp, scope, tableId, steeringPref,
                    true, commands));
            if (defaultNetworkKey == null || defaultNetworkKey.equals(key)) {
                // First network, or a re-enrollment of the current default: it is (and stays) what
                // new containers get. A SECOND network never steals the default — the drain manager
                // flips it explicitly.
                defaultNetworkKey = key;
            }
            refreshCurrentReadiness();
            log.info("sandbox.egress: ENFORCED network={} overlayIp={} iface={} scope={} table={} "
                    + "networks={} default={}", key, overlayIp, iface, scope, tableId,
                    networks.size(), defaultNetworkKey);
        }
    }

    /** Every network {@link SandboxNetworkEnsure} confirms is brought under these rules first. */
    @PostConstruct
    void registerNetworkCoverage() {
        SandboxNetworkEnsure.setCoverageHook(this::coverSandboxNetwork);
    }

    @PreDestroy
    void unregisterNetworkCoverage() {
        SandboxNetworkEnsure.setCoverageHook(null);
    }

    /**
     * Brings a sandbox network created after the last rule build under the host firewall before any
     * container attaches to it ({@link SandboxNetworkEnsure.CoverageHook}). The rules are built from
     * the networks discovered at overlay bring-up, but {@code sqs-jobs} first appears with the first
     * job that needs internet: that job's bridge was not filtered at all — the host's own services,
     * cloud metadata and private ranges reachable — until the next bring-up.
     */
    public void coverSandboxNetwork(String networkName) {
        if (mode == EgressMode.OFF) {
            return;
        }
        synchronized (networkLock) {
            if (networks.isEmpty()) {
                // No rule set yet, so nothing is ENFORCED and networked starts are refused; the
                // bring-up that builds it discovers this network along with the others.
                return;
            }
            List<SandboxNet> discovered = networkDiscovery.apply(podmanCli);
            List<SandboxNet> present = discovered == null ? List.of() : discovered;
            if (present.stream().noneMatch(net -> net.name().equals(networkName))) {
                // Pre-netavark (CNI) podman reports no subnet, so there is nothing to hook.
                log.warn("sandbox.egress: sandbox network {} reports no subnet — it cannot be hooked "
                        + "into the host firewall", networkName);
                return;
            }
            List<SandboxNet> added = new ArrayList<>();
            for (SandboxNet net : present) {
                if (lastNets.stream().noneMatch(known -> known.subnetCidr().equals(net.subnetCidr()))) {
                    added.add(net);
                }
            }
            if (added.isEmpty()) {
                return;
            }
            PodmanNetnsTarget machine = netns;
            String failure = machine == null ? null : machine.ensureAnchor();
            if (failure == null) {
                failure = applyAll(OverlayEgressRules.addSubnetCommands(added, present));
            }
            if (failure != null) {
                log.error("sandbox.egress: sandbox network {} could not be hooked into the host firewall "
                        + "({}) — its containers are refused until it is", networkName, failure);
                throw new IllegalStateException("sandbox network '" + networkName
                        + "' is not filtered by the host firewall: " + failure);
            }
            List<SandboxNet> covered = new ArrayList<>(lastNets);
            covered.addAll(added);
            lastNets = List.copyOf(covered);
            log.info("sandbox.egress: sandbox network(s) {} hooked into the host firewall before first use",
                    added.stream().map(net -> net.name() + "=" + net.subnetCidr()).toList());
        }
    }

    /** Mark ALL networks unavailable (total overlay loss); rules left in place. */
    public synchronized void overlayDown(String reason) {
        String r = reason == null || reason.isBlank() ? "overlay-down" : reason;
        current = new EgressReadiness(EgressReadiness.State.UNAVAILABLE, r, "");
    }

    /**
     * Re-asserts the boot-time baseline after a bootstrap that skipped overlay bring-up entirely
     * (overlay disabled, or no data plane on this host). {@link #overlayDown} is otherwise reversed
     * only by {@code networkUp} — a path such hosts never take — so a down latch raised by an earlier
     * bootstrap would outlive the condition that raised it.
     */
    public synchronized void reassertBaselineWithoutOverlay() {
        EgressReadiness baseline = initialReadiness();
        if (!baseline.equals(current)) {
            log.info("sandbox.egress: no overlay bring-up on this host — readiness re-asserted "
                    + "from {}({}) to baseline {}({})",
                    current.state(), current.reason(), baseline.state(), baseline.reason());
        }
        current = baseline;
    }

    /**
     * Rootless Podman recreates the sandbox namespace EMPTY after its last container exits or a
     * machine restarts, and the rules went with the old one. Checks one marker rule and, when it is
     * gone, refuses new networked starts (UNAVAILABLE) until the whole set is back.
     */
    @Scheduled(fixedDelayString = "${sqs.sandbox.netns-egress.verify-ms:15000}",
            initialDelayString = "${sqs.sandbox.netns-egress.verify-ms:15000}")
    public void verifyNetnsEgress() {
        NetworkRequest req = lastRequest;
        if (netnsLocator == null || req == null || mode == EgressMode.OFF) {
            return;
        }
        EgressReadiness now = current;
        boolean enforced = now.state() == EgressReadiness.State.ENFORCED;
        // Only this loop's own outcomes are re-driven: a latch set elsewhere (overlay loss) is
        // for its owner to lift.
        if (!enforced && !isNetnsRetryable(now)) {
            return;
        }
        if (!locateNetns()) {
            return;
        }
        PodmanNetnsTarget machine = netns;
        if (enforced) {
            if (machine.ensureAnchor() == null && machine.run(sentinelCommand()).ok()) {
                return;
            }
            log.warn("sandbox.egress: rules missing in {} (namespace recreated?) — "
                    + "re-applying; networked starts wait until they are back", machine.describe());
            current = new EgressReadiness(EgressReadiness.State.UNAVAILABLE, NETNS_RULES_MISSING, "");
        } else if (System.nanoTime() - lastNetnsRetryNanos < NETNS_RETRY_BACKOFF_NANOS) {
            return;
        }
        lastNetnsRetryNanos = System.nanoTime();
        synchronized (networkLock) {
            // Forget the old state so networkUp rebuilds the whole skeleton, not just a delta.
            networks.clear();
            defaultNetworkKey = null;
        }
        // The containers those grants belonged to died with the old namespace. Carrying their
        // rules over could hand a dead container's grants to a new one that reuses its address.
        markGrantChainStale();
        networkUp(req.key(), req.iface(), req.overlayCidr(), req.overlayScope(), req.pinnedTableId());
    }

    /** Forgets every tracked grant; the next full rebuild flushes the chain they were in. */
    private void markGrantChainStale() {
        synchronized (grantLock) {
            liveGrants.clear();
        }
        grantChainStale = true;
    }

    /** States {@link #verifyNetnsEgress} produced (or a missing machine) — the ones it may re-drive. */
    private static boolean isNetnsRetryable(EgressReadiness r) {
        String reason = r.reason() == null ? "" : r.reason();
        return !r.operatorOptOut()
                && (reason.startsWith(VM_GAP_PREFIX)
                || reason.startsWith("overlay-egress rules failed")
                || reason.equals(NETNS_RULES_MISSING));
    }

    /** The FORWARD jump of the first sandbox subnet; the chain itself when none was discovered. */
    private List<String> sentinelCommand() {
        List<SandboxNet> nets = lastNets;
        if (nets == null || nets.isEmpty()) {
            return List.of("iptables", "-n", "-L", OverlayEgressRules.CHAIN_FWD);
        }
        return List.of("iptables", "-C", "FORWARD", "-s", nets.get(0).subnetCidr(),
                "-j", OverlayEgressRules.CHAIN_FWD);
    }

    /** The network NEW containers are assigned to (null when no network is enrolled). */
    public String defaultNetworkKey() {
        synchronized (networkLock) {
            return defaultNetworkKey;
        }
    }

    /**
     * Makes {@code key} the network NEW containers are assigned to.
     */
    public void setDefaultNetwork(String key) {
        synchronized (networkLock) {
            if (key == null || !networks.containsKey(key)) {
                log.warn("sandbox.egress: refusing to make unknown network {} the default", key);
                return;
            }
            defaultNetworkKey = key;
            refreshCurrentReadiness();
            log.info("sandbox.egress: new containers now assigned to network {} (table {})",
                    key, networks.get(key).tableId());
        }
    }

    /** Read-only snapshot of the enrolled networks (drain status surface). */
    public List<NetworkView> networks() {
        synchronized (networkLock) {
            return networks.values().stream()
                    .map(n -> new NetworkView(n.key(), n.iface(), n.overlayIp(), n.overlayScope(),
                            n.tableId(), n.enforced()))
                    .toList();
        }
    }

    /** Registers the drain manager's occupancy nudge; a null listener restores the no-op. */
    public void setContainerReleaseListener(Consumer<String> listener) {
        this.containerReleaseListener = listener == null ? ip -> {
        } : listener;
    }

    // ============================================================================================
    // Per-container network assignment (which tunnel does THIS container ride)
    // ============================================================================================.

    /**
     * Assigns one container its network at start: the current default network. Outside a drain the
     * subnet-wide rules already steer everything and this records the assignment only; during a drain
     * the container is additionally PINNED to the new network's table with a {@code /32} rule
     * (del-before-add, so a reused bridge IP never inherits a stale pin).
     */
    public String assignContainerNetwork(String containerIp) {
        String key;
        synchronized (networkLock) {
            key = defaultNetworkKey;
        }
        return assignContainerNetworkTo(containerIp, key);
    }

    /**
     * As {@link #assignContainerNetwork} but to an EXPLICIT network — the path a surviving stateful
     * instance takes after a mid-drain daemon restart (its container label says which network it
     * belongs to, which may be the OLD one), and the drain successor's start.
     */
    public String assignContainerNetworkTo(String containerIp, String key) {
        NetworkState net;
        synchronized (networkLock) {
            net = key == null ? null : networks.get(key);
            if (net == null || !net.enforced()) {
                return null;
            }
            // Host default route — bookkeeping only.
            assignedNetworks.put(containerIp, new AssignedNetwork(containerIp, key));
            return key;
        }
    }

    /** Releases a container's network bookkeeping at teardown and notifies the release listener. */
    public void releaseContainerNetwork(String containerIp) {
        synchronized (networkLock) {
            assignedNetworks.remove(containerIp);
        }
        containerReleaseListener.accept(containerIp);
    }

    /** The network one live container rides (null = unassigned / default). */
    public String networkOfContainer(String containerIp) {
        synchronized (networkLock) {
            AssignedNetwork a = assignedNetworks.get(containerIp);
            return a == null ? null : a.networkKey();
        }
    }

    // ============================================================================================
    // Network Access Control — per-container overlay grants (caller side)
    // ============================================================================================

    /**
     * Dispatch hot-path gate: whether a container starting now on the default network would get
     * at least one overlay grant. An engine without outbound Network Access Control rules
     * answers from one volatile read, so its containers pay nothing — no bridge-IP lookup, no
     * rule programming.
     */
    public boolean holdsOutboundGrants() {
        if (networkRuleStore == null || peerDirectory == null
                || networkRuleStore.outboundSnapshot().isEmpty()) {
            return false;
        }
        NetworkState net;
        synchronized (networkLock) {
            net = enforcedNetwork(defaultNetworkKey);
        }
        return net != null && !resolveGrants(net.key()).isEmpty();
    }

    /**
     * Programs this engine's outbound Network Access Control grants for one container into {@link
     * OverlayEgressRules#CHAIN_PERBOX}: one ACCEPT per {@code (peer /32, port)} that resolves through
     * the peer directory on the container's own network. Idempotent (check-then-insert), so a warm
     * slot re-dispatched with the same grants never stacks duplicate rules.
     */
    public boolean applyContainerGrants(String containerIp) {
        NetworkState net = networkForContainer(containerIp);
        if (net == null) {
            return true;
        }
        List<List<String>> desired = ContainerOverlayGrants.rulesFor(
                containerIp, net.iface(), resolveGrants(net.key()));
        synchronized (grantLock) {
            List<List<String>> applied = new ArrayList<>(desired.size());
            for (List<String> rule : desired) {
                List<String> check = new ArrayList<>(rule);
                check.set(1, "-C");
                if (runElevated(check).ok()) {
                    applied.add(rule);
                    continue;
                }
                ExecResult r = runElevated(rule);
                if (!r.ok() && !toleratedFailure(rule, r)) {
                    log.warn("sandbox.egress: overlay grant apply failed for {}: {}",
                            containerIp, firstLine(r.stderr()));
                    // Track what DID land so teardown removes exactly those, never a recompute.
                    liveGrants.put(containerIp, new LiveGrants(net.key(), applied));
                    return false;
                }
                applied.add(rule);
            }
            liveGrants.put(containerIp, new LiveGrants(net.key(), applied));
        }
        return true;
    }

    /**
     * Removes one container's grant rules at teardown, before its slot is released. Best-effort: an
     * absent rule is the desired end state, so a miss is not an error.
     */
    public void clearContainerGrants(String containerIp) {
        LiveGrants live;
        synchronized (grantLock) {
            live = liveGrants.remove(containerIp);
        }
        if (live == null) {
            return;
        }
        for (List<String> rule : live.appliedRules()) {
            List<String> del = new ArrayList<>(rule);
            del.set(1, "-D");
            ExecResult r = runElevated(del);
            if (!r.ok()) {
                log.debug("sandbox.egress: overlay grant clear miss for {}: {}",
                        containerIp, firstLine(r.stderr()));
            }
        }
    }

    /**
     * The address-churn path. A granted engine that no longer resolves simply drops out of the desired
     * set (fail closed; its old rule is removed).
     */
    public void reprogramLiveGrants() {
        synchronized (grantLock) {
            if (liveGrants.isEmpty()) {
                return;
            }
            for (Map.Entry<String, LiveGrants> e : liveGrants.entrySet()) {
                LiveGrants live = e.getValue();
                NetworkState net = enforcedNetwork(live.networkKey());
                if (net == null) {
                    // The container's network is gone (finalized under it should not happen — the
                    // drain waits for empty — but fail closed rather than recompute against a
                    // network it is not on).
                    for (List<String> rule : live.appliedRules()) {
                        List<String> del = new ArrayList<>(rule);
                        del.set(1, "-D");
                        runTolerated(del);
                    }
                    e.setValue(new LiveGrants(live.networkKey(), List.of()));
                    continue;
                }
                List<List<String>> desired = ContainerOverlayGrants.rulesFor(
                        e.getKey(), net.iface(), resolveGrants(net.key()));
                List<List<String>> applied = live.appliedRules();
                int added = 0;
                int removed = 0;
                for (List<String> rule : desired) {
                    if (applied.contains(rule)) {
                        continue;
                    }
                    List<String> check = new ArrayList<>(rule);
                    check.set(1, "-C");
                    if (!runElevated(check).ok()) {
                        ExecResult r = runElevated(rule);
                        if (!r.ok() && !toleratedFailure(rule, r)) {
                            log.warn("sandbox.egress: grant reprogram insert failed for {}: {}",
                                    e.getKey(), firstLine(r.stderr()));
                            continue;
                        }
                    }
                    added++;
                }
                for (List<String> rule : applied) {
                    if (desired.contains(rule)) {
                        continue;
                    }
                    List<String> del = new ArrayList<>(rule);
                    del.set(1, "-D");
                    ExecResult r = runElevated(del);
                    if (!r.ok()) {
                        log.debug("sandbox.egress: grant reprogram remove miss for {}: {}",
                                e.getKey(), firstLine(r.stderr()));
                    }
                    removed++;
                    // Residual: the ACCEPT is gone, but the chain's ESTABLISHED,RELATED rule would keep an
                    // already-open connection alive until it ends — a revoke must kill the live flow too (mirrors the
                    // gateway's conntrack delete).
                    killEstablishedFlowForRemovedGrant(rule);
                }
                if (added > 0 || removed > 0) {
                    log.info("sandbox.egress: reprogrammed {} (+{} -{} rule(s)) after peer/rule change",
                            e.getKey(), added, removed);
                }
                e.setValue(new LiveGrants(live.networkKey(), desired));
            }
        }
    }

    // ============================================================================================
    // internals
    // ============================================================================================

    /** The network a container rides: its recorded assignment, else the current default. */
    private NetworkState networkForContainer(String containerIp) {
        synchronized (networkLock) {
            AssignedNetwork assigned = assignedNetworks.get(containerIp);
            String key = assigned != null ? assigned.networkKey() : defaultNetworkKey;
            NetworkState net = key == null ? null : networks.get(key);
            return net != null && net.enforced() ? net : null;
        }
    }

    /** {@code key}'s network when it is enrolled and enforced, else null. */
    private NetworkState enforcedNetwork(String key) {
        synchronized (networkLock) {
            NetworkState net = key == null ? null : networks.get(key);
            return net != null && net.enforced() ? net : null;
        }
    }

    /**
     * Resolves the outbound projection to dialable grants through the same-owner peer directory ON THE
     * CONTAINER'S NETWORK. Fail closed by construction: an id the directory cannot resolve there
     * yields NO grant, so the per-container chain only ever names a current, reachable, same-owner
     * address the container's own tunnel actually routes to.
     */
    private List<ContainerOverlayGrants.OverlayGrant> resolveGrants(String networkKey) {
        if (networkRuleStore == null || peerDirectory == null) {
            return List.of();
        }
        List<ContainerOverlayGrants.OverlayGrant> out = new ArrayList<>();
        for (NetworkRuleStore.OutboundRule r : networkRuleStore.outboundSnapshot()) {
            peerDirectory.addressOn(networkKey, r.targetEnvId()).ifPresent(ip -> {
                // A grant becomes `-d IP/32 ACCEPT` on the host; a sibling address that is not an overlay host
                // address cannot be a same-owner peer, so no rule.
                if (!OverlayAddresses.isOverlayAddress(ip)) {
                    log.warn("sandbox.egress.grant.refused env={} ip={} reason=outside-overlay",
                            r.targetEnvId(), ip);
                    return;
                }
                out.add(new ContainerOverlayGrants.OverlayGrant(ip, r.port()));
            });
        }
        return out;
    }

    /**
     * Residual — deletes the conntrack entry for a per-container grant rule that was just removed, so
     * a revoked (or address-churned) grant closes the LIVE connection instead of letting it run out
     * under the chain's ESTABLISHED,RELATED accept. Only a grant ACCEPT with a {@code --dport} names a
     * flow to kill.
     */
    private void killEstablishedFlowForRemovedGrant(List<String> ruleArgv) {
        List<String> bare = stripSudo(ruleArgv);
        if (!bare.contains("ACCEPT") || !bare.contains("--dport")) {
            return;
        }
        String src = argAfter(bare, "-s");
        String dst = argAfter(bare, "-d");
        String dport = argAfter(bare, "--dport");
        if (src == null || dst == null || dport == null) {
            return;
        }
        int slash = dst.indexOf('/');
        String dstIp = slash >= 0 ? dst.substring(0, slash) : dst;
        ExecResult r = runElevated(List.of(
                "conntrack", "-D", "-s", src, "-d", dstIp, "-p", "tcp", "--dport", dport));
        if (!r.ok() && conntrackWarned.compareAndSet(false, true)) {
            String err = firstLine(r.stderr()).toLowerCase(Locale.ROOT);
            if (err.contains("not found") || err.contains("no such file") || err.contains("command")) {
                log.warn("sandbox.egress: conntrack(8) unavailable — revoked grants close new "
                        + "packets but live flows persist until they end (install conntrack-tools "
                        + "and cover it in /etc/sudoers.d/studioquack-wg)");
            }
            // Other failures (e.g. "no such flow") are the desired end state — stay quiet.
        }
    }

    /** The value following {@code flag} in {@code argv}, or null when absent. */
    private static String argAfter(List<String> argv, String flag) {
        int i = argv.indexOf(flag);
        return (i >= 0 && i + 1 < argv.size()) ? argv.get(i + 1) : null;
    }

    /** Applies one argv list; returns the first hard failure's stderr line, or null on success. */
    private String applyAll(List<List<String>> commands) {
        PodmanNetnsTarget machine = netns;
        if (machine != null) {
            // One SSH round trip per rule set; per-command it would cost seconds per apply.
            return machine.applyAll(commands);
        }
        for (int i = 0; i < commands.size(); i++) {
            List<String> cmd = commands.get(i);
            ExecResult r = runElevated(cmd);
            // check-then-insert: success means the jump already exists — skip the paired insert.
            if (isCheck(cmd)) {
                if (r.ok()) {
                    i++;
                }
                continue;
            }
            if (!r.ok() && !toleratedFailure(cmd, r)) {
                return firstLine(r.stderr());
            }
        }
        return null;
    }

    /** Runs a command tolerating every failure (exact removals, tolerate-absent dels). */
    private void runTolerated(List<String> cmd) {
        ExecResult r = runElevated(cmd);
        if (!r.ok()) {
            log.debug("sandbox.egress: tolerated failure {}: {}", cmd, firstLine(r.stderr()));
        }
    }

    private void markNetworkUnavailable(String key, String reason) {
        synchronized (networkLock) {
            networks.remove(key);
            refreshCurrentReadiness(reason);
        }
    }

    /** Recomputes the hot-path readiness from the DEFAULT network's state. */
    private void refreshCurrentReadiness() {
        refreshCurrentReadiness(null);
    }

    private void refreshCurrentReadiness(String downReason) {
        NetworkState def = defaultNetworkKey == null ? null : networks.get(defaultNetworkKey);
        if (def != null && def.enforced()) {
            PodmanNetnsTarget machine = netns;
            current = new EgressReadiness(EgressReadiness.State.ENFORCED,
                    machine != null ? "overlay-egress enforced inside " + machine.describe()
                            : "overlay-egress enforced via " + def.iface(),
                    def.overlayIp());
        } else {
            current = new EgressReadiness(EgressReadiness.State.UNAVAILABLE,
                    downReason == null ? "overlay-not-up-yet" : downReason, "");
        }
    }

    private EgressReadiness initialReadiness() {
        if (mode == EgressMode.OFF) {
            return optedOutReadiness();
        }
        // A macOS / Windows host is located at overlay bring-up, not at boot: until then it is
        // pending like a Linux host, not yet unsupported.
        if (!platform().ok() && netnsLocator == null) {
            return unsupportedReadiness();
        }
        if (!channelEnabled || !netEnabled) {
            return notConfiguredReadiness();
        }
        return new EgressReadiness(EgressReadiness.State.UNAVAILABLE, "overlay-not-up-yet", "");
    }

    /**
     * The only readiness that lets a networked container start without enforcement: {@code
     * overlay-egress=off} is an explicit operator decision, so it carries the opt-out bit. AUTO on an
     * unsupported / unconfigured host is DEGRADED without the bit and fails closed.
     */
    private static EgressReadiness optedOutReadiness() {
        return new EgressReadiness(EgressReadiness.State.DEGRADED, "overlay-egress disabled", "", true);
    }

    private EgressReadiness unsupportedReadiness() {
        Platform p = platform();
        String gap = p.gapReason() == null || p.gapReason().isBlank()
                ? "unsupported platform" : p.gapReason();
        if (mode == EgressMode.ENFORCED) {
            return new EgressReadiness(EgressReadiness.State.UNAVAILABLE, gap, "");
        }
        return new EgressReadiness(EgressReadiness.State.DEGRADED, gap, "");
    }

    private EgressReadiness notConfiguredReadiness() {
        if (mode == EgressMode.ENFORCED) {
            return new EgressReadiness(EgressReadiness.State.UNAVAILABLE,
                    "overlay not configured", "");
        }
        return new EgressReadiness(EgressReadiness.State.DEGRADED, "overlay not configured", "");
    }

    private ExecResult runElevated(List<String> cmd) {
        PodmanNetnsTarget machine = netns;
        if (machine != null) {
            return machine.run(cmd);
        }
        List<String> argv = elevate ? elevated(cmd) : cmd;
        return exec.run(argv, Duration.ofSeconds(15));
    }

    private static List<String> elevated(List<String> cmd) {
        List<String> out = new ArrayList<>(cmd.size() + 2);
        out.add("sudo");
        out.add("-n");
        out.addAll(cmd);
        return out;
    }

    /**
     * Tolerate absent-rule checks ({@code -C} miss), chain-exists on {@code -N}, and
     * {@code ip rule del} misses — same posture as other idempotent overlay-rule markers.
     */
    private static boolean isCheck(List<String> argv) {
        return stripSudo(argv).contains("-C");
    }

    static boolean toleratedFailure(List<String> argv, ExecResult r) {
        if (r.ok()) {
            return true;
        }
        List<String> bare = stripSudo(argv);
        String err = r.stderr().toLowerCase(Locale.ROOT);
        if (bare.contains("-N") && (err.contains("exist") || err.contains("file exists"))) {
            return true;
        }
        if (bare.contains("rule") && bare.contains("del")) {
            return true;
        }
        return err.contains("exist") || err.contains("already");
    }

    private static List<String> stripSudo(List<String> argv) {
        if (argv.size() >= 2 && "sudo".equals(argv.get(0))) {
            return argv.subList(2, argv.size());
        }
        return argv;
    }

    private static String hostOfCidr(String cidr) {
        if (cidr == null || cidr.isBlank()) {
            return "";
        }
        int slash = cidr.indexOf('/');
        return (slash < 0 ? cidr : cidr.substring(0, slash)).trim();
    }

    private static String firstLine(String stderr) {
        if (stderr == null || stderr.isBlank()) {
            return "unknown";
        }
        int nl = stderr.indexOf('\n');
        return (nl < 0 ? stderr : stderr.substring(0, nl)).trim();
    }

    /** The host platform, or supported once the sandbox namespace was found (see netnsLocator). */
    private Platform platform() {
        return netnsLocator != null && netns != null ? Platform.supported() : platform;
    }

    /** (Re)finds the sandbox namespace when it is not the host's own. True when it is available. */
    private boolean locateNetns() {
        if (netns != null) {
            return true;
        }
        PodmanNetnsTarget found = netnsLocator.get();
        if (found != null) {
            netns = found;
            log.info("sandbox.egress: sandbox bridges live in {} — egress rules are applied inside it",
                    found.describe());
        }
        return found != null;
    }

    private static Platform vmGap(String osName) {
        return Platform.unsupported(VM_GAP_PREFIX + " (host os=" + osFamily(osName) + ")");
    }

    /**
     * Platform support = Linux + iptables + iproute2 + podman. Not "kernel WireGuard".
     */
    private static Platform detectPlatform(String cliPath) {
        String osName = System.getProperty("os.name", "");
        if (!isLinux(osName)) {
            return Platform.unsupported(
                    "overlay-egress requires native Linux + iptables/iproute2 + podman (host os="
                            + osFamily(osName) + ")");
        }
        if (!commandPresent("iptables") || !commandPresent("ip")) {
            return Platform.unsupported("overlay-egress requires iptables and iproute2");
        }
        if (cliPath == null || cliPath.isBlank()) {
            return Platform.unsupported("overlay-egress requires a resolvable podman CLI");
        }
        return Platform.supported();
    }

    private static boolean isLinux(String osName) {
        String n = osName == null ? "" : osName.toLowerCase(Locale.ROOT);
        return n.contains("nux") || n.contains("nix") || n.contains("aix");
    }

    private static String osFamily(String osName) {
        String n = osName == null ? "" : osName.toLowerCase(Locale.ROOT);
        if (n.contains("win")) {
            return "windows";
        }
        if (n.contains("mac") || n.contains("darwin")) {
            return "macos";
        }
        if (isLinux(osName)) {
            return "linux";
        }
        return "other";
    }

    /** {@code name} is a constant we control (iptables / ip), never user input. */
    private static boolean commandPresent(String name) {
        try {
            CliRunner.Result r = CliRunner.run(
                    List.of("sh", "-c", "command -v " + name), Duration.ofSeconds(2));
            return r.ok() && r.stdout() != null && !r.stdout().isBlank();
        } catch (Exception e) {
            return false;
        }
    }

    private static String resolveCli(ObjectProvider<ContainerRuntime> runtimeProvider) {
        if (runtimeProvider == null) {
            return null;
        }
        ContainerRuntime rt = runtimeProvider.getIfAvailable();
        return rt == null ? null : rt.cli();
    }

    private static boolean needsElevate() {
        try {
            CliRunner.Result r = CliRunner.run(List.of("id", "-u"), Duration.ofSeconds(2));
            return !(r.ok() && "0".equals(r.stdout() == null ? "" : r.stdout().trim()));
        } catch (Exception e) {
            return true;
        }
    }

    private static CommandExec defaultExec() {
        return (argv, timeout) -> {
            CliRunner.Result r = CliRunner.run(argv, timeout == null ? Duration.ofSeconds(15) : timeout);
            return new ExecResult(r.ok(), r.exitCode(), r.stderr());
        };
    }

    private static List<SandboxNet> discoverNetworks(String cli) {
        SandboxNetworkEnsure.Exec runner = (argv, timeout) -> {
            CliRunner.Result r = CliRunner.run(argv, timeout);
            return new SandboxNetworkEnsure.ExecResult(r.ok(), r.exitCode(), r.stdout(), r.stderr());
        };
        return discoverNetworks(cli, runner);
    }

    static List<SandboxNet> discoverNetworks(String cli, SandboxNetworkEnsure.Exec runner) {
        List<SandboxNet> out = new ArrayList<>(3);
        SandboxNetworkEnsure.SandboxNetworkInfo jobsInfo =
                SandboxNetworkEnsure.inspect(cli, SandboxNetworks.JOBS, runner);
        if (jobsInfo == null) {
            try {
                // Ensure sqs-jobs exists before PodmanNetnsTarget.ensureAnchor() attaches the anchor pod to it.
                SandboxNetworkEnsure.ensureWithoutCoverage(cli, SandboxNetworks.JOBS, runner);
                jobsInfo = SandboxNetworkEnsure.inspect(cli, SandboxNetworks.JOBS, runner);
            } catch (IllegalStateException ex) {
                log.debug("sandbox.egress: could not pre-ensure {} before discovery: {}",
                        SandboxNetworks.JOBS, ex.getMessage());
            }
        }
        if (jobsInfo != null && jobsInfo.subnetCidr() != null && jobsInfo.gatewayIp() != null) {
            out.add(new SandboxNet(jobsInfo.name(), jobsInfo.subnetCidr(), jobsInfo.gatewayIp()));
        }
        for (String name : List.of(SandboxNetworks.STATEFUL, SandboxNetworks.BUILD)) {
            SandboxNetworkEnsure.SandboxNetworkInfo info =
                    SandboxNetworkEnsure.inspect(cli, name, runner);
            if (info != null && info.subnetCidr() != null && info.gatewayIp() != null) {
                out.add(new SandboxNet(info.name(), info.subnetCidr(), info.gatewayIp()));
            }
        }
        return out;
    }
}
