package com.studioquack.sqs.channel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.security.envelope.DaemonAuthCredential;
import com.studioquack.sqs.channel.command.DaemonCommandRegistry;
import com.studioquack.sqs.channel.config.HttpsEndpointPolicy;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.channel.net.HubAddress;
import com.studioquack.sqs.channel.net.HubList;
import com.studioquack.sqs.channel.net.ListenerKeyStore;
import com.studioquack.sqs.channel.net.MeshPeerHttp;
import com.studioquack.sqs.channel.net.MeshTls;
import com.studioquack.sqs.channel.net.NetAgentClient;
import com.studioquack.sqs.channel.net.NetAgentStatus;
import com.studioquack.sqs.channel.net.NetHandshakeClient;
import com.studioquack.sqs.channel.net.NetHandshakeException;
import com.studioquack.sqs.channel.net.NetHandshakeResult;
import com.studioquack.sqs.channel.net.NetStatusWatcher;
import com.studioquack.sqs.channel.net.NetTelemetry;
import com.studioquack.sqs.channel.net.NetWatchdog;
import com.studioquack.sqs.channel.net.OverlayAddressChangedEvent;
import com.studioquack.sqs.channel.overlay.OverlayPeerListener;
import com.studioquack.sqs.channel.overlay.OverlayReverseListener;
import com.studioquack.sqs.channel.wg.MeshPeerSetChangedEvent;
import com.studioquack.sqs.channel.wg.WindowsOverlayFirewall;
import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import com.studioquack.sqs.maintenance.HostServiceUninstaller;
import com.studioquack.sqs.maintenance.ServiceManagerDetector;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperExecuteService;
import com.studioquack.sqs.sandbox.stateful.OciImageBlobIngestService;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.secrets.DaemonFileCustody;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.nio.file.Path;

/**
 * Orchestrates the secure channel once {@code sqs.channel.enabled=true}: enroll → derive the envelope IKM →
 * (optionally) handshake v2 + {@code sq-net up} → start the heartbeat + exec-channel loops. Runs on
 * {@link ApplicationReadyEvent} on a background thread so a slow / unreachable BE never blocks app startup.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class SqsChannelBootstrap {

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final MtlsIdentity identity;
    private final ChannelHttpClients httpClients;
    private final DaemonCommandRegistry registry;
    private final ObjectMapper mapper;
    /**
     * Optional: absent only if the quota bean failed to assemble. A null meter admits every start.
     */
    @Autowired(required = false)
    private com.studioquack.sqs.engine.quota.NodeExecutionMeter nodeExecutionMeter;
    /**
     * Optional: absent in bootstrap slice tests. A null guard never halts.
     */
    @Autowired(required = false)
    private com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;
    private final com.studioquack.sqs.channel.wg.OverlayPeerDirectory overlayPeerDirectory;
    /**
     * Null when the host has no {@code sq-net} CLI — {@code no-data-plane}.
     */
    private final NetAgentClient agent;
    private final ObjectProvider<HeartbeatResourceProvider> resourceProvider;
    private final ObjectProvider<DispatchExecutor> dispatchExecutor;
    private final ObjectProvider<OciImageBlobIngestService> ociIngest;
    private final OutboundBaseHolder outboundBaseHolder;
    private final ObjectProvider<NetWatchdog> watchdog;
    private final ObjectProvider<DispatchExecutionRegistry> dispatchRegistry;
    private final SandboxEgressManager egressManager;
    private final com.studioquack.sqs.sandbox.reverse.NetworkRuleStore networkRuleStore;
    private final com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay reverseRelay;
    private final StatefulInstanceManager statefulInstanceManager;
    private final TamperEventReporter tamperEventReporter;
    private final ObjectProvider<HostServiceUninstaller> hostServiceUninstaller;
    private final ObjectProvider<ServiceManagerDetector> serviceManagerDetector;
    private final ObjectProvider<DaemonLifecycleService.Exiter> daemonExiter;
    private final int overlayExecutePort;
    private final ObjectProvider<HelperExecuteService> helperExecuteService;
    private final com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity;
    private final com.studioquack.sqs.channel.wg.GrantedPeerDirectory grantedPeerDirectory;
    private final com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore crossEnvGrantStore;
    private final ObjectProvider<com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient>
            crossEnvAuthorizationClient;
    private final com.studioquack.sqs.aimodel.AiModelProviderProxy aiModelProviderProxy;
    private final HubList hubList;
    private final MeshPeerHttp meshPeerHttp;
    private final NetTelemetry telemetry;
    private final NetStatusWatcher statusWatcher;
    private final ObjectProvider<DaemonFileCustody> fileCustody;

    /**
     * Kicked on egress-up so a build that failed the gate retries immediately (nullable).
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private ObjectProvider<com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager> builtinNodeLibrary;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private ObjectProvider<ChannelKemIkm> channelKemIkm;

    private volatile HeartbeatClient heartbeatClient;
    private volatile ExecChannelClient execChannelClient;
    private volatile NetHandshakeClient handshakeClient;
    private volatile ListenerKeyStore listenerKeys;
    private volatile NetHandshakeResult lastHandshake;
    private volatile OverlayReverseListener overlayListener;
    private volatile OverlayPeerListener overlayPeerListener;
    private volatile MtlsRotationLoop mtlsRotationLoop;
    private volatile EnvelopeClient envelopeClient;
    private volatile BackendBaseResolver resolver;
    /**
     * Debounced re-handshake after {@link MeshPeerSetChangedEvent}: the access-rule bridge pushes one command per
     * sibling, and one handshake resyncs grants.
     */
    private volatile java.util.concurrent.ScheduledExecutorService meshRefresh;
    private java.util.concurrent.ScheduledFuture<?> meshRefreshPending;
    private final Object meshRefreshLock = new Object();
    private static final long MESH_REFRESH_DEBOUNCE_MS = 2_000L;
    /**
     * Minimum interval between on-demand mesh identity refreshes (401 reactive path).
     */
    private static final long MESH_REFRESH_FLOOR_NANOS = 5_000_000_000L;
    private volatile long lastMeshRefreshNanos;
    private final java.util.concurrent.atomic.AtomicBoolean bootstrapping =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile Thread bootstrapThread;
    private volatile boolean abortBootstrap;
    /**
     * Last handshake / bring-up failure surfaced by {@link #netLastError()}.
     */
    private volatile String lastBringUpError;
    /**
     * Present on a full daemon. Offline bootstrap tests leave it null.
     */
    private com.studioquack.sqs.engine.update.WorkIntakeGate workIntakeGate;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setWorkIntakeGate(com.studioquack.sqs.engine.update.WorkIntakeGate workIntakeGate) {
        this.workIntakeGate = workIntakeGate;
    }

    /**
     * Enrolment outcome for {@code /ops/health}; offline bootstrap tests use the local default.
     */
    private EnrollmentStatus enrollmentStatus = new EnrollmentStatus();

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setEnrollmentStatus(EnrollmentStatus enrollmentStatus) {
        this.enrollmentStatus = enrollmentStatus;
    }

    public SqsChannelBootstrap(SqsChannelProperties props,
                               @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                               MtlsIdentity channelMtlsIdentity,
                               ChannelHttpClients httpClients,
                               DaemonCommandRegistry registry,
                               ObjectMapper mapper,
                               ObjectProvider<NetAgentClient> netAgent,
                               ObjectProvider<HeartbeatResourceProvider> resourceProvider,
                               ObjectProvider<DispatchExecutor> dispatchExecutor,
                               ObjectProvider<OciImageBlobIngestService> ociIngest,
                               OutboundBaseHolder outboundBaseHolder,
                               ObjectProvider<NetWatchdog> watchdog,
                               com.studioquack.sqs.channel.wg.OverlayPeerDirectory overlayPeerDirectory,
                               ObjectProvider<DispatchExecutionRegistry> dispatchRegistry,
                               ObjectProvider<SandboxEgressManager> egressManager,
                               ObjectProvider<com.studioquack.sqs.sandbox.reverse.NetworkRuleStore> networkRuleStore,
                               ObjectProvider<com.studioquack.sqs.sandbox.reverse.OverlayReversePortRelay> reverseRelay,
                               ObjectProvider<StatefulInstanceManager> statefulInstanceManager,
                               TamperEventReporter tamperEventReporter,
                               ObjectProvider<HostServiceUninstaller> hostServiceUninstaller,
                               ObjectProvider<ServiceManagerDetector> serviceManagerDetector,
                               ObjectProvider<DaemonLifecycleService.Exiter> daemonExiter,
                               @Value("${sqs.overlay.execute-port:8443}") int overlayExecutePort,
                               ObjectProvider<HelperExecuteService> helperExecuteService,
                               com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity,
                               com.studioquack.sqs.channel.wg.GrantedPeerDirectory grantedPeerDirectory,
                               com.studioquack.sqs.channel.wg.mesh.CrossEnvGrantStore crossEnvGrantStore,
                               ObjectProvider<com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient>
                                       crossEnvAuthorizationClient,
                               com.studioquack.sqs.aimodel.AiModelProviderProxy aiModelProviderProxy,
                               HubList hubList,
                               MeshPeerHttp meshPeerHttp,
                               NetTelemetry telemetry,
                               NetStatusWatcher statusWatcher,
                               ObjectProvider<DaemonFileCustody> fileCustody) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.identity = channelMtlsIdentity;
        this.httpClients = httpClients;
        this.registry = registry;
        this.mapper = mapper;
        this.agent = netAgent == null ? null : netAgent.getIfAvailable();
        this.resourceProvider = resourceProvider;
        this.dispatchExecutor = dispatchExecutor;
        this.ociIngest = ociIngest;
        this.outboundBaseHolder = outboundBaseHolder;
        this.watchdog = watchdog;
        this.overlayPeerDirectory = overlayPeerDirectory;
        this.dispatchRegistry = dispatchRegistry;
        this.egressManager = egressManager == null ? null : egressManager.getIfAvailable();
        this.networkRuleStore = networkRuleStore == null ? null : networkRuleStore.getIfAvailable();
        this.reverseRelay = reverseRelay == null ? null : reverseRelay.getIfAvailable();
        this.statefulInstanceManager = statefulInstanceManager == null
                ? null : statefulInstanceManager.getIfAvailable();
        this.tamperEventReporter = tamperEventReporter;
        this.hostServiceUninstaller = hostServiceUninstaller;
        this.serviceManagerDetector = serviceManagerDetector;
        this.daemonExiter = daemonExiter;
        this.overlayExecutePort = overlayExecutePort;
        this.helperExecuteService = helperExecuteService;
        this.meshIdentity = meshIdentity;
        this.grantedPeerDirectory = grantedPeerDirectory;
        this.crossEnvGrantStore = crossEnvGrantStore;
        this.crossEnvAuthorizationClient = crossEnvAuthorizationClient;
        this.aiModelProviderProxy = aiModelProviderProxy;
        this.hubList = hubList;
        this.meshPeerHttp = meshPeerHttp;
        this.telemetry = telemetry;
        this.statusWatcher = statusWatcher;
        this.fileCustody = fileCustody;
    }

    /**
     * Kicks off enrolment + loop startup on a background thread when the app is ready.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        startBootstrapIfPermitted("startup");
    }

    /**
     * Starts the channel unless enrolment config is missing or the backend base is not HTTPS.
     */
    private void startBootstrapIfPermitted(String trigger) {
        if (props.getEnvId() == null || props.getEnvId().isBlank()
                || props.getBearer() == null || props.getBearer().isBlank()) {
            log.warn("channel: enabled but envId/bearer not configured — channel idle "
                    + "(set sqs.channel.env-id + sqs.channel.bearer)");
            return;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.warn("channel: enabled but sqs.backend.base-url not set — channel idle");
            return;
        }
        String httpsRefusal = HttpsEndpointPolicy.refusal(backendBaseUrl);
        if (httpsRefusal != null) {
            log.error("channel: NOT started ({}) — {}. Refusing to enrol.", trigger, httpsRefusal);
            return;
        }
        if (!bootstrapping.compareAndSet(false, true)) {
            log.info("channel: bootstrap already in progress — ignoring {}", trigger);
            return;
        }
        abortBootstrap = false;
        Thread t = new Thread(() -> {
            try {
                bootstrapWithBoundedRetry();
            } finally {
                bootstrapping.set(false);
            }
        }, "sqs-channel-bootstrap");
        this.bootstrapThread = t;
        t.setDaemon(true);
        t.start();
    }

    /**
     * Runs one best-effort step, logging rather than propagating so the next step still runs.
     */
    private void quietly(String what, Runnable step) {
        try {
            step.run();
        } catch (Exception ex) {
            log.warn("channel: {} failed ({})", what, ex.toString());
        }
    }

    private void bootstrap() throws Exception {
        String environmentBase = props.environmentBase(backendBaseUrl);
        log.info("channel: bootstrapping env={} base={}", props.getEnvId(), environmentBase);
        // Public base first; a confirmed agent Running + hub HTTPS probe flips to overlay.
        BackendBaseResolver resolver =
                new BackendBaseResolver(environmentBase, props.getNet().getOutboundFailureThreshold());
        this.resolver = resolver;
        outboundBaseHolder.bind(resolver);
        KemIkmStore kemStore = KemIkmStore.under(Path.of(props.getDataDir()), kemIkmCustody());
        byte[] persistedIkm = kemStore.load().orElse(null);
        if (persistedIkm != null) {
            props.installChannelCredential(
                    DaemonAuthCredential.fromKemIkm(persistedIkm, props.getEnvId()));
            log.info("channel: loaded persisted kemIkm; channel auth=kemIkm-proof env={}",
                    props.getEnvId());
        }

        EnrollmentClient enrollment = new EnrollmentClient(
                environmentBase, props.getEnvId(), props.getBearer(), identity, httpClients, mapper);
        EnrollmentClient.Result result = enrollment.enroll(
                new EnrollmentClient.Params(null, null, null), props.getAgentVersion(), persistedIkm);
        log.info("channel: enrolled status={} hybridIkm={} mtls={}",
                result.status(), result.hybridIkm(), result.mtlsEnrolled());

        if (result.hybridIkm() && result.ikm() != null) {
            if (persistedIkm == null) {
                kemStore.save(result.ikm());
                log.info("channel: persisted kemIkm under {} env={}", kemStore.path(), props.getEnvId());
            }
            props.installChannelCredential(
                    DaemonAuthCredential.fromKemIkm(result.ikm(), props.getEnvId()));
        }

        this.envelopeClient = EnvelopeClient.withIkm(props.getEnvId(), result.ikm(), mapper);
        ChannelKemIkm kemHolder = channelKemIkm == null ? null : channelKemIkm.getIfAvailable();
        if (kemHolder != null && result.ikm() != null) {
            kemHolder.install(props.getEnvId(), result.ikm());
            log.info("channel: command MAC required from now on env={} ikmSource={}",
                    props.getEnvId(), result.hybridIkm() ? "hybrid" : "bearer-hash");
        }

        // Enroll + net/handshake always ride the public base (the tunnel does not exist yet).
        WgLivenessProvider liveness = startNetIfEnabled(environmentBase);
        maybeActivateOverlayOutbound(resolver);
        startHeartbeatIfEnabled(resolver, liveness);
        startExecChannelIfEnabled(resolver);
        startOverlayListenerIfEnabled();
        startOverlayPeerListenerIfEnabled();
        startNetWatchdog(resolver);
        startNetStatusWatcher();
        startMtlsRotationLoop(environmentBase);
        log.info("channel: bootstrap complete env={}", props.getEnvId());
    }

    /**
     * First-boot enroll retried for as long as the daemon runs: 60 s + jitter for the first
     * {@code sqs.channel.enroll-retry-minutes} (the fast phase an operator watching an install expects), then a
     * slow phase of {@link #ENROLL_SLOW_PHASE_INTERVAL_MS} + jitter forever. A daemon must never end in a gave-up
     * state that only a restart escapes: a BE outage longer than the cap (or a first boot into a down control
     * plane) recovers by itself the moment the BE is back.
     */
    private void bootstrapWithBoundedRetry() {
        long capMinutes = Math.max(0L, props.getEnrollRetryMinutes());
        long deadlineNanos = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(capMinutes);
        int attempt = 0;
        boolean slowPhaseLogged = false;
        enrollmentStatus.enrolling();
        while (!abortBootstrap) {
            attempt++;
            try {
                bootstrap();
                enrollmentStatus.enrolled();
                if (attempt > 1) {
                    log.info("channel: bootstrap succeeded env={} after {} attempt(s)",
                            props.getEnvId(), attempt);
                }
                return;
            } catch (Exception ex) {
                if (isEnvDeletedOnBackend(ex)) {
                    handleDeletedEngine();
                    return;
                }
                EnrollmentStatus.State previous = enrollmentStatus.state();
                EnrollmentStatus.State outcome = enrollmentStatus.failed(ex);
                if (outcome != previous) {
                    logTokenOutcome(outcome);
                }
                if (!isRetryableBootstrapFailure(ex)) {
                    log.error("channel: bootstrap failed env={} — control plane not started ({}). "
                            + "Fix the BE/config and restart.", props.getEnvId(), ex.toString());
                    return;
                }
                boolean fastPhase = enrollRetryBudgetRemaining(deadlineNanos, System.nanoTime());
                long delayMs = fastPhase ? enrollRetryDelayMs() : enrollSlowPhaseDelayMs();
                if (!fastPhase && !slowPhaseLogged) {
                    slowPhaseLogged = true;
                    log.warn("channel: bootstrap still failing env={} after {} attempt(s) over {} min — "
                                    + "switching to the slow retry phase (every {} min, forever). "
                                    + "The daemon recovers by itself when the BE is reachable again; "
                                    + "no restart is needed. Last error: {}",
                            props.getEnvId(), attempt, capMinutes,
                            java.util.concurrent.TimeUnit.MILLISECONDS.toMinutes(
                                    ENROLL_SLOW_PHASE_INTERVAL_MS), ex.toString());
                } else if (fastPhase) {
                    log.warn("channel: bootstrap attempt {} failed env={} — retrying in {}s ({})",
                            attempt, props.getEnvId(),
                            java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(delayMs), ex.toString());
                } else {
                    log.info("channel: bootstrap slow-phase attempt {} failed env={} — retrying in {}s ({})",
                            attempt, props.getEnvId(),
                            java.util.concurrent.TimeUnit.MILLISECONDS.toSeconds(delayMs), ex.toString());
                }
                if (!sleepInterruptible(delayMs) || abortBootstrap) {
                    return;
                }
            }
        }
    }

    /**
     * One actionable line when the bearer itself is the problem (logged on state change only).
     */
    private void logTokenOutcome(EnrollmentStatus.State outcome) {
        if (outcome == EnrollmentStatus.State.TOKEN_EXPIRED) {
            log.warn("channel: the install token for env={} EXPIRED before this engine could enrol. "
                    + "Open the engine in StudioQuack, copy a fresh install command and run it on this "
                    + "host — the engine keeps retrying and enrols with the new token.", props.getEnvId());
        } else if (outcome == EnrollmentStatus.State.TOKEN_REJECTED) {
            log.warn("channel: StudioQuack rejected this engine's install token for env={} — a newer "
                    + "install command was generated for it. Run the newest install command from the "
                    + "engine page on this host.", props.getEnvId());
        }
    }

    static final long ENROLL_RETRY_INTERVAL_MS = 60_000L;
    static final int ENROLL_RETRY_JITTER_MS = 5_000;
    /**
     * Slow phase: one bootstrap attempt every 5 min (+ jitter), forever.
     */
    static final long ENROLL_SLOW_PHASE_INTERVAL_MS = 300_000L;

    /**
     * Transient vs terminal first-boot failure. {@code 404 Error_EnvNotFound} is (self-disable) and a non-HTTPS
     * base URL is a config error.
     */
    static boolean isRetryableBootstrapFailure(Throwable ex) {
        return !isEnvDeletedOnBackend(ex) && !isHttpsRefusal(ex);
    }

    /**
     * Terminal: enroll or handshake answered {@code 404 Error_EnvNotFound}.
     */
    static boolean isEnvDeletedOnBackend(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof NetHandshakeException nhe && nhe.envNotFound()) {
                return true;
            }
            if (t instanceof EnrollmentClient.EnrollmentException) {
                String m = t.getMessage();
                if (m != null && (m.contains("Error_EnvNotFound") || m.contains("status 404"))) {
                    return true;
                }
            }
        }
        return false;
    }

    static boolean isHttpsRefusal(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            String m = t.getMessage();
            if (m != null && m.contains(HttpsEndpointPolicy.REFUSAL)) {
                return true;
            }
        }
        return false;
    }

    static long enrollRetryDelayMs(int jitterMs, java.util.function.IntUnaryOperator rng) {
        int bound = Math.max(0, jitterMs);
        int jitter = bound == 0 ? 0 : Math.max(0, rng.applyAsInt(bound));
        return ENROLL_RETRY_INTERVAL_MS + jitter;
    }

    static long enrollRetryDelayMs() {
        return enrollRetryDelayMs(ENROLL_RETRY_JITTER_MS, java.util.concurrent.ThreadLocalRandom.current()::nextInt);
    }

    /**
     * Slow-phase delay: {@link #ENROLL_SLOW_PHASE_INTERVAL_MS} + up to 30 s jitter.
     */
    static long enrollSlowPhaseDelayMs() {
        return ENROLL_SLOW_PHASE_INTERVAL_MS
                + java.util.concurrent.ThreadLocalRandom.current().nextInt(30_000);
    }

    static boolean enrollRetryBudgetRemaining(long deadlineNanos, long nowNanos) {
        return nowNanos < deadlineNanos;
    }

    private void abortEnrollRetry() {
        abortBootstrap = true;
        Thread t = this.bootstrapThread;
        if (t != null && t != Thread.currentThread()) {
            t.interrupt();
        }
    }

    /**
     * Terminal path: in-process purge before the side-car teardown (optional in slice tests).
     */
    @Autowired(required = false)
    private ObjectProvider<com.studioquack.sqs.maintenance.UninstallService> uninstallPurge;

    /**
     * Terminal path: detached teardown that outlives this launchd/systemd job.
     */
    @Autowired(required = false)
    private ObjectProvider<com.studioquack.sqs.maintenance.SidecarUninstaller> sidecarUninstaller;

    /**
     * Env deleted on the BE while this daemon was offline. Disable host supervision and exit 0 so a supervisor
     * does not restart-loop.
     */
    private void handleDeletedEngine() {
        log.error("channel: env={} is unknown to the backend (404 Error_EnvNotFound) — this "
                + "engine was deleted from the control plane while the daemon was offline. The "
                + "enrolment identity can never work again; disabling the host service and exiting. "
                + "Reinstall the engine on this host to bring it back.", props.getEnvId());


        // Race then finds nothing to reuse, and the host stops advertising itself as installed.
        com.studioquack.sqs.maintenance.UninstallService purgeSvc =
                uninstallPurge == null ? null : uninstallPurge.getIfAvailable();
        if (purgeSvc != null) {
            try {
                purgeSvc.purge(false);
            } catch (Exception purgeEx) {
                log.warn("channel: env-deleted purge failed ({}) — continuing with teardown",
                        purgeEx.toString());
            }
        }
        // The detached side-car outlives this launchd/systemd job: it unloads the unit AFTER we

        // It also removes the mesh agent
        // service/keys — a deleted engine must not leave the host on the StudioQuack network.
        boolean sidecarSpawned = false;
        try {
            com.studioquack.sqs.maintenance.SidecarUninstaller sidecar =
                    sidecarUninstaller == null ? null : sidecarUninstaller.getIfAvailable();
            ServiceManagerDetector detector = serviceManagerDetector == null
                    ? null : serviceManagerDetector.getIfAvailable();
            ServiceManagerDetector.ServiceManager mgr = detector == null ? null : detector.detect();
            if (sidecar != null && mgr != null) {
                sidecarSpawned = sidecar.spawn(mgr, false, false, 2_000L).spawned();
                log.info("channel: env-deleted side-car teardown spawned={}", sidecarSpawned);
            }
        } catch (Exception sideEx) {
            log.warn("channel: env-deleted side-car spawn failed ({})", sideEx.toString());
        }
        if (sidecarSpawned) {
            scheduleTerminalExit();
            return;
        }
        try {
            HostServiceUninstaller uninstaller = hostServiceUninstaller == null
                    ? null : hostServiceUninstaller.getIfAvailable();
            if (uninstaller != null) {
                ServiceManagerDetector detector = serviceManagerDetector == null
                        ? null : serviceManagerDetector.getIfAvailable();
                ServiceManagerDetector.ServiceManager mgr = detector == null ? null : detector.detect();
                HostServiceUninstaller.UnloadResult unloaded = uninstaller.unload(mgr);
                log.info("channel: host supervision cleared={} label='{}' notes={}",
                        unloaded.hostSupervisionCleared(), unloaded.label(), unloaded.notes());
            }
        } catch (Exception unloadEx) {
            log.warn("channel: host-service unload failed ({}) — if the supervisor relaunches this "
                    + "daemon it will exit again on the same 404.", unloadEx.toString());
        }
        scheduleTerminalExit();
    }

    /**
     * Exit 0 after the env-deleted teardown; the side-car (or the legacy unload) owns supervision.
     */
    private void scheduleTerminalExit() {
        DaemonLifecycleService.Exiter exiter = daemonExiter == null ? null : daemonExiter.getIfAvailable();
        if (exiter != null) {
            exiter.scheduleExit(0, 0, "env-deleted-on-backend");
        } else {
            log.error("channel: no exit seam available — the daemon stays up WITHOUT a control "
                    + "plane; stop it from the host.");
        }
    }

    /**
     * Repoints outbound control-plane calls onto a hub after a confirmed agent Running + HTTPS hub probe. Origin
     * is always {@code https://hub.address:hub.port} — never plain HTTP and never a hardcoded overlay IP.
     */
    private void maybeActivateOverlayOutbound(BackendBaseResolver resolver) {
        try {
            SqsChannelProperties.Net net = props.getNet();
            boolean netUp = agent != null && lastHandshake != null && agentRunning();
            String backend = agent == null ? null : agent.backend();
            String skip = outboundOverlaySkipReason(
                    net.isOutboundOverOverlay(), netUp, backend, agentRunning());
            if (skip != null) {
                log.info("channel: outbound stays on public base (reason={}, backend={})",
                        skip, backend == null ? "none" : backend);
                return;
            }
            SSLContext ssl = hubSslContext();
            HubAddress reachable = probeHubsUntilHandshaked(ssl);
            if (reachable == null) {
                log.warn("channel: no overlay hub reachable over HTTPS — outbound stays on the "
                        + "public base and BE commands fall back to the heartbeat queue");
                return;
            }
            hubList.markInUse(reachable);
            if (telemetry != null) {
                telemetry.updateHubInUse(reachable.address() + ":" + reachable.port());
            }
            String overlayOrigin = "https://" + reachable.address() + ":" + reachable.port();
            String overlayBase = props.environmentBase(overlayOrigin);
            resolver.activateOverlay(overlayBase);
            log.info("channel: outbound repointed to overlay base {} (public fallback armed after {} failures)",
                    overlayBase, net.getOutboundFailureThreshold());
        } catch (Exception ex) {
            log.warn("channel: overlay outbound repoint check failed ({}); staying on public base",
                    ex.toString());
        }
    }

    private static final long HUB_PROBE_BUDGET_MS = 15_000L;
    private static final long HUB_PROBE_RETRY_DELAY_MS = 1_000L;

    /**
     * Walks {@link HubList} in order until one hub completes an HTTPS handshake or the probe budget expires.
     */
    private HubAddress probeHubsUntilHandshaked(SSLContext ssl) {
        java.util.List<HubAddress> hubs = hubList == null ? java.util.List.of() : hubList.snapshot();
        if (hubs.isEmpty()) {
            return null;
        }
        long deadline = System.nanoTime() + HUB_PROBE_BUDGET_MS * 1_000_000L;
        int attempt = 0;
        while (true) {
            attempt++;
            for (HubAddress hub : hubs) {
                if (hub != null && hub.usable()
                        && OverlayHealthProbe.httpsReachable(hub.address(), hub.port(), ssl,
                                OverlayHealthProbe.DEFAULT_TIMEOUT_MS)) {
                    log.info("channel: overlay hub {}:{} reachable on attempt {}",
                            hub.address(), hub.port(), attempt);
                    return hub;
                }
            }
            if (System.nanoTime() >= deadline) {
                return null;
            }
            if (!sleepInterruptible(HUB_PROBE_RETRY_DELAY_MS)) {
                return null;
            }
        }
    }

    /**
     * Hub-leg TLS is the same system-trust context as the public channel. The hub is dialed by overlay IP;
     * {@link BackendIdentityTrustManager} also accepts the public backend hostname.
     */
    private SSLContext hubSslContext() {
        return httpClients.sslContext();
    }

    /**
     * Pure gate for the OUTBOUND overlay repoint. Reason strings stay stable so existing skip-reason tests keep
     * passing ({@code wg-not-up}/{@code wg-down} mean the mesh agent is not Running).
     */
    static String outboundOverlaySkipReason(boolean switchOn, boolean wgUp, String backend, boolean statsUp) {
        if (!switchOn) {
            return "outbound-over-overlay-disabled";
        }
        if (backend == null || backend.isBlank()) {
            return "no-data-plane";
        }
        if (!wgUp) {
            return "wg-not-up";
        }
        if (!statsUp) {
            return "wg-down";
        }
        return null;
    }

    private WgLivenessProvider startNetIfEnabled(String environmentBase) {
        String skip = netBringUpSkipReason(props.getNet().isEnabled(), agent != null);
        if (skip != null) {
            switch (skip) {
                case "disabled" -> log.info("channel/net: overlay disabled "
                        + "(sqs.channel.net.enabled=false) — all backend traffic stays on public "
                        + "HTTPS heartbeat/exec-channel");
                default -> log.error("channel/net: overlay ENABLED but this host has NO sq-net data "
                        + "plane — skipping handshake. Outbound stays on public HTTPS, the reverse "
                        + "listener will not start, and BE commands arrive on the heartbeat queue. "
                        + "Install sq-net-agent (the installer does this) and restart the daemon.");
            }
            // A host that never brings the overlay up also never reaches notifyEgressUp, so an
            // overlayDown latch raised by an earlier bootstrap would stick for the process
            // lifetime; this is the only point such a host's egress posture gets restored.
            if (egressManager != null) {
                egressManager.reassertBaselineWithoutOverlay();
            }
            return WgLivenessProvider.WgLiveness::down;
        }
        logExistingAgentState();
        log.info("channel/net: bring-up start cli-backend={} handshakeUrl={}/net/handshake",
                agent.backend(), environmentBase);
        try {
            DaemonFileCustody custody = fileCustody == null ? null : fileCustody.getIfAvailable();
            ListenerKeyStore keys = ListenerKeyStore.loadOrCreate(
                    Path.of(props.getDataDir()), props.getEnvId(),
                    custody == null ? null : custody.netListenerKey());
            this.listenerKeys = keys;
            NetHandshakeClient hs = new NetHandshakeClient(
                    environmentBase, props.getEnvId(), props.getBearer(),
                    httpClients, mapper, agent, keys, meshIdentity, grantedPeerDirectory,
                    overlayPeerDirectory == null ? null : overlayPeerDirectory::replaceGrantedEnvIds,
                    networkRuleStore);
            hs.setCrossEnvGrantStore(crossEnvGrantStore);
            this.handshakeClient = hs;
        } catch (Exception ex) {
            log.warn("channel/net: could not initialise the handshake client ({}); staying on "
                    + "heartbeat/exec-channel", ex.toString());
            this.lastBringUpError = ex.toString();
            return WgLivenessProvider.WgLiveness::down;
        }
        return bringUpWithRetry(environmentBase);
    }

    /**
     * Pure gate for the net handshake + agent bring-up. Side-effect-free so the skip reasons are unit-testable
     * without the CLI.
     */
    static String netBringUpSkipReason(boolean enabled, boolean hasDataPlane) {
        if (!enabled) {
            return "disabled";
        }
        if (!hasDataPlane) {
            return "no-data-plane";
        }
        return null;
    }

    /**
     * Bounded exponential backoff for the initial handshake: 5 retries at 2s → 4s → 8s → 16s → 32s.
     */
    private static final long[] HANDSHAKE_RETRY_BACKOFF_MS = {2_000L, 4_000L, 8_000L, 16_000L, 32_000L};

    /**
     * Handshake v2 with bounded backoff. 404 {@code Error_EnvNotFound} is terminal.
     */
    private WgLivenessProvider bringUpWithRetry(String environmentBase) {
        NetHandshakeClient hs = this.handshakeClient;
        int maxRetries = HANDSHAKE_RETRY_BACKOFF_MS.length;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            if (attempt > 0) {
                long delayMs = HANDSHAKE_RETRY_BACKOFF_MS[attempt - 1];
                log.info("channel/net: handshake retry {}/{} in {}ms (previous attempt failed)",
                        attempt, maxRetries, delayMs);
                if (!sleepInterruptible(delayMs)) {
                    return WgLivenessProvider.WgLiveness::down;
                }
            }
            long startNanos = System.nanoTime();
            try {
                NetHandshakeResult result = hs.handshake();
                applyHandshakeResult(result);
                this.lastBringUpError = null;
                log.info("channel/net: handshake OK cell={} hubs={} peers={} bringUpMs={} attempt={}",
                        result.cellId(), result.hubAddresses().size(), result.meshPeers().size(),
                        (System.nanoTime() - startNanos) / 1_000_000L, attempt + 1);
                return this::agentLiveness;
            } catch (NetHandshakeException ex) {
                this.lastBringUpError = ex.getMessage();
                if (ex.envNotFound()) {
                    throw ex;
                }
                if (ex.networkUnavailable()) {
                    log.warn("channel/net: handshake 503 NetworkUnavailable after {}ms (Retry-After={}) "
                                    + "— staying on public HTTPS. {}",
                            (System.nanoTime() - startNanos) / 1_000_000L, ex.retryAfterSeconds(),
                            ex.toString());
                } else if (ex.noCellCapacity()) {
                    log.warn("channel/net: handshake 409 NoCellCapacity after {}ms — retry later. {}",
                            (System.nanoTime() - startNanos) / 1_000_000L, ex.toString());
                } else {
                    log.warn("channel/net: handshake attempt {} FAILED after {}ms ({}). "
                                    + "A connect/timeout means the daemon cannot reach {}/net/handshake.",
                            attempt + 1, (System.nanoTime() - startNanos) / 1_000_000L, ex.toString(),
                            environmentBase);
                }
            } catch (Exception ex) {
                this.lastBringUpError = ex.toString();
                log.warn("channel/net: bring-up attempt {} FAILED after {}ms ({})",
                        attempt + 1, (System.nanoTime() - startNanos) / 1_000_000L, ex.toString());
            }
        }
        notifyEgressDown("handshake-failed");
        log.warn("channel/net: bring-up FAILED after {} attempts; staying on heartbeat/exec-channel — "
                + "the overlay watchdog will keep retrying on its interval", maxRetries + 1);
        return WgLivenessProvider.WgLiveness::down;
    }

    /**
     * Install mesh CA, cell telemetry, hub list, egress; bind listeners to agent primary IP.
     */
    private void applyHandshakeResult(NetHandshakeResult result) {
        this.lastHandshake = result;
        if (meshPeerHttp != null) {
            meshPeerHttp.installMeshCa(result.meshCaCertPem());
        }
        if (telemetry != null) {
            telemetry.updateCell(result.cellId());
        }
        if (hubList != null) {
            hubList.replace(result.hubAddresses());
        }
        String overlayIp = currentOverlayIp();
        notifyEgressUp(overlayIp);
        if (statusWatcher != null && overlayIp != null) {
            statusWatcher.seedIp(overlayIp);
        }
    }

    /**
     * Bare overlay IP from agent status (listeners never bind {@code 0.0.0.0}).
     */
    private String currentOverlayIp() {
        try {
            if (agent == null) {
                return statusWatcher == null ? null : statusWatcher.lastIp();
            }
            return agent.status().primaryIp().orElseGet(
                    () -> statusWatcher == null ? null : statusWatcher.lastIp());
        } catch (Exception ex) {
            return statusWatcher == null ? null : statusWatcher.lastIp();
        }
    }

    private boolean agentRunning() {
        try {
            return agent != null && agent.status().running();
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * {@link WgLivenessProvider}: up iff the agent reports {@code BackendState=Running}.
     */
    private WgLivenessProvider.WgLiveness agentLiveness() {
        try {
            NetAgentStatus st = agent == null ? null : agent.status();
            if (st != null && st.running()) {
                return new WgLivenessProvider.WgLiveness(true, System.currentTimeMillis() / 1000L);
            }
        } catch (Exception ignored) {
            // Status can throw before first up
        }
        return WgLivenessProvider.WgLiveness.down();
    }

    private static boolean sleepInterruptible(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Logs any pre-existing agent session before the handshake. Never throws.
     */
    private void logExistingAgentState() {
        try {
            NetAgentStatus st = agent.status();
            if (st != null && st.running()) {
                log.info("channel/net: existing agent Running before handshake (backend={} ip={})",
                        st.backendState(), st.primaryIp().orElse("none"));
            } else {
                log.info("channel/net: agent not Running before handshake (state={}) — proceeding",
                        st == null ? "null" : st.backendState());
            }
        } catch (Exception ex) {
            log.info("channel/net: could not read agent status ({}); proceeding with handshake",
                    ex.toString());
        }
    }

    /**
     * Arms {@link NetWatchdog} after the initial bring-up. Started only when net is enabled and a data plane
     * exists (otherwise recovery is impossible).
     */
    private void startNetWatchdog(BackendBaseResolver resolver) {
        NetWatchdog wd = watchdog == null ? null : watchdog.getIfAvailable();
        if (wd == null) {
            return;
        }
        if (!props.getNet().isEnabled() || agent == null) {
            return;
        }
        // Non-null overlayBaseFor tells the watchdog to wrap https://hub:port with environmentBase.
        wd.start(new NetWatchdog.Context(
                resolver, agent, () -> this.handshakeClient,
                hubList, hubSslContext(), resolver.publicBase(),
                this::onNetWatchdogRecovered, this::ensureInboundListeners));
    }

    /**
     * Idempotent: binds whichever inbound overlay listener is missing, provided a handshake has been applied and
     * the agent is Running. Runs on every watchdog tick (15 s) and on the overlay-IP event, so a listener skipped
     * at boot (agent still coming up after a restart), one that lost its bind, or one on a cell whose hubs never
     * answer is bound within a tick — inbound reachability never waits for the outbound hub probe.
     */
    void ensureInboundListeners() {
        if (lastHandshake == null || !agentRunning()) {
            return;
        }
        if (this.overlayListener == null) {
            startOverlayListenerIfEnabled();
        }
        if (this.overlayPeerListener == null) {
            startOverlayPeerListenerIfEnabled();
        }
    }

    /**
     * Last skip reason logged per listener — the 15 s ensure loop must not repeat a stable reason.
     */
    private volatile String lastReverseListenerSkip;
    private volatile String lastPeerListenerSkip;

    /**
     * Logs a listener skip once per distinct reason (INFO on change, DEBUG while unchanged).
     */
    private void logListenerSkip(String listener, String reason, String detail, boolean reverse) {
        String prev = reverse ? lastReverseListenerSkip : lastPeerListenerSkip;
        if (reverse) {
            lastReverseListenerSkip = reason;
        } else {
            lastPeerListenerSkip = reason;
        }
        if (reason.equals(prev)) {
            log.debug("channel/overlay: {} still not started ({})", listener, detail);
        } else {
            log.info("channel/overlay: {} not started ({})", listener, detail);
        }
    }

    /**
     * Starts the status poller and seeds the last-known overlay IP.
     */
    private void startNetStatusWatcher() {
        if (statusWatcher == null || agent == null) {
            return;
        }
        String ip = currentOverlayIp();
        if (ip != null) {
            statusWatcher.seedIp(ip);
        }
        statusWatcher.start();
    }

    /**
     * Watchdog recovery hook: persist the handshake result and ensure listeners are bound. Idempotent when
     * listeners are already up.
     */
    private void onNetWatchdogRecovered(NetHandshakeResult result) {
        if (result != null) {
            this.lastBringUpError = null;
            applyHandshakeResult(result);
        }
        if (this.overlayListener != null) {
            log.info("channel/overlay: reverse listener already running after watchdog reconnect");
        } else {
            startOverlayListenerIfEnabled();
        }
        if (this.overlayPeerListener != null) {
            log.info("channel/overlay: peer listener already running after watchdog reconnect");
        } else {
            startOverlayPeerListenerIfEnabled();
        }
        HeartbeatClient hb = this.heartbeatClient;
        if (hb != null) {
            hb.beatNow("overlay-recovered");
        }
    }

    /**
     * Apply host egress rules after a successful overlay assignment.
     */
    private void notifyEgressUp(String overlayIp) {
        if (overlayIp == null || overlayIp.isBlank()) {
            return;
        }
        String networkKey = overlayInterfaceName();
        if (egressManager != null) {
            try {
                egressManager.networkUp(networkKey, networkKey, overlayIp, null, null);
            } catch (Exception ex) {
                log.error("channel/egress: overlayUp failed ({}) — sandboxes fail closed until recovery",
                        ex.toString());
                quietly("egress overlayDown after overlayUp failure",
                        () -> egressManager.overlayDown("overlay-egress rules failed"));
            }
        }
        if (reverseRelay != null) {
            quietly("reverse-relay overlayUp", () -> reverseRelay.overlayUp(networkKey, overlayIp));
        }
        overlayPeerDirectory.setPrimaryKey(networkKey);
        if (statefulInstanceManager != null) {
            quietly("reverse-relay republish surviving",
                    statefulInstanceManager::republishSurvivingReversePorts);
        }
        // The built-in node library build runs behind the egress gate; an attempt that failed
        // while the overlay was down is retried the moment egress is back, not minutes later.
        com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager lib =
                builtinNodeLibrary == null ? null : builtinNodeLibrary.getIfAvailable();
        if (lib != null) {
            quietly("builtin-lib retry on egress up", lib::retryNow);
        }
    }

    private String overlayInterfaceName() {
        try {
            if (agent != null) {
                NetAgentStatus st = agent.status();
                if (st != null && st.interfaceName() != null && !st.interfaceName().isBlank()) {
                    return st.interfaceName();
                }
            }
        } catch (Exception ignored) {
            // Fall through to config default
        }
        return props.getNet().getInterfaceName();
    }

    /**
     * Mark egress unavailable (rules left in place for fail-closed).
     */
    private void notifyEgressDown(String reason) {
        if (egressManager != null) {
            quietly("egress overlayDown", () -> egressManager.overlayDown(reason));
        }
        if (reverseRelay != null) {
            quietly("reverse-relay overlayDown", () -> reverseRelay.overlayDown(reason));
        }
    }

    /**
     * Operator-triggered overlay reconnect. Kicks {@link NetWatchdog} past its backoff and wakes the heartbeat so
     * the BE learns the outcome.
     */
    public void retryNet() {
        NetWatchdog wd = watchdog == null ? null : watchdog.getIfAvailable();
        if (wd != null) {
            wd.recoverNow();
        }
        HeartbeatClient hb = this.heartbeatClient;
        if (hb != null) {
            hb.beatNow("net-reconnect-requested");
        }
        log.info("channel/net: operator requested overlay reconnect — watchdog will attempt bring-up now");
    }

    /**
     * On-demand mesh identity refresh for the reactive 401 path: a peer rejected this daemon's mesh token (expiry
     * against the peer's clock), so mint a fresh one NOW instead of waiting for the watchdog's healthy-path
     * refresh. {@link NetHandshakeClient#handshake()} is synchronized, so concurrent 401s serialize; a 5-second
     * floor keeps a 401 storm (e.g. a systematically skewed peer clock) from hammering the control plane.
     *
     * @return {@code true} when a usable token is held afterwards (a fresh one, or the existing one when the floor
     * skipped the network call)
     */
    public boolean refreshMeshIdentityNow() {
        NetHandshakeClient hs = handshakeClient;
        if (hs == null) {
            return false;
        }
        long now = System.nanoTime();
        if (now - lastMeshRefreshNanos < MESH_REFRESH_FLOOR_NANOS) {
            return meshIdentity != null && meshIdentity.currentToken().isPresent();
        }
        lastMeshRefreshNanos = now;
        try {
            applyHandshakeResult(hs.handshake());
            log.info("channel/net: on-demand mesh identity refresh OK (peer rejected the previous token)");
            return true;
        } catch (Exception ex) {
            log.info("channel/net: on-demand mesh identity refresh failed ({})", ex.toString());
            return false;
        }
    }

    public String netLastError() {
        NetWatchdog wd = watchdog == null ? null : watchdog.getIfAvailable();
        String watchdogError = wd == null ? null : wd.lastError();
        return watchdogError != null ? watchdogError : lastBringUpError;
    }

    private void startHeartbeatIfEnabled(BackendBaseResolver resolver, WgLivenessProvider wgLiveness) {
        if (!props.getHeartbeat().isEnabled() || this.heartbeatClient != null) {
            return;
        }
        SqsChannelProperties.Heartbeat hbProps = props.getHeartbeat();
        HeartbeatCadence cadence = new HeartbeatCadence(
                hbProps.getPeriodMs(), hbProps.getLongPollWaitMs(),
                hbProps.getWgKeepaliveSeconds() * 1_000L,
                hbProps.getFallbackStartSeconds() * 1_000L,
                hbProps.getFallbackMinSeconds() * 1_000L,
                hbProps.getWgKeepaliveGraceSeconds() * 1_000L,
                hbProps.getWgSilentFreshnessSeconds() * 1_000L,
                hbProps.getWgSilentCheckSeconds() * 1_000L,
                () -> overlayReverseChannelHealthy(resolver),
                this::runActive);
        HeartbeatClient hb = new HeartbeatClient(
                resolver::current, resolver, props.getBearer(), props.getAgentVersion(), props.getInstanceId(),
                props.getGeneration(), cadence, httpClients, mapper, registry,
                resourceProvider.getIfAvailable(DefaultHeartbeatResourceProvider::new), wgLiveness,
                tamperEventReporter);
        hb.setNodeExecutionMeter(nodeExecutionMeter);
        hb.setExecutionHaltGuard(executionHaltGuard);
        hb.setNetTelemetry(telemetry);
        hb.setWorkIntakeGate(workIntakeGate);
        // Safety net: a definitive 404 Error_EnvNotFound on a steady-state beat means the
        // engine was deleted while this daemon was alive and every uninstall delivery path
        // missed — take the same host-supervision disable + exit as the bootstrap path.
        hb.setEnvGoneHandler(this::handleDeletedEngine);
        hb.start();
        this.heartbeatClient = hb;
    }

    /**
     * Local signals that the outbound overlay hub is reachable and the reverse command listener is bound —
     * necessary but not sufficient for keepalive.
     */
    private boolean overlayReverseChannelHealthy(BackendBaseResolver resolver) {
        OverlayReverseListener listener = this.overlayListener;
        return resolver.overlayActive() && listener != null && listener.isRunning();
    }

    /**
     * Whether this daemon has a node dispatch in flight. Read from the wait-state registry so the beat loop never
     * queries the engine.
     */
    private boolean runActive() {
        DispatchExecutionRegistry reg = dispatchRegistry == null ? null : dispatchRegistry.getIfAvailable();
        return reg != null && reg.pending() > 0;
    }

    private void startExecChannelIfEnabled(BackendBaseResolver resolver) {
        if (!props.getExecChannel().isEnabled() || this.execChannelClient != null) {
            return;
        }
        ExecChannelClient ec = new ExecChannelClient(
                resolver::current, resolver, props.getBearer(), props.getInstanceId(), httpClients, mapper,
                dispatchExecutor.getIfAvailable(), props.getExecChannel().getSseIdleWindowMs(),
                props.getExecChannel().getPullWaitMs(), props.getExecChannel().getDedupTtlMs());
        ec.start();
        this.execChannelClient = ec;
    }

    /**
     * Starts the inbound overlay reverse listener only when the agent is Running and the mTLS identity is
     * enrolled. Bind host is the agent primary IP.
     */
    private void startOverlayListenerIfEnabled() {
        if (this.overlayListener != null) {
            return;
        }
        SqsChannelProperties.Net.OverlayListener cfg = props.getNet().getOverlayListener();
        boolean netUp = agent != null && lastHandshake != null && agentRunning();
        String backend = agent == null ? null : agent.backend();
        String skip = overlaySkipReason(
                cfg.isEnabled(), netUp, backend,
                agentRunning(), identity != null && identity.isEnrolled());
        if (skip != null) {
            logListenerSkip("reverse listener", skip, "reason=" + skip + ", backend="
                    + (backend == null ? "none" : backend) + "; control plane stays on public HTTPS", true);
            return;
        }
        String overlayIp = currentOverlayIp();
        String bindHost = resolveOverlayBindHost(cfg.getBindAddress(), overlayIp);
        if (bindHost == null || bindHost.isBlank()) {
            logListenerSkip("reverse listener", "no-bind-address", "no overlay bind address yet (ip="
                    + overlayIp + " override='" + cfg.getBindAddress() + "') — retried on the next tick", true);
            return;
        }
        lastReverseListenerSkip = null;
        int port = props.getNet().getDaemonApiPort();
        WindowsOverlayFirewall.ensureInboundAllow(port);
        try {
            OverlayReverseListener listener = OverlayReverseListener.forMtls(
                    bindHost, port, identity,
                    dispatchExecutor.getIfAvailable(), ociIngest.getIfAvailable(), registry, mapper);
            listener.setCommandActivityHook(() -> {
                HeartbeatClient hb = this.heartbeatClient;
                if (hb != null) {
                    hb.cadence().noteActivity();
                    hb.beatNow("overlay-command");
                }
            });
            listener.setInboundTrafficHook(() -> {
                HeartbeatClient hb = this.heartbeatClient;
                if (hb != null) {
                    hb.cadence().noteInboundOverlayTraffic();
                }
                // An authenticated overlay request is control-plane contact too: an OVERLAY_SILENT
                // daemon sends no beat, and counting only beats halted it as OFFLINE after 5 min.
                if (executionHaltGuard != null) {
                    executionHaltGuard.onOverlayContact();
                }
            });
            listener.start();
            this.overlayListener = listener;
            log.info("channel/overlay: reverse listener started bind={} port={} (mTLS, client-cert required)",
                    bindHost, port);
        } catch (Exception ex) {
            log.warn("channel/overlay: reverse listener start failed ({}); overlay direct-dispatch "
                    + "unavailable, control plane unaffected", ex.toString());
        }
    }

    /**
     * Pure gate for the overlay reverse listener. {@code wgUp}/{@code statsUp} mean the mesh agent is Running —
     * reason strings stay stable for existing tests.
     */
    static String overlaySkipReason(boolean enabled, boolean wgUp, String backend,
                                    boolean statsUp, boolean mtlsEnrolled) {
        if (!enabled) {
            return "disabled";
        }
        if (backend == null || backend.isBlank()) {
            return "no-data-plane";
        }
        if (!wgUp) {
            return "wg-not-up";
        }
        if (!statsUp) {
            return "wg-down";
        }
        if (!mtlsEnrolled) {
            return "mtls-not-enrolled";
        }
        return null;
    }

    /**
     * Starts the daily mTLS leaf rotation loop. The enrolment callable is built fresh per attempt so it always
     * presents the CURRENT channel credential (kemIkm proof — exempt from the installer-bearer TTL, so
     * re-enrolment can never be locked out).
     */
    private void startMtlsRotationLoop(String environmentBase) {
        MtlsIdentity id = this.identity;
        if (id == null) {
            return;
        }
        // Idempotent: never two loops.
        MtlsRotationLoop previous = this.mtlsRotationLoop;
        if (previous != null) {
            this.mtlsRotationLoop = null;
            quietly("mtls-rotation stop (re-bootstrap)", previous::stop);
        }
        MtlsRotationLoop loop = new MtlsRotationLoop(id, () ->
                new EnrollmentClient(environmentBase, props.getEnvId(), props.getBearer(),
                        id, httpClients, mapper).enrolMtls());
        loop.start();
        this.mtlsRotationLoop = loop;
    }

    /**
     * Daemon→daemon peer listener on the agent primary IP. Listeners do not self-rebind —
     * {@link #onOverlayAddressChanged} restarts them.
     */
    private void startOverlayPeerListenerIfEnabled() {
        if (this.overlayPeerListener != null) {
            return;
        }
        SqsChannelProperties.Net.OverlayListener cfg = props.getNet().getOverlayListener();
        boolean netUp = agent != null && lastHandshake != null && agentRunning();
        String backend = agent == null ? null : agent.backend();
        String skip = overlayPeerSkipReason(
                cfg.isEnabled(), netUp, backend, agentRunning());
        if (skip != null) {
            logListenerSkip("peer listener", skip, "reason=" + skip + ", backend="
                    + (backend == null ? "none" : backend), false);
            return;
        }
        String overlayIp = currentOverlayIp();
        String bindHost = resolveOverlayBindHost(cfg.getBindAddress(), overlayIp);
        if (bindHost == null || bindHost.isBlank()) {
            logListenerSkip("peer listener", "no-bind-address", "no overlay bind address yet (ip="
                    + overlayIp + " override='" + cfg.getBindAddress() + "') — retried on the next tick", false);
            return;
        }
        lastPeerListenerSkip = null;
        HelperExecuteService peerExecute = helperExecuteService == null
                ? null : helperExecuteService.getIfAvailable();
        if (peerExecute == null) {
            log.warn("channel/overlay: helper execute service unavailable — peer listener skipped");
            return;
        }
        javax.net.ssl.SSLContext ssl = peerListenerSsl();
        if (ssl == null) {
            log.warn("channel/overlay: peer listener leaf not installed — skipping "
                    + "(production bind is TLS-only)");
            return;
        }
        WindowsOverlayFirewall.ensureInboundAllow(overlayExecutePort);
        try {
            OverlayPeerListener listener = new OverlayPeerListener(
                    bindHost, overlayExecutePort, peerExecute, meshPeerAuthenticator(),
                    aiModelProviderProxy, mapper, ssl);
            listener.start();
            this.overlayPeerListener = listener;
            log.info("channel/overlay: peer listener started bind={} port={} (mesh HTTPS inside the tunnel)",
                    bindHost, overlayExecutePort);
        } catch (Exception ex) {
            log.warn("channel/overlay: peer listener start failed ({}); cross-env execute inbound "
                    + "unavailable, control plane unaffected", ex.toString());
        }
    }

    /**
     * Mesh peer authenticator from the shared singletons. Fails CLOSED when any collaborator is unavailable.
     */
    private com.studioquack.sqs.channel.overlay.PeerCallAuthenticator meshPeerAuthenticator() {
        com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient authzClient =
                crossEnvAuthorizationClient == null ? null : crossEnvAuthorizationClient.getIfAvailable();
        if (meshIdentity == null || crossEnvGrantStore == null || authzClient == null) {
            log.warn("channel/overlay: mesh peer authentication is not fully wired (identity={}, "
                            + "grantStore={}, authorizeClient={}) — the peer listener will REFUSE every "
                            + "call (fail closed)",
                    meshIdentity != null, crossEnvGrantStore != null, authzClient != null);
            return (authorizationHeader, scope) -> com.studioquack.sqs.channel.overlay
                    .PeerCallAuthenticator.AuthResult.deny(403, "Error_AuthorizationUnavailable",
                            "mesh peer authentication is not wired on this daemon — refusing peer "
                                    + "calls (fail closed)");
        }
        return new com.studioquack.sqs.channel.overlay.MeshPeerAuthenticator(
                new com.studioquack.sqs.channel.wg.mesh.MeshTokenVerifier(meshIdentity),
                crossEnvGrantStore, authzClient, props.getEnvId());
    }

    /**
     * Listener TLS, or {@code null} when handshake has not installed a cert yet. The context follows
     * {@link ListenerKeyStore#leaf()} (rotating key manager), so the 24-h leaf the BE re-signs on every
     * re-handshake is presented without a listener restart.
     */
    private javax.net.ssl.SSLContext peerListenerSsl() {
        ListenerKeyStore keys = this.listenerKeys;
        if (keys == null || keys.leaf() == null) {
            return null;
        }
        try {
            return MeshTls.serverContext(keys, null);
        } catch (Exception ex) {
            log.warn("channel/overlay: peer listener SSLContext failed ({})", ex.toString());
            return null;
        }
    }

    /**
     * Pure gate for the overlay PEER listener: identical to {@link #overlaySkipReason} minus mTLS enrolment
     * (mesh-CA TLS is the peer identity).
     */
    static String overlayPeerSkipReason(boolean enabled, boolean wgUp, String backend,
                                        boolean statsUp) {
        if (!enabled) {
            return "disabled";
        }
        if (backend == null || backend.isBlank()) {
            return "no-data-plane";
        }
        if (!wgUp) {
            return "wg-not-up";
        }
        if (!statsUp) {
            return "wg-down";
        }
        return null;
    }

    /**
     * Resolves the overlay listener bind host: the config override wins; otherwise the overlay IP (bare or CIDR —
     * {@link #stripCidr} still works).
     */
    static String resolveOverlayBindHost(String override, String overlayCidr) {
        if (override != null && !override.isBlank()) {
            return override.trim();
        }
        return stripCidr(overlayCidr);
    }

    /**
     * Strips a trailing {@code /NN} CIDR suffix, returning the bare host.
     */
    static String stripCidr(String cidr) {
        if (cidr == null) {
            return null;
        }
        int slash = cidr.indexOf('/');
        return slash >= 0 ? cidr.substring(0, slash) : cidr;
    }

    /**
     * {@link NetStatusWatcher} published a new overlay IP. Listeners do not self-rebind — stop and restart them on
     * the new address.
     */
    @EventListener(OverlayAddressChangedEvent.class)
    public void onOverlayAddressChanged(OverlayAddressChangedEvent event) {
        if (event == null || event.currentIp() == null) {
            return;
        }
        log.info("channel/overlay: overlay IP {} → {} — restarting listeners",
                event.previousIp(), event.currentIp());
        OverlayReverseListener listener = this.overlayListener;
        if (listener != null) {
            this.overlayListener = null;
            quietly("overlay listener stop for rebind", listener::stop);
        }
        OverlayPeerListener peerListener = this.overlayPeerListener;
        if (peerListener != null) {
            this.overlayPeerListener = null;
            quietly("overlay peer listener stop for rebind", peerListener::stop);
        }
        notifyEgressUp(event.currentIp());
        startOverlayListenerIfEnabled();
        startOverlayPeerListenerIfEnabled();
    }

    /**
     * Granted-peer set changed between handshakes. Re-handshake (debounced) so the directory and mesh CA stay
     * current.
     */
    @EventListener(MeshPeerSetChangedEvent.class)
    public void onMeshPeerSetChanged() {
        if (!props.getNet().isEnabled() || handshakeClient == null) {
            return;
        }
        synchronized (meshRefreshLock) {
            java.util.concurrent.ScheduledExecutorService scheduler = this.meshRefresh;
            if (scheduler == null || scheduler.isShutdown()) {
                scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "sqs-net-mesh-refresh");
                    t.setDaemon(true);
                    return t;
                });
                this.meshRefresh = scheduler;
            }
            if (meshRefreshPending != null) {
                meshRefreshPending.cancel(false);
            }
            meshRefreshPending = scheduler.schedule(() -> {
                log.info("channel/net: granted-peer set changed — re-handshaking now");
                try {
                    NetHandshakeResult result = handshakeClient.handshake();
                    applyHandshakeResult(result);
                    if (this.resolver != null) {
                        maybeActivateOverlayOutbound(this.resolver);
                    }
                } catch (NetHandshakeException ex) {
                    if (ex.envNotFound()) {
                        handleDeletedEngine();
                        return;
                    }
                    log.info("channel/net: mesh-refresh handshake failed ({}) — watchdog retries",
                            ex.toString());
                } catch (Exception ex) {
                    log.info("channel/net: mesh-refresh handshake failed ({}) — watchdog retries",
                            ex.toString());
                }
            }, MESH_REFRESH_DEBOUNCE_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }

    public EnvelopeClient envelopeClient() {
        return envelopeClient;
    }

    private com.studioquack.sqs.secrets.DaemonBoundSealedStore kemIkmCustody() {
        return DaemonFileCustody.resolveKemIkm(fileCustody, props);
    }

    @PreDestroy
    public void shutdown() {
        abortEnrollRetry();
        NetWatchdog wd = watchdog == null ? null : watchdog.getIfAvailable();
        if (wd != null) {
            quietly("watchdog stop", wd::stop);
        }
        if (statusWatcher != null) {
            quietly("net-status stop", statusWatcher::stop);
        }
        synchronized (meshRefreshLock) {
            java.util.concurrent.ScheduledExecutorService refresh = this.meshRefresh;
            if (refresh != null) {
                this.meshRefresh = null;
                this.meshRefreshPending = null;
                quietly("mesh-refresh stop", refresh::shutdownNow);
            }
        }
        if (overlayListener != null) {
            overlayListener.stop();
        }
        if (overlayPeerListener != null) {
            overlayPeerListener.stop();
        }
        if (heartbeatClient != null) {
            heartbeatClient.stop();
        }
        if (mtlsRotationLoop != null) {
            mtlsRotationLoop.stop();
        }
        if (execChannelClient != null) {
            execChannelClient.stop();
        }

        // Taking it down here made every daemon restart a network event: the node flapped
        // in headscale, peers lost this engine mid-call, and the relaunched daemon raced the agent
        // coming back ("wg-not-up" at boot — the source of the Online-but-Unreachable restarts).
        // Uninstall keeps its explicit teardown (the side-car's mesh logout). A stopped daemon
        // leaves nothing listening on the overlay IP, so the standing tunnel exposes no new surface.
    }
}
