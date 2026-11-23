package com.studioquack.sqs.engine.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.maintenance.DaemonLifecycleService;
import com.studioquack.sqs.maintenance.ServiceManagerDetector.ServiceManager;
import com.studioquack.sqs.release.ReleaseHttpClient;
import com.studioquack.sqs.release.ReleaseSignatureVerifier;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.RunTerminationService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.ProcessEngine;
import org.activiti.engine.impl.asyncexecutor.AsyncExecutor;
import org.activiti.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-place boot-jar swap. Closes intake first so new starts stay on the control
 * plane queue, downloads and verifies the release, kills every run that still has
 * a node in flight (the operator confirmed this in Engine Settings), then hands the
 * verified jar to the supervisor. A token that finishes a node meanwhile is parked
 * by the park listener and continues on the next boot; message and timer waits are
 * persisted and survive the restart untouched. Waiting for nodes to finish was
 * dropped on purpose: one long or stuck node held every update for hours.
 * The manifest signature is checked before the jar sha256; a bad signature
 * aborts the update before anything is killed.
 *
 * <p>Hand-over goes through {@link DaemonLifecycleService#relaunch}, the same path
 * as restart, and never stops the supervisor from inside its own process tree (the
 * earlier helpers did, and were killed with it). The jar is staged as
 * {@code <jar>.new}. Under systemd ({@code Restart=on-failure}) and launchd
 * ({@code KeepAlive}) this process exits with {@link #RELAUNCH_EXIT_CODE} and the
 * launcher swaps it in before the JVM opens it, restoring {@code <jar>.previous}
 * if the new jar fails three boots. A Windows scheduled task (Windows locks a
 * running jar) and installs no supervisor watches hand over to the detached
 * {@code DaemonRelauncher}. Whether anything can start the process again is
 * checked before the download, so such a host never drains for nothing.
 */
@Slf4j
@Component
public class DaemonUpdateService {

    /** Delete reason in the engine history of a run the update killed. */
    static final String KILL_REASON = "killed by an engine update";
    /** Kill passes: a later pass catches a run whose next node registered while the previous one ran. */
    static final int KILL_PASSES = 3;
    /** Non-zero on purpose: systemd's {@code Restart=on-failure} does not relaunch a clean exit. */
    public static final int RELAUNCH_EXIT_CODE = 75;
    private static final long HANDOVER_GRACE_MS = 3_000L;
    private static final Duration HTTP_TIMEOUT = Duration.ofMinutes(5);
    private static final String RELEASE_PREFIX = "/releases/client/";

    private final WorkIntakeGate intake;
    private final DispatchExecutionRegistry dispatchRegistry;
    private final RunTerminationService runTermination;
    private final ProcessEngine processEngine;
    private final ReleaseHttpClient releaseHttp;
    private final ObjectMapper objectMapper;
    private final ReleaseSignatureVerifier releaseSignatures;
    private final String backendBaseUrl;
    private final DaemonLifecycleService lifecycle;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "sqs-daemon-update");
        t.setDaemon(true);
        return t;
    });
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DaemonUpdateService(
            WorkIntakeGate intake,
            DispatchExecutionRegistry dispatchRegistry,
            RunTerminationService runTermination,
            ProcessEngine processEngine,
            ReleaseHttpClient releaseHttp,
            ObjectMapper objectMapper,
            ReleaseSignatureVerifier releaseSignatures,
            DaemonLifecycleService lifecycle,
            @Value("${sqs.backend.base-url:}") String backendBaseUrl) {
        this.intake = intake;
        this.dispatchRegistry = dispatchRegistry;
        this.runTermination = runTermination;
        this.processEngine = processEngine;
        this.releaseHttp = releaseHttp;
        this.objectMapper = objectMapper;
        this.releaseSignatures = releaseSignatures;
        this.lifecycle = lifecycle;
        this.backendBaseUrl = backendBaseUrl == null ? "" : backendBaseUrl.trim();
    }

    /**
     * Publish the build identity. The launcher's {@code <jar>.update-pending} is NOT cleared here:
     * a process that merely started has not proven the update works — {@link UpdateProbation}
     * clears it on the first 2xx heartbeat and rolls back when that never comes.
     * The boot may also be the OLD jar after a rollback — {@link #reconcileUpdateAttempt}
     * runs once the jar hash lands and turns that into a visible FAILED instead of silence.
     */
    @EventListener(ApplicationReadyEvent.class)
    void onReady() {
        try {
            Path jar = runningJar();
            Files.deleteIfExists(jar.resolveSibling(jar.getFileName() + ".new.part"));
        } catch (Exception ex) {
            log.warn("daemon-update: could not clear the partial download: {}", ex.toString());
        }
        DaemonBuild.computeJarSha256Async();
        reconcileUpdateAttempt();
    }

    /**
     * Reads the {@code <jar>.update-attempt} marker the previous process wrote just before handing
     * over, once the running jar's sha is known. Target == running: the update landed, nothing to
     * say. Anything else: the new build never came up and a launcher restored the previous jar —
     * without this, that failure was invisible everywhere (the FE kept showing "Update available"
     * as if nothing had happened).
     */
    private void reconcileUpdateAttempt() {
        Thread t = new Thread(() -> {
            try {
                Path jar = runningJar();
                Path marker = jar.resolveSibling(jar.getFileName() + ".update-attempt");
                if (!Files.isRegularFile(marker)) {
                    return;
                }
                String target = Files.readString(marker).trim();
                Files.deleteIfExists(marker);
                if (target.isEmpty()) {
                    return;
                }
                String running = DaemonBuild.awaitJarSha256(Duration.ofMinutes(2));
                String rollback = attemptRollbackMessage(target, running);
                if (rollback == null) {
                    if (running != null) {
                        log.info("daemon-update: updated build {} is running", abbrev(target));
                    }
                } else {
                    // UpdateProbation may already have reported the precise reason (no control-plane
                    // contact) for this very rollback — keep that instead of the generic message.
                    if (!DaemonBuild.hasFailure()) {
                        DaemonBuild.updateFailed(rollback);
                    }
                    log.warn("daemon-update: {} (running {})", rollback, abbrev(running));
                }
            } catch (Exception ex) {
                log.debug("daemon-update: attempt reconciliation skipped: {}", ex.toString());
            }
        }, "sqs-daemon-update-reconcile");
        t.setDaemon(true);
        t.start();
    }

    /** Runs an update would kill right now: those with a node in flight, Designer Runs included. */
    public int runningInstances() {
        return dispatchRegistry.processInstancesInFlight().size();
    }

    /**
     * Closes intake and schedules the swap. Returns immediately so the command
     * result can leave on the next heartbeat, before this process is stopped.
     */
    public String begin() {
        if (!running.compareAndSet(false, true)) {
            return "update already in progress";
        }
        intake.close(WorkIntakeGate.Reason.UPDATE);
        DaemonBuild.updateStarted();
        worker.execute(this::runUpdate);
        return "update accepted; new runs wait, running instances are killed once the release is verified, then the swap starts";
    }

    @PreDestroy
    void shutdown() {
        worker.shutdownNow();
    }

    private void runUpdate() {
        pauseJobAcquisition();
        Path staged = null;
        try {
            Path jar = runningJar();
            lifecycle.requireRelaunchable(jar);
            // Before anything downloads or is killed: on a full disk the new build cannot come up.
            String disk = UpdateDiskCheck.shortfall(jar, UpdateDiskCheck.dataDir(jar));
            if (disk != null) {
                throw new IllegalStateException(disk);
            }
            staged = jar.resolveSibling(jar.getFileName().toString() + ".new");
            String sha = downloadVerified(jar, staged);
            String current = sha256(jar);
            if (sha.equalsIgnoreCase(current)) {
                Files.deleteIfExists(staged);
                resumeJobAcquisition();
                intake.open(WorkIntakeGate.Reason.UPDATE);
                running.set(false);
                DaemonBuild.updateIdle();
                log.info("daemon-update: running jar already matches the published release");
                return;
            }
            killRunningInstances();
            writeUpdateAttempt(jar, sha);
            ServiceManager via = lifecycle.relaunch(jar, staged, HANDOVER_GRACE_MS, "update");
            log.info("daemon-update: verified release staged; this process exits and {} starts it again",
                    via.relaunchesOnExit() ? via : "the relaunch helper");
        } catch (Exception ex) {
            discardStaged(staged);
            deleteUpdateAttempt();
            resumeJobAcquisition();
            intake.open(WorkIntakeGate.Reason.UPDATE);
            running.set(false);
            DaemonBuild.updateFailed(describeFailure(ex));
            log.warn("daemon-update: aborted, intake reopened: {}", describeFailure(ex), ex);
        }
    }

    /**
     * The sha of the build this process is about to hand over to, written just before the relaunch.
     * The next boot compares it with the jar that actually came up: a mismatch means a launcher
     * restored the previous build — the only place that rollback can still be seen.
     */
    private static void writeUpdateAttempt(Path jar, String targetSha) {
        try {
            Files.writeString(jar.resolveSibling(jar.getFileName() + ".update-attempt"), targetSha);
        } catch (IOException ex) {
            // Best-effort: losing the marker loses the rollback report, never the update itself.
            log.warn("daemon-update: could not write the attempt marker: {}", ex.getMessage());
        }
    }

    /**
     * An aborted update leaves no attempt marker, or the next boot would misread it as a rollback.
     * Best-effort like {@link #writeUpdateAttempt}: the running jar is re-resolved and simply
     * skipped when even that fails (the marker then matches nothing the next boot checks for
     * unless an attempt was really written — and then it was written by this same path).
     */
    private static void deleteUpdateAttempt() {
        try {
            Path jar = runningJar();
            Files.deleteIfExists(jar.resolveSibling(jar.getFileName() + ".update-attempt"));
        } catch (Exception ex) {
            log.debug("daemon-update: could not remove the attempt marker: {}", ex.toString());
        }
    }

    /** A failed update leaves no staged jar behind for the next launcher start to swap in. */
    private static void discardStaged(Path staged) {
        if (staged == null) {
            return;
        }
        try {
            Files.deleteIfExists(staged);
        } catch (IOException ex) {
            log.warn("daemon-update: could not remove the staged jar {}: {}", staged, ex.toString());
        }
    }

    private static String describeFailure(Throwable ex) {
        Throwable t = ex;
        while (t != null) {
            if (t.getMessage() != null && !t.getMessage().isBlank()) {
                return t.getClass().getSimpleName() + ": " + t.getMessage();
            }
            t = t.getCause();
        }
        return ex.toString();
    }

    private void pauseJobAcquisition() {
        AsyncExecutor executor = asyncExecutor();
        if (executor != null && executor.isActive()) {
            // Stops taking new timer starts and async jobs. Jobs already running finish.
            // Due timers stay in the database and fire after the new process boots.
            executor.shutdown();
        }
    }

    private void resumeJobAcquisition() {
        AsyncExecutor executor = asyncExecutor();
        if (executor != null && !executor.isActive()) {
            executor.start();
        }
    }

    private AsyncExecutor asyncExecutor() {
        if (processEngine.getProcessEngineConfiguration() instanceof ProcessEngineConfigurationImpl cfg) {
            return cfg.getAsyncExecutor();
        }
        return null;
    }

    /**
     * Kills every run that still has a node in flight, Designer Runs included, through the same
     * path as the control plane's {@code cancel-instance}: containers reaped, the run deleted and
     * reported CANCELED, history kept. Its wait-states are discarded so the next boot does not
     * hydrate them. A kill that fails is logged and skipped — the swap still goes ahead, and that
     * run's wait-state is discarded as an orphan on the next boot. Never retries one run twice.
     */
    void killRunningInstances() {
        Set<String> attempted = new HashSet<>();
        int killed = 0;
        for (int pass = 0; pass < KILL_PASSES; pass++) {
            Set<String> running = dispatchRegistry.processInstancesInFlight();
            running.removeAll(attempted);
            if (running.isEmpty()) {
                break;
            }
            // Operator-facing: the FE shows this under "Updating…" instead of a bare spinner.
            DaemonBuild.updateDetail("stopping " + running.size() + " running instance(s)");
            for (String processInstanceId : running) {
                attempted.add(processInstanceId);
                try {
                    if (runTermination.terminate(processInstanceId, KILL_REASON).terminated()) {
                        killed++;
                    }
                    dispatchRegistry.discardRun(processInstanceId, KILL_REASON);
                } catch (RuntimeException ex) {
                    log.warn("daemon-update: could not kill instance {}: {}", processInstanceId, ex.toString());
                }
            }
        }
        DaemonBuild.updateDetail(null);
        if (killed > 0) {
            log.info("daemon-update: killed {} running instance(s) before the swap", killed);
        }
    }

    private String downloadVerified(Path jar, Path staged) throws Exception {
        if (backendBaseUrl.isBlank()) {
            throw new IllegalStateException("sqs.backend.base-url is empty");
        }
        String os = osToken();
        String product = jar.getFileName().toString().contains("sqs-pro") ? "sqs-pro" : "sqs";
        String manifestUrl = backendBaseUrl.replaceAll("/+$", "") + RELEASE_PREFIX + "release-manifest.json";
        HttpRequest req = releaseHttp.get(URI.create(manifestUrl), HTTP_TIMEOUT).build();
        HttpResponse<byte[]> manifestResp =
                releaseHttp.client().send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (manifestResp.statusCode() != 200) {
            throw new IOException("manifest HTTP " + manifestResp.statusCode());
        }
        releaseSignatures.verify("client release manifest", manifestResp.body(), signatureBeside(manifestUrl));
        JsonNode artifacts = objectMapper.readTree(manifestResp.body()).path("artifacts");
        JsonNode match = null;
        for (JsonNode artifact : artifacts) {
            if (!"bootjar".equals(artifact.path("kind").asText())) {
                continue;
            }
            if (!os.equals(artifact.path("os").asText())) {
                continue;
            }
            String path = artifact.path("path").asText("");
            if (path.startsWith(product + "/")) {
                match = artifact;
                break;
            }
        }
        if (match == null) {
            throw new IllegalStateException("no " + product + " " + os + " bootjar in the release manifest");
        }
        String sha = match.path("sha256").asText("");
        String path = match.path("path").asText("");
        if (sha.isBlank() || path.isBlank()) {
            throw new IllegalStateException("release artifact is missing sha256 or path");
        }
        String jarUrl = backendBaseUrl.replaceAll("/+$", "") + RELEASE_PREFIX + path;
        HttpRequest jarReq = releaseHttp.get(URI.create(jarUrl), HTTP_TIMEOUT).build();
        // Only a verified jar may ever sit at <jar>.new — the launcher swaps whatever is there.
        Path part = staged.resolveSibling(staged.getFileName() + ".part");
        HttpResponse<Path> jarResp =
                releaseHttp.client().send(jarReq, HttpResponse.BodyHandlers.ofFile(part));
        if (jarResp.statusCode() != 200) {
            Files.deleteIfExists(part);
            throw new IOException("bootjar HTTP " + jarResp.statusCode());
        }
        String got = sha256(part);
        if (!sha.equalsIgnoreCase(got)) {
            Files.deleteIfExists(part);
            throw new IOException("bootjar sha256 mismatch");
        }
        Files.move(part, staged, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return sha;
    }

    /**
     * {@code <manifest>.sig.v2} — the current key. {@code null} when the origin serves
     * none (the verifier then refuses, because signing is required). The legacy
     * {@code .sig} file is never requested.
     */
    private byte[] signatureBeside(String manifestUrl) throws Exception {
        HttpRequest req = releaseHttp.get(URI.create(manifestUrl + ".sig.v2"), HTTP_TIMEOUT).build();
        HttpResponse<byte[]> resp =
                releaseHttp.client().send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() == 404) {
            return null;
        }
        if (resp.statusCode() != 200) {
            throw new IOException("manifest signature HTTP " + resp.statusCode());
        }
        byte[] body = resp.body();
        if (body == null || body.length == 0 || body.length > 4096) {
            throw new IOException("manifest signature is missing or oversized");
        }
        return body;
    }

    public static Path runningJar() {
        String loc = DaemonUpdateService.class.getProtectionDomain().getCodeSource().getLocation().toString();
        return jarPathFrom(loc, System.getProperty("java.class.path", ""));
    }

    /**
     * The boot jar on disk. Classes load from inside it, so the code source reads
     * {@code jar:nested:/…/sqs.jar/!BOOT-INF/classes/!/}; resolving that as-is gave a read-only
     * nested-jar path, and every update died writing the download beside it.
     */
    static Path jarPathFrom(String codeSource, String classPath) {
        String loc = codeSource;
        if (loc.startsWith("jar:")) {
            loc = loc.substring(4);
        }
        int bang = loc.indexOf('!');
        if (bang > 0) {
            loc = loc.substring(0, bang);
        }
        if (loc.startsWith("nested:")) {
            loc = "file:" + loc.substring("nested:".length());
        }
        while (loc.endsWith("/")) {
            loc = loc.substring(0, loc.length() - 1);
        }
        if (loc.startsWith("file:") && loc.endsWith(".jar")) {
            return Path.of(URI.create(loc));
        }
        String first = classPath.split(java.io.File.pathSeparator, 2)[0];
        if (first.endsWith(".jar")) {
            return Path.of(first).toAbsolutePath();
        }
        throw new IllegalStateException("daemon is not running from a boot jar: " + codeSource);
    }

    /**
     * {@code null} when the recorded attempt landed (target == running) or cannot be judged;
     * otherwise the operator-facing rollback message. A mismatch can only mean a launcher
     * restored the previous jar after the new one failed to come up.
     */
    static String attemptRollbackMessage(String targetSha, String runningSha) {
        if (targetSha == null || targetSha.isBlank() || runningSha == null || runningSha.isBlank()) {
            return null;
        }
        return targetSha.equalsIgnoreCase(runningSha) ? null
                : "the update to build " + abbrev(targetSha) + " did not come up; the previous build was restored";
    }

    private static String abbrev(String sha) {
        return sha == null ? "?" : sha.substring(0, Math.min(12, sha.length()));
    }

    private static String osToken() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            return "windows";
        }
        if (os.contains("mac")) {
            return "darwin";
        }
        return "linux";
    }

    static String sha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0) {
                digest.update(buf, 0, n);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
