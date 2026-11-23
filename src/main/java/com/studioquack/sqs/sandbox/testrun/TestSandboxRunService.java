package com.studioquack.sqs.sandbox.testrun;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.sandbox.egress.EgressReadiness;
import com.studioquack.sqs.sandbox.egress.SandboxDns;
import com.studioquack.sqs.sandbox.egress.SandboxEgressManager;
import com.studioquack.sqs.sandbox.helperchan.HelperJob;
import com.studioquack.sqs.sandbox.helperchan.HelperPipeChannel;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter;
import com.studioquack.sqs.sandbox.helperchan.HelperRequestRouter.RoutedResponse;
import com.studioquack.sqs.sandbox.helperchan.HelperResult;
import com.studioquack.sqs.sandbox.oci.CgroupCaps;
import com.studioquack.sqs.sandbox.oci.HardeningArgs;
import com.studioquack.sqs.sandbox.oci.HelperBroker;
import com.studioquack.sqs.sandbox.oci.OciBuildContextStore;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService;
import com.studioquack.sqs.sandbox.oci.OciImageBuildService.SyncBuildResult;
import com.studioquack.sqs.sandbox.oci.SandboxNetworkEnsure;
import com.studioquack.sqs.sandbox.oci.SandboxNetworks;
import com.studioquack.sqs.sandbox.oci.ShExec;
import com.studioquack.sqs.sandbox.oci.SandboxOwnership;
import com.studioquack.sqs.sandbox.oci.StdoutMarkerParser;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulLauncher;
import com.studioquack.sqs.sandbox.testrun.TestSandboxRunCodec.StagedFile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Daemon-owned test image build+run. The helper router calls {@link #buildRun} for {@code POST
 * /v1/helper/test/build-run}.
 */
@Slf4j
@Component
public class TestSandboxRunService {

    /**
     * Listen probe: succeed only when the in-instance app returns any HTTP status.
     * Connection-refused / no client must NOT look like listening (curl present ≠ app up).
     */
    static final String LISTEN_SCRIPT = String.join("\n",
            "code=000",
            "if command -v curl >/dev/null 2>&1; then",
            "  code=$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 1 --max-time 2"
                    + " http://127.0.0.1:8080/invoke || true)",
            "elif command -v python3 >/dev/null 2>&1; then",
            "  code=$(python3 -c 'import urllib.request,urllib.error",
            "try:",
            "  urllib.request.urlopen(\"http://127.0.0.1:8080/invoke\", timeout=2)",
            "  print(200)",
            "except urllib.error.HTTPError as e:",
            "  print(e.code)",
            "except Exception:",
            "  print(0)')",
            "elif command -v wget >/dev/null 2>&1; then",
            "  wget -q -O /dev/null --timeout=2 http://127.0.0.1:8080/invoke && code=200 || code=000",
            "fi",
            "case \"$code\" in [1-9][0-9][0-9]) exit 0;; esac",
            "exit 1");

    private static final Duration CREATE_TIMEOUT = Duration.ofSeconds(120);
    private static final Duration SHORT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration PLANT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration RM_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration RMI_TIMEOUT = Duration.ofSeconds(30);

    private final OciCli cli;
    private final OciBuildContextStore contextStore;
    private final OciImageBuildService buildService;
    private final HelperRequestRouter helperRouter;
    private final SandboxEgressManager egressManager;
    private final SandboxOwnership ownership;
    private final TestBuildProperties props;
    private final List<String> dnsServers;
    private final BrokerStarter brokerStarter;
    private final Semaphore permits;
    private final TestSandboxRunCodec codec;

    /** Production + test constructor. {@code environment} null → {@link SandboxDns#defaults()}. */
    @Autowired
    public TestSandboxRunService(OciCli cli,
                                 OciBuildContextStore contextStore,
                                 OciImageBuildService buildService,
                                 @Lazy HelperRequestRouter helperRouter,
                                 @Nullable SandboxEgressManager egressManager,
                                 SandboxOwnership ownership,
                                 TestBuildProperties props,
                                 ObjectMapper mapper,
                                 @Nullable Environment environment,
                                 @Nullable BrokerStarter brokerStarter) {
        this.cli = cli;
        this.contextStore = contextStore;
        this.buildService = buildService;
        this.helperRouter = helperRouter;
        this.egressManager = egressManager;
        this.ownership = ownership == null ? SandboxOwnership.unowned() : ownership;
        this.props = props;
        this.dnsServers = environment == null
                ? SandboxDns.defaults()
                : SandboxDns.parse(environment.getProperty(
                        SandboxDns.CONFIG_KEY, SandboxDns.DEFAULT_SERVERS));
        this.brokerStarter = brokerStarter != null ? brokerStarter : new DefaultBrokerStarter(cli);
        this.permits = new Semaphore(props == null ? 2 : Math.max(0, props.getMaxConcurrent()));
        this.codec = new TestSandboxRunCodec(mapper, props);
    }

    /**
     * Gate for the helper router: blank/null allow-list disables the feature. An entry with
     * {@code :} is exact; without a tag it matches that repo with any tag.
     */
    public boolean allowsCaller(String callerImageRef) {
        if (callerImageRef == null || callerImageRef.isBlank()) {
            return false;
        }
        String raw = props == null ? null : props.getAllowedImages();
        if (raw == null || raw.isBlank()) {
            return false;
        }
        String ref = callerImageRef.trim();
        for (String part : raw.split(",")) {
            String entry = part.trim();
            if (entry.isEmpty()) {
                continue;
            }
            if (entry.indexOf(':') >= 0) {
                if (entry.equals(ref)) {
                    return true;
                }
            } else if (entry.equals(ref) || ref.startsWith(entry + ":")) {
                return true;
            }
        }
        return false;
    }

    /** Synchronous build+run. Validation 400s; infra failures are a 200 envelope with {@code error} set. */
    public RoutedResponse buildRun(HelperJob caller, byte[] body) {
        if (!permits.tryAcquire()) {
            return codec.envelope(false, "", false, false, false, null, "", "", -1,
                    List.of(), List.of(), null, "test build-run saturated");
        }
        String buildId = null;
        String childToken = null;
        String containerId = null;
        String ref = null;
        HelperPipeChannel channel = null;
        boolean imageBuilt = false;
        try {
            JsonNode root = codec.parseObject(body);
            if (root == null) {
                return codec.json400("body must be a JSON object");
            }
            List<StagedFile> files = codec.parseFiles(root);
            int timeoutSeconds = codec.capRunTimeout(codec.intOr(root, "timeout_seconds", 120));
            int buildWallSecs = codec.capBuildWall(codec.intOr(root, "build_wall_clock_seconds", 600));

            buildId = "test-" + UUID.randomUUID();
            for (StagedFile f : files) {
                byte[] bytes = f.content();
                contextStore.writeFile(buildId, f.path(), bytes);
                // Log only buildId/path/size — never file contents.
                log.info("testbuild: staged buildId={} path={} sizeBytes={}", buildId, f.path(), bytes.length);
            }

            HelperJob child = codec.childJob(buildId, root, caller);
            childToken = helperRouter.register(child);
            log.info("testbuild: registered child buildId={} files={}", buildId, files.size());

            String callerEnvId = TestSandboxRunCodec.str(TestSandboxRunCodec.ecGet(caller, "envId"));
            ref = "sq-local/node-test-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
            SyncBuildResult sync = buildService.buildSync(callerEnvId, buildId, ref, buildWallSecs);
            log.info("testbuild: built buildId={} ref={} success={}", buildId, ref, sync.success());
            if (!sync.success()) {
                return codec.envelope(false, codec.cap(sync.log()), false, false, false, null, "", "", -1,
                        List.of(), List.of(), null, sync.error() == null ? "" : sync.error());
            }
            imageBuilt = true;

            RoutedResponse gated = egressGate(sync);
            if (gated != null) {
                return gated;
            }

            SandboxNetworkEnsure.ensure(cli.cliPath(), SandboxNetworks.JOBS, (argv, timeout) -> {
                OciCli.Result r = cli.run(argv, timeout);
                return new SandboxNetworkEnsure.ExecResult(r.ok(), r.exitCode(), r.stdout(), r.stderr());
            });

            HardeningArgs hardened = hardeningArgs(ref, childToken, callerEnvId, timeoutSeconds);
            // Bearer values ride the podman child environment, not -e NAME=value.
            OciCli.Result created = cli.run(hardened.toArgv(), null, CREATE_TIMEOUT, hardened.containerEnv());
            // `run -d` stderr is the runtime's own: a cgroup controller crun cannot open is
            // dropped and the create retried, at most once per controller.
            while (!created.ok() && CgroupCaps.noteDetachedStartFailure(created.stderr())) {
                created = cli.run(hardened.toArgv(), null, CREATE_TIMEOUT, hardened.containerEnv());
            }
            if (!created.ok() || created.out().isBlank()) {
                return codec.envelope(true, codec.cap(sync.log()), true, false, false, null, "", "", -1,
                        List.of(), List.of(), null, created.firstErrLine());
            }
            containerId = created.out();
            log.info("testbuild: started buildId={} ref={} container={}",
                    buildId, ref, TestSandboxRunCodec.shortId(containerId));

            boolean brokerReady = false;
            OciCli.Result py = cli.run(List.of(cli.cliPath(), "exec", containerId, "sh", "-c",
                    "command -v python3"), PLANT_TIMEOUT);
            if (py.ok()) {
                channel = brokerStarter.start(cli.cliPath(), containerId, helperRouter);
                brokerReady = channel != null;
            }

            ListenOutcome listen = waitForListen(containerId, timeoutSeconds);
            if (listen.timedOutRunning()) {
                String logs = codec.cap(cli.run(List.of(cli.cliPath(), "logs", containerId),
                        Duration.ofSeconds(timeoutSeconds)).stdout());
                return codec.envelope(true, codec.cap(sync.log()), true, false, brokerReady, null, logs, "", -1,
                        TestSandboxRunCodec.copyList(child.mockInvocations()),
                        TestSandboxRunCodec.copyList(child.logs()),
                        TestSandboxRunCodec.failureMap(child),
                        "app did not listen within timeout");
            }

            Map<String, Object> output;
            String stdout;
            String stderr;
            int exitCode;
            if (!listen.listening()) {
                OciCli.Result logs = cli.run(List.of(cli.cliPath(), "logs", containerId),
                        Duration.ofSeconds(Math.max(1, timeoutSeconds)));
                stdout = codec.cap(logs.stdout());
                stderr = codec.cap(logs.stderr());
                exitCode = inspectExitCode(containerId);
                output = new LinkedHashMap<>(StdoutMarkerParser.parse(logs.stdout()));
            } else {
                String requestJson = codec.buildRequestJson(root, child, childToken, brokerReady);
                // Plant-and-run wrapper: stdin already carries the request JSON, and a
                // quote-bearing sh -c argv script shatters on Windows hosts (see ShExec).
                List<String> argv = List.of(cli.cliPath(), "exec", "-i", containerId, "sh", "-c",
                        ShExec.plantAndRun(StatefulLauncher.bootstrap(), "sq_launcher"));
                OciCli.Result invoked = cli.run(argv, requestJson.getBytes(StandardCharsets.UTF_8),
                        Duration.ofSeconds(Math.max(1, timeoutSeconds)));
                stdout = codec.cap(invoked.stdout());
                stderr = codec.cap(invoked.stderr());
                exitCode = invoked.ok() ? 0 : invoked.exitCode();
                output = codec.parseResponseBody(invoked.stdout());
            }
            output = mergeOutputs(childToken, output);
            return codec.envelope(true, codec.cap(sync.log()), true, listen.listening(), brokerReady, output,
                    stdout, stderr, exitCode,
                    TestSandboxRunCodec.copyList(child.mockInvocations()),
                    TestSandboxRunCodec.copyList(child.logs()),
                    TestSandboxRunCodec.failureMap(child), "");
        } catch (IllegalArgumentException e) {
            return codec.json400(e.getMessage() == null ? "invalid request" : e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return codec.envelope(imageBuilt, "", imageBuilt, false, false, null, "", "", -1,
                    List.of(), List.of(), null, "test build-run interrupted");
        } catch (Exception e) {
            log.warn("testbuild: infra failure buildId={} ref={} container={}: {}",
                    buildId, ref, TestSandboxRunCodec.shortId(containerId),
                    e.getClass().getSimpleName() + ": " + TestSandboxRunCodec.str(e.getMessage()));
            return codec.envelope(imageBuilt, "", imageBuilt, false, false, null, "", "", -1,
                    List.of(), List.of(), null, TestSandboxRunCodec.str(e.getMessage()));
        } finally {
            cleanup(childToken, channel, containerId, ref, buildId);
            permits.release();
        }
    }

    private RoutedResponse egressGate(SyncBuildResult sync) {
        if (egressManager == null) {
            return null;
        }
        EgressReadiness readiness = egressManager.readiness();
        if (readiness == null || readiness.internetAllowed()) {
            return null;
        }
        return codec.envelope(true, codec.cap(sync.log()), false, false, false, null, "", "", -1,
                List.of(), List.of(), null,
                TestSandboxRunCodec.str(readiness.refusalMessage()));
    }

    private HardeningArgs hardeningArgs(String ref, String childToken, String callerEnvId, int timeoutSeconds) {
        int ramMb = TestSandboxRunCodec.parseRamMb(props.getRunMemory());
        double cpus = props.getRunCpus() > 0 ? props.getRunCpus() : 1.0;
        return HardeningArgs.builder(cli.cliPath(), null, ref)
                .detached(true)
                .autoRemove(false)
                .ownership(ownership)
                .role(HardeningArgs.ROLE_JOB)
                .egress(EgressPolicy.OPEN)
                .limits(SandboxLimits.of(ramMb, timeoutSeconds * 1000L))
                .cpus(cpus)
                .dnsServers(dnsServers)
                .env("SQ_APP_PORT", "8080")
                .env("SQ_APP_PATH", "/invoke")
                .env("SQ_STATEFUL_RUNNER", "true")
                .env("SQ_HELPER_SOCKET", HelperBroker.SOCKET_PATH)
                .env("SQ_HELPER_BEARER", childToken)
                .env("SQVM_HELPER_TOKEN", childToken)
                .env("SQ_HELPER_CALLER_ENV_ID", callerEnvId)
                .build();
    }

    private ListenOutcome waitForListen(String cid, int timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Math.max(1, timeoutSeconds));
        while (System.nanoTime() < deadline) {
            OciCli.Result inspect = cli.run(List.of(cli.cliPath(), "inspect", "-f", "{{.State.Running}}", cid),
                    SHORT_TIMEOUT);
            if (!"true".equals(inspect.out())) {
                return ListenOutcome.stateless();
            }
            // The listen probe rides stdin (sh -s): its double quotes would be shattered by the Win32
            // command-line round-trip on Windows hosts.
            OciCli.Result probe = cli.run(ShExec.stdinArgv(cli.cliPath(), cid),
                    ShExec.utf8(LISTEN_SCRIPT), SHORT_TIMEOUT);
            if (probe.ok() || TestSandboxRunCodec.looksLikeHttpCode(probe.stdout())) {
                return ListenOutcome.stateful();
            }
            TimeUnit.MILLISECONDS.sleep(200);
        }
        return ListenOutcome.timeout();
    }

    private int inspectExitCode(String cid) {
        OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", "-f", "{{.State.ExitCode}}", cid),
                SHORT_TIMEOUT);
        try {
            return Integer.parseInt(r.out());
        } catch (RuntimeException e) {
            return -1;
        }
    }

    private Map<String, Object> mergeOutputs(String helperToken, Map<String, Object> base) {
        Map<String, Object> out = base == null ? new LinkedHashMap<>() : new LinkedHashMap<>(base);
        if (helperToken == null) {
            return out;
        }
        Optional<HelperJob> job = helperRouter.lookup(helperToken);
        if (job.isPresent() && job.get().result() != null) {
            HelperResult r = job.get().result();
            if (r.items() != null) {
                for (HelperResult.ResultItem item : r.items()) {
                    if (item != null && item.port() != null) {
                        out.put(item.port(), item.value());
                    }
                }
            }
            if ((r.items() == null || r.items().isEmpty()) && r.payload() != null) {
                out.putAll(r.payload());
            }
        }
        return out;
    }

    private void cleanup(String childToken, HelperPipeChannel channel, String containerId,
                         String ref, String buildId) {
        if (childToken != null) {
            helperRouter.revoke(childToken);
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception ignore) {
                // best-effort
            }
        }
        if (containerId != null && !containerId.isBlank()) {
            cli.run(List.of(cli.cliPath(), "rm", "-f", containerId), RM_TIMEOUT);
        }
        if (ref != null && !ref.isBlank()) {
            cli.run(List.of(cli.cliPath(), "rmi", "-f", ref), RMI_TIMEOUT);
        }
        if (buildId != null) {
            contextStore.deleteContext(buildId);
        }
    }

    private record ListenOutcome(boolean listening, boolean timedOutRunning) {
        static ListenOutcome stateful() { return new ListenOutcome(true, false); }
        static ListenOutcome stateless() { return new ListenOutcome(false, false); }
        static ListenOutcome timeout() { return new ListenOutcome(false, true); }
    }
}
