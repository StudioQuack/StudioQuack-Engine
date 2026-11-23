package com.studioquack.sqs.deps;

import com.studioquack.sqs.deps.DependencyInstaller.DepsConfig;
import com.studioquack.sqs.deps.DependencyInstaller.InstallRunner;
import com.studioquack.sqs.sandbox.EgressPolicy;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DependencyInstaller}: the {@code queued → installing → installed/failed}
 * state machine, the arg-safety of the pip/npm argv builders, the egress gate, unsupported
 * languages, and idempotency. The package manager is faked via {@link InstallRunner} so no real
 * process (or network) is spawned.
 */
class DependencyInstallerTest {

    @TempDir
    Path tmp;

    private DepsConfig config(EgressPolicy egress, String npm) {
        return new DepsConfig(true, "python3", npm, tmp, Duration.ofSeconds(30), egress, 1);
    }

    private static ContainerRuntime containerRuntime() {
        ContainerRuntime rt = mock(ContainerRuntime.class);
        when(rt.available()).thenReturn(true);
        when(rt.cli()).thenReturn("podman");
        when(rt.isolationEnforced()).thenReturn(false);
        return rt;
    }

    private DependencyInstaller hardenedInstaller() {
        return new DependencyInstaller(new DependencyManifestHolder(), mock(DependencyStatusReporter.class),
                config(EgressPolicy.OPEN, "npm"),
                (argv, timeout, env) -> new CliRunner.Result(0, "", "", false, 1),
                Executors.newSingleThreadExecutor(), containerRuntime());
    }

    private static int pair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return i;
            }
        }
        return -1;
    }

    /** Records every (name:state) transition the installer reports, in order. */
    private DependencyStatusReporter recordingReporter(List<String> events) {
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.report(any(), anyString(), any(), any(), any(), any())).thenAnswer(inv -> {
            DependencyEntry e = inv.getArgument(0);
            String state = inv.getArgument(1);
            events.add(e.name() + ":" + state);
            return true;
        });
        return reporter;
    }

    private DependencyEntry dep(String language, String name, String version) {
        return new DependencyEntry(language, name, version, "queued", false, null, null, null, null);
    }

    private void reconcileAndAwait(DependencyInstaller installer, ExecutorService exec, DependencyEntry... deps)
            throws Exception {
        installer.reconcile(List.of(deps));
        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS), "install tasks completed");
    }

    // ------------------------------------------------------------------
    // State machine
    // ------------------------------------------------------------------

    @Test
    void pythonInstallSuccess_reportsInstallingThenInstalled() throws Exception {
        List<String> events = new ArrayList<>();
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        AtomicReference<String> installedVersion = new AtomicReference<>();
        AtomicReference<String> installedDigest = new AtomicReference<>();
        when(reporter.report(any(), anyString(), any(), any(), any(), any())).thenAnswer(inv -> {
            DependencyEntry e = inv.getArgument(0);
            String state = inv.getArgument(1);
            events.add(e.name() + ":" + state);
            if ("installed".equals(state)) {
                installedVersion.set(inv.getArgument(2));
                installedDigest.set(inv.getArgument(3));
            }
            return true;
        });
        InstallRunner runner = (argv, timeout, env) ->
                new CliRunner.Result(0, "Successfully installed requests-2.31.0\n", "", false, 12);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec,
                containerRuntime());

        reconcileAndAwait(installer, exec, dep("python", "requests", ""));

        assertEquals(List.of("requests:installing", "requests:installed"), events);
        assertEquals("2.31.0", installedVersion.get(), "resolved version parsed from pip stdout");
        assertNotNull(installedDigest.get());
        assertTrue(installedDigest.get().startsWith("sha256:"));
    }

    @Test
    void pythonInstallFailure_reportsFailedWithBoundedStderr() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicReference<String> failMsg = new AtomicReference<>();
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.report(any(), anyString(), any(), any(), any(), any())).thenAnswer(inv -> {
            events.add(((DependencyEntry) inv.getArgument(0)).name() + ":" + inv.getArgument(1));
            if ("failed".equals(inv.getArgument(1))) {
                failMsg.set(inv.getArgument(5));
            }
            return true;
        });
        InstallRunner runner = (argv, timeout, env) ->
                new CliRunner.Result(1, "", "ERROR: No matching distribution found for nope==9.9.9", false, 5);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec,
                containerRuntime());

        reconcileAndAwait(installer, exec, dep("python", "nope", "9.9.9"));

        assertEquals(List.of("nope:installing", "nope:failed"), events);
        assertTrue(failMsg.get().contains("No matching distribution"), "stderr surfaced in failure message");
    }

    @Test
    void timeout_reportsFailed() throws Exception {
        List<String> events = new ArrayList<>();
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> new CliRunner.Result(-1, "", "", true, 30_000);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec,
                containerRuntime());

        reconcileAndAwait(installer, exec, dep("python", "slowpkg", ""));

        assertEquals(List.of("slowpkg:installing", "slowpkg:failed"), events);
    }

    // ------------------------------------------------------------------
    // Egress + validation + unsupported languages
    // ------------------------------------------------------------------

    @Test
    void egressNone_marksFailedWithoutRunning() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicBoolean ran = new AtomicBoolean(false);
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> {
            ran.set(true);
            return new CliRunner.Result(0, "", "", false, 1);
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.NONE, "npm"), runner, exec);

        reconcileAndAwait(installer, exec, dep("python", "requests", ""));

        assertEquals(List.of("requests:installing", "requests:failed"), events);
        assertFalse(ran.get(), "no package manager runs when egress is disabled");
    }

    @Test
    void maliciousVersion_rejectedByArgSafety_withoutRunning() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicBoolean ran = new AtomicBoolean(false);
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> {
            ran.set(true);
            return new CliRunner.Result(0, "", "", false, 1);
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec);

        // A shell-injection attempt in the version must be rejected before any exec.
        reconcileAndAwait(installer, exec, dep("python", "requests", "1.0.0; rm -rf /"));

        assertEquals(List.of("requests:installing", "requests:failed"), events);
        assertFalse(ran.get(), "an invalid version never reaches the package manager");
    }

    @Test
    void maliciousName_rejectedByArgSafety() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicBoolean ran = new AtomicBoolean(false);
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> {
            ran.set(true);
            return new CliRunner.Result(0, "", "", false, 1);
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec);

        // A leading '-' would be an option-injection attempt; the validator rejects it.
        reconcileAndAwait(installer, exec, dep("python", "--index-url=http://evil", ""));

        assertEquals(List.of("--index-url=http://evil:installing", "--index-url=http://evil:failed"), events);
        assertFalse(ran.get());
    }

    @Test
    void unsupportedLanguage_marksFailed() throws Exception {
        List<String> events = new ArrayList<>();
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> new CliRunner.Result(0, "", "", false, 1);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec);

        reconcileAndAwait(installer, exec, dep("ruby", "rails", ""));

        assertEquals(List.of("rails:installing", "rails:failed"), events);
    }

    @Test
    void javascript_withoutNpm_marksFailed() throws Exception {
        List<String> events = new ArrayList<>();
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> new CliRunner.Result(0, "", "", false, 1);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        // A binary that cannot resolve on PATH → npmAvailable() is false.
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter,
                config(EgressPolicy.OPEN, "__no_such_npm_binary__"), runner, exec);

        reconcileAndAwait(installer, exec, dep("javascript", "lodash", ""));

        assertEquals(List.of("lodash:installing", "lodash:failed"), events);
    }

    @Test
    void installDisabled_isNoOp() throws Exception {
        List<String> events = new ArrayList<>();
        DependencyStatusReporter reporter = recordingReporter(events);
        InstallRunner runner = (argv, timeout, env) -> new CliRunner.Result(0, "", "", false, 1);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DepsConfig disabled = new DepsConfig(false, "python3", "npm", tmp, Duration.ofSeconds(5),
                EgressPolicy.OPEN, 1);
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, disabled, runner, exec);

        reconcileAndAwait(installer, exec, dep("python", "requests", ""));

        assertTrue(events.isEmpty(), "disabled pipeline reports nothing");
        verify(reporter, never()).report(any(), anyString(), any(), any(), any(), any());
    }

    @Test
    void idempotent_installedTupleNotReinstalled() throws Exception {
        AtomicReference<Integer> runCount = new AtomicReference<>(0);
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.report(any(), anyString(), any(), any(), any(), any())).thenReturn(true);
        InstallRunner runner = (argv, timeout, env) -> {
            runCount.set(runCount.get() + 1);
            return new CliRunner.Result(0, "Successfully installed requests-2.31.0\n", "", false, 1);
        };
        // A single-thread pool that we DON'T shut down between reconciles.
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec,
                containerRuntime());

        installer.reconcile(List.of(dep("python", "requests", "2.31.0")));
        // Wait for the first install to finish (it records the tuple as installed).
        waitForRunCount(runCount, 1);
        // A second reconcile with the same tuple must NOT reinstall.
        installer.reconcile(List.of(dep("python", "requests", "2.31.0")));
        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, runCount.get(), "an already-installed tuple is not reinstalled on the next reconcile");
    }

    @Test
    void installedRowWithNullDigest_doesNotAbortQueuedRows() throws Exception {
        AtomicInteger runs = new AtomicInteger();
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.report(any(), anyString(), any(), any(), any(), any())).thenReturn(true);
        InstallRunner runner = (argv, timeout, env) -> {
            runs.incrementAndGet();
            return new CliRunner.Result(0, "Successfully installed leftpad-0.1.2\n", "", false, 1);
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec,
                containerRuntime());
        DependencyEntry installed = new DependencyEntry(
                "python", "numpy", "2.4.6", "installed", false, null, null, null, null);

        installer.reconcile(List.of(installed, dep("python", "leftpad", "")));
        exec.shutdown();
        assertTrue(exec.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(1, runs.get(), "a null digest on an installed row must not skip the queued rows");
    }

    private void waitForRunCount(AtomicReference<Integer> counter, int target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (counter.get() < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }

    // ------------------------------------------------------------------
    // Pure arg-safety helpers
    // ------------------------------------------------------------------

    @Test
    void runtimeUnavailable_failsClosedWithoutHostExec() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicReference<String> failMsg = new AtomicReference<>();
        AtomicBoolean ran = new AtomicBoolean(false);
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.report(any(), anyString(), any(), any(), any(), any())).thenAnswer(inv -> {
            events.add(((DependencyEntry) inv.getArgument(0)).name() + ":" + inv.getArgument(1));
            if ("failed".equals(inv.getArgument(1))) {
                failMsg.set(inv.getArgument(5));
            }
            return true;
        });
        InstallRunner runner = (argv, timeout, env) -> {
            ran.set(true);
            return new CliRunner.Result(0, "", "", false, 1);
        };
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = new DependencyInstaller(
                new DependencyManifestHolder(), reporter, config(EgressPolicy.OPEN, "npm"), runner, exec);

        reconcileAndAwait(installer, exec, dep("python", "requests", "2.31.0"));

        assertEquals(List.of("requests:installing", "requests:failed"), events);
        assertFalse(ran.get(), "no host pip/npm when the container runtime is absent");
        assertTrue(failMsg.get().contains("container runtime"), failMsg.get());
    }

    @Test
    void containerPipArgv_isHardenedOnTheBuildNetwork_andPinsTheSpec() throws Exception {
        DependencyInstaller installer = hardenedInstaller();
        java.util.Map<String, String> env = java.util.Map.of("HOME", "/tmp", "PIP_CACHE_DIR", "/tmp/.pipcache");
        List<String> argv = installer.buildContainerPipArgv("podman", "python:3.12-slim",
                "/tmp/deps/python", "requests", "2.31.0", "sq-dep", env);
        assertEquals("podman", argv.get(0));
        assertTrue(pair(argv, "--network", "sqs-build") >= 0, argv.toString());
        assertFalse(argv.contains("bridge"));
        assertTrue(pair(argv, "--cap-drop", "ALL") >= 0);
        assertTrue(pair(argv, "--security-opt", "no-new-privileges") >= 0);
        assertTrue(pair(argv, "--pids-limit", "1024") >= 0);
        assertTrue(pair(argv, "--memory", "2048m") >= 0);
        assertTrue(pair(argv, "--memory-swap", "2048m") >= 0);
        assertTrue(pair(argv, "--cpus", "2") >= 0);
        assertTrue(pair(argv, "-e", "HOME") >= 0);
        assertTrue(pair(argv, "-e", "PIP_CACHE_DIR") >= 0);
        assertFalse(argv.stream().anyMatch(a -> a.contains("/tmp/.pipcache") || "HOME=/tmp".equals(a)));
        assertTrue(argv.contains("/tmp/deps/python:/target:rw"));
        int dashIdx = argv.indexOf("--");
        assertEquals("requests==2.31.0", argv.get(argv.size() - 1));
        assertEquals(argv.size() - 1, dashIdx + 1);
        assertFalse(argv.contains("--upgrade"));
        assertFalse(argv.contains("--read-only"));
        assertFalse(argv.contains("--user"));
    }

    @Test
    void containerPipArgv_latestAddsUpgrade() throws Exception {
        List<String> argv = hardenedInstaller().buildContainerPipArgv("podman", "python:3.12-slim",
                "/tmp/deps/python", "requests", "", "sq-dep", java.util.Map.of("HOME", "/tmp"));
        assertTrue(argv.contains("--upgrade"));
        assertEquals("requests", argv.get(argv.size() - 1));
    }

    @Test
    void containerNpmArgv_usesPrefixAndBuildNetwork() throws Exception {
        List<String> argv = hardenedInstaller().buildContainerNpmArgv("podman", "node:22-slim",
                "/tmp/deps/node", "lodash", "4.17.21", "sq-dep", java.util.Map.of("HOME", "/tmp"));
        assertTrue(pair(argv, "--network", "sqs-build") >= 0);
        assertTrue(argv.contains("--prefix"));
        assertEquals("lodash@4.17.21", argv.get(argv.size() - 1));
        assertFalse(argv.contains("bridge"));
    }

    @Test
    void parseInstalledVersion_handlesHyphenatedNames() {
        String stdout = "Successfully installed python-dateutil-2.9.0 six-1.16.0";
        assertEquals("2.9.0", DependencyInstaller.parseInstalledVersion(stdout, "python-dateutil"));
        assertEquals("1.16.0", DependencyInstaller.parseInstalledVersion(stdout, "six"));
    }

    @Test
    void pep503_normalisesNameForms() {
        assertEquals("foo-bar", DependencyInstaller.pep503("Foo_Bar"));
        assertEquals("foo-bar", DependencyInstaller.pep503("foo.bar"));
        assertEquals("lodash", DependencyInstaller.pep503("@types/lodash".replace("@types/", "")));
        assertEquals("requests", DependencyInstaller.pep503("requests[security]"));
    }

    @Test
    void normaliseVersion_stripsLeadingDoubleEquals() {
        assertEquals("2.3.3", DependencyInstaller.normaliseVersion("==2.3.3"));
        assertEquals("", DependencyInstaller.normaliseVersion("  "));
        assertEquals("1.0.0", DependencyInstaller.normaliseVersion(" 1.0.0 "));
    }

    // On-disk dependency-cache discovery

    /** Records every discovered (name@version) the installer echoes via reportDiscovered. */
    private DependencyStatusReporter recordingDiscoveryReporter(List<String> discovered) {
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.reportDiscovered(any(), any(), any(), any(), any())).thenAnswer(inv -> {
            DependencyEntry e = inv.getArgument(0);
            discovered.add(e.name() + "@" + e.version());
            return true;
        });
        return reporter;
    }

    private DependencyInstaller installerWith(DependencyStatusReporter reporter, ExecutorService exec) {
        return new DependencyInstaller(new DependencyManifestHolder(), reporter,
                config(EgressPolicy.OPEN, "npm"),
                (argv, timeout, env) -> new CliRunner.Result(0, "", "", false, 1), exec);
    }

    @Test
    void discoverCachedDeps_reportsPythonDistInfoPackages() throws Exception {
        // pip --target layout under <root>/python: two COMPLETE installs (each has a RECORD, which
        // pip writes last) plus one partial/failed dist-info with NO RECORD that must be skipped.
        Path py = tmp.resolve("python");
        Files.createDirectories(py.resolve("requests-2.31.0.dist-info"));
        Files.writeString(py.resolve("requests-2.31.0.dist-info").resolve("RECORD"),
                "requests/__init__.py,sha256=abc,123\nrequests/api.py,sha256=def,456\n");
        Files.createDirectories(py.resolve("urllib3-2.2.1.dist-info"));
        Files.writeString(py.resolve("urllib3-2.2.1.dist-info").resolve("RECORD"),
                "urllib3/__init__.py,sha256=ghi,789\n");
        // Partial/failed install: dist-info without RECORD → NOT a real install.
        Files.createDirectories(py.resolve("halfbaked-9.9.9.dist-info"));

        List<String> discovered = new ArrayList<>();
        DependencyStatusReporter reporter = recordingDiscoveryReporter(discovered);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = installerWith(reporter, exec);

        installer.discoverCachedDeps();
        exec.shutdownNow();

        assertTrue(discovered.contains("requests@2.31.0"), "discovered: " + discovered);
        assertTrue(discovered.contains("urllib3@2.2.1"), "discovered: " + discovered);
        assertFalse(discovered.contains("halfbaked@9.9.9"),
                "a dist-info without RECORD is a partial install and must not be reported: " + discovered);
    }

    @Test
    void discoverCachedDeps_reportsNodePackagesIncludingScoped() throws Exception {
        // npm --prefix layout under <root>/node/node_modules, incl. a scoped @types/node.
        Path nm = tmp.resolve("node").resolve("node_modules");
        Files.createDirectories(nm.resolve("lodash"));
        Files.writeString(nm.resolve("lodash").resolve("package.json"),
                "{\n  \"name\": \"lodash\",\n  \"version\": \"4.17.21\"\n}\n");
        Files.createDirectories(nm.resolve("@types").resolve("node"));
        Files.writeString(nm.resolve("@types").resolve("node").resolve("package.json"),
                "{\"name\":\"@types/node\",\"version\":\"20.11.5\"}");
        // Noise that must be skipped (hidden dir + a package without package.json).
        Files.createDirectories(nm.resolve(".bin"));
        Files.createDirectories(nm.resolve("brokenpkg"));

        List<String> discovered = new ArrayList<>();
        DependencyStatusReporter reporter = recordingDiscoveryReporter(discovered);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = installerWith(reporter, exec);

        installer.discoverCachedDeps();
        exec.shutdownNow();

        assertTrue(discovered.contains("lodash@4.17.21"), "discovered: " + discovered);
        assertTrue(discovered.contains("@types/node@20.11.5"), "discovered: " + discovered);
        assertEquals(2, discovered.size(), "hidden dirs + package.json-less dirs are skipped: " + discovered);
    }

    @Test
    void discoverCachedDeps_runsOnlyOncePerProcess() throws Exception {
        Path di = tmp.resolve("python").resolve("requests-2.31.0.dist-info");
        Files.createDirectories(di);
        Files.writeString(di.resolve("RECORD"), "requests/__init__.py,sha256=abc,1\n");
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        when(reporter.reportDiscovered(any(), any(), any(), any(), any())).thenReturn(true);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = installerWith(reporter, exec);

        installer.discoverCachedDeps();
        installer.discoverCachedDeps(); // one-shot guard → no-op second time
        exec.shutdownNow();

        verify(reporter, times(1)).reportDiscovered(any(), any(), any(), any(), any());
    }

    @Test
    void discoverCachedDeps_emptyCacheReportsNothing() throws Exception {
        DependencyStatusReporter reporter = mock(DependencyStatusReporter.class);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        DependencyInstaller installer = installerWith(reporter, exec);

        installer.discoverCachedDeps(); // no <root>/python or <root>/node → nothing to report
        exec.shutdownNow();

        verify(reporter, never()).reportDiscovered(any(), any(), any(), any(), any());
    }

    @Test
    void jsonStringField_extractsFieldsAndNullWhenAbsent() {
        String pj = "{ \"name\": \"lodash\", \"version\": \"4.17.21\", \"main\": \"index.js\" }";
        assertEquals("lodash", DependencyInstaller.jsonStringField(pj, "name"));
        assertEquals("4.17.21", DependencyInstaller.jsonStringField(pj, "version"));
        assertNull(DependencyInstaller.jsonStringField(pj, "absent"));
    }
}
