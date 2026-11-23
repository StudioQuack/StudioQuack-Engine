package com.studioquack.sqs.maintenance;

import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.warmpool.NoopWarmer;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.secrets.inventory.ClientSecretInventoryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Uninstall must invalidate enrollment.properties so a KeepAlive relaunch cannot re-enroll a
 * deleted env and spam the server with 404/401, and it must leave the host looking uninstalled to
 * the platform installer's existing-installation guard.
 */
class UninstallServiceEnrollmentTest {

    @TempDir
    Path temp;

    /** Purge wired to a temp prefix: SQS_DATA_DIR is read from the Environment before getenv. */
    private static UninstallService serviceFor(MockEnvironment env) throws Exception {
        ClientSecretInventoryService inventory = mock(ClientSecretInventoryService.class);
        when(inventory.purgeAllOwned()).thenReturn(0);
        ContainerRuntime runtime = mock(ContainerRuntime.class);
        when(runtime.available()).thenReturn(false);
        return new UninstallService(inventory, runtime, env, new WarmPool(new NoopWarmer(), Map.of()));
    }

    @Test
    void purge_renamesEnrollmentProperties_andClearsChannelDir() throws Exception {
        Path prefix = temp.resolve("sqs");
        Path data = prefix.resolve("data");
        Path channel = data.resolve("channel");
        Path config = prefix.resolve("config");
        Files.createDirectories(channel);
        Files.createDirectories(config);
        Files.writeString(channel.resolve("identity.json"), "{\"k\":1}");
        Path enrollment = config.resolve("enrollment.properties");
        Files.writeString(enrollment, "sqs.channel.env-id=env-1\nsqs.channel.bearer=secret\n");

        ClientSecretInventoryService inventory = mock(ClientSecretInventoryService.class);
        when(inventory.purgeAllOwned()).thenReturn(0);
        ContainerRuntime runtime = mock(ContainerRuntime.class);
        when(runtime.available()).thenReturn(false);
        Environment env = new MockEnvironment();

        // Point the purge at our temp data dir via process env is awkward in unit tests —
        // UninstallService reads SQS_DATA_DIR from System.getenv. Use the stock sibling path by
        // setting the property fallback only when getenv is blank; instead we rely on resolving via
        // a subprocess-free approach: set SQS_DATA_DIR is not injectable.
        ((MockEnvironment) env).setProperty("SQS_DATA_DIR", data.toString());

        UninstallService service =
                new UninstallService(inventory, runtime, env, new WarmPool(new NoopWarmer(), Map.of()));
        UninstallService.UninstallSummary summary = service.purge(false);

        assertFalse(Files.exists(enrollment), "live enrollment.properties must be gone");
        assertTrue(Files.exists(config.resolve("enrollment.properties.uninstalled")));
        assertTrue(Files.readString(config.resolve("enrollment.properties.uninstalled"))
                .contains("INVALIDATED by uninstall"));
        assertFalse(Files.exists(channel), "channel data dir must be cleared");
        assertTrue(summary.notes().stream().anyMatch(n -> n.contains("enrollment config invalidated")));
    }

    @Test
    void purge_clearsLegacyWgConfAndPrivateKeyDir() throws Exception {
        Path prefix = temp.resolve("sqs-wg");
        Path data = prefix.resolve("data");
        Path wg = data.resolve("wg");
        Files.createDirectories(wg);
        Files.writeString(wg.resolve("sqvm0.conf"), "PrivateKey = leftover\n");
        Files.writeString(wg.resolve("private.key"), "wg-key");

        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());
        UninstallService.UninstallSummary summary = serviceFor(env).purge(false);
        assertFalse(Files.exists(wg), "legacy data/wg must be jailed-deleted");
        assertTrue(summary.notes().stream().anyMatch(n -> n.contains("legacy wg dir")));
    }

    /**
     * The installers read the bootJar + run launcher as "an engine is installed here". Leaving them
     * behind is what made a delete-then-reinstall on the same host demand a typed REPLACE.
     */
    @Test
    void purge_removesTheArtifactsTheInstallerGuardDetects_andMarksTheHostUninstalled() throws Exception {
        Path prefix = temp.resolve("sqs");
        Path data = prefix.resolve("data");
        Files.createDirectories(data);
        Path jar = prefix.resolve("sqs-client-daemon-1.0.0.jar");
        Path run = prefix.resolve("run.sh");
        Path pid = prefix.resolve("daemon.pid");
        Files.writeString(jar, "PK");
        Files.writeString(run, "#!/usr/bin/env bash\n");
        Files.writeString(pid, "4242\n");
        // Logs are the only post-mortem left after a delete, so they must survive.
        Path log = prefix.resolve("logs").resolve("daemon.log");
        Files.createDirectories(log.getParent());
        Files.writeString(log, "boot\n");

        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());
        env.setProperty("sqs.channel.env-id", "3270cdc9-dead-beef");

        UninstallService.UninstallSummary summary = serviceFor(env).purge(false);

        assertFalse(Files.exists(jar), "bootJar must be gone");
        assertFalse(Files.exists(run), "run launcher must be gone");
        assertFalse(Files.exists(pid), "stale pid file must be gone");
        assertTrue(Files.exists(log), "logs must survive an uninstall");
        assertTrue(Files.exists(data), "data dir must survive an uninstall");
        Path marker = prefix.resolve(".sq-uninstalled");
        assertTrue(Files.exists(marker), "installer-visible uninstall marker must be written");
        assertTrue(Files.readString(marker).contains("env-id=3270cdc9-dead-beef"));
        assertTrue(summary.notes().stream().anyMatch(n -> n.contains("install footprint cleared")));
    }

    /** The process-name shadow runtime is symlinks into the real JDK — unlink them, never follow. */
    @Test
    void purge_removesTheProcessNameShadowRuntime_withoutTouchingTheJdkItPointsAt() throws Exception {
        Path prefix = temp.resolve("sqs");
        Path data = prefix.resolve("data");
        Files.createDirectories(data);
        Files.writeString(prefix.resolve("run.sh"), "#!/usr/bin/env bash\n");
        Path jdkLib = temp.resolve("jdk").resolve("lib");
        Files.createDirectories(jdkLib);
        Files.writeString(jdkLib.resolve("modules"), "modules");
        Path shadowBin = prefix.resolve("runtime").resolve("bin");
        Files.createDirectories(shadowBin);
        Files.writeString(shadowBin.resolve("StudioQuack"), "ELF");
        try {
            Files.createSymbolicLink(prefix.resolve("runtime").resolve("lib"), jdkLib);
        } catch (Exception ex) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "symlinks not available: " + ex);
        }

        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());

        serviceFor(env).purge(false);

        assertFalse(Files.exists(prefix.resolve("runtime")), "shadow runtime must be gone");
        assertTrue(Files.exists(jdkLib.resolve("modules")), "the real JDK must be untouched");
    }

    /**
     * When the launcher had to place the renamed binary outside the prefix it publishes the path;
     * only a name of ours may be deleted through it.
     */
    @Test
    void purge_removesThePublishedProcessAlias_onlyWhenItIsOneOfOurs() throws Exception {
        Path prefix = temp.resolve("sqs");
        Path data = prefix.resolve("data");
        Files.createDirectories(data);
        Files.writeString(prefix.resolve("run.sh"), "#!/usr/bin/env bash\n");
        Path jdkBin = temp.resolve("jdk").resolve("bin");
        Files.createDirectories(jdkBin);
        Path ours = jdkBin.resolve("StudioQuack");
        Path theirs = jdkBin.resolve("java");
        Files.writeString(ours, "ELF");
        Files.writeString(theirs, "ELF");

        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());
        env.setProperty("SQ_PROCESS_ALIAS", ours.toString());
        serviceFor(env).purge(false);
        assertFalse(Files.exists(ours), "our renamed launcher must be removed");

        Files.writeString(prefix.resolve("run.sh"), "#!/usr/bin/env bash\n");
        MockEnvironment hostile = new MockEnvironment();
        hostile.setProperty("SQS_DATA_DIR", data.toString());
        hostile.setProperty("SQ_PROCESS_ALIAS", theirs.toString());
        UninstallService.UninstallSummary summary = serviceFor(hostile).purge(false);
        assertTrue(Files.exists(theirs), "a path that is not one of our aliases must be left alone");
        assertTrue(summary.notes().stream()
                .anyMatch(n -> n.contains("SQ_PROCESS_ALIAS with an unexpected name")));
    }

    @Test
    void deleteJailedTree_unlinksSymlinkAndLeavesItsTarget() throws Exception {
        Path root = temp.resolve("channel");
        Files.createDirectories(root);
        Path outside = temp.resolve("outside-secret.txt");
        Files.writeString(outside, "keep-me");
        Path link = root.resolve("escape");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (Exception ex) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false, "symlinks not available: " + ex);
        }
        UninstallService.deleteJailedTree(root);
        assertTrue(Files.exists(outside), "walk must not follow the symlink into the target");
        assertFalse(Files.exists(link, java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }

    /** A daemon run out of a plain working dir must not drop files next to whatever else lives there. */
    @Test
    void purge_writesNoMarker_whenThePrefixHoldsNoInstalledArtifacts() throws Exception {
        Path prefix = temp.resolve("workdir");
        Path data = prefix.resolve("data");
        Files.createDirectories(data);
        Files.writeString(prefix.resolve("notes.txt"), "unrelated");

        MockEnvironment env = new MockEnvironment();
        env.setProperty("SQS_DATA_DIR", data.toString());

        UninstallService.UninstallSummary summary = serviceFor(env).purge(false);

        assertFalse(Files.exists(prefix.resolve(".sq-uninstalled")), "no marker without an install");
        assertTrue(Files.exists(prefix.resolve("notes.txt")), "unrelated files must be left alone");
        assertTrue(summary.notes().stream().anyMatch(n -> n.contains("install footprint already clear")));
    }
}
