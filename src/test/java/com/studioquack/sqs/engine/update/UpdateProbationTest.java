package com.studioquack.sqs.engine.update;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A swapped-in build is not "updated" until it reaches the control plane: the first 2xx beat
 * confirms it, silence past the probation restores the previous build, and the restored build
 * reports the failure without ever rolling back again.
 */
class UpdateProbationTest {

    @TempDir
    Path dir;

    private Path jar;

    @BeforeEach
    void setUp() throws Exception {
        UpdateProbation.reset();
        DaemonBuild.updateIdle();
        jar = dir.resolve("sqs-client-daemon-1.0.0.jar");
        Files.writeString(jar, "NEW BUILD");
        Files.writeString(dir.resolve("sqs-client-daemon-1.0.0.jar.previous"), "OLD BUILD");
    }

    @AfterEach
    void tearDown() {
        UpdateProbation.reset();
        DaemonBuild.updateIdle();
    }

    private Path marker(String suffix) {
        return dir.resolve(jar.getFileName() + suffix);
    }

    @Test
    void theFirstSuccessfulHeartbeat_confirmsTheUpdate() throws Exception {
        Files.writeString(marker(UpdateProbation.PENDING), "0");
        UpdateProbation probation = new UpdateProbation(null, 60_000L);

        probation.start(jar);
        assertThat(marker(UpdateProbation.PENDING)).exists();
        UpdateProbation.confirmControlPlaneContact();

        assertThat(marker(UpdateProbation.PENDING)).doesNotExist();
        probation.shutdown();
    }

    @Test
    void contactBeforeReady_confirmsImmediately() throws Exception {
        Files.writeString(marker(UpdateProbation.PENDING), "0");
        UpdateProbation.confirmControlPlaneContact();

        new UpdateProbation(null, 60_000L).start(jar);

        assertThat(marker(UpdateProbation.PENDING)).doesNotExist();
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void noContactWithinProbation_restoresThePreviousBuild_andLeavesTheReason() throws Exception {
        Files.writeString(marker(UpdateProbation.PENDING), "0");
        UpdateProbation probation = new UpdateProbation(null, 60_000L);
        probation.start(jar);

        probation.expire(jar);

        assertThat(Files.readString(jar)).isEqualTo("OLD BUILD");
        assertThat(Files.readString(marker(UpdateProbation.FAILED_JAR))).isEqualTo("NEW BUILD");
        assertThat(marker(".previous")).doesNotExist();
        assertThat(marker(UpdateProbation.PENDING)).doesNotExist();
        assertThat(Files.readString(marker(UpdateProbation.ROLLBACK))).contains("could not reach the control plane");
        probation.shutdown();
    }

    @Test
    void theRestoredBuild_reportsTheFailure_andNeverEntersProbation() throws Exception {
        Files.writeString(marker(UpdateProbation.ROLLBACK), "the updated build could not reach the control plane");
        Files.writeString(marker(UpdateProbation.PENDING), "0"); // e.g. the Windows helper's swap

        new UpdateProbation(null, 60_000L).start(jar);

        assertThat(marker(UpdateProbation.ROLLBACK)).doesNotExist();
        assertThat(marker(UpdateProbation.PENDING)).doesNotExist();
        Map<?, ?> update = (Map<?, ?>) DaemonBuild.snapshot().get("update");
        assertThat(update.get("state")).isEqualTo("FAILED");
        assertThat(update.get("error").toString()).contains("could not reach the control plane");
    }

    @Test
    void aBuildThatWasNotJustSwappedIn_isNeverOnProbation() throws Exception {
        UpdateProbation probation = new UpdateProbation(null, 1L);
        probation.start(jar);
        probation.expire(jar);

        assertThat(Files.readString(jar)).isEqualTo("NEW BUILD");
        assertThat(marker(UpdateProbation.FAILED_JAR)).doesNotExist();
        probation.shutdown();
    }

    @Test
    void aConfirmedBuild_isNeverRolledBack() throws Exception {
        Files.writeString(marker(UpdateProbation.PENDING), "0");
        UpdateProbation probation = new UpdateProbation(null, 60_000L);
        probation.start(jar);
        UpdateProbation.confirmControlPlaneContact();

        probation.expire(jar);

        assertThat(Files.readString(jar)).isEqualTo("NEW BUILD");
        probation.shutdown();
    }
}
