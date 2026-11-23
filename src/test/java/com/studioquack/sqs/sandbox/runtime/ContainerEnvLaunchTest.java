package com.studioquack.sqs.sandbox.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Env values stay off argv and are visible to the child process. */
class ContainerEnvLaunchTest {

    @TempDir
    Path dir;

    @Test
    void name_only_argv_and_child_environment_carry_the_secret() throws Exception {
        Map<String, String> env = Map.of(
                "SQ_CONTEXT", "blob-secret-value",
                "SQ_HELPER_BEARER", "bearer-secret");
        List<String> argv = new ArrayList<>(List.of("sh", "-c", "printf %s \"$SQ_CONTEXT\""));
        ContainerEnv.appendNameOnly(argv, env);
        assertTrue(pair(argv, "-e", "SQ_CONTEXT") >= 0);
        assertTrue(pair(argv, "-e", "SQ_HELPER_BEARER") >= 0);
        assertFalse(argv.stream().anyMatch(a -> a.contains("blob-secret-value") || a.contains("bearer-secret")));

        String previous = System.setProperty("sqs.dataDir", dir.toString());
        try {
            Path out = dir.resolve("out.txt");
            List<String> launch = new ArrayList<>(List.of("sh", "-c",
                    "printf %s \"$SQ_CONTEXT:$SQ_HELPER_BEARER\" > '" + out + "'"));
            ContainerEnv.appendNameOnly(launch, env);
            CliRunner.Result result = CliRunner.run(launch, null, Duration.ofSeconds(5), 1024, 1024, env);
            assertEquals(0, result.exitCode(), result.stderr());
            assertEquals("blob-secret-value:bearer-secret", Files.readString(out));
        } finally {
            if (previous == null) {
                System.clearProperty("sqs.dataDir");
            } else {
                System.setProperty("sqs.dataDir", previous);
            }
        }
    }

    @Test
    void client_control_names_are_not_applied_to_the_podman_process() throws Exception {
        Map<String, String> env = Map.of("LD_PRELOAD", "/tmp/evil.so", "SQ_CONTEXT", "blob");
        List<String> argv = new ArrayList<>(List.of("podman", "exec", "-i", "cid"));
        ContainerEnv.appendNameOnly(argv, env);
        ContainerEnv.Prepared prepared = ContainerEnv.prepare(argv, env, dir);
        assertEquals("blob", prepared.childEnv().get("SQ_CONTEXT"));
        assertFalse(prepared.childEnv().containsKey("LD_PRELOAD"));
        assertFalse(prepared.argv().contains("LD_PRELOAD"));
        assertFalse(prepared.argv().stream().anyMatch(a -> a.contains("/tmp/evil.so") || a.contains("blob")));
        assertTrue(prepared.argv().contains("--env-file"));
        String file = Files.readString(prepared.envFile());
        assertTrue(file.contains("LD_PRELOAD=/tmp/evil.so"));
        assertFalse(file.contains("SQ_CONTEXT"));
    }

    private static int pair(List<String> argv, String a, String b) {
        for (int i = 0; i + 1 < argv.size(); i++) {
            if (argv.get(i).equals(a) && argv.get(i + 1).equals(b)) {
                return i;
            }
        }
        return -1;
    }
}
