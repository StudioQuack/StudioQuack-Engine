package com.studioquack.sqs.sandbox.stateful;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContainerScopeTest {

    private static final String SYSTEM_UNIT = "0::/system.slice/studioquack-sqs-pro.service\n";
    private static final String USER_UNIT =
            "0::/user.slice/user-1000.slice/user@1000.service/app.slice/studioquack-sqs.service\n";
    private static final List<String> PODMAN_RUN =
            List.of("/usr/bin/podman", "run", "-d", "--name", "sq-stateful-abc", "img");

    @Test
    void systemServiceWrapsRunInOwnSystemScope() {
        ContainerScope scope = ContainerScope.detect(SYSTEM_UNIT, argv -> true);

        assertTrue(scope.enabled());
        assertEquals(List.of("systemd-run", "--scope", "--quiet", "--collect",
                        "--unit=sq-stateful-abc-42", "/usr/bin/podman", "run", "-d", "--name",
                        "sq-stateful-abc", "img"),
                scope.wrap(PODMAN_RUN, "sq-stateful-abc", 42L));
    }

    @Test
    void userServiceUsesUserScope() {
        assertEquals(List.of("systemd-run", "--user", "--scope", "--quiet", "--collect"),
                ContainerScope.prefixFor(USER_UNIT));
    }

    @Test
    void probeRunsThePrefixWithTrue() {
        List<List<String>> probed = new ArrayList<>();
        ContainerScope.detect(SYSTEM_UNIT, argv -> probed.add(argv));

        assertEquals(List.of(List.of("systemd-run", "--scope", "--quiet", "--collect", "true")), probed);
    }

    @Test
    void failedProbeFallsBackToPlainRun() {
        ContainerScope scope = ContainerScope.detect(SYSTEM_UNIT, argv -> false);

        assertFalse(scope.enabled());
        assertSame(PODMAN_RUN, scope.wrap(PODMAN_RUN, "sq-stateful-abc", 42L));
    }

    @Test
    void notUnderSystemdNeverProbes() {
        List<List<String>> probed = new ArrayList<>();
        for (String cgroup : List.of("", "0::/user.slice/user-501.slice/session-3.scope\n", "0::/init.scope\n")) {
            ContainerScope scope = ContainerScope.detect(cgroup, argv -> probed.add(argv));
            assertFalse(scope.enabled(), cgroup);
        }
        assertTrue(probed.isEmpty());
    }

    @Test
    void testDoublesNeverWrap() {
        OciCli fake = new OciCli() {
            @Override public boolean available() { return true; }
            @Override public String cliPath() { return "podman"; }
            @Override public Result run(List<String> argv, byte[] stdin, java.time.Duration timeout) {
                throw new AssertionError("a test double must not be probed");
            }
        };
        assertFalse(ContainerScope.detect(fake).enabled());
    }

    @Test
    void unitNameIsSanitised() {
        assertEquals("sq_stateful_a_b", ContainerScope.unitSafe("sq stateful/a@b"));
        assertEquals("sq-stateful", ContainerScope.unitSafe(" "));
    }
}
