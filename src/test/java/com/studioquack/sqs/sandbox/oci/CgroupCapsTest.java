package com.studioquack.sqs.sandbox.oci;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CgroupCapsTest {

    /** crun's refusal on a rootless WSL podman machine (2026-11-03, Windows engine). */
    static final String CRUN_MEMORY = "Error: crun: open `memory.max` for writing: No such file or directory: "
            + "OCI runtime attempted to invoke a command that was not found";

    @AfterEach
    void reset() {
        CgroupCaps.resetForTests();
    }

    @Test
    void parsesTheControllerCrunCouldNotOpen() {
        assertEquals(CgroupCaps.Controller.MEMORY, CgroupCaps.missingController(CRUN_MEMORY));
        assertEquals(CgroupCaps.Controller.MEMORY, CgroupCaps.missingController(
                "crun: open `memory.swap.max` for writing: No such file or directory"));
        assertEquals(CgroupCaps.Controller.CPU, CgroupCaps.missingController(
                "crun: open `cpu.max` for writing: No such file or directory"));
        assertEquals(CgroupCaps.Controller.PIDS, CgroupCaps.missingController(
                "runc create failed: open /sys/fs/cgroup/x/pids.max: no such file or directory"));
        // Same Windows engine, after the memory limit was dropped (2026-11-04).
        assertEquals(CgroupCaps.Controller.PIDS, CgroupCaps.missingController(
                "Error: crun: controller `pids` is not available under /sys/fs/cgroup/non-systemd/user.slice/"
                        + "user-1000.slice/user@1000.service/app.slice/ad33/cgroup.controllers: OCI runtime error"));
        assertEquals(CgroupCaps.Controller.CPU, CgroupCaps.missingController(
                "Error: crun: controller `cpu` is not available under /sys/fs/cgroup/x/cgroup.controllers"));
        assertNull(CgroupCaps.missingController("controller `io` is not available under /x/cgroup.controllers"));
        assertNull(CgroupCaps.missingController("Error: image not known"));
        assertNull(CgroupCaps.missingController("memory.max: permission denied"));
        assertNull(CgroupCaps.missingController(null));
    }

    @Test
    void marksEachControllerOnceAndKeepsTheOthers() {
        assertTrue(CgroupCaps.noteDetachedStartFailure(CRUN_MEMORY));
        assertFalse(CgroupCaps.noteDetachedStartFailure(CRUN_MEMORY), "already known: no second retry");
        assertFalse(CgroupCaps.noteDetachedStartFailure("Error: image not known"));

        assertFalse(CgroupCaps.applies(CgroupCaps.Controller.MEMORY));
        assertTrue(CgroupCaps.applies(CgroupCaps.Controller.CPU));
        assertTrue(CgroupCaps.applies(CgroupCaps.Controller.PIDS));
    }

    @Test
    void sandboxArgvDropsOnlyTheMissingControllersFlags() {
        CgroupCaps.noteDetachedStartFailure(CRUN_MEMORY);

        List<String> argv = HardeningArgs.builder("/usr/bin/podman", null, "docker.io/library/alpine:3")
                .detached(true)
                .build()
                .toArgv();

        assertFalse(argv.contains("--memory"));
        assertFalse(argv.contains("--memory-swap"));
        assertTrue(argv.contains("--cpus"));
        assertTrue(argv.contains("--pids-limit"));
    }
}
