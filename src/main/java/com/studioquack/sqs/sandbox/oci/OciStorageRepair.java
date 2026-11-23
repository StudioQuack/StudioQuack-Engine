package com.studioquack.sqs.sandbox.oci;

import com.studioquack.sqs.sandbox.runtime.CliRunner;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

/**
 * Self-heal for the Podman overlay-store corruption a WireGuard drop mid-pull (or a podman-machine
 * restart) leaves behind: the layer diff dir is gone but the image still references it, so every
 * {@code create} for that image fails with {@code can't stat lower layer ../overlay/diff} (exit
 * 127) forever. Detect the exact failure, drop the broken image and prune orphans, and let the
 * caller retry against a clean store.
 */
@Slf4j
public final class OciStorageRepair {

    /** Short op budget — {@code rmi} / {@code prune} are local-only. */
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(15);

    private OciStorageRepair() {
    }

    /** True when the error text is the known orphaned-overlay-lower-layer failure. */
    public static boolean isOverlayStorageCorruption(String stderr) {
        return stderr != null && stderr.contains("lower layer") && stderr.contains("no such file or directory");
    }

    /**
     * Removes the broken image (so the next attempt re-pulls clean layers) and prunes
     * orphaned containers/volumes. Never throws — a failed repair only means the next
     * attempt sees the same corruption again.
     */
    public static void repair(String cli, String imageRef) {
        try {
            if (imageRef != null && !imageRef.isBlank()) {
                CliRunner.run(List.of(cli, "rmi", "-f", imageRef), OP_TIMEOUT);
                log.warn("oci.storage-repair image={} removed after overlay corruption", imageRef);
            }
            CliRunner.run(List.of(cli, "container", "prune", "-f"), OP_TIMEOUT);
        } catch (RuntimeException e) {
            log.warn("oci.storage-repair image={} failed: {}", imageRef, e.toString());
        }
    }
}
