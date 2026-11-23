package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;

/**
 * Host capability probe: can this Podman store honour {@code --storage-opt size=} (the
 * per-container overlay writable-layer quota behind {@link StatefulCaps#diskMb()})?
 * Containers/storage enforces that quota only when the graphroot's backing filesystem is XFS
 * (pquota); on ext4/btrfs/… {@code podman run} HARD-FAILS at container create ("storage option
 * overlay.size … only supported for backingFS XFS", exit 125).
 */
@Slf4j
public class OverlaySizeQuotaProbe {

    /** Bounded like every container op; {@code info} is local and fast. */
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(15);
    /** graphStatus key containers/storage fills with the graphroot's backing FS ("xfs"/"extfs"/…). */
    static final String BACKING_FS_KEY = "Backing Filesystem";
    /** The only backing FS whose project quota the overlay driver accepts for {@code size=}. */
    private static final String SUPPORTED_FS = "xfs";

    private static final ObjectMapper JSON = new ObjectMapper();

    private final OciCli cli;
    /** Lifetime cache; null = not probed yet. Benign race: a duplicate probe is idempotent. */
    private volatile Boolean supported;

    public OverlaySizeQuotaProbe(OciCli cli) {
        this.cli = cli;
    }

    /** True when the runtime's store can enforce {@code --storage-opt size=}. Cached; fail-safe. */
    public boolean sizeQuotaSupported() {
        Boolean hit = supported;
        if (hit != null) {
            return hit;
        }
        boolean fresh = probeSafely();
        supported = fresh;
        return fresh;
    }

    private boolean probeSafely() {
        try {
            if (cli == null || !cli.available() || cli.cliPath() == null || cli.cliPath().isBlank()) {
                return false;
            }
            // Format json (the whole document) instead of a .store.* Go template: some Podman builds (observed
            // on a Windows remote client) refuse the template with "can't evaluate field store in type
            // system.infoReport". The JSON document's lowercase store.graphStatus keys are stable across
            // versions — and this exact call already works on every host the daemon detects runtimes on
            // (OciRuntimeDetector uses it).
            OciCli.Result r = cli.run(
                    List.of(cli.cliPath(), "info", "--format", "json"),
                    OP_TIMEOUT);
            if (!r.ok()) {
                log.info("sandbox.stateful disk-quota probe: podman info failed ({}) — "
                        + "reporting unsupported", r.firstErrLine());
                return false;
            }
            String backingFs = backingFilesystem(graphStatusJson(r.out()));
            boolean ok = SUPPORTED_FS.equalsIgnoreCase(backingFs);
            // One line per daemon lifetime (the result is cached) — support gold when a
            // customer's stateful starts behave differently across hosts.
            log.info("sandbox.stateful disk-quota supported={} (podman store backing filesystem: {})",
                    ok, backingFs == null ? "unknown" : backingFs);
            return ok;
        } catch (Exception e) {
            log.info("sandbox.stateful disk-quota probe failed (reporting unsupported): {}", e.toString());
            return false;
        }
    }

    /**
     * The {@code store.graphStatus} sub-object out of a full {@code podman info --format json}
     * document, or {@code null} when absent/unparseable. Package-visible so tests pin the parse
     * without a process exec.
     */
    static String graphStatusJson(String infoJson) {
        if (infoJson == null || infoJson.isBlank()) {
            return null;
        }
        try {
            JsonNode gs = JSON.readTree(infoJson.trim()).path("store").path("graphStatus");
            return gs.isMissingNode() || gs.isNull() ? null : gs.toString();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * The {@code Backing Filesystem} value out of a graphStatus payload,
     * or {@code null} when absent/unparseable. Package-visible so tests pin the parse without a
     * process exec.
     */
    static String backingFilesystem(String graphStatusJson) {
        if (graphStatusJson == null || graphStatusJson.isBlank()) {
            return null;
        }
        try {
            JsonNode node = JSON.readTree(graphStatusJson.trim());
            String v = node.path(BACKING_FS_KEY).asText("").trim();
            return v.isEmpty() ? null : v;
        } catch (Exception e) {
            return null;
        }
    }
}
