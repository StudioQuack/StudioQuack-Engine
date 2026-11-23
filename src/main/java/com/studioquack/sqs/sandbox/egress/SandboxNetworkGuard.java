package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.oci.SandboxNetworks;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Host-side hard refusal of any sandbox argv that lets a workload pick its own network attachment
 * or publish a non-loopback port. A workload must not opt out of the platform-chosen Podman
 * network.
 */
public final class SandboxNetworkGuard {

    /** Networks a {@code podman run} sandbox may attach to. */
    private static final Set<String> RUN_NETWORKS = Set.of(
            "none",
            SandboxNetworks.JOBS,
            SandboxNetworks.STATEFUL,
            SandboxNetworks.BUILD);

    /**
     * Networks a {@code podman build} may use. Builds need internet for dependency fetch but
     * must stay on the dedicated build net (or offline {@code none}) — never jobs/stateful.
     */
    private static final Set<String> BUILD_NETWORKS = Set.of(
            "none",
            SandboxNetworks.BUILD);

    private SandboxNetworkGuard() {
    }

    /**
     * Validates a {@code podman run} (job/warm/stateful) argv. Allowed {@code --network} values:
     * {@code none}, {@link SandboxNetworks#JOBS}, {@link SandboxNetworks#STATEFUL}, {@link
     * SandboxNetworks#BUILD}.
     */
    public static void validate(List<String> argv) {
        scan(argv, RUN_NETWORKS, "run", -1);
    }

    /**
     * As {@link #validate(List)} plus the DNS-tail refusal: podman lets the LAST {@code --dns} win, so
     * a workload appending one in its command tail (after the image) would override the daemon's pins
     * and re-point resolution at an attacker resolver (or the bridge gateway, reopening the
     * internal-namespace leak).
     */
    public static void validate(List<String> argv, int imageIndex) {
        scan(argv, RUN_NETWORKS, "run", imageIndex);
    }

    /**
     * Validates a {@code podman build} argv. Build-time {@code --network} is a different flag
     * surface than {@code run}, but the refusal semantics match: only
     * {@link SandboxNetworks#BUILD} or {@code none} are accepted.
     */
    public static void validateBuildNetwork(List<String> argv) {
        // A build argv is fully daemon-built (no image positional, no attacker tail), so the
        // DNS-tail check does not apply — builds legitimately carry the daemon's --dns pins.
        scan(argv, BUILD_NETWORKS, "build", Integer.MAX_VALUE);
    }

    /**
     * DEGRADED-host build validation. On a host where sandbox egress is not enforceable (every
     * self-hosted macOS/Windows install), containers already start on the default Podman bridge
     * (degraded posture) — and a rootless buildah RUN step cannot join a named netavark network at all
     * ({@code setup network: cannot use networks as rootless}), so the build argv must state NO
     * network flag and ride the implicit default bridge.
     */
    public static void validateBuildNetwork(List<String> argv, boolean degraded) {
        if (!degraded) {
            validateBuildNetwork(argv);
            return;
        }
        if (argv == null || argv.isEmpty()) {
            throw new IllegalArgumentException(
                    "refused sandbox build argv: empty argv (fail closed)");
        }
        for (String tok : argv) {
            if (tok == null) {
                continue;
            }
            if ("--network".equals(tok) || "--net".equals(tok)
                    || tok.startsWith("--network=") || tok.startsWith("--net=")) {
                throw new IllegalArgumentException(
                        "refused sandbox build argv on a DEGRADED host: " + tok
                                + " (a rootless build cannot join a named network; the implicit"
                                + " default bridge must be left unstated)");
            }
        }
    }

    private static void scan(List<String> argv, Set<String> allowedNetworks, String kind,
                             int imageIndex) {
        if (argv == null || argv.isEmpty()) {
            throw new IllegalArgumentException(
                    "refused sandbox " + kind + " argv: missing --network (fail closed; "
                            + "argv builders must always state the platform-chosen network)");
        }

        String networkValue = null;
        int networkCount = 0;

        for (int i = 0; i < argv.size(); i++) {
            String tok = argv.get(i);
            if (tok == null) {
                continue;
            }

            // DNS flags are daemon-only: before the image the builder's own pins pass; AT/AFTER the image
            // boundary every token is workload-controlled, and podman's "last --dns wins" would let a tail
            // override the pins. Refuse the whole family (space and = forms) there.
            boolean tailZone = imageIndex < 0 || i >= imageIndex;
            if (tailZone && ("--dns".equals(tok) || tok.startsWith("--dns=")
                    || "--dns-opt".equals(tok) || tok.startsWith("--dns-opt=")
                    || "--dns-search".equals(tok) || tok.startsWith("--dns-search="))) {
                String flag = tok.contains("=") ? tok.substring(0, tok.indexOf('=')) : tok;
                throw new IllegalArgumentException(
                        "refused " + flag + ": DNS resolver selection is daemon-owned "
                                + "(T-17 — the pinned --dns flags sit before the image; a "
                                + "workload-supplied value would win podman's last-flag "
                                + "precedence and re-open internal-DNS resolution)");
            }

            // Net alias: podman accepts it; refuse so the guard cannot be bypassed by spelling.
            if ("--net".equals(tok) || tok.startsWith("--net=")) {
                throw new IllegalArgumentException(
                        "refused --net: the --net alias is never permitted; use --network with a "
                                + "platform-chosen value (none|" + String.join("|", allowedNetworks) + ")");
            }

            if ("--network".equals(tok)) {
                if (i + 1 >= argv.size()) {
                    throw new IllegalArgumentException(
                            "refused --network with no value (fail closed)");
                }
                networkCount++;
                networkValue = argv.get(++i);
                refuseIfForbiddenNetwork(networkValue, allowedNetworks);
                continue;
            }
            if (tok.startsWith("--network=")) {
                networkCount++;
                networkValue = tok.substring("--network=".length());
                refuseIfForbiddenNetwork(networkValue, allowedNetworks);
                continue;
            }

            // Namespace sharing = another path into a host-like netns.
            if ("--pod".equals(tok) || tok.startsWith("--pod=")
                    || "--pod-id-file".equals(tok) || tok.startsWith("--pod-id-file=")
                    || "--share".equals(tok) || tok.startsWith("--share=")) {
                String flag = tok.contains("=") ? tok.substring(0, tok.indexOf('=')) : tok;
                throw new IllegalArgumentException(
                        "refused " + flag + ": namespace sharing is never permitted "
                                + "(D90 — workloads may not join a host-like netns)");
            }

            // Port publishing: only loopback-pinned daemon-managed publishes.
            if ("-P".equals(tok) || "--publish-all".equals(tok)) {
                throw new IllegalArgumentException(
                        "refused " + tok + ": publish-all is never permitted "
                                + "(D90 — internet is outbound-only)");
            }
            if ("-p".equals(tok) || "--publish".equals(tok)) {
                if (i + 1 >= argv.size()) {
                    throw new IllegalArgumentException(
                            "refused " + tok + " with no value (fail closed)");
                }
                refuseIfForbiddenPublish(argv.get(++i));
                continue;
            }
            if (tok.startsWith("-p=") || tok.startsWith("--publish=")) {
                int eq = tok.indexOf('=');
                refuseIfForbiddenPublish(tok.substring(eq + 1));
            }
        }

        if (networkCount == 0) {
            throw new IllegalArgumentException(
                    "refused sandbox " + kind + " argv: missing --network (fail closed; "
                            + "argv builders must always state the platform-chosen network)");
        }
        if (networkCount > 1) {
            throw new IllegalArgumentException(
                    "refused sandbox " + kind + " argv: multiple --network selections "
                            + "(exactly one platform-chosen network is required)");
        }
        // networkValue already checked; keep for clarity if a future path skips refuseIfForbidden.
        if (networkValue == null || !allowedNetworks.contains(networkValue)) {
            refuseIfForbiddenNetwork(networkValue, allowedNetworks);
        }
    }

    private static void refuseIfForbiddenNetwork(String value, Set<String> allowed) {
        if (value != null && allowed.contains(value)) {
            return;
        }
        String shown = value == null ? "<null>" : value;
        String lower = value == null ? "" : value.toLowerCase(Locale.ROOT);

        if ("host".equals(lower)) {
            throw new IllegalArgumentException(
                    "refused --network " + shown + ": host networking is never permitted "
                            + "(D90 — host netns voids every other containment rule)");
        }
        if ("bridge".equals(lower) || "private".equals(lower)
                || lower.startsWith("ns:")
                || lower.startsWith("container:")
                || lower.startsWith("slirp4netns")
                || lower.startsWith("pasta")
                || lower.startsWith("path:")) {
            throw new IllegalArgumentException(
                    "refused --network " + shown + ": not a platform-chosen network "
                            + "(allowed: " + String.join("|", allowed) + ")");
        }
        throw new IllegalArgumentException(
                "refused --network " + shown + ": not a platform-chosen network "
                        + "(allowed: " + String.join("|", allowed) + ")");
    }

    private static void refuseIfForbiddenPublish(String value) {
        if (value != null && value.startsWith("127.0.0.1:")) {
            return;
        }
        String shown = value == null ? "<null>" : value;
        throw new IllegalArgumentException(
                "refused port publish " + shown + ": only loopback-pinned publishes "
                        + "(127.0.0.1:host:container) are permitted (D90 — internet is outbound-only)");
    }
}
