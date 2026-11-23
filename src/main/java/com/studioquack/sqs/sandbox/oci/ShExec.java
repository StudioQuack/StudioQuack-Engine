package com.studioquack.sqs.sandbox.oci;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Windows-immune transports for running a shell script INSIDE a container via {@code podman exec}.
 * On Windows the daemon spawns {@code podman.exe} through a Win32 command-line string ({@code
 * CreateProcess}); the remote client re-parses that string with quote-toggle rules, so a single
 * argv element that carries BOTH whitespace and double quotes — i.e. a whole {@code sh -c
 * "<script>"} — is shattered at every embedded quote before it ever reaches the container.
 */
public final class ShExec {

    private ShExec() {
    }

    /**
     * {@code podman exec -i <containerId> sh -s} — run the script piped on stdin. The argv is
     * quote-free, so it survives the Win32 command-line round-trip verbatim.
     */
    public static List<String> stdinArgv(String cliPath, String containerId) {
        return List.of(cliPath, "exec", "-i", containerId, "sh", "-s");
    }

    /** The script bytes to pipe into {@link #stdinArgv}'s stdin. */
    public static byte[] utf8(String script) {
        return script.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A {@code sh -c} wrapper (single quotes only — Windows-safe) that base64-plants {@code script} at
     * a per-exec {@code /tmp} path ({@code $$} = the wrapper shell's PID, so concurrent execs into the
     * same long-lived instance never collide) and runs it with the exec's own stdin — for scripts that
     * read a payload from stdin, where {@code sh -s} would eat the payload as program source. The
     * planted file is removed afterwards; the script's exit code propagates.
     */
    public static String plantAndRun(String script, String fileStem) {
        String path = "/tmp/" + fileStem + ".$$.sh";
        return plantPrefix(script, path)
                + " && sh " + path + "; rc=$?; rm -f " + path + "; exit $rc";
    }

    /**
     * As {@link #plantAndRun} but {@code exec}s the planted script (no extra process layer, no
     * cleanup) — for single-use job containers that are destroyed right after the run.
     */
    public static String plantAndExec(String script, String fileStem) {
        String path = "/tmp/" + fileStem + ".$$.sh";
        return plantPrefix(script, path) + " && exec sh " + path;
    }

    /** {@code printf %s '<b64>' | base64 -d > <path>} — single-quote-only, Windows-safe. */
    private static String plantPrefix(String script, String path) {
        String b64 = Base64.getEncoder().encodeToString(script.getBytes(StandardCharsets.UTF_8));
        return "printf %s '" + b64 + "' | base64 -d > " + path;
    }
}
