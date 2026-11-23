package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.runtime.CliRunner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Inspect helpers for OCI images the warm pool / cold path need before spawn. */
public final class ImageInspection {

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER = new ObjectMapper();

    private ImageInspection() {
    }

    /**
     * The image's own start argv (Entrypoint + Cmd, Docker semantics: Cmd is the default argv to
     * Entrypoint), captured via {@code podman inspect --type image}. Used to exec a custom
     * (entrypoint) image's app inside a warm/cold container whose real command is {@code sleep}.
     *
     * @return Entrypoint+cmd merged; EMPTY when the image declares neither (caller fails the warm.
     */
    public static List<String> entrypointArgv(String cli, String imageRef) {
        CliRunner.Result result = CliRunner.run(
                List.of(cli, "inspect", "--type", "image", "-f",
                        "{{json .Config.Entrypoint}}|{{json .Config.Cmd}}", imageRef),
                Duration.ofSeconds(30));
        if (!result.ok()) {
            String err = firstStderrLine(result.stderr());
            throw new IllegalStateException(err.isEmpty()
                    ? "podman inspect failed for image"
                    : err);
        }
        String stdout = result.stdout() == null ? "" : result.stdout().trim();
        String[] parts = stdout.split("\\|", 2);
        List<String> entrypoint = parseJsonStringArray(parts.length > 0 ? parts[0] : "");
        List<String> cmd = parseJsonStringArray(parts.length > 1 ? parts[1] : "");
        List<String> merged = new ArrayList<>(entrypoint.size() + cmd.size());
        merged.addAll(entrypoint);
        merged.addAll(cmd);
        return List.copyOf(merged);
    }

    /** Tolerate literal {@code null}, empty, or non-array → empty list. */
    private static List<String> parseJsonStringArray(String raw) {
        if (raw == null) {
            return List.of();
        }
        String t = raw.trim();
        if (t.isEmpty() || "null".equals(t)) {
            return List.of();
        }
        try {
            List<String> parsed = MAPPER.readValue(t, new TypeReference<List<String>>() {});
            return parsed == null || parsed.isEmpty() ? List.of() : List.copyOf(parsed);
        } catch (Exception e) {
            return List.of();
        }
    }

    private static String firstStderrLine(String stderr) {
        if (stderr == null || stderr.isBlank()) {
            return "";
        }
        int nl = stderr.indexOf('\n');
        return (nl < 0 ? stderr : stderr.substring(0, nl)).trim();
    }
}
