package com.studioquack.sqs.sandbox;

import java.util.List;

/**
 * The languages the sandbox can run, with their in-container script file extension and the pinned
 * interpreter argv that runs {@code main.<ext>}. Interpreter binaries are pinned (never {@code
 * python}) so a malicious image cannot rebind the interpreter to a wrapper script.
 */
public enum SandboxLanguage {

    PYTHON("python", ".py", "python:3.11-slim"),
    // Node defaults are pinned to an exact patch release, never a floating major: a host pulls a
    // tag only when it is missing, so a floating tag keeps whatever (unpatched) build it already
    // has. Bump on every Node.js security release (26.12.1 = October 2026 security release).
    JAVASCRIPT("javascript", ".js", "node:26.12.1-slim"),
    // The TypeScript entrypoint relies on Node's built-in type stripping (--experimental-strip-types),
    // which exists only from Node 22.6 — an older image rejects the flag ("bad option"). Trixie-slim
    // is the same image digest as JavaScript's -slim tag, but a distinct ref: the warm pool, the
    // script-mode image→language map and the Runtime rows are keyed by image ref, so two languages on
    // one ref would merge (TS slots bootstrapped as JS).
    TYPESCRIPT("typescript", ".ts", "node:26.12.1-trixie-slim"),
    BASH("bash", ".sh", null),
    GO("go", ".go", null),
    RUBY("ruby", ".rb", null);

    private final String wire;
    private final String fileExt;
    private final String defaultImage;

    SandboxLanguage(String wire, String fileExt, String defaultImage) {
        this.wire = wire;
        this.fileExt = fileExt;
        this.defaultImage = defaultImage;
    }

    public String wire() {
        return wire;
    }

    /** In-container script file name, e.g. {@code main.py}. */
    public String scriptFileName() {
        return "main" + fileExt;
    }

    /**
     * Default published OCI image for this language, or {@code null} for languages whose sandbox
     * image is baked locally and not published (bash/go/ruby — operators register those via
     * an image override). Mirrors {@code DefaultOciImageMap()}.
     */
    public String defaultImage() {
        return defaultImage;
    }

    /**
     * The pinned interpreter argv that runs {@code <sandboxDir>/main.<ext>}. Startup flags mirror the
     * throughput tuning (Python {@code -OO -X frozen_modules=on -B}; Node {@code --no-warnings
     * --no-deprecation}).
     */
    public List<String> entrypoint(String sandboxDir) {
        String main = sandboxDir + "/" + scriptFileName();
        return switch (this) {
            case PYTHON -> List.of("python3", "-OO", "-X", "frozen_modules=on", "-B", main);
            case JAVASCRIPT -> List.of("node", "--no-warnings", "--no-deprecation", main);
            case TYPESCRIPT -> List.of("node", "--no-warnings", "--no-deprecation",
                    "--experimental-strip-types", main);
            case BASH -> List.of("bash", main);
            case GO -> List.of("go", "run", main);
            case RUBY -> List.of("ruby", main);
        };
    }

    /**
     * Resolves a wire language string (case-insensitive; a few aliases like {@code js}/{@code py}
     * are accepted) to a {@link SandboxLanguage}, or {@code null} when unknown.
     */
    public static SandboxLanguage fromWire(String value) {
        if (value == null) {
            return null;
        }
        return switch (value.trim().toLowerCase()) {
            case "python", "py", "python3" -> PYTHON;
            case "javascript", "js", "node", "nodejs" -> JAVASCRIPT;
            case "typescript", "ts" -> TYPESCRIPT;
            case "bash", "sh", "shell" -> BASH;
            case "go", "golang" -> GO;
            case "ruby", "rb" -> RUBY;
            default -> null;
        };
    }
}
