package com.studioquack.sqs.sandbox.secrets;

import java.util.List;

/**
 * The outcome of a {@link SecretResolver#resolveToEnv} pass: the rendered dotenv text (or null
 * when no secret resolved), how many placeholders were rewritten, and the per-leaf failures. The
 * dotenv text exists only in this object's memory.
 */
public final class SecretResolution {

    private final String envContent;
    private final int resolvedCount;
    private final List<ResolveFailure> failures;

    public SecretResolution(String envContent, int resolvedCount, List<ResolveFailure> failures) {
        this.envContent = envContent;
        this.resolvedCount = resolvedCount;
        this.failures = failures == null ? List.of() : List.copyOf(failures);
    }

    /** Dotenv text ({@code KEY='plaintext'} lines) for the sandbox, or null when nothing resolved. */
    public String envContent() {
        return envContent;
    }

    /** True when at least one secret resolved (dotenv text is present). */
    public boolean hasEnv() {
        return envContent != null;
    }

    /** Number of placeholders rewritten to plaintext. */
    public int resolvedCount() {
        return resolvedCount;
    }

    /** Per-leaf failures (unresolvable placeholders); empty on full success. */
    public List<ResolveFailure> failures() {
        return failures;
    }
}
