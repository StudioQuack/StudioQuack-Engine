package com.studioquack.sqs.sandbox;

import java.util.function.Function;

/**
 * Single source for "which image does this job run on" on the ephemeral path. Resolution order:
 * the node's own {@code ociImageRef} when non-blank (builtin or env-registered custom ref) → the
 * {@code sqs.sandbox.image.<lang>} property override → {@code lang.defaultImage()}.
 */
public final class SandboxImageResolution {

    private SandboxImageResolution() {}

    /** The image a job resolves to. {@code property} maps property name → value (Environment::getProperty). */
    public static String resolve(String ociImageRef, SandboxLanguage lang, Function<String, String> property) {
        if (ociImageRef != null && !ociImageRef.isBlank()) {
            return ociImageRef.trim();
        }
        return languageFallback(lang, property);
    }

    /** The image the language path would use WITHOUT the node's ociImageRef (property override, else default). */
    public static String languageFallback(SandboxLanguage lang, Function<String, String> property) {
        if (lang == null) return null;
        String override = property == null ? null : property.apply("sqs.sandbox.image." + lang.wire());
        if (override != null && !override.isBlank()) {
            return override.trim();
        }
        return lang.defaultImage();
    }

    /**
     * True when the job carries an explicit custom image — resolved image differs from the language
     * fallback. An entrypoint image owns its start argv: the daemon skips the script bake + stdin
     * bootstrap and execs the image's recorded entrypoint with SQ_CONTEXT.
     */
    public static boolean isEntrypointImage(String ociImageRef, SandboxLanguage lang, Function<String, String> property) {
        if (ociImageRef == null || ociImageRef.isBlank()) {
            return false;
        }
        String fallback = languageFallback(lang, property);
        return fallback == null || !fallback.equals(ociImageRef.trim());
    }
}
