package com.studioquack.sqs.execution.transform;

import java.util.Optional;

/**
 * Supplies the language-specific helper script prepended to a node's user script.
 * An empty result means the raw user script is dispatched.
 */
public interface HelperScriptProvider {

    /**
     * @param language normalised language key (e.g. {@code "python"}, {@code "javascript"}).
     * @return the helper template body for {@code language}, or empty when none is
     * registered (the caller then dispatches the raw user script).
     */
    Optional<String> helperScriptFor(String language);

    /**
     * Restricted edge-transform helper template (source/target only — no node SDK).
     * Default empty; {@code SandboxHelperScriptProvider} supplies classpath templates.
     */
    default Optional<String> transformHelperScriptFor(String language) {
        return Optional.empty();
    }
}
