package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * The structured typed error the in-sandbox SDK records via {@code POST /v1/helper/fail} (e.g. the
 * Python {@code studioquack.fail(code, message)} helper). It is the client-side wire contract the
 * SDK posts so a script can raise a typed business error that maps to a specific BPMN error
 * boundary, instead of an untyped non-zero exit which always folds to {@link
 * com.studioquack.sqs.sandbox.SandboxJobResponse#ERROR_SANDBOX_FAILED}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HelperFailure(String errorCode, String message, Map<String, Object> data) {

    /** True when a concrete, non-blank typed code was supplied. */
    public boolean hasCode() {
        return errorCode != null && !errorCode.isBlank();
    }
}
