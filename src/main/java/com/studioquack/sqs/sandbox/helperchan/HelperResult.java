package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * What the in-sandbox SDK records via {@code POST /v1/helper/result} (the modern per-port output
 * channel). Wire shape so the python/node/bash SDK contract stays byte-identical.
 *
 * @param ok Whether the user code declared success.
 * @param payload Optional structured payload (legacy whole-object return).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HelperResult(boolean ok,
                           Map<String, Object> payload,
                           List<ResultItem> items,
                           String error) {

    /** One {@code return_result([...])} entry — {@code value} is the (encoded) per-port output. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ResultItem(String port, String value) {
    }
}
