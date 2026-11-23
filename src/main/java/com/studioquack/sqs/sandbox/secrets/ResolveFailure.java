package com.studioquack.sqs.sandbox.secrets;

/**
 * One placeholder the resolver could not turn into plaintext. Collected per-leaf so a single
 * missing secret does not bring down the whole dispatch; the dispatcher logs one breadcrumb per
 * failure with the public {@code ref} id + {@code keyHint} — never the value.
 *
 * @param ref The offending wire string ({@code sec_<24hex>:vN}, or the raw placeholder) — saf.
 */
public record ResolveFailure(String ref, String keyHint, String detail) {
}
