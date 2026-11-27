package com.studioquack.sqs.sandbox.redactor;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Scrubs PII / secret-like substrings from any value before it leaves the daemon process. It must
 * run before ANY log is persisted ("Redact before persist") — the on-disk {@code
 * sandbox_execution_logs} row is the redacted form, never a copy of the raw stream.
 */
@Component
public class Redactor {

    /** One redaction rule: a compiled pattern + its replacement. */
    public record RedactionPattern(String name, Pattern regex, String replacement) {
        static RedactionPattern of(String name, String regex, String replacement) {
            return new RedactionPattern(name, Pattern.compile(regex), replacement);
        }
    }

    /**
     * The default pattern table. Order matters: broad structural secrets (private keys, JWTs) run
     * before narrow scalar patterns.
     */
    public static final List<RedactionPattern> DEFAULT_PATTERNS = List.of(
            RedactionPattern.of("ssh-priv",
                    "-----BEGIN [A-Z ]*PRIVATE KEY-----[\\s\\S]*?-----END [A-Z ]*PRIVATE KEY-----",
                    "[redacted-private-key]"),
            RedactionPattern.of("jwt",
                    "eyJ[A-Za-z0-9_\\-]+\\.eyJ[A-Za-z0-9_\\-]+\\.[A-Za-z0-9_\\-]+", "[redacted-jwt]"),
            RedactionPattern.of("aws-key", "AKIA[0-9A-Z]{16}", "[redacted-aws-key]"),
            RedactionPattern.of("gcp-key", "AIza[0-9A-Za-z\\-_]{35}", "[redacted-gcp-key]"),
            RedactionPattern.of("webhook-key", "sqwh_[A-Za-z0-9_\\-]+", "[redacted-webhook-key]"),
            RedactionPattern.of("bearer-token",
                    "(?i)bearer\\s+[A-Za-z0-9._\\-]{12,}", "Bearer [redacted-token]"),
            RedactionPattern.of("llm-job-token",
                    "SQVM_LLM_JOB_TOKEN=[A-Za-z0-9._\\-]{12,}", "SQVM_LLM_JOB_TOKEN=[redacted-token]"),
            RedactionPattern.of("helper-bearer-env",
                    "SQ_HELPER_BEARER=[A-Za-z0-9._\\-]{12,}", "SQ_HELPER_BEARER=[redacted-token]"),
            RedactionPattern.of("helper-token-env",
                    "SQVM_HELPER_TOKEN=[A-Za-z0-9._\\-]{12,}", "SQVM_HELPER_TOKEN=[redacted-token]"),
            // RFC 5321-bounded (local ≤ 64, DNS labels ≤ 63): the unbounded `[.]+@` form was O(n²) on long
            // runs of class characters with no '@' (e.g. a multi-MiB base64 file envelope on the stateful
            // result transport) and could pin a dispatch thread for minutes. Label-structured domain keeps the
            // classes disjoint from the '.' separator, so matching stays linear without atomic-group false
            // negatives.
            RedactionPattern.of("email",
                    "(?i)[a-z0-9._%+-]{1,64}@(?:[a-z0-9-]{1,63}\\.)+[a-z]{2,}", "[redacted-email]"),
            RedactionPattern.of("ipv6",
                    "\\b(?:[0-9a-fA-F]{1,4}:){5,7}[0-9a-fA-F]{1,4}\\b", "[redacted-ip]"),
            RedactionPattern.of("ipv4", "\\b\\d{1,3}(?:\\.\\d{1,3}){3}\\b", "[redacted-ip]"),
            RedactionPattern.of("phone-tr",
                    "(?:^|[^A-Za-z0-9_])((?:\\+?90|0)?[\\s\\-]?\\(?5\\d{2}\\)?[\\s\\-]?\\d{3}[\\s\\-]?\\d{2}[\\s\\-]?\\d{2})(?:$|[^A-Za-z0-9_])",
                    "[redacted-phone]"),
            RedactionPattern.of("tckn",
                    "(?:^|[^A-Za-z0-9_])([1-9]\\d{10})(?:$|[^A-Za-z0-9_])", "[redacted-tckn]"),
            RedactionPattern.of("credit-card",
                    "(?:^|[^A-Za-z0-9_])((?:\\d[ -]?){13,19})(?:$|[^A-Za-z0-9_])", "[redacted-card]"));

    private final List<RedactionPattern> patterns;

    /**
     * Default constructor (Spring bean) uses {@link #DEFAULT_PATTERNS}. Annotated because the
     * class declares a second constructor, so Spring cannot infer which one to use and fails
     * with "No default constructor found" at startup without this.
     */
    @org.springframework.beans.factory.annotation.Autowired
    public Redactor() {
        this(DEFAULT_PATTERNS);
    }

    public Redactor(List<RedactionPattern> patterns) {
        this.patterns = (patterns == null || patterns.isEmpty()) ? DEFAULT_PATTERNS : List.copyOf(patterns);
    }

    /** Placeholder a resolved-secret literal is replaced with. */
    private static final String SECRET_REPLACEMENT = "[redacted-secret]";

    /** Returns {@code s} with every pattern applied. Empty/null short-circuits. */
    public String redact(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        String out = s;
        for (RedactionPattern p : patterns) {
            out = p.regex().matcher(out).replaceAll(java.util.regex.Matcher.quoteReplacement(p.replacement()));
        }
        return out;
    }

    /**
     * Redacts {@code s} with the default patterns AND any explicit secret literals resolved for a job
     * — the OS-key-manager plaintext just bound into the sandbox context. Literals are matched
     * verbatim (not as regexes) and replaced with {@code [redacted-secret]} so a script that echoes a
     * resolved password/token to stdout/stderr can never land it in the persisted {@code
     * sandbox_execution_logs}.
     */
    public String redact(String s, java.util.Collection<String> secretLiterals) {
        String out = redact(s);
        if (out == null || out.isEmpty() || secretLiterals == null || secretLiterals.isEmpty()) {
            return out;
        }
        for (String literal : secretLiterals) {
            if (literal != null && !literal.isEmpty()) {
                out = out.replace(literal, SECRET_REPLACEMENT);
            }
        }
        return out;
    }

    /**
     * Recursively redacts every string value inside a JSON-shaped tree (maps/lists), returning a
     * fresh structure; the input is not mutated. Non-string scalars pass through.
     */
    @SuppressWarnings("unchecked")
    public Object redactTree(Object v) {
        return redactTree(v, null);
    }

    /**
     * Same as {@link #redactTree(Object)} plus verbatim replacement of resolved secret literals.
     */
    @SuppressWarnings("unchecked")
    public Object redactTree(Object v, java.util.Collection<String> secretLiterals) {
        if (v == null) {
            return null;
        }
        if (v instanceof String s) {
            return redact(s, secretLiterals);
        }
        if (v instanceof List<?> list) {
            List<Object> out = new java.util.ArrayList<>(list.size());
            for (Object e : list) {
                out.add(redactTree(e, secretLiterals));
            }
            return out;
        }
        if (v instanceof Map<?, ?> map) {
            Map<String, Object> out = new java.util.LinkedHashMap<>(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.put(String.valueOf(e.getKey()), redactTree(e.getValue(), secretLiterals));
            }
            return out;
        }
        return v;
    }
}
