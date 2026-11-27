package com.studioquack.sqs.sandbox.secrets;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves the six {@code $secretRef} placeholder forms into in-memory dotenv text that the
 * sandbox receives over stdin into its {@code --tmpfs /sandbox} scratch, consuming the {@link
 * SecretKeyStore} SPI. Fail-soft: a per-leaf keystore miss appends a {@link ResolveFailure} and
 * the walk continues — one missing secret never blanks the working ones.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SecretResolver {

    private static final Pattern COMPACT = Pattern.compile("^\\$secretRef:(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})$");
    private static final Pattern JSON_WRAPPER =
            Pattern.compile("^\\{\"\\$secretRef\":\"(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})\"}$");
    private static final Pattern BARE = Pattern.compile("^(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})$");
    private static final Pattern LEGACY_ENV = Pattern.compile("^\\$\\{SQVM_SECRET__([A-Z0-9_]+)}$");

    /** Lower-cased type discriminators the control plane uses to flag a parameter as carrying a secret. */
    private static final Set<String> SECRET_SHAPED_TYPES = Set.of(
            "password", "api_key", "apikey", "token", "secret", "credential", "credentials",
            "connection_string", "connectionstring", "private_key", "privatekey");

    private final SecretKeyStore keyStore;

    /**
     * Walks {@code tree} (mutating maps/lists in place so resolved placeholders become plaintext) and
     * renders the resolved {@code KEY='plaintext'} lines as in-memory dotenv text. Nothing is written
     * to the host: the text is piped over the container's stdin into its tmpfs scratch.
     */
    public SecretResolution resolveToEnv(Object tree) {
        Map<String, String> entries = new LinkedHashMap<>();
        List<ResolveFailure> failures = new ArrayList<>();
        walk(tree, "", entries, failures);

        String envContent = entries.isEmpty() ? null : renderDotenv(entries);
        if (!failures.isEmpty()) {
            for (ResolveFailure f : failures) {
                log.warn("secret resolve failed ref={} keyHint={} detail={}", f.ref(), f.keyHint(), f.detail());
            }
        }
        return new SecretResolution(envContent, entries.size(), failures);
    }

    /**
     * Resolves every {@code $secretRef} placeholder found under {@code tree} in place (mutating the
     * maps/lists so the handles become their OS-key-manager plaintext), for the just-in-time
     * sandbox-injection path where the resolved value must live INLINE in the node's execution context
     * (what the {@code studioquack} SDK reads from the baked {@code __SQ_CONTEXT__} / helper channel),
     * not in a side-channel {@code .env}. Unlike {@link #resolveToEnv} it renders no dotenv text.
     */
    public Set<String> resolveInline(Object tree) {
        if (tree == null) {
            return Set.of();
        }
        Map<String, String> entries = new LinkedHashMap<>();
        List<ResolveFailure> failures = new ArrayList<>();
        walk(tree, "", entries, failures);
        if (!failures.isEmpty()) {
            for (ResolveFailure f : failures) {
                log.warn("secret resolve (inline) failed ref={} keyHint={} detail={}",
                        f.ref(), f.keyHint(), f.detail());
            }
        }
        return new LinkedHashSet<>(entries.values());
    }

    /** Recursive walker returning the (possibly replaced) node so the parent can update its slot. */
    @SuppressWarnings("unchecked")
    private Object walk(Object v, String parentKey, Map<String, String> entries, List<ResolveFailure> failures) {
        if (v instanceof Map<?, ?> rawMap) {
            Map<String, Object> map = (Map<String, Object>) rawMap;
            // Preserve {"$webhookKeyRef":"whk_..."} placeholders untouched: webhook keys are
            // non-secret reference metadata resolved only by the daemon's webhook key store.
            if (map.containsKey("$webhookKeyRef")) {
                return map;
            }
            // Forms 1 & 2: a $secretRef key (regardless of siblings) collapses to plaintext.
            Object refVal = map.get("$secretRef");
            if (refVal instanceof String refStr) {
                String resolved = resolveRef(refStr, parentKey, entries, failures);
                return resolved != null ? resolved : v;
            }
            String hint = pickKeyHint(map, parentKey);
            // Form 4: secret-shaped type + a bare-ref string value.
            Object typeVal = map.get("type");
            Object valueVal = map.get("value");
            if (typeVal instanceof String t && SECRET_SHAPED_TYPES.contains(t.trim().toLowerCase())
                    && valueVal instanceof String vs && BARE.matcher(vs.trim()).matches()) {
                String resolved = resolveRef(vs.trim(), hint, entries, failures);
                if (resolved != null) {
                    map.put("value", resolved);
                }
            }
            for (Map.Entry<String, Object> e : new ArrayList<>(map.entrySet())) {
                String passKey = ("value".equals(e.getKey()) || "values".equals(e.getKey())) ? hint : e.getKey();
                map.put(e.getKey(), walk(e.getValue(), passKey, entries, failures));
            }
            return map;
        }
        if (v instanceof List<?> list) {
            List<Object> out = (List<Object>) list;
            for (int i = 0; i < out.size(); i++) {
                out.set(i, walk(out.get(i), parentKey, entries, failures));
            }
            return out;
        }
        if (v instanceof String s) {
            Matcher m;
            if ((m = COMPACT.matcher(s)).matches()) {
                String r = resolveRef(m.group(1), parentKey, entries, failures);
                return r != null ? r : v;
            }
            if ((m = JSON_WRAPPER.matcher(s)).matches()) {
                String r = resolveRef(m.group(1), parentKey, entries, failures);
                return r != null ? r : v;
            }
            if ((m = LEGACY_ENV.matcher(s)).matches()) {
                failures.add(new ResolveFailure(s, m.group(1),
                        "legacy ${SQVM_SECRET__<KEY>} form not resolvable by the daemon — BE must "
                                + "emit the object form before dispatch"));
                return v;
            }
        }
        return v;
    }

    /** Resolves one ref via the keystore, recording the {@code .env} entry + returning plaintext, or null. */
    private String resolveRef(String refStr, String hint, Map<String, String> entries,
                              List<ResolveFailure> failures) {
        var parsed = SecretRef.parse(refStr);
        if (parsed.isEmpty()) {
            failures.add(new ResolveFailure(refStr, hint, "malformed ref"));
            return null;
        }
        var resolved = keyStore.resolve(parsed.get());
        if (resolved.isEmpty()) {
            failures.add(new ResolveFailure(parsed.get().wire(), hint, "keystore has no entry"));
            return null;
        }
        SecretKeyStore.ResolvedSecret sec = resolved.get();
        String envKey = sanitizeEnvKey(chooseKey(hint, sec.key()));
        entries.put(envKey, sec.value());
        return sec.value();
    }

    private String chooseKey(String parentHint, String keystoreKey) {
        if (keystoreKey != null && !keystoreKey.isBlank()) {
            return keystoreKey;
        }
        return parentHint == null || parentHint.isBlank() ? "SQ_SECRET" : parentHint;
    }

    private String pickKeyHint(Map<String, Object> m, String parentKey) {
        for (String h : List.of("shortCode", "short_code", "key", "name", "targetPortValue", "target_port_value")) {
            Object val = m.get(h);
            if (val instanceof String s && !s.isEmpty()) {
                return s;
            }
        }
        return parentKey;
    }

    /** Sanitises a key to a valid POSIX env-var name (leading digit → prefixed with {@code _}). */
    private String sanitizeEnvKey(String raw) {
        String s = raw.replaceAll("[^A-Za-z0-9_]", "_");
        if (s.isEmpty() || Character.isDigit(s.charAt(0))) {
            s = "_" + s;
        }
        return s;
    }

    private static String renderDotenv(Map<String, String> entries) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : entries.entrySet()) {
            // Single-quote the value and escape embedded single quotes so multi-line / special
            // chars survive a POSIX `set -a; . .env` load in the sandbox helper.
            sb.append(e.getKey()).append("='")
                    .append(e.getValue().replace("'", "'\\''")).append("'\n");
        }
        return sb.toString();
    }
}
