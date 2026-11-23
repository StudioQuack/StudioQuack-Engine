package com.studioquack.sqs.sandbox.helperchan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Helper-facing copy of the job execution context. {@code currentNode} keeps this node's resolved
 * values.
 */
final class NodeScopedHelperContext {

    private static final Pattern COMPACT =
            Pattern.compile("^\\$secretRef:(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})$");
    private static final Pattern JSON_WRAPPED =
            Pattern.compile("^\\{\"\\$secretRef\":\"(sec_[0-9a-f]{24}:v[1-9][0-9]{0,5})\"}$");
    private static final Pattern BARE_REF =
            Pattern.compile("^sec_[0-9a-f]{24}:v[1-9][0-9]{0,5}$");

    private NodeScopedHelperContext() {}

    static Object project(Object tree) {
        return redact(tree, false);
    }

    @SuppressWarnings("unchecked")
    private static Object redact(Object tree, boolean keepSecrets) {
        if (tree instanceof Map<?, ?> raw) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                String k = String.valueOf(e.getKey());
                boolean childKeep = keepSecrets || "currentNode".equals(k);
                if (!childKeep && "$secretRef".equals(k)) {
                    continue;
                }
                out.put(k, redact(e.getValue(), childKeep));
            }
            return out;
        }
        if (tree instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object e : list) {
                out.add(redact(e, keepSecrets));
            }
            return out;
        }
        if (tree instanceof String s && !keepSecrets && isForeignHandle(s)) {
            return "";
        }
        return tree;
    }

    private static boolean isForeignHandle(String s) {
        return COMPACT.matcher(s).matches()
                || JSON_WRAPPED.matcher(s).matches()
                || BARE_REF.matcher(s).matches();
    }
}
