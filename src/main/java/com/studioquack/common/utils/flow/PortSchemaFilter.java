package com.studioquack.common.utils.flow;

import com.studioquack.common.dto.flow.SchemaDto;
import org.apache.commons.collections4.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Drops map keys on a port value that are not declared in the port schema.
 * A null or empty schema, or a non-map value, passes through unchanged.
 */
public final class PortSchemaFilter {

    private PortSchemaFilter() {
        throw new AssertionError("utility class");
    }

    /** Returns {@code value} with undeclared map keys removed. */
    public static Object filter(List<SchemaDto> schema, Object value) {
        if (CollectionUtils.isEmpty(schema)) return value;
        if (!(value instanceof Map<?, ?> map)) return value;

        Set<String> allowed = schema.stream()
                .map(SchemaDto::getShortCode)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (allowed.isEmpty()) return value;

        Map<String, Object> filtered = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (!allowed.contains(key)) continue;
            SchemaDto matched = schema.stream()
                    .filter(s -> key.equals(s.getShortCode()))
                    .findFirst()
                    .orElse(null);
            if (matched != null && CollectionUtils.isNotEmpty(matched.getChildren())) {
                filtered.put(key, filter(matched.getChildren(), entry.getValue()));
            } else {
                filtered.put(key, entry.getValue());
            }
        }
        return filtered;
    }
}
