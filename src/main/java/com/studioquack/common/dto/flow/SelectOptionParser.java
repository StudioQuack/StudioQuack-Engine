package com.studioquack.common.dto.flow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pure, stateless parser for {@code select}-type parameter options in the
 * {@code value:label:role} grammar.
 *
 * <p>Grammar rules (mirrored identically across control plane, engine daemons, and designer):
 * <ol>
 *   <li>Supports {@code \:} escaping for literal colons inside values or labels
 *       (e.g. {@code urn\:foo:URN Foo}).</li>
 *   <li>Preserves URL scheme colons ({@code ://}) as literal characters rather than
 *       segment delimiters so URLs like {@code https://example.com} stay intact.</li>
 *   <li>Splits remaining unescaped {@code :} segments:
 *     <ul>
 *       <li>1 segment → {@code (p0, p0, NORMAL)}</li>
 *       <li>2 segments → {@code (p0, p1.isEmpty() ? p0 : p1, NORMAL)}</li>
 *       <li>3+ segments → if the last segment matches a known {@link SelectOptionRole}
 *           (case-insensitive), take it as {@code role}, {@code p0} as {@code value},
 *           and the middle segments joined by {@code :} as {@code label} (falling back
 *           to {@code value} when blank); otherwise treat {@code p0} as {@code value},
 *           all remaining segments joined by {@code :} as {@code label}, and
 *           {@code role = NORMAL}.</li>
 *     </ul>
 *   </li>
 *   <li>Invariant: persisted {@link ParameterDto#getValue()} ALWAYS stores the pure
 *       {@code value} ({@code p0}), never the {@code :label:role} suffix.</li>
 * </ol>
 */
public final class SelectOptionParser {

    private static final String SELECT_PARAM_TYPE = "select";

    private SelectOptionParser() {
    }

    /** Parsed representation of one select option entry. */
    public record ParsedSelectOption(String value, String label, SelectOptionRole role) {
    }

    /** Identifies a specific parameter and its parsed {@link SelectOptionRole#LISTEN} option. */
    public record ListenOptionMatch(String parameterShortCode, ParsedSelectOption option) {
    }

    /**
     * Parse one raw option entry into {@code (value, label, role)}.
     * Null or blank entries yield {@code ("", "", NORMAL)}.
     */
    public static ParsedSelectOption parseOption(String raw) {
        if (raw == null || raw.isBlank()) {
            return new ParsedSelectOption("", "", SelectOptionRole.NORMAL);
        }
        List<String> segments = splitUnescapedColons(raw.trim());
        int count = segments.size();
        if (count == 1) {
            String value = segments.get(0);
            return new ParsedSelectOption(value, value, SelectOptionRole.NORMAL);
        }
        if (count == 2) {
            String value = segments.get(0);
            String label = segments.get(1).isEmpty() ? value : segments.get(1);
            return new ParsedSelectOption(value, label, SelectOptionRole.NORMAL);
        }

        // 3 or more segments: check whether the trailing segment is a known role token.
        String value = segments.get(0);
        String last = segments.get(count - 1);
        Optional<SelectOptionRole> parsedRole = SelectOptionRole.fromToken(last);
        if (parsedRole.isPresent()) {
            String middle = joinSegments(segments, 1, count - 1);
            String label = middle.isEmpty() ? value : middle;
            return new ParsedSelectOption(value, label, parsedRole.get());
        }

        // Trailing segment is not a role (e.g. URL with port or free-form label with colons).
        String remainder = joinSegments(segments, 1, count);
        String label = remainder.isEmpty() ? value : remainder;
        return new ParsedSelectOption(value, label, SelectOptionRole.NORMAL);
    }

    /** Parse all raw option strings for a select parameter. Null lists yield an empty list. */
    public static List<ParsedSelectOption> parseOptions(List<String> rawOptions) {
        if (rawOptions == null || rawOptions.isEmpty()) {
            return List.of();
        }
        List<ParsedSelectOption> parsed = new ArrayList<>(rawOptions.size());
        for (String raw : rawOptions) {
            if (raw != null && !raw.isBlank()) {
                parsed.add(parseOption(raw));
            }
        }
        return parsed;
    }

    /** Extract only the pure persisted wire value from a raw option entry. */
    public static String extractValue(String raw) {
        return parseOption(raw).value();
    }

    /**
     * Resolve the active {@link SelectOptionRole} for a node template based on the current
     * values of its {@code select}-type configuration parameters.
     *
     * <p>Returns the first non-{@link SelectOptionRole#NORMAL} role matched by an active
     * select parameter value, or {@link SelectOptionRole#NORMAL} when none match.
     */
    public static SelectOptionRole resolveActiveRole(TemplateDto template) {
        if (template == null || template.getConfigurationParameters() == null) {
            return SelectOptionRole.NORMAL;
        }
        for (ParameterDto param : template.getConfigurationParameters()) {
            if (!isSelectParameter(param) || param.getOptions() == null || param.getOptions().isEmpty()) {
                continue;
            }
            String activeValue = TemplateDto.stringifyParameterValue(param.getValue()).trim();
            if (activeValue.isEmpty()) {
                continue;
            }
            for (ParsedSelectOption option : parseOptions(param.getOptions())) {
                if (option.value().equals(activeValue) && option.role() != SelectOptionRole.NORMAL) {
                    return option.role();
                }
            }
        }
        return SelectOptionRole.NORMAL;
    }

    /**
     * True when the template declares at least one {@code select}-type parameter containing
     * an option with role {@link SelectOptionRole#LISTEN}.
     */
    public static boolean hasListenOption(TemplateDto template) {
        return findListenOption(template).isPresent();
    }

    /**
     * Find the first {@code select}-type parameter option carrying {@link SelectOptionRole#LISTEN}
     * on the given template.
     */
    public static Optional<ListenOptionMatch> findListenOption(TemplateDto template) {
        if (template == null || template.getConfigurationParameters() == null) {
            return Optional.empty();
        }
        for (ParameterDto param : template.getConfigurationParameters()) {
            if (!isSelectParameter(param) || param.getOptions() == null || param.getOptions().isEmpty()) {
                continue;
            }
            for (ParsedSelectOption option : parseOptions(param.getOptions())) {
                if (option.role() == SelectOptionRole.LISTEN) {
                    return Optional.of(new ListenOptionMatch(param.getShortCode(), option));
                }
            }
        }
        return Optional.empty();
    }

    private static boolean isSelectParameter(ParameterDto param) {
        return param != null
                && param.getType() != null
                && SELECT_PARAM_TYPE.equalsIgnoreCase(param.getType().trim());
    }

    /**
     * Split on unescaped {@code :} while unescaping {@code \:} and preserving URL scheme
     * colons ({@code ://}) inside the current segment.
     */
    private static List<String> splitUnescapedColons(String input) {
        List<String> parts = new ArrayList<>(3);
        StringBuilder current = new StringBuilder(input.length());
        int len = input.length();
        for (int i = 0; i < len; i++) {
            char c = input.charAt(i);
            if (c == '\\' && i + 1 < len && input.charAt(i + 1) == ':') {
                current.append(':');
                i++;
                continue;
            }
            if (c == ':') {
                // Keep URL scheme separator "://" intact inside the current segment.
                if (i + 2 < len && input.charAt(i + 1) == '/' && input.charAt(i + 2) == '/') {
                    current.append("://");
                    i += 2;
                    continue;
                }
                parts.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        parts.add(current.toString().trim());
        return parts;
    }

    private static String joinSegments(List<String> segments, int fromInclusive, int toExclusive) {
        StringBuilder sb = new StringBuilder();
        for (int i = fromInclusive; i < toExclusive; i++) {
            if (i > fromInclusive) {
                sb.append(':');
            }
            sb.append(segments.get(i));
        }
        return sb.toString().trim();
    }
}
