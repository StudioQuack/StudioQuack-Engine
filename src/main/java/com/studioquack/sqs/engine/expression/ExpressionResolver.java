package com.studioquack.sqs.engine.expression;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded recursive-descent expression resolver for {@code $(…)} addressing and
 * gateway/parameter conditions. <b>Not</b> SpEL/JUEL — no user code, no Spring beans.
 *
 * <p><b>Node outputs ({@code $(@shortCode.port.path)}):</b> looks up {@code nodes} by
 * {@link TemplateDto#getNodeShortCode}, loads producer outputs via
 * {@link ExecutionContextStore#collectOutputsByNode(String)}. The first remaining path
 * segment is a <em>port selector</em> when it is a property (not an index): match
 * {@link InOutValueDto#getPortId} first, then {@link InOutValueDto#getPortValue}.
 * A matching port walks only the remaining segments on that entry's value. An unmatched
 * selector falls back to the first output walked with the <em>full</em> path (so
 * {@code $(@cart.total)} still reads a field on the primary output). An index-first
 * path ({@code @shortCode[0]}) is never treated as a port. Remaining segments are
 * walked as dot properties + zero-based {@code [n]} / {@code (n)} indexes
 * ({@code (n)} is the FE-stored form; {@code [n]} stays for compatibility) and
 * {@code (*)} wildcards at any depth (nested lists flatten).
 *
 * <p><b>Wildcard ({@code (*)}):</b> the ref is multi-valued (one value per list
 * item, recursively flattened). A comparison is true when <em>any</em> element
 * satisfies it; a bare multi-valued ref in a boolean position is truthy when any
 * element is truthy. Expansion is capped at {@link #MAX_WILDCARD_VALUES} (512);
 * extra items are truncated, never an error. Out-of-range {@code (n)}/{@code [n]}
 * is missing/null (condition false), never an exception.
 *
 * <p><b>Globals:</b> {@code $(wf.key)} / {@code $(Workflow_Globals.key)} read
 * {@link ExecutionContext#getWorkflowVariables}; {@code $(project.key)} /
 * {@code $(Project_Globals.key)} read {@link ExecutionContext#getProjectVariables}.
 *
 * <p><b>Bare operands</b> in conditions: {@code @shortCode.port.path},
 * {@code Workflow_Globals.key}, and {@code Project_Globals.key} (same resolution as
 * the {@code $(…)} forms). Mixed text still substitutes only {@code $(…)}.
 *
 * <p><b>Literals</b> in conditions: single-quoted ({@code 'foo'}) and double-quoted
 * ({@code "foo"}) strings parse to the same String operand (the opening quote must
 * match the closer; no escape sequences). Also numbers and
 * {@code true}/{@code false}/{@code null}.
 *
 * <p>{@code $secretRef…} placeholders are never rewritten by string substitution.
 */
/** Bean name avoids Activiti {@code ConnectorsAutoConfiguration#expressionResolver}. */
@Component("sqsExpressionResolver")
public class ExpressionResolver {

    public static final int MAX_EXPR_LEN = 2048;
    public static final int MAX_REFS_PER_VALUE = 64;
    /** Max values a single {@code (*)} expansion may produce; extras are dropped. */
    public static final int MAX_WILDCARD_VALUES = 512;

    private static final Logger log = LoggerFactory.getLogger(ExpressionResolver.class);

    private final ExecutionContextStore contextStore;

    public ExpressionResolver(ExecutionContextStore contextStore) {
        this.contextStore = contextStore;
    }

    /**
     * Resolves a parameter / mapping value. Ordinary strings (no {@code $(} and not a
     * sole bare ref) return unchanged. A sole {@code $(…)} or sole bare
     * {@code @path} / {@code Workflow_Globals.key} / {@code Project_Globals.key}
     * (no whitespace) returns the raw Object; mixed text substitutes only {@code $(…)}.
     */
    public Object resolveValue(String expr, ExecutionContext ctx) {
        if (expr == null) {
            return null;
        }
        String trimmed = expr.trim();
        boolean hasDollar = expr.contains("$(");
        boolean soleBare = !hasDollar && isSoleBareRef(trimmed);
        if (!hasDollar && !soleBare) {
            return expr;
        }
        if (expr.length() > MAX_EXPR_LEN) {
            log.warn("ExpressionResolver: resolveValue exceeds MAX_EXPR_LEN ({})", MAX_EXPR_LEN);
            return expr;
        }
        if (soleBare) {
            try {
                return unwrapMulti(resolveRefBody(trimmed, ctx));
            } catch (RuntimeException ex) {
                log.warn("ExpressionResolver: resolveValue ref failed: {}", ex.getMessage());
                return null;
            }
        }
        if (isSoleRef(trimmed)) {
            try {
                return unwrapMulti(resolveRefBody(trimmed.substring(2, trimmed.length() - 1), ctx));
            } catch (RuntimeException ex) {
                log.warn("ExpressionResolver: resolveValue ref failed: {}", ex.getMessage());
                return null;
            }
        }
        return substituteInString(expr, ctx);
    }

    /** {@code result=false} and {@code error} non-null on parse/eval failure. {@code error} is {@code "parse"} or {@code "eval"}. */
    public record ConditionOutcome(boolean result, String error) {}

    /**
     * Parses and evaluates a bounded condition. Never throws into the engine —
     * parse/eval errors and illegal ordering ops yield {@code false} + WARN.
     */
    public boolean evaluateCondition(String expr, ExecutionContext ctx) {
        return evaluateConditionOutcome(expr, ctx).result();
    }

    /**
     * Same fail-closed evaluation as {@link #evaluateCondition}, plus an error marker
     * for gateway decision recording. Blank/null is not a parse failure.
     */
    public ConditionOutcome evaluateConditionOutcome(String expr, ExecutionContext ctx) {
        if (expr == null || expr.isBlank()) {
            return new ConditionOutcome(false, null);
        }
        if (expr.length() > MAX_EXPR_LEN) {
            log.warn("ExpressionResolver: condition exceeds MAX_EXPR_LEN ({})", MAX_EXPR_LEN);
            return new ConditionOutcome(false, "parse");
        }
        try {
            Parser p = new Parser(expr, ctx, this);
            Object result = p.parseOr();
            p.expectEnd();
            return new ConditionOutcome(toBoolean(result), null);
        } catch (RuntimeException ex) {
            log.warn("ExpressionResolver: condition eval failed: {}", ex.getMessage());
            return new ConditionOutcome(false, isParseFailureMessage(ex.getMessage()) ? "parse" : "eval");
        }
    }

    /** Heuristic: parser IAE messages vs unexpected eval-time failures. */
    static boolean isParseFailureMessage(String message) {
        if (message == null || message.isEmpty()) {
            return false;
        }
        String m = message.toLowerCase();
        return m.contains("unexpected token")
                || m.contains("trailing input")
                || m.contains("unclosed")
                || m.contains("missing ')'")
                || m.contains("bad number")
                || m.contains("expected operand")
                || m.contains("unexpected end")
                || m.contains("expected '('")
                || m.contains("empty ident")
                || m.contains("empty property")
                || m.contains("empty ref")
                || m.contains("bad index")
                || m.contains("bad wildcard")
                || m.contains("unknown ref")
                || m.contains("unknown function")
                || m.contains("unexpected path")
                || m.contains("missing nodeshortcode")
                || m.contains("missing variable")
                || m.contains("expects ");
    }

    /**
     * Substitutes {@code $(…)} refs inside {@code text}. Skips {@code $secretRef} patterns.
     * Caps at {@link #MAX_REFS_PER_VALUE} refs; each substituted value is capped at
     * {@link #MAX_EXPR_LEN} characters.
     */
    public String substituteInString(String text, ExecutionContext ctx) {
        if (text == null || !text.contains("$(")) {
            return text;
        }
        StringBuilder out = new StringBuilder(text.length());
        int i = 0;
        int refs = 0;
        while (i < text.length()) {
            int dollar = text.indexOf('$', i);
            if (dollar < 0) {
                out.append(text, i, text.length());
                break;
            }
            out.append(text, i, dollar);
            // Leave $secretRef… alone (parameter resolver must not touch secrets).
            if (text.startsWith("$secretRef", dollar)) {
                out.append("$secretRef");
                i = dollar + "$secretRef".length();
                continue;
            }
            if (dollar + 1 >= text.length() || text.charAt(dollar + 1) != '(') {
                out.append('$');
                i = dollar + 1;
                continue;
            }
            int close = findMatchingParen(text, dollar + 1);
            if (close < 0) {
                out.append(text, dollar, text.length());
                break;
            }
            refs++;
            if (refs > MAX_REFS_PER_VALUE) {
                log.warn("ExpressionResolver: exceeded MAX_REFS_PER_VALUE ({})", MAX_REFS_PER_VALUE);
                out.append(text, dollar, text.length());
                break;
            }
            String body = text.substring(dollar + 2, close);
            Object resolved;
            try {
                resolved = resolveRefBody(body, ctx);
            } catch (RuntimeException ex) {
                log.warn("ExpressionResolver: ref substitution failed: {}", ex.getMessage());
                resolved = null;
            }
            String piece = stringifyForSubstitution(resolved);
            if (piece.length() > MAX_EXPR_LEN) {
                log.warn("ExpressionResolver: substituted value exceeds MAX_EXPR_LEN ({})", MAX_EXPR_LEN);
                piece = piece.substring(0, MAX_EXPR_LEN);
            }
            out.append(piece);
            i = close + 1;
        }
        return out.toString();
    }

    // ---- Ref resolution ---------------------------------------------------

    Object resolveRefBody(String body, ExecutionContext ctx) {
        if (body == null) {
            return null;
        }
        String s = body.trim();
        if (s.isEmpty()) {
            throw new IllegalArgumentException("empty ref");
        }
        if (s.startsWith("@")) {
            return resolveNodeRef(s.substring(1), ctx);
        }
        if (s.startsWith("wf.")) {
            return resolveScopedMap(ctx != null ? ctx.getWorkflowVariables() : null, s.substring(3));
        }
        if (s.startsWith("project.")) {
            return resolveScopedMap(ctx != null ? ctx.getProjectVariables() : null, s.substring(8));
        }
        if (s.startsWith("Workflow_Globals.")) {
            return resolveScopedMap(ctx != null ? ctx.getWorkflowVariables() : null,
                    s.substring("Workflow_Globals.".length()));
        }
        if (s.startsWith("Project_Globals.")) {
            return resolveScopedMap(ctx != null ? ctx.getProjectVariables() : null,
                    s.substring("Project_Globals.".length()));
        }
        throw new IllegalArgumentException("unknown ref scope: " + s);
    }

    private Object resolveNodeRef(String remainder, ExecutionContext ctx) {
        PathParse pp = parseLeadingIdentThenPath(remainder);
        String shortCode = pp.ident;
        if (shortCode == null || shortCode.isEmpty()) {
            throw new IllegalArgumentException("missing nodeShortCode");
        }
        NodeDto node = findNodeByShortCode(ctx, shortCode);
        if (node == null) {
            return null;
        }
        String wfeId = ctx.getWorkflowExecutionId();
        if (wfeId == null || contextStore == null) {
            return null;
        }
        Map<String, List<InOutValueDto>> byNode = contextStore.collectOutputsByNode(wfeId);
        List<InOutValueDto> outs = byNode != null ? byNode.get(node.getId()) : null;
        if (outs == null || outs.isEmpty()) {
            return null;
        }
        List<PathSeg> segs = pp.segments;
        // First property segment is a port selector (id, then shortCode). Index-first
        // paths and unmatched selectors keep the first-output + full-path fallback.
        if (!segs.isEmpty() && segs.get(0).property != null) {
            InOutValueDto port = findOutputByPortSelector(outs, segs.get(0).property);
            if (port != null) {
                return walkResolved(port.getValue(), segs.subList(1, segs.size()));
            }
        }
        return walkResolved(outs.get(0).getValue(), segs);
    }

    /** Port id first, then portValue (shortCode). No single-output fallback. */
    private static InOutValueDto findOutputByPortSelector(List<InOutValueDto> outs, String selector) {
        for (InOutValueDto dto : outs) {
            if (dto != null && selector.equals(dto.getPortId())) {
                return dto;
            }
        }
        for (InOutValueDto dto : outs) {
            if (dto != null && selector.equals(dto.getPortValue())) {
                return dto;
            }
        }
        return null;
    }

    private Object resolveScopedMap(Map<String, Object> map, String path) {
        PathParse pp = parseLeadingIdentThenPath(path);
        if (pp.ident == null) {
            throw new IllegalArgumentException("missing variable key");
        }
        Object root = map == null ? null : map.get(pp.ident);
        return walkResolved(root, pp.segments);
    }

    private static NodeDto findNodeByShortCode(ExecutionContext ctx, String shortCode) {
        if (ctx == null || ctx.getNodes() == null) {
            return null;
        }
        for (NodeDto n : ctx.getNodes()) {
            if (n == null || n.getData() == null) {
                continue;
            }
            if (shortCode.equals(n.getData().getNodeShortCode())) {
                return n;
            }
        }
        return null;
    }

    /** Single-value walk, or {@link MultiValue} when the path contains {@code (*)}. */
    static Object walkResolved(Object root, List<PathSeg> segments) {
        if (hasWildcard(segments)) {
            return new MultiValue(walkPathExpand(root, segments));
        }
        return walkPath(root, segments);
    }

    static Object walkPath(Object root, List<PathSeg> segments) {
        Object cur = root;
        for (PathSeg seg : segments) {
            if (cur == null) {
                return null;
            }
            if (seg.wildcard) {
                return null;
            }
            if (seg.index != null) {
                cur = indexAt(cur, seg.index);
            } else {
                if (cur instanceof Map<?, ?> m) {
                    cur = m.get(seg.property);
                } else {
                    return null;
                }
            }
        }
        return cur;
    }

    /**
     * Flattens {@code (*)} at each depth. Working set is capped at
     * {@link #MAX_WILDCARD_VALUES}; extras are dropped, never an error.
     */
    private static List<Object> walkPathExpand(Object root, List<PathSeg> segments) {
        List<Object> cur = new ArrayList<>();
        cur.add(root);
        for (PathSeg seg : segments) {
            List<Object> next = new ArrayList<>();
            for (Object item : cur) {
                if (next.size() >= MAX_WILDCARD_VALUES) {
                    break;
                }
                if (item == null) {
                    continue;
                }
                if (seg.wildcard) {
                    appendListItemsCapped(item, next);
                } else if (seg.index != null) {
                    Object v = indexAt(item, seg.index);
                    if (v != null) {
                        addCapped(next, v);
                    }
                } else if (item instanceof Map<?, ?> m) {
                    addCapped(next, m.get(seg.property));
                }
            }
            cur = next;
        }
        return cur;
    }

    private static boolean hasWildcard(List<PathSeg> segments) {
        for (PathSeg seg : segments) {
            if (seg.wildcard) {
                return true;
            }
        }
        return false;
    }

    /** List/array index; out-of-range or non-list → {@code null} (never throws). */
    private static Object indexAt(Object cur, int idx) {
        if (cur instanceof List<?> list) {
            if (idx < 0 || idx >= list.size()) {
                return null;
            }
            return list.get(idx);
        }
        if (cur instanceof Object[] arr) {
            if (idx < 0 || idx >= arr.length) {
                return null;
            }
            return arr[idx];
        }
        return null;
    }

    private static void appendListItemsCapped(Object cur, List<Object> dest) {
        if (cur instanceof List<?> list) {
            for (Object e : list) {
                if (!addCapped(dest, e)) {
                    return;
                }
            }
        } else if (cur instanceof Object[] arr) {
            for (Object e : arr) {
                if (!addCapped(dest, e)) {
                    return;
                }
            }
        }
    }

    private static boolean addCapped(List<Object> dest, Object v) {
        if (dest.size() >= MAX_WILDCARD_VALUES) {
            return false;
        }
        dest.add(v);
        return true;
    }

    private static Object unwrapMulti(Object v) {
        if (v instanceof MultiValue mv) {
            return mv.values;
        }
        return v;
    }

    // ---- Path parse helpers -----------------------------------------------

    /** Marker for a {@code (*)} expansion — not a user-visible List value. */
    static final class MultiValue {
        final List<Object> values;

        MultiValue(List<Object> values) {
            this.values = values == null ? List.of() : values;
        }
    }

    private static final class PathSeg {
        final String property;
        final Integer index;
        final boolean wildcard;

        PathSeg(String property) {
            this.property = property;
            this.index = null;
            this.wildcard = false;
        }

        PathSeg(int index) {
            this.property = null;
            this.index = index;
            this.wildcard = false;
        }

        static PathSeg star() {
            return new PathSeg(true);
        }

        private PathSeg(boolean wildcard) {
            this.property = null;
            this.index = null;
            this.wildcard = wildcard;
        }
    }

    private static final class PathParse {
        final String ident;
        final List<PathSeg> segments;

        PathParse(String ident, List<PathSeg> segments) {
            this.ident = ident;
            this.segments = segments;
        }
    }

    private static PathParse parseLeadingIdentThenPath(String s) {
        String in = s == null ? "" : s.trim();
        int i = 0;
        StringBuilder id = new StringBuilder();
        while (i < in.length()) {
            char c = in.charAt(i);
            if (isIdentChar(c)) {
                id.append(c);
                i++;
            } else {
                break;
            }
        }
        List<PathSeg> segs = new ArrayList<>();
        while (i < in.length()) {
            char c = in.charAt(i);
            if (c == '.') {
                i++;
                StringBuilder prop = new StringBuilder();
                while (i < in.length() && isIdentChar(in.charAt(i))) {
                    prop.append(in.charAt(i++));
                }
                if (prop.isEmpty()) {
                    throw new IllegalArgumentException("empty property in path");
                }
                segs.add(new PathSeg(prop.toString()));
            } else if (c == '[') {
                i++;
                int start = i;
                while (i < in.length() && Character.isDigit(in.charAt(i))) {
                    i++;
                }
                if (start == i || i >= in.length() || in.charAt(i) != ']') {
                    throw new IllegalArgumentException("bad index in path");
                }
                segs.add(new PathSeg(Integer.parseInt(in.substring(start, i))));
                i++; // ]
            } else if (c == '(') {
                i = consumeParenIndex(in, i, segs);
            } else {
                throw new IllegalArgumentException("unexpected path char: " + c);
            }
        }
        return new PathParse(id.toString(), segs);
    }

    /**
     * {@code i} points at {@code '('}. Consumes {@code (*)} or {@code (digits)}.
     * @return index after the closing {@code ')'}
     */
    private static int consumeParenIndex(String in, int i, List<PathSeg> segs) {
        i++; // (
        if (i < in.length() && in.charAt(i) == '*') {
            i++;
            if (i >= in.length() || in.charAt(i) != ')') {
                throw new IllegalArgumentException("bad wildcard in path");
            }
            segs.add(PathSeg.star());
            return i + 1;
        }
        int start = i;
        while (i < in.length() && Character.isDigit(in.charAt(i))) {
            i++;
        }
        if (start == i || i >= in.length() || in.charAt(i) != ')') {
            throw new IllegalArgumentException("bad index in path");
        }
        segs.add(new PathSeg(Integer.parseInt(in.substring(start, i))));
        return i + 1;
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '-';
    }

    private static boolean isSoleRef(String trimmed) {
        if (trimmed.length() < 3 || !trimmed.startsWith("$(") || !trimmed.endsWith(")")) {
            return false;
        }
        return findMatchingParen(trimmed, 1) == trimmed.length() - 1;
    }

    /** Sole bare @path / Workflow_Globals.x / Project_Globals.x — no whitespace. */
    private static boolean isSoleBareRef(String trimmed) {
        if (trimmed.isEmpty()) {
            return false;
        }
        for (int i = 0; i < trimmed.length(); i++) {
            if (Character.isWhitespace(trimmed.charAt(i))) {
                return false;
            }
        }
        return trimmed.charAt(0) == '@'
                || trimmed.startsWith("Workflow_Globals.")
                || trimmed.startsWith("Project_Globals.");
    }

    /** {@code openIdx} points at {@code '('}. */
    private static int findMatchingParen(String s, int openIdx) {
        int depth = 0;
        for (int i = openIdx; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static String stringifyForSubstitution(Object v) {
        if (v instanceof MultiValue mv) {
            v = mv.values;
        }
        if (v == null) {
            return "";
        }
        if (v instanceof CharSequence || v instanceof Number || v instanceof Boolean) {
            return v.toString();
        }
        return String.valueOf(v);
    }

    static boolean toBoolean(Object v) {
        if (v instanceof MultiValue mv) {
            for (Object e : mv.values) {
                if (toBoolean(e)) {
                    return true;
                }
            }
            return false;
        }
        if (v instanceof Boolean b) {
            return b;
        }
        if (v == null) {
            return false;
        }
        if (v instanceof Number n) {
            return n.doubleValue() != 0.0;
        }
        String s = String.valueOf(v);
        return !"false".equalsIgnoreCase(s) && !"null".equalsIgnoreCase(s) && !s.isEmpty();
    }

    // ---- Condition parser -------------------------------------------------

    private final class Parser {
        private final String src;
        private final ExecutionContext ctx;
        private final ExpressionResolver resolver;
        private int pos;

        Parser(String src, ExecutionContext ctx, ExpressionResolver resolver) {
            this.src = src;
            this.ctx = ctx;
            this.resolver = resolver;
            this.pos = 0;
        }

        Object parseOr() {
            Object left = parseAnd();
            while (matchOp("||")) {
                Object right = parseAnd();
                left = toBoolean(left) || toBoolean(right);
            }
            return left;
        }

        Object parseAnd() {
            Object left = parseNot();
            while (matchOp("&&")) {
                Object right = parseNot();
                left = toBoolean(left) && toBoolean(right);
            }
            return left;
        }

        Object parseNot() {
            if (matchOp("!")) {
                return !toBoolean(parseNot());
            }
            return parseComparison();
        }

        Object parseComparison() {
            Object left = parsePrimary();
            skipWs();
            String op = peekComparisonOp();
            if (op == null) {
                return left;
            }
            pos += op.length();
            Object right = parsePrimary();
            return compare(left, op, right);
        }

        Object parsePrimary() {
            skipWs();
            if (pos >= src.length()) {
                throw new IllegalArgumentException("unexpected end");
            }
            if (src.charAt(pos) == '(') {
                pos++;
                Object v = parseOr();
                skipWs();
                if (pos >= src.length() || src.charAt(pos) != ')') {
                    throw new IllegalArgumentException("missing ')'");
                }
                pos++;
                return v;
            }
            if (lookingAtIdent("contains") || lookingAtIdent("startsWith")
                    || lookingAtIdent("endsWith") || lookingAtIdent("exists")) {
                return parseFunction();
            }
            return parseOperand();
        }

        Object parseFunction() {
            skipWs();
            String name = readIdent();
            skipWs();
            if (pos >= src.length() || src.charAt(pos) != '(') {
                throw new IllegalArgumentException("expected '(' after " + name);
            }
            pos++;
            List<Object> args = new ArrayList<>();
            skipWs();
            if (pos < src.length() && src.charAt(pos) != ')') {
                args.add(parseOr());
                skipWs();
                while (pos < src.length() && src.charAt(pos) == ',') {
                    pos++;
                    args.add(parseOr());
                    skipWs();
                }
            }
            if (pos >= src.length() || src.charAt(pos) != ')') {
                throw new IllegalArgumentException("missing ')' in " + name);
            }
            pos++;
            return evalFunction(name, args);
        }

        Object parseOperand() {
            skipWs();
            if (pos >= src.length()) {
                throw new IllegalArgumentException("expected operand");
            }
            // $(…) ref
            if (pos + 1 < src.length() && src.charAt(pos) == '$' && src.charAt(pos + 1) == '(') {
                int open = pos + 1;
                int close = findMatchingParen(src, open);
                if (close < 0) {
                    throw new IllegalArgumentException("unclosed $(");
                }
                String body = src.substring(open + 1, close);
                pos = close + 1;
                return resolver.resolveRefBody(body, ctx);
            }
            // Bare @path (ident(.ident|[n])*)
            if (src.charAt(pos) == '@') {
                int start = pos;
                pos++;
                consumePathToken();
                return resolver.resolveRefBody(src.substring(start, pos), ctx);
            }
            char c = src.charAt(pos);
            // Same closer as opener; no escapes. 'foo' and "foo" are the same String.
            if (c == '\'' || c == '"') {
                return readQuoted(c);
            }
            if (c == '-' || Character.isDigit(c)) {
                return readNumber();
            }
            if (lookingAtIdent("true")) {
                pos += 4;
                return Boolean.TRUE;
            }
            if (lookingAtIdent("false")) {
                pos += 5;
                return Boolean.FALSE;
            }
            if (lookingAtIdent("null")) {
                pos += 4;
                return null;
            }
            if (lookingAtIdent("Workflow_Globals") || lookingAtIdent("Project_Globals")) {
                int start = pos;
                consumePathToken();
                return resolver.resolveRefBody(src.substring(start, pos), ctx);
            }
            throw new IllegalArgumentException("unexpected token at " + pos);
        }

        /** ident(.ident|[n]|(n)|(*))* — same rules as parseLeadingIdentThenPath. */
        private void consumePathToken() {
            int identStart = pos;
            while (pos < src.length() && isIdentChar(src.charAt(pos))) {
                pos++;
            }
            if (pos == identStart) {
                throw new IllegalArgumentException("empty ident in path");
            }
            while (pos < src.length()) {
                char ch = src.charAt(pos);
                if (ch == '.') {
                    pos++;
                    int propStart = pos;
                    while (pos < src.length() && isIdentChar(src.charAt(pos))) {
                        pos++;
                    }
                    if (pos == propStart) {
                        throw new IllegalArgumentException("empty property in path");
                    }
                } else if (ch == '[') {
                    pos++;
                    int digitStart = pos;
                    while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
                        pos++;
                    }
                    if (digitStart == pos || pos >= src.length() || src.charAt(pos) != ']') {
                        throw new IllegalArgumentException("bad index in path");
                    }
                    pos++;
                } else if (ch == '(') {
                    pos++;
                    if (pos < src.length() && src.charAt(pos) == '*') {
                        pos++;
                        if (pos >= src.length() || src.charAt(pos) != ')') {
                            throw new IllegalArgumentException("bad wildcard in path");
                        }
                        pos++;
                    } else {
                        int digitStart = pos;
                        while (pos < src.length() && Character.isDigit(src.charAt(pos))) {
                            pos++;
                        }
                        if (digitStart == pos || pos >= src.length() || src.charAt(pos) != ')') {
                            throw new IllegalArgumentException("bad index in path");
                        }
                        pos++;
                    }
                } else {
                    break;
                }
            }
        }

        private Object evalFunction(String name, List<Object> args) {
            switch (name) {
                case "exists" -> {
                    if (args.size() != 1) {
                        throw new IllegalArgumentException("exists expects 1 arg");
                    }
                    Object a0 = args.get(0);
                    if (a0 instanceof MultiValue mv) {
                        for (Object e : mv.values) {
                            if (e != null) {
                                return true;
                            }
                        }
                        return false;
                    }
                    return a0 != null;
                }
                case "contains" -> {
                    requireArity(name, args, 2);
                    String a = asString(args.get(0));
                    String b = asString(args.get(1));
                    return a != null && b != null && a.contains(b);
                }
                case "startsWith" -> {
                    requireArity(name, args, 2);
                    String a = asString(args.get(0));
                    String b = asString(args.get(1));
                    return a != null && b != null && a.startsWith(b);
                }
                case "endsWith" -> {
                    requireArity(name, args, 2);
                    String a = asString(args.get(0));
                    String b = asString(args.get(1));
                    return a != null && b != null && a.endsWith(b);
                }
                default -> throw new IllegalArgumentException("unknown function: " + name);
            }
        }

        private static void requireArity(String name, List<Object> args, int n) {
            if (args.size() != n) {
                throw new IllegalArgumentException(name + " expects " + n + " args");
            }
        }

        private Object compare(Object left, String op, Object right) {
            if (left instanceof MultiValue || right instanceof MultiValue) {
                List<Object> lefts = left instanceof MultiValue mv ? mv.values : List.of(left);
                List<Object> rights = right instanceof MultiValue mv ? mv.values : List.of(right);
                for (Object l : lefts) {
                    for (Object r : rights) {
                        if (Boolean.TRUE.equals(compareScalar(l, op, r))) {
                            return true;
                        }
                    }
                }
                return false;
            }
            return compareScalar(left, op, right);
        }

        private Object compareScalar(Object left, String op, Object right) {
            Double ln = asNumber(left);
            Double rn = asNumber(right);
            if (ln != null && rn != null) {
                int cmp = Double.compare(ln, rn);
                return switch (op) {
                    case "==" -> cmp == 0;
                    case "!=" -> cmp != 0;
                    case ">" -> cmp > 0;
                    case ">=" -> cmp >= 0;
                    case "<" -> cmp < 0;
                    case "<=" -> cmp <= 0;
                    default -> false;
                };
            }
            // Non-numeric: == / != via string compare; ordering ⇒ false + WARN
            if ("==".equals(op) || "!=".equals(op)) {
                boolean eq;
                if (left == null && right == null) {
                    eq = true;
                } else if (left == null || right == null) {
                    eq = false;
                } else {
                    eq = Objects.equals(stringifyComparable(left), stringifyComparable(right));
                }
                return "==".equals(op) ? eq : !eq;
            }
            log.warn("ExpressionResolver: ordering op '{}' on non-numeric operands", op);
            return false;
        }

        private String readQuoted(char quote) {
            pos++; // opening ' or "
            StringBuilder sb = new StringBuilder();
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == quote) {
                    pos++;
                    return sb.toString();
                }
                sb.append(c);
                pos++;
            }
            throw new IllegalArgumentException("unclosed string");
        }

        private Number readNumber() {
            int start = pos;
            if (src.charAt(pos) == '-') {
                pos++;
            }
            boolean seenDot = false;
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (Character.isDigit(c)) {
                    pos++;
                } else if (c == '.' && !seenDot) {
                    seenDot = true;
                    pos++;
                } else {
                    break;
                }
            }
            String lit = src.substring(start, pos);
            if (lit.equals("-") || lit.isEmpty() || lit.equals(".")) {
                throw new IllegalArgumentException("bad number");
            }
            if (seenDot) {
                return Double.parseDouble(lit);
            }
            try {
                return Long.parseLong(lit);
            } catch (NumberFormatException ex) {
                return Double.parseDouble(lit);
            }
        }

        private String readIdent() {
            int start = pos;
            while (pos < src.length() && isIdentChar(src.charAt(pos))) {
                pos++;
            }
            return src.substring(start, pos);
        }

        private boolean lookingAtIdent(String word) {
            skipWs();
            if (!src.startsWith(word, pos)) {
                return false;
            }
            int after = pos + word.length();
            return after >= src.length() || !isIdentChar(src.charAt(after));
        }

        private boolean matchOp(String op) {
            skipWs();
            if (src.startsWith(op, pos)) {
                // Avoid matching '!' of '!=' when looking for '!'
                if ("!".equals(op) && src.startsWith("!=", pos)) {
                    return false;
                }
                pos += op.length();
                return true;
            }
            return false;
        }

        private String peekComparisonOp() {
            if (src.startsWith("==", pos) || src.startsWith("!=", pos)
                    || src.startsWith(">=", pos) || src.startsWith("<=", pos)) {
                return src.substring(pos, pos + 2);
            }
            if (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == '>' || c == '<') {
                    return String.valueOf(c);
                }
            }
            return null;
        }

        void expectEnd() {
            skipWs();
            if (pos < src.length()) {
                throw new IllegalArgumentException("trailing input at " + pos);
            }
        }

        private void skipWs() {
            while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) {
                pos++;
            }
        }
    }

    private static String asString(Object v) {
        if (v == null) {
            return null;
        }
        return stringifyComparable(v);
    }

    private static String stringifyComparable(Object v) {
        if (v instanceof CharSequence) {
            return v.toString();
        }
        if (v instanceof Boolean || v instanceof Number) {
            return v.toString();
        }
        return String.valueOf(v);
    }

    /** Best-effort numeric coercion for comparisons. */
    static Double asNumber(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number n) {
            return n.doubleValue();
        }
        if (v instanceof CharSequence cs) {
            String s = cs.toString().trim();
            if (s.isEmpty()) {
                return null;
            }
            try {
                return Double.parseDouble(s);
            } catch (NumberFormatException ex) {
                return null;
            }
        }
        if (v instanceof Boolean) {
            return null;
        }
        return null;
    }
}
