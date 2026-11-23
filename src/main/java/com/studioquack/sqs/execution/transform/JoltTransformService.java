package com.studioquack.sqs.execution.transform;

import com.bazaarvoice.jolt.Chainr;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Turns edge transform rows into a JOLT spec and applies that spec to a payload.
 *
 * <ol>
 * <li><b>Spec builder</b> — {@link #convertJoltSpec(List, Map)} turns {@code EdgeTransformDto}
 * rows into the BV-shaped op list. Rows without {@code operator} emit the legacy
 * {@code shift / modify-overwrite-beta / custom-to-string} shape byte-identically, except
 * that a row onto a wrapper target lands on that wrapper's child named by its source path.
 * Rows with {@code operator} append catalog ops (Chainr stock functions + default /
 * remove / sort / cardinality) after the primary native shift.</li>
 * <li><b>Spec applier</b> — {@link #transform(Object, List)} keeps native idempotent
 * {@code shift}, the five legacy coercions, and {@code custom-to-string}. New catalog
 * ops are applied via {@link Chainr} (soft-fail: passthrough on error).</li>
 * </ol>
 *
 * <p>Failure policy matches both sides: a coercion that can't apply leaves the value in
 * place; the engine never throws on malformed spec entries (they are skipped).
 */
@Slf4j
@Service
public class JoltTransformService {

    private static final String CUSTOM_TO_STRING_OP = "custom-to-string";
    private static final String SCRATCH_ROOT = "__args";

    /** Exact legacy modify RHS values — must stay on the native Long/Boolean path. */
    private static final Set<String> LEGACY_MODIFY_OPS = Set.of(
            "=toInteger", "=toDouble", "=toBoolean", "=toString", "=toList");

    /**
     * ArgDriven stock functions whose first evaluated arg is the driver (sep / index),
     * not the primary source value — FE stores the source in sourcePath and the driver
     * in args[0], so we reorder when emitting the modify expression.
     */
    private static final Set<String> DRIVER_FIRST_FUNCTIONS = Set.of("split", "join", "elementAt");

    /** Statement ops with no primary source copy. */
    private static final Set<String> NO_PRIMARY_SHIFT = Set.of("remove", "sort");

    /** DuckTape wildcard (wrapper) port types — keep in sync with the designer's {@code WILDCARD_TYPES}. */
    private static final Set<String> WRAPPER_TYPES = Set.of("wrapper", "any");

    /** Stock modify function names registered in jolt-core 0.1.7 Modifier.STOCK_FUNCTIONS. */
    private static final Set<String> STOCK_FUNCTIONS = Set.of(
            "toLower", "toUpper", "concat", "join", "split", "substring", "trim",
            "leftPad", "rightPad", "min", "max", "abs", "avg",
            "intSum", "doubleSum", "longSum", "intSubtract", "doubleSubtract", "longSubtract",
            "divide", "divideAndRound",
            "toInteger", "toDouble", "toLong", "toBoolean", "toString",
            "size", "squashNulls", "recursivelySquashNulls", "squashDuplicates",
            "noop", "isPresent", "notNull", "isNull",
            "firstElement", "lastElement", "elementAt", "toList", "sort");

    private static final Pattern NUMERIC_LITERAL = Pattern.compile("^-?\\d+(\\.\\d+)?$");

    private final ObjectMapper objectMapper;

    public JoltTransformService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /* Spec applier. */

    /**
 * Applies a BV-shaped JOLT spec list to {@code inputJson} and returns the transformed
 * value. Mirrors the control plane {@code transform(Object, List)} entry point but with the
 * the daemon's idempotent shift so a double (the control plane + in-process) apply never wipes data.
 *
 * <p>File ContentBlocks are REF-only on this path: any {@code sq-file:} handle keeps
 * metadata ({@code name}, {@code mime_type}, {@code kind}, {@code fileId}) but never
 * inline base64. Bytes are materialised elsewhere (sandbox bake, Docling input, Duckbot
 * end-push to the control plane).
 */
    @SuppressWarnings("unchecked")
    public Object transform(Object inputJson, List<Object> specJson) {
        Object cur = normalise(inputJson);
        // WHY: JOLT only remaps/coerces structure — parsing or copying multi-MB base64 here is
        // wasted CPU/memory and a PHI-leak surface (logs, double-apply copies). Keep the ref;
        // materialise bytes only at sandbox bake / Docling / Duckbot end-push.
        // Strip even when the spec is empty so a no-op transform cannot ferry inline bytes.
        cur = FileRefSanitizer.stripInlineFileBytes(cur);
        // Leaf-mapped FILE params never copy fileId; keep the stripped tree so the rider
        // can restore a resolvable sq-file: handle after JOLT rebuilds the ContentBlock.
        // Empty-spec riding is a no-op when this same object already has fileId.
        Object handleSource = cur;
        if (specJson == null || specJson.isEmpty()) {
            return FileHandleRider.rideHandles(handleSource, cur);
        }
        for (Object raw : specJson) {
            if (!(raw instanceof Map)) {
                continue;
            }
            Map<String, Object> op = (Map<String, Object>) raw;
            String opName = op.get("operation") instanceof String s ? s : "";
            Map<String, Object> spec = op.get("spec") instanceof Map
                    ? (Map<String, Object>) op.get("spec") : null;
            switch (opName) {
                case "shift" -> cur = applyShift(cur, spec);
                case "modify-overwrite-beta" -> {
                    // Legacy 5 coercions stay native (Long/Boolean semantics). Catalog
                    // modify RHS (=toUpper(...), nested maps, etc.) goes through Chainr.
                    if (isLegacyModifyOnly(spec)) {
                        cur = applyModify(cur, spec);
                    } else {
                        cur = applyChainrOp(cur, op);
                    }
                }
                case CUSTOM_TO_STRING_OP -> cur = applyToStringOp(cur, spec);
                default -> {
                    // Tolerate the control plane's resolveCustomOperations FQN rewrite.
                    if (opName.endsWith("ToStringTransform")) {
                        cur = applyToStringOp(cur, spec);
                    } else if (!opName.isEmpty()) {
                        // Catalog statement ops: default / remove / sort / cardinality.
                        cur = applyChainrOp(cur, op);
                    }
                }
            }
        }
        return FileHandleRider.rideHandles(handleSource, cur);
    }

    /**
     * Soft-fail Chainr apply: never throw to the caller — log and passthrough {@code cur}.
     */
    @SuppressWarnings("unchecked")
    private Object applyChainrOp(Object cur, Map<String, Object> op) {
        try {
            // Chainr mutates maps in place for some ops; deep-copy via Jackson so a failed
            // mid-chain cannot leave a half-written structure if we need to retry — and so
            // immutable Map.of fixtures in tests don't blow up Defaultr/Removr.
            Object input = cur == null ? null : objectMapper.convertValue(cur, Object.class);
            Object out = Chainr.fromSpec(List.of(op)).transform(input);
            return out == null ? cur : out;
        } catch (Exception e) {
            log.warn("JOLT Chainr op '{}' failed — passthrough: {}", op.get("operation"), e.toString());
            return cur;
        }
    }

    /**
     * True when this modify op is the flat legacy shape ({@code "path": "=toInteger"}) so the
     * native Long/Boolean coercions apply. Nested maps are catalog/Chainr territory — native
     * {@link #applyModify} only understands flat dotted keys.
     */
    private static boolean isLegacyModifyOnly(Map<String, Object> spec) {
        if (spec == null || spec.isEmpty()) {
            return true;
        }
        for (Object v : spec.values()) {
            if (!(v instanceof String s) || !LEGACY_MODIFY_OPS.contains(s)) {
                return false;
            }
        }
        return true;
    }

    /**
 * JOLT shift. <b>Idempotency contract (Go jolt.go):</b> when NONE of the spec's source
 * paths are present on the input, the input is returned unchanged (not {@code {}}). This
 * is what keeps a chained the control plane→in-process double application safe.
 */
    @SuppressWarnings("unchecked")
    private Object applyShift(Object input, Map<String, Object> spec) {
        if (spec == null || spec.isEmpty()) {
            return input;
        }
        // A list-valued input root (function-port Add-Node merge: N invocation payloads)
        // is walked with the same list-transparency as a nested list: one hit unwraps,
        // several collect in order. Without this the shift would no-op (non-Map) and
        // `name → prompt` over [{name:a},{name:b}] would leave the raw list unshaped.
        if (input instanceof List<?>) {
            Map<String, Object> out = new LinkedHashMap<>();
            boolean matched = false;
            for (Map.Entry<String, Object> e : spec.entrySet()) {
                Object[] hit = getPathFromAny(input, e.getKey());
                if (hit == null) {
                    continue;
                }
                matched = true;
                writeShiftTargets(out, e.getValue(), hit[0]);
            }
            return matched ? out : input;
        }
        if (!(input instanceof Map)) {
            return input;
        }
        Map<String, Object> in = (Map<String, Object>) input;
        Map<String, Object> out = new LinkedHashMap<>();
        boolean matched = false;
        for (Map.Entry<String, Object> e : spec.entrySet()) {
            Object[] hit = getPath(in, e.getKey());
            if (hit == null) {
                continue;
            }
            matched = true;
            writeShiftTargets(out, e.getValue(), hit[0]);
        }
        // Already-shifted shape → leave it alone (no {} data loss on double-apply).
        return matched ? out : input;
    }

    /** Writes one shift hit onto every target path in the spec RHS. */
    private static void writeShiftTargets(Map<String, Object> out, Object targets, Object val) {
        if (targets instanceof String ts) {
            setPath(out, ts, val);
        } else if (targets instanceof List<?> list) {
            for (Object ti : list) {
                if (ti instanceof String ts) {
                    setPath(out, ts, val);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object applyModify(Object input, Map<String, Object> spec) {
        if (spec == null || !(input instanceof Map)) {
            return input;
        }
        Map<String, Object> in = (Map<String, Object>) input;
        for (Map.Entry<String, Object> e : spec.entrySet()) {
            if (!(e.getValue() instanceof String op)) {
                continue;
            }
            switch (op) {
                case "=toInteger" -> coercePath(in, e.getKey(), JoltTransformService::toInteger);
                case "=toDouble" -> coercePath(in, e.getKey(), JoltTransformService::toDouble);
                case "=toBoolean" -> coercePath(in, e.getKey(), JoltTransformService::toBoolean);
                case "=toString" -> coercePath(in, e.getKey(), JoltTransformService::toStringCoerce);
                case "=toList" -> coercePath(in, e.getKey(), JoltTransformService::toList);
                default -> { /* unknown modifier — no-op */ }
            }
        }
        return input;
    }

    @SuppressWarnings("unchecked")
    private Object applyToStringOp(Object input, Map<String, Object> spec) {
        if (spec == null || !(input instanceof Map)) {
            return input;
        }
        Map<String, Object> in = (Map<String, Object>) input;
        for (String path : spec.keySet()) {
            coercePath(in, path, this::jsonStringify);
        }
        return input;
    }

    /* ---- path helpers (dot-separated; stripRoot already collapsed prefixes) ---- */

    /** @return a 1-element array holding the value when the dotted path exists, else null. */
    private static Object[] getPath(Map<String, Object> m, String path) {
        return getPathFromAny(m, path);
    }

    /**
     * Same as {@link #getPath} but the walk root may be a list (function-port merged
     * invocations) or a map. One hit unwraps; several collect in order.
     */
    private static Object[] getPathFromAny(Object root, String path) {
        if (path == null || path.isEmpty()) {
            return new Object[]{root};
        }
        List<Object> hits = new ArrayList<>();
        collectPath(root, path.split("\\."), 0, hits);
        if (hits.isEmpty()) {
            return null;
        }
        return new Object[]{hits.size() == 1 ? hits.get(0) : hits};
    }

    /**
     * Dot-segment walk where a LIST is transparent: when the current value is a list and segments
     * remain, every element is walked at the SAME segment and the leaf hits are collected in
     * order. Schema-shaped producers publish list-valued roots (e.g. a {@code texts} attribute
     * declared {@code type: "list"} of ContentBlocks), and an authored {@code texts.data → prompt}
     * row must navigate into the list — when the walk instead died at the list, the shift
     * reported no match and the consumer received the producer's raw payload (an unknown
     * {@code texts} parameter) untransformed, bypassing the edge's authored mapping entirely.
     */
    private static void collectPath(Object cur, String[] segs, int i, List<Object> hits) {
        if (i == segs.length) {
            hits.add(cur);
            return;
        }
        if (cur instanceof Map<?, ?> mp) {
            if (mp.containsKey(segs[i])) {
                collectPath(mp.get(segs[i]), segs, i + 1, hits);
            }
            return;
        }
        if (cur instanceof List<?> list) {
            for (Object element : list) {
                collectPath(element, segs, i, hits);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void setPath(Map<String, Object> m, String path, Object value) {
        if (path == null || path.isEmpty()) {
            return;
        }
        String[] segs = path.split("\\.");
        Map<String, Object> cur = m;
        for (int i = 0; i < segs.length; i++) {
            if (i == segs.length - 1) {
                cur.put(segs[i], value);
                return;
            }
            Object next = cur.get(segs[i]);
            if (!(next instanceof Map)) {
                Map<String, Object> fresh = new LinkedHashMap<>();
                cur.put(segs[i], fresh);
                cur = fresh;
            } else {
                cur = (Map<String, Object>) next;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void coercePath(Map<String, Object> m, String path, Function<Object, Coerced> f) {
        if (path == null || path.isEmpty()) {
            return;
        }
        String[] segs = path.split("\\.");
        Map<String, Object> cur = m;
        for (int i = 0; i < segs.length; i++) {
            if (i == segs.length - 1) {
                if (cur.containsKey(segs[i])) {
                    Coerced c = f.apply(cur.get(segs[i]));
                    if (c.changed()) {
                        cur.put(segs[i], c.value());
                    }
                }
                return;
            }
            Object next = cur.get(segs[i]);
            if (!(next instanceof Map)) {
                return;
            }
            cur = (Map<String, Object>) next;
        }
    }

    /** Result of a coercion attempt: the (possibly new) value + whether it changed. */
    private record Coerced(Object value, boolean changed) {
    }

    private static Coerced toInteger(Object v) {
        if (v instanceof Number n) {
            return new Coerced(n.longValue(), true);
        }
        if (v instanceof Boolean b) {
            return new Coerced(b ? 1L : 0L, true);
        }
        if (v instanceof String s) {
            try {
                return new Coerced(Long.parseLong(s.trim()), true);
            } catch (NumberFormatException ignore) {
                try {
                    return new Coerced((long) Double.parseDouble(s.trim()), true);
                } catch (NumberFormatException ignore2) {
                    return new Coerced(v, false);
                }
            }
        }
        return new Coerced(v, false);
    }

    private static Coerced toDouble(Object v) {
        if (v instanceof Number n) {
            return new Coerced(n.doubleValue(), true);
        }
        if (v instanceof Boolean b) {
            return new Coerced(b ? 1.0 : 0.0, true);
        }
        if (v instanceof String s) {
            try {
                return new Coerced(Double.parseDouble(s.trim()), true);
            } catch (NumberFormatException ignore) {
                return new Coerced(v, false);
            }
        }
        return new Coerced(v, false);
    }

    private static Coerced toBoolean(Object v) {
        if (v instanceof Boolean b) {
            return new Coerced(b, true);
        }
        if (v instanceof Number n) {
            return new Coerced(n.doubleValue() != 0, true);
        }
        if (v instanceof String s) {
            String t = s.trim().toLowerCase();
            if (t.equals("true") || t.equals("1") || t.equals("yes")) {
                return new Coerced(true, true);
            }
            if (t.equals("false") || t.equals("0") || t.equals("no") || t.isEmpty()) {
                return new Coerced(false, true);
            }
        }
        return new Coerced(v, false);
    }

    private static Coerced toStringCoerce(Object v) {
        if (v == null) {
            return new Coerced("", true);
        }
        if (v instanceof String s) {
            return new Coerced(s, true);
        }
        if (v instanceof Boolean || v instanceof Number) {
            return new Coerced(String.valueOf(v), true);
        }
        return new Coerced(v, false);
    }

    private static Coerced toList(Object v) {
        if (v instanceof List) {
            return new Coerced(v, true);
        }
        if (v == null) {
            return new Coerced(new ArrayList<>(), true);
        }
        List<Object> list = new ArrayList<>();
        list.add(v);
        return new Coerced(list, true);
    }

    private Coerced jsonStringify(Object v) {
        // A file ContentBlock (or a list holding one) is NEVER stringified: the block is REF-only
        // on this path, so the string would bury the sq-file: handle where no consumer walks
        // (Docling logged "no sq-file handle" and skipped the conversion). The toString comes
        // from the target leaf's schema type (voices → files.data), not from author intent.
        if (isFileShapedValue(v)) {
            return new Coerced(v, false);
        }
        // Duckbot start publishes the thread as [{role, content}, …] on `msg` when full history
        // is on. A string consumer (Jira summary, custom-node `msg`, HTTP body field) wants the
        // current turn's text, not that JSON array. List/object ports never reach this coerce.
        if (isChatTranscript(v)) {
            return new Coerced(chatTranscriptText(v), true);
        }
        if (v instanceof Map || v instanceof List) {
            try {
                return new Coerced(objectMapper.writeValueAsString(v), true);
            } catch (Exception e) {
                return new Coerced(v, false);
            }
        }
        return new Coerced(v, false);
    }

    /**
     * True when {@code v} is a file ContentBlock ({@code fileId} handle, or {@code data} beside
     * {@code name}/{@code mime_type}/{@code kind}) or a list containing one.
     */
    private static boolean isFileShapedValue(Object v) {
        if (v instanceof Map<?, ?> map) {
            if (map.containsKey("fileId")) {
                return true;
            }
            return map.containsKey("data")
                    && (map.containsKey("name") || map.containsKey("mime_type") || map.containsKey("kind"));
        }
        if (v instanceof List<?> list) {
            for (Object element : list) {
                if (isFileShapedValue(element)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Duckbot {@code {role, content}} transcript: a single turn, or a non-empty list of only those
     * turns. Matches {@code DuckbotBridge#readHistory} and nothing else — a generic object that
     * happens to have a {@code content} key is still JSON-stringified.
     */
    static boolean isChatTranscript(Object v) {
        if (v instanceof List<?> list) {
            if (list.isEmpty()) {
                return false;
            }
            for (Object item : list) {
                if (!isChatTurn(item)) {
                    return false;
                }
            }
            return true;
        }
        return isChatTurn(v);
    }

    private static boolean isChatTurn(Object v) {
        if (!(v instanceof Map<?, ?> map)) {
            return false;
        }
        Object role = map.get("role");
        Object content = map.get("content");
        return role instanceof String && content != null;
    }

    /**
     * The current user turn (last {@code role=user} {@code content}). The start node appends this
     * run's prompt before reading history, so that is the message that triggered the run.
     */
    static String chatTranscriptText(Object v) {
        if (v instanceof List<?> list) {
            for (int i = list.size() - 1; i >= 0; i--) {
                String user = userContent(list.get(i));
                if (user != null) {
                    return user;
                }
            }
            return list.isEmpty() ? "" : turnContent(list.get(list.size() - 1));
        }
        String user = userContent(v);
        return user != null ? user : turnContent(v);
    }

    private static String userContent(Object v) {
        if (!(v instanceof Map<?, ?> map)) {
            return null;
        }
        if (!"user".equalsIgnoreCase(String.valueOf(map.get("role")))) {
            return null;
        }
        Object content = map.get("content");
        return content == null ? "" : String.valueOf(content);
    }

    private static String turnContent(Object v) {
        if (v instanceof Map<?, ?> map && map.get("content") != null) {
            return String.valueOf(map.get("content"));
        }
        return v == null ? "" : String.valueOf(v);
    }

    /**
 * Turns a JSON-looking string into a Map/List so object specs can operate on it —
 * mirrors the Go {@code normalise} + the control plane {@code parseJsonValue}.
 */
    private Object normalise(Object v) {
        if (v instanceof String s) {
            String t = s.trim();
            if ((t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"))) {
                try {
                    return objectMapper.readValue(t, Object.class);
                } catch (Exception ignore) {
                    return v;
                }
            }
        }
        return v;
    }

    /* ================================================================== */
    /* Spec builder. */
    /* ================================================================== */

    public List<Object> convertJoltSpec(List<EdgeTransformDto> edgeTransforms) {
        return convertJoltSpec(edgeTransforms, null);
    }

    public List<Object> convertJoltSpec(List<EdgeTransformDto> edgeTransforms,
                                        Map<String, String> targetSchemaTypeByShortCode) {
        Map<String, Object> shiftSpec = new LinkedHashMap<>();
        Map<String, Object> modifySpec = new LinkedHashMap<>();
        Map<String, Object> toStringSpec = new LinkedHashMap<>();
        boolean requiresModify = false;
        boolean requiresToString = false;
        // Catalog ops appended AFTER the legacy shift/modify/toString block.
        List<Object> catalogOps = new ArrayList<>();
        boolean needsScratchCleanup = false;
        // Authored row values ride as Defaultr literals: a source-side default is
        // planted BEFORE the shift (so the mapping carries it when the upstream port
        // produced nothing), a target-side default AFTER the shift (so it fills only
        // when the connection delivered no value for that path).
        Map<String, Object> sourceDefaultSpec = new LinkedHashMap<>();
        Map<String, Object> targetDefaultSpec = new LinkedHashMap<>();

        if (edgeTransforms == null) {
            edgeTransforms = List.of();
        }

        for (EdgeTransformDto t : edgeTransforms) {
            if (t == null) {
                continue;
            }
            String sourceKey = stripRoot(t.getSourcePath());
            String targetVal = stripRoot(t.getTargetPath());
            // Coercion source-of-truth: port schema type > EdgeTransformDto.targetDataType.
            String effectiveType = resolveEffectiveType(targetVal, t.getTargetDataType(),
                    targetSchemaTypeByShortCode);
            String landing = landingPath(targetVal, sourceKey, effectiveType);
            collectAuthoredDefault(t.getSourcePath(), t.getSourcePathValue(), sourceDefaultSpec);
            collectAuthoredDefault(landing, t.getTargetPathValue(), targetDefaultSpec);
            String operator = t.getOperator() == null ? "" : t.getOperator().trim();
            if (operator.isEmpty()) {
                // ---- Legacy path (byte-identical to pre-catalog builder, wrapper targets aside) ----
                @SuppressWarnings("unchecked")
                List<Object> targets = (List<Object>) shiftSpec.computeIfAbsent(sourceKey, k -> new ArrayList<>());
                targets.add(landing);

                if (effectiveType != null) {
                    if ("string".equalsIgnoreCase(effectiveType)) {
                        toStringSpec.put(targetVal, "toString");
                        requiresToString = true;
                    } else {
                        String typeFunction = getJoltTypeFunction(effectiveType);
                        if (typeFunction != null) {
                            modifySpec.put(targetVal, typeFunction);
                            requiresModify = true;
                        }
                    }
                }
            } else {
                // ---- Catalog path (operator present) ----
                boolean scratched = appendCatalogRow(t, landing, operator, shiftSpec, toStringSpec, catalogOps);
                if (scratched) {
                    needsScratchCleanup = true;
                }
                // operator=toString uses native custom-to-string (object/list stringify).
                if ("toString".equals(operator)) {
                    requiresToString = true;
                }
            }
        }

        List<Object> specList = new ArrayList<>();

        if (!sourceDefaultSpec.isEmpty()) {
            specList.add(opMap("default", sourceDefaultSpec));
        }

        Map<String, Object> shiftOperation = new LinkedHashMap<>();
        shiftOperation.put("operation", "shift");
        shiftOperation.put("spec", shiftSpec);
        specList.add(shiftOperation);

        if (!targetDefaultSpec.isEmpty()) {
            specList.add(opMap("default", targetDefaultSpec));
        }

        if (requiresModify) {
            Map<String, Object> modifyOperation = new LinkedHashMap<>();
            modifyOperation.put("operation", "modify-overwrite-beta");
            modifyOperation.put("spec", modifySpec);
            specList.add(modifyOperation);
        }

        if (requiresToString) {
            Map<String, Object> toStringOperation = new LinkedHashMap<>();
            toStringOperation.put("operation", CUSTOM_TO_STRING_OP);
            toStringOperation.put("spec", toStringSpec);
            specList.add(toStringOperation);
        }

        specList.addAll(catalogOps);

        if (needsScratchCleanup) {
            Map<String, Object> cleanup = new LinkedHashMap<>();
            cleanup.put("operation", "remove");
            cleanup.put("spec", Map.of(SCRATCH_ROOT, ""));
            specList.add(cleanup);
        }

        return specList;
    }

    /**
     * Lowers one catalog {@code EdgeTransformDto} into shift entries + Chainr ops.
     *
     * @param tp where the row's value lands ({@link #landingPath}); every op of the row targets it
     * @return true when scratch {@code __args.*} keys were created (needs cleanup remove)
     */
    private boolean appendCatalogRow(EdgeTransformDto t,
                                     String tp,
                                     String operator,
                                     Map<String, Object> shiftSpec,
                                     Map<String, Object> toStringSpec,
                                     List<Object> catalogOps) {
        String primarySp = stripRoot(t.getSourcePath());
        List<String> rawArgs = t.getArgs() == null ? List.of() : t.getArgs();
        List<DecodedArg> decoded = new ArrayList<>();
        for (String raw : rawArgs) {
            decoded.add(decodeArg(raw));
        }

        boolean scratched = false;

        // Statement ops with no primary source: skip the primary shift for this row.
        if (!NO_PRIMARY_SHIFT.contains(operator)
                && primarySp != null && !primarySp.isEmpty()
                && tp != null && !tp.isEmpty()) {
            addShiftTarget(shiftSpec, primarySp, tp);
        }

        // Shift each source-ref arg into scratch so native shift stays idempotent.
        for (int i = 0; i < decoded.size(); i++) {
            DecodedArg a = decoded.get(i);
            if (a.kind == ArgKind.SOURCE_REF && a.path != null && !a.path.isEmpty()
                    && tp != null && !tp.isEmpty()) {
                String scratch = scratchPath(tp, i);
                addShiftTarget(shiftSpec, a.path, scratch);
                scratched = true;
            }
        }

        switch (operator) {
            case "toString" -> {
                // Keep object/list via native custom-to-string (do not use Chainr toString).
                if (tp != null && !tp.isEmpty()) {
                    toStringSpec.put(tp, "toString");
                }
            }
            case "default" -> catalogOps.add(buildDefaultOp(tp, decoded));
            case "remove" -> {
                if (tp != null && !tp.isEmpty()) {
                    catalogOps.add(opMap("remove", nestPath(tp, "")));
                }
            }
            case "sort" -> {
                // Whole-document Sortr when no target; otherwise stock =sort on the list/map at tp.
                if (tp == null || tp.isEmpty()) {
                    Map<String, Object> sortOp = new LinkedHashMap<>();
                    sortOp.put("operation", "sort");
                    catalogOps.add(sortOp);
                } else {
                    catalogOps.add(opMap("modify-overwrite-beta",
                            nestPath(tp, "=sort")));
                }
            }
            case "cardinality" -> {
                // FE encodes args=["ONE"] or ["MANY"] as bare tokens (optional quotes).
                String card = "ONE";
                if (!rawArgs.isEmpty() && rawArgs.get(0) != null) {
                    String tok = rawArgs.get(0).trim();
                    if (tok.length() >= 2 && tok.startsWith("\"") && tok.endsWith("\"")) {
                        tok = tok.substring(1, tok.length() - 1);
                    }
                    if ("MANY".equalsIgnoreCase(tok) || "ONE".equalsIgnoreCase(tok)) {
                        card = tok.toUpperCase(Locale.ROOT);
                    }
                }
                if (tp != null && !tp.isEmpty()) {
                    catalogOps.add(opMap("cardinality", nestPath(tp, card)));
                }
            }
            default -> {
                if (STOCK_FUNCTIONS.contains(operator)) {
                    // Type casts that match legacy RHS stay as bare =toInteger etc. so the
                    // applier can keep the native Long path when the whole modify op is legacy-only.
                    String expr = buildFunctionExpr(operator, tp, decoded);
                    if (expr != null && tp != null && !tp.isEmpty()) {
                        catalogOps.add(opMap("modify-overwrite-beta", nestPath(tp, expr)));
                    }
                } else {
                    // Unknown operator — soft-fail: primary shift (if any) already copied; no op.
                    log.warn("Unknown JOLT catalog operator '{}' — treating as plain copy", operator);
                }
            }
        }
        return scratched;
    }

    private static void addShiftTarget(Map<String, Object> shiftSpec, String sourceKey, String targetVal) {
        @SuppressWarnings("unchecked")
        List<Object> targets = (List<Object>) shiftSpec.computeIfAbsent(sourceKey, k -> new ArrayList<>());
        targets.add(targetVal);
    }

    private static String scratchPath(String tp, int argIndex) {
        return SCRATCH_ROOT + "." + tp + "." + argIndex;
    }

    private static Map<String, Object> opMap(String operation, Map<String, Object> spec) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("operation", operation);
        m.put("spec", spec);
        return m;
    }

    /** Nest a dotted path into a Map tree so Chainr Modifier/Defaultr/Removr see nested keys. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> nestPath(String dottedPath, Object leafValue) {
        Map<String, Object> root = new LinkedHashMap<>();
        if (dottedPath == null || dottedPath.isEmpty()) {
            return root;
        }
        String[] segs = dottedPath.split("\\.");
        Map<String, Object> cur = root;
        for (int i = 0; i < segs.length; i++) {
            if (i == segs.length - 1) {
                cur.put(segs[i], leafValue);
            } else {
                Object next = cur.get(segs[i]);
                if (!(next instanceof Map)) {
                    Map<String, Object> fresh = new LinkedHashMap<>();
                    cur.put(segs[i], fresh);
                    cur = fresh;
                } else {
                    cur = (Map<String, Object>) next;
                }
            }
        }
        return root;
    }

    /**
     * Fold one authored row value into the default spec for its side. Blank paths or
     * values contribute nothing. Container rows author their default as JSON text —
     * parse object/array literals so the default lands as structured data; every
     * other value stays a string and the downstream coercion ops (modify / toString)
     * cast it to the target schema type.
     */
    private void collectAuthoredDefault(String path, String value, Map<String, Object> defaultSpec) {
        if (path == null || path.isBlank() || value == null || value.isBlank()) {
            return;
        }
        Object literal = value;
        String trimmed = value.trim();
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            try {
                literal = objectMapper.readValue(trimmed, Object.class);
            } catch (Exception e) {
                literal = value; // not valid JSON — keep the raw text as the default
            }
        }
        deepMergeSpec(defaultSpec, nestPath(stripRoot(path), literal));
    }

    /** Deep-merge one nested default spec into another (shared parents merge, leaves overwrite). */
    @SuppressWarnings("unchecked")
    private static void deepMergeSpec(Map<String, Object> target, Map<String, Object> addition) {
        for (Map.Entry<String, Object> e : addition.entrySet()) {
            Object existing = target.get(e.getKey());
            if (existing instanceof Map && e.getValue() instanceof Map) {
                deepMergeSpec((Map<String, Object>) existing, (Map<String, Object>) e.getValue());
            } else {
                target.put(e.getKey(), e.getValue());
            }
        }
    }

    private Map<String, Object> buildDefaultOp(String tp, List<DecodedArg> decoded) {
        Object defaultVal = null;
        for (DecodedArg a : decoded) {
            if (a.kind == ArgKind.SOURCE_REF) {
                // TODO: Defaultr cannot resolve runtime path refs; multi-source default
                // would need modify-default-beta. Prefer literal-arg default.
                continue;
            }
            defaultVal = a.value;
            break;
        }
        if (defaultVal == null) {
            defaultVal = "";
        }
        if (tp == null || tp.isEmpty()) {
            return opMap("default", new LinkedHashMap<>());
        }
        return opMap("default", nestPath(tp, defaultVal));
    }

    /**
     * Build a modify-overwrite-beta RHS like {@code =toUpper} or
     * {@code =leftPad(@(1,code), 5, '0')}.
     */
    private String buildFunctionExpr(String operator, String tp, List<DecodedArg> decoded) {
        if (tp == null || tp.isEmpty()) {
            return null;
        }
        int walkUp = Math.max(1, tp.split("\\.").length);
        String selfRef = "@(" + walkUp + "," + tp + ")";

        // Bare self form for zero-arg unary stock functions.
        if (decoded.isEmpty() && !DRIVER_FIRST_FUNCTIONS.contains(operator)) {
            // toInteger/toDouble/toBoolean/toString/toList as bare tokens keep native path
            // when this modify op contains only those RHS values.
            if (Set.of("toInteger", "toDouble", "toBoolean", "toString", "toList").contains(operator)) {
                return "=" + operator;
            }
            return "=" + operator;
        }

        List<String> exprArgs = new ArrayList<>();
        if (DRIVER_FIRST_FUNCTIONS.contains(operator)) {
            // Driver (sep / index) from args[0], then primary value, then remaining args.
            if (!decoded.isEmpty()) {
                exprArgs.add(argToExpr(decoded.get(0), tp, 0, walkUp));
            }
            exprArgs.add(selfRef);
            for (int i = 1; i < decoded.size(); i++) {
                exprArgs.add(argToExpr(decoded.get(i), tp, i, walkUp));
            }
        } else {
            exprArgs.add(selfRef);
            for (int i = 0; i < decoded.size(); i++) {
                exprArgs.add(argToExpr(decoded.get(i), tp, i, walkUp));
            }
        }
        return "=" + operator + "(" + String.join(", ", exprArgs) + ")";
    }

    private static String argToExpr(DecodedArg a, String tp, int index, int walkUp) {
        return switch (a.kind) {
            case SOURCE_REF -> "@(" + walkUp + "," + scratchPath(tp, index) + ")";
            case STRING_LIT -> "'" + escapeJoltSingleQuoted(String.valueOf(a.value)) + "'";
            case NUMBER, BOOLEAN -> String.valueOf(a.value);
            case RAW -> {
                // Cardinality tokens and unknown bare words — quote as string for modify.
                String s = a.value == null ? "" : String.valueOf(a.value);
                if (NUMERIC_LITERAL.matcher(s).matches() || "true".equals(s) || "false".equals(s)) {
                    yield s;
                }
                yield "'" + escapeJoltSingleQuoted(s) + "'";
            }
        };
    }

    private static String escapeJoltSingleQuoted(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("'", "\\'");
    }

    private enum ArgKind { SOURCE_REF, STRING_LIT, NUMBER, BOOLEAN, RAW }

    private record DecodedArg(ArgKind kind, Object value, String path) {
    }

    /**
     * Decode one FE-encoded arg token:
     * <ul>
     *   <li>{@code source.*} → path ref (strip prefix)</li>
     *   <li>double-quoted → string literal</li>
     *   <li>{@code ^-?\d+(\.\d+)?$} / true/false → literal</li>
     *   <li>else raw string</li>
     * </ul>
     */
    private static DecodedArg decodeArg(String raw) {
        if (raw == null) {
            return new DecodedArg(ArgKind.RAW, "", null);
        }
        String s = raw.trim();
        if (s.startsWith("source.")) {
            String path = stripRootStatic(s.substring("source.".length()));
            return new DecodedArg(ArgKind.SOURCE_REF, path, path);
        }
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            String inner = s.substring(1, s.length() - 1).replace("\\\"", "\"");
            return new DecodedArg(ArgKind.STRING_LIT, inner, null);
        }
        if ("true".equals(s) || "false".equals(s)) {
            return new DecodedArg(ArgKind.BOOLEAN, Boolean.parseBoolean(s), null);
        }
        if (NUMERIC_LITERAL.matcher(s).matches()) {
            if (s.contains(".")) {
                return new DecodedArg(ArgKind.NUMBER, Double.parseDouble(s), null);
            }
            try {
                long l = Long.parseLong(s);
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    return new DecodedArg(ArgKind.NUMBER, (int) l, null);
                }
                return new DecodedArg(ArgKind.NUMBER, l, null);
            } catch (NumberFormatException e) {
                return new DecodedArg(ArgKind.NUMBER, Double.parseDouble(s), null);
            }
        }
        return new DecodedArg(ArgKind.RAW, s, null);
    }

    private static String stripRootStatic(String path) {
        if (path != null) {
            if (path.startsWith("root.")) {
                return path.substring(5);
            } else if (path.startsWith("undefined.")) {
                return path.substring(10);
            }
        }
        return path;
    }

    private String stripRoot(String path) {
        return stripRootStatic(path);
    }

    /**
     * Where a row's value lands. A wrapper target holds every source mapped onto it as its own
     * child at the source's path ({@code status → response} lands on {@code response.status}),
     * so N rows onto one wrapper all survive instead of overwriting each other down to the last.
     * A port-root source (blank path) has no key to nest under and is the wrapper's whole value.
     */
    private static String landingPath(String targetPath, String sourcePath, String targetType) {
        if (targetType == null || !WRAPPER_TYPES.contains(targetType.toLowerCase(Locale.ROOT))
                || targetPath == null || targetPath.isEmpty()
                || sourcePath == null || sourcePath.isEmpty()) {
            return targetPath;
        }
        return targetPath + "." + sourcePath;
    }

    /**
     * Picks the coercion type, preferring the consuming port's schema type (when supplied)
     * over the persisted {@code EdgeTransform.targetDataType}. Exact path match first, then
     * the dotted path's leaf segment, then the persisted type.
     */
    private static String resolveEffectiveType(String targetPath,
                                               String persistedDataType,
                                               Map<String, String> schemaTypes) {
        if (schemaTypes != null && !schemaTypes.isEmpty() && targetPath != null) {
            String hit = schemaTypes.get(targetPath);
            if (hit != null && !hit.isEmpty()) {
                return hit;
            }
            int dot = targetPath.lastIndexOf('.');
            if (dot >= 0 && dot + 1 < targetPath.length()) {
                String leaf = targetPath.substring(dot + 1);
                String leafHit = schemaTypes.get(leaf);
                if (leafHit != null && !leafHit.isEmpty()) {
                    return leafHit;
                }
            }
        }
        return persistedDataType;
    }

    private String getJoltTypeFunction(String dataType) {
        if (dataType == null) {
            return null;
        }
        return switch (dataType.toLowerCase()) {
            case "int", "integer" -> "=toInteger";
            case "double", "float", "number" -> "=toDouble";
            case "boolean" -> "=toBoolean";
            case "string" -> "=toString";
            case "list", "array" -> "=toList";
            default -> null;
        };
    }
}
