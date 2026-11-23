package com.studioquack.sqs.execution.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.execution.context.ExecutionContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Builds the runnable node script for the in-process sandbox.
 * Prepends the language helper and bakes the execution context into {@code __SQ_CONTEXT__}.
 *
 * <p>Three responsibilities preserved:
 * <ol>
 * <li><b>Helper prepend</b> — the language helper template (via {@link HelperScriptProvider})
 * is prepended so the user script's {@code studioquack} SDK calls resolve.</li>
 * <li><b>{@code __SQ_CONTEXT__} bake</b> — the execution context is Base64-encoded and
 * substituted into the helper's {@code __SQ_CONTEXT__} placeholder. Because the
 * pipeline pre-bakes inbound JOLT onto {@code currentNode.inputList[].value}
 * <b>before</b> this call (Mapping Invariant 2), the baked context reflects the
 * transformed inputs the user script actually reads.</li>
 * <li><b>Directive strip (Mapping Invariant 6)</b> — the {@code __sq_transformation__} /
 * {@code __sq_function_port_transforms__} directives never enter the baked context:
 * they are {@code @JsonIgnore} on {@link ExecutionContext}, so serialising the context
 * here strips them from what the user script sees.</li>
 * </ol>
 */
@Slf4j
@Component
public class GenerateScriptService {

    /** Named placeholder for the Base64-encoded execution context. */
    static final String NAMED_PLACEHOLDER_CONTEXT = "__SQ_CONTEXT__";
    /** Base64 JSON of the sanitized inbound value for an edge-transform job. */
    static final String NAMED_PLACEHOLDER_TRANSFORM_INPUT = "__SQ_TRANSFORM_INPUT__";
    /** Insertion point for the author transform script inside the transform helper. */
    static final String NAMED_PLACEHOLDER_TRANSFORM_USER_SCRIPT = "__SQ_TRANSFORM_USER_SCRIPT__";
    /** Marker in node helpers replaced with same-language transform prelude(s), or removed. */
    static final String EDGE_TRANSFORM_HOOK = "__SQ_EDGE_TRANSFORM_HOOK__";
    /** Legacy positional placeholder (older helper templates use {@code %s}). */
    static final String LEGACY_PERCENT_S = "%s";
    private static final String HELPER_SCRIPT_SEPARATOR = "\n\n";

    private final ObjectMapper objectMapper;
    private final HelperScriptProvider helperScriptProvider;

    public GenerateScriptService(ObjectMapper objectMapper, HelperScriptProvider helperScriptProvider) {
        this.objectMapper = objectMapper;
        this.helperScriptProvider = helperScriptProvider;
    }

    /**
 * Builds the helper-prepended, context-baked source the sandbox writes to
 * {@code main.<ext>}. When no helper template is registered for the language the raw
 * user script is returned (control plane parity).
 */
    public BuildSourceResult buildScriptSource(ExecutionContext ctx) {
        String language = ctx.findCurrentNodeLanguage();
        String contextBase64 = bakeContextBase64(ctx);
        String helperBody = helperScriptProvider == null
                ? ""
                : helperScriptProvider.helperScriptFor(language)
                        .map(tmpl -> interpolateHelperPlaceholders(tmpl, contextBase64))
                        .orElse("");
        String userScript = StringUtils.defaultString(ctx.findCurrentNodeScript());
        String source = StringUtils.isNotEmpty(helperBody)
                ? helperBody + HELPER_SCRIPT_SEPARATOR + userScript
                : userScript;
        return new BuildSourceResult(source, contextBase64);
    }

    /**
     * Base64 of the JSON-serialised execution context. The {@code @JsonIgnore} transform
     * fields are excluded here — this is where Mapping Invariant 6 (strip the directive from
     * the context the user script sees) is enforced on the bake side.
     */
    public String bakeContextBase64(ExecutionContext ctx) {
        try {
            return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(ctx));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to bake __SQ_CONTEXT__ for node "
                    + (ctx == null ? "<null>" : ctx.getCurrentNodeIdSafe()) + ": " + e.getMessage(), e);
        }
    }

    /**
     * Variant of {@link #buildScriptSource(ExecutionContext)} that bakes a PRE-SERIALISED, already
     * secret-resolved context map instead of re-serialising an {@link ExecutionContext}. The dispatch
     * delegate produces this map as a private per-dispatch snapshot in which the current node's
     * {@code $secretRef} handles have been replaced with their OS-key-manager plaintext (just-in-time
     * secret binding), so the value the SDK reads from {@code __SQ_CONTEXT__} is the real secret —
     * WITHOUT the resolved plaintext ever being written back onto the persisted {@link ExecutionContext}
     * (which keeps the {@code $secretRef} handles for the REF-ONLY + at-rest-seal invariants).
     * {@code language} / {@code userScript} are the current node's, resolved by the caller.
     */
    public BuildSourceResult buildScriptSourceFromContext(Map<String, Object> resolvedContext,
                                                          String language, String userScript) {
        String contextBase64 = bakeContextBase64(resolvedContext);
        String helperBody = helperScriptProvider == null
                ? ""
                : helperScriptProvider.helperScriptFor(language)
                        .map(tmpl -> interpolateHelperPlaceholders(tmpl, contextBase64))
                        .orElse("");
        String safeUser = StringUtils.defaultString(userScript);
        String source = StringUtils.isNotEmpty(helperBody)
                ? helperBody + HELPER_SCRIPT_SEPARATOR + safeUser
                : safeUser;
        return new BuildSourceResult(source, contextBase64);
    }

    /** Base64 of a pre-serialised context map (the resolved per-dispatch sandbox snapshot). */
    public String bakeContextBase64(Map<String, Object> contextMap) {
        try {
            return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(contextMap));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to bake __SQ_CONTEXT__ from resolved context map: " + e.getMessage(), e);
        }
    }

    /**
     * Replaces {@code __SQ_CONTEXT__} and leftover {@code %s} markers in a helper template.
     * Stray {@code %} characters are left as-is.
     */
    static String interpolateHelperPlaceholders(String helperScript, String encodedContext) {
        if (helperScript == null) {
            return "";
        }
        String safeContext = encodedContext == null ? "" : encodedContext;
        String result = helperScript.replace(NAMED_PLACEHOLDER_CONTEXT, safeContext);
        // Neutralise the first three legacy %s positions (api_url, token, context); the daemon
        // owns url + token, so only the context slot carries a value.
        result = replaceFirstNPositional(result, LEGACY_PERCENT_S, new String[]{"", "", safeContext});
        return result;
    }

    private static String replaceFirstNPositional(String source, String marker, String[] values) {
        if (StringUtils.isEmpty(source) || StringUtils.isEmpty(marker) || values == null) {
            return source;
        }
        StringBuilder sb = new StringBuilder(source.length() + 1024);
        int cursor = 0;
        int markerLen = marker.length();
        for (String value : values) {
            int hit = source.indexOf(marker, cursor);
            if (hit < 0) {
                break;
            }
            sb.append(source, cursor, hit);
            sb.append(value == null ? "" : value);
            cursor = hit + markerLen;
        }
        sb.append(source, cursor, source.length());
        return sb.toString();
    }

    /**
     * Bake transform-helper + user script for a language-mismatch hop (or standalone transform job).
     * Inbound must already be sanitized (no inline file bytes). Never logs the payload.
     */
    public BuildSourceResult buildEdgeTransformSource(Object inboundValue, String language, String userScript) {
        String inputBase64 = bakeJsonBase64(inboundValue);
        String helperBody = helperScriptProvider == null
                ? ""
                : helperScriptProvider.transformHelperScriptFor(language).orElse("");
        if (StringUtils.isEmpty(helperBody)) {
            throw new IllegalStateException(
                    "No transform helper template registered for language: " + language);
        }
        String source = interpolateTransformPlaceholders(
                helperBody, inputBase64, StringUtils.defaultString(userScript));
        return new BuildSourceResult(source, inputBase64);
    }

    /**
     * Language-match concat: transform helper+script(s) then a shim that patches decoded context,
     * then the normal node helper+user script. Replaces {@code __SQ_EDGE_TRANSFORM_HOOK__} in the
     * node helper (after context load) with the transform block; removes the marker when empty.
     *
     * <p>At minimum supports one transform prelude + node script. Multiple coalesced same-lang
     * transforms run sequentially in the inserted block, each with its own source/target bound
     * to the matching inbound value, then patched into {@code currentNode.inputList}.
     */
    public BuildSourceResult buildCombinedTransformAndNodeSource(
            Map<String, Object> resolvedContext,
            String language,
            String nodeUserScript,
            List<EdgeTransformBake> transforms) {
        String contextBase64 = bakeContextBase64(resolvedContext);
        String nodeHelper = helperScriptProvider == null
                ? ""
                : helperScriptProvider.helperScriptFor(language)
                        .map(tmpl -> interpolateHelperPlaceholders(tmpl, contextBase64))
                        .orElse("");
        String hookBlock = buildEdgeTransformHookBlock(
                language, transforms == null ? Collections.emptyList() : transforms);
        if (StringUtils.isNotEmpty(nodeHelper)) {
            // Marker appears as a comment line ("# __SQ_…" / "// __SQ_…"); strip the whole line
            // when empty, else replace the marker token with the prelude (comment prefix remains).
            if (StringUtils.isEmpty(hookBlock)) {
                nodeHelper = nodeHelper.replaceAll(
                        "(?m)^[ \\t]*(?://|#)\\s*" + EDGE_TRANSFORM_HOOK + "[ \\t]*\\R?", "");
            } else {
                nodeHelper = nodeHelper.replace(EDGE_TRANSFORM_HOOK, "\n" + hookBlock + "\n");
            }
        } else if (StringUtils.isNotEmpty(hookBlock)) {
            nodeHelper = hookBlock;
        }
        String safeUser = StringUtils.defaultString(nodeUserScript);
        String source = StringUtils.isNotEmpty(nodeHelper)
                ? nodeHelper + HELPER_SCRIPT_SEPARATOR + safeUser
                : safeUser;
        return new BuildSourceResult(source, contextBase64);
    }

    /** One edge-transform bake unit for {@link #buildCombinedTransformAndNodeSource}. */
    public record EdgeTransformBake(
            Object inboundValue,
            String userScript,
            String sourceNodeId,
            String targetNodeId,
            String targetPortKey) {
    }

    /**
     * Result of {@link #buildScriptSource(ExecutionContext)}: the helper-prepended source
     * plus the Base64 context that was baked into it (exposed so callers/tests can assert the
     * pre-bake → bake ordering held).
     */
    public record BuildSourceResult(String source, String contextBase64) {
    }

    /** Base64 of arbitrary JSON (transform inbound). Never log the decoded payload. */
    String bakeJsonBase64(Object value) {
        try {
            return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(value));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to bake __SQ_TRANSFORM_INPUT__: " + e.getMessage(), e);
        }
    }

    static String interpolateTransformPlaceholders(String helperScript, String encodedInput,
                                                   String userScript) {
        if (helperScript == null) {
            return "";
        }
        String safeInput = encodedInput == null ? "" : encodedInput;
        String safeUser = userScript == null ? "" : userScript;
        return helperScript
                .replace(NAMED_PLACEHOLDER_TRANSFORM_INPUT, safeInput)
                .replace(NAMED_PLACEHOLDER_TRANSFORM_USER_SCRIPT, safeUser);
    }

    /**
     * Builds the inlined prelude inserted at {@code __SQ_EDGE_TRANSFORM_HOOK__}.
     * Uses the transform helper template (definitions + denials) once, then runs each edge
     * user script against a fresh source/target and patches the result into the node context.
     */
    private String buildEdgeTransformHookBlock(String language, List<EdgeTransformBake> transforms) {
        if (transforms == null || transforms.isEmpty()) {
            return "";
        }
        String tmpl = helperScriptProvider == null
                ? ""
                : helperScriptProvider.transformHelperScriptFor(language).orElse("");
        if (StringUtils.isEmpty(tmpl)) {
            throw new IllegalStateException(
                    "No transform helper template registered for language: " + language);
        }
        String lang = language == null ? "" : language.trim().toLowerCase();
        boolean python = lang.equals("python") || lang.equals("py") || lang.equals("python3");
        // Strip the standalone job footer (user-script marker through end) — combined mode
        // rebinds source/target per edge and patches ctx instead of posting/exiting.
        int userMarker = tmpl.indexOf(NAMED_PLACEHOLDER_TRANSFORM_USER_SCRIPT);
        String definitions = userMarker >= 0 ? tmpl.substring(0, userMarker) : tmpl;
        // Leave input placeholder empty in definitions; each edge rebakes its own inbound.
        definitions = definitions.replace(NAMED_PLACEHOLDER_TRANSFORM_INPUT, "");

        StringBuilder sb = new StringBuilder();
        sb.append(definitions);
        if (python) {
            sb.append('\n').append(PYTHON_CTX_PATCH_HELPER).append('\n');
            for (EdgeTransformBake t : transforms) {
                String b64 = bakeJsonBase64(t.inboundValue());
                String port = StringUtils.defaultString(t.targetPortKey());
                sb.append("\n_sq_inbound = _sq_json.loads(_sq_b64.b64decode('")
                        .append(b64).append("').decode('utf-8'))\n");
                sb.append("source = _TransformSource(_sq_inbound)\n");
                sb.append("target = _TransformTarget()\n");
                sb.append(StringUtils.defaultString(t.userScript())).append('\n');
                sb.append("_sq_edge_out = target._result_or_empty()\n");
                sb.append("_sq_patch_edge_transform_into_ctx(_sq_ctx, ")
                        .append(objectMapperValueLiteral(port))
                        .append(", _sq_edge_out)\n");
            }
        } else {
            sb.append('\n').append(JS_CTX_PATCH_HELPER).append('\n');
            for (EdgeTransformBake t : transforms) {
                String b64 = bakeJsonBase64(t.inboundValue());
                String port = StringUtils.defaultString(t.targetPortKey());
                sb.append("\n_sqInbound = JSON.parse(Buffer.from('")
                        .append(b64).append("', 'base64').toString('utf-8'));\n");
                sb.append("var source = _sqMakeSource(_sqInbound);\n");
                sb.append("var target = _sqMakeTarget();\n");
                sb.append(StringUtils.defaultString(t.userScript())).append('\n');
                sb.append("var _sqEdgeOut = target._resultOrEmpty();\n");
                sb.append("_sqPatchEdgeTransformIntoCtx(_sqCtx, ")
                        .append(objectMapperValueLiteral(port))
                        .append(", _sqEdgeOut);\n");
            }
        }
        return sb.toString();
    }

    private String objectMapperValueLiteral(String value) {
        try {
            return objectMapper.writeValueAsString(value == null ? "" : value);
        } catch (Exception e) {
            return "\"\"";
        }
    }

    private static final String PYTHON_CTX_PATCH_HELPER = """
            def _sq_patch_edge_transform_into_ctx(ctx, port_key, value):
                if not isinstance(ctx, dict):
                    return
                node = ctx.get("currentNode")
                if not isinstance(node, dict):
                    return
                inputs = node.get("inputList")
                if not isinstance(inputs, list):
                    return
                for entry in inputs:
                    if not isinstance(entry, dict):
                        continue
                    key = entry.get("targetPortValue") or entry.get("portValue")
                    if key == port_key:
                        entry["value"] = value
                        return
                inputs.append({"portValue": port_key, "targetPortValue": port_key, "value": value})
            """;

    private static final String JS_CTX_PATCH_HELPER = """
            function _sqPatchEdgeTransformIntoCtx(ctx, portKey, value) {
              if (!ctx || typeof ctx !== "object") return;
              const node = ctx.currentNode;
              if (!node || typeof node !== "object") return;
              if (!Array.isArray(node.inputList)) node.inputList = [];
              for (const entry of node.inputList) {
                if (!entry || typeof entry !== "object") continue;
                const key = entry.targetPortValue || entry.portValue;
                if (key === portKey) {
                  entry.value = value;
                  return;
                }
              }
              node.inputList.push({ portValue: portKey, targetPortValue: portKey, value: value });
            }
            """;
}
