package com.studioquack.common.dto.webhook;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Helper for recognizing and extracting {@code {"$webhookKeyRef": "<uuid|whk_...>"}} structured
 * parameter placeholders across node configuration parameters and boundary-event ingress
 * bindings.
 *
 * <p>Plaintext API/webhook keys never appear in workflow documents, deploy payloads, or logs;
 * only this reference placeholder is persisted.
 */
public final class WebhookKeyRefHelper {

    /** Canonical JSON field name for a webhook key reference placeholder. */
    public static final String REF_FIELD = "$webhookKeyRef";

    /** Parameter type constant for webhook key bindings. */
    public static final String PARAM_TYPE_WEBHOOK_KEY = "webhook_key";

    /** Valid key reference id pattern: accepts UUIDs and {@code whk_...} identifiers. */
    public static final Pattern REF_ID_PATTERN = Pattern.compile("^[A-Za-z0-9_-]{6,64}$");

    private static final ObjectMapper JSON = new ObjectMapper();

    private WebhookKeyRefHelper() {
    }

    /** True when {@code refId} matches the accepted UUID / {@code whk_...} reference id pattern. */
    public static boolean isValidRefId(String refId) {
        return refId != null && REF_ID_PATTERN.matcher(refId.trim()).matches();
    }

    /** Construct the canonical placeholder map {@code {"$webhookKeyRef": refId}}. */
    public static Map<String, String> placeholder(String refId) {
        Map<String, String> map = new LinkedHashMap<>(1);
        map.put(REF_FIELD, refId);
        return map;
    }

    /**
     * Extract a valid key reference id (UUID or {@code whk_...}) from a parameter value, accepting
     * either a deserialized {@link Map} ({@code {"$webhookKeyRef": "..."}}) or a JSON-encoded
     * string representation of the same object.
     */
    public static Optional<String> extractWebhookKeyRef(Object rawValue) {
        if (rawValue == null) {
            return Optional.empty();
        }
        if (rawValue instanceof Map<?, ?> map) {
            Object ref = map.get(REF_FIELD);
            if (ref instanceof String s && isValidRefId(s)) {
                return Optional.of(s.trim());
            }
            return Optional.empty();
        }
        if (rawValue instanceof CharSequence cs) {
            String text = cs.toString().trim();
            if (text.startsWith("{") && text.contains(REF_FIELD)) {
                try {
                    JsonNode node = JSON.readTree(text);
                    JsonNode refNode = node.get(REF_FIELD);
                    if (refNode != null && refNode.isTextual() && isValidRefId(refNode.asText())) {
                        return Optional.of(refNode.asText().trim());
                    }
                } catch (Exception ignored) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    /**
     * True when {@code rawValue} represents a structured {@code $webhookKeyRef} placeholder.
     */
    public static boolean isWebhookKeyRef(Object rawValue) {
        return extractWebhookKeyRef(rawValue).isPresent();
    }

    /**
     * Collect every distinct {@code whk_...} reference id referenced anywhere in the workflow
     * graph: both top-level node {@code configurationParameters} and
     * {@code boundaryEventConfigs[].config.ingressNodeBinding.configurationParameters}.
     */
    public static Set<String> collectWebhookKeyRefs(List<NodeDto> nodes) {
        Set<String> refs = new LinkedHashSet<>();
        if (nodes == null || nodes.isEmpty()) {
            return refs;
        }
        for (NodeDto node : nodes) {
            TemplateDto data = node == null ? null : node.getData();
            if (data == null) {
                continue;
            }
            collectFromParameters(data.getConfigurationParameters(), refs);
            collectFromBoundaries(data.getBoundaryEventConfigs(), refs);
        }
        return refs;
    }

    /**
     * Collect every distinct {@code whk_...} reference id inside a single
     * {@code ingressNodeBinding} map's {@code configurationParameters} list.
     */
    public static Set<String> collectFromIngressBinding(Map<String, Object> ingressBinding) {
        Set<String> refs = new LinkedHashSet<>();
        if (ingressBinding == null || ingressBinding.isEmpty()) {
            return refs;
        }
        Object rawParams = ingressBinding.get("configurationParameters");
        if (rawParams instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof ParameterDto param) {
                    extractWebhookKeyRef(param.getValue()).ifPresent(refs::add);
                } else if (item instanceof Map<?, ?> map) {
                    extractWebhookKeyRef(map.get("value")).ifPresent(refs::add);
                }
            }
        }
        return refs;
    }

    /**
     * Collect every distinct non-blank {@code ingressNodeBinding.templateId} referenced by
     * {@code MESSAGE} boundary events across the workflow graph.
     */
    public static Set<String> collectBoundaryIngressTemplateIds(List<NodeDto> nodes) {
        Set<String> templateIds = new LinkedHashSet<>();
        if (nodes == null || nodes.isEmpty()) {
            return templateIds;
        }
        for (NodeDto node : nodes) {
            TemplateDto data = node == null ? null : node.getData();
            if (data == null || data.getBoundaryEventConfigs() == null) {
                continue;
            }
            for (BoundaryEventConfig boundary : data.getBoundaryEventConfigs()) {
                if (boundary == null || !boundary.isIngressNodeSource()) {
                    continue;
                }
                Map<String, Object> binding = boundary.getIngressNodeBinding();
                if (binding != null) {
                    Object tplId = binding.get("templateId");
                    if (tplId instanceof String s && !s.isBlank()) {
                        templateIds.add(s.trim());
                    }
                }
            }
        }
        return templateIds;
    }

    private static void collectFromParameters(List<ParameterDto> params, Set<String> out) {
        if (params == null) {
            return;
        }
        for (ParameterDto param : params) {
            if (param == null) {
                continue;
            }
            extractWebhookKeyRef(param.getValue()).ifPresent(out::add);
        }
    }

    private static void collectFromBoundaries(List<BoundaryEventConfig> boundaries, Set<String> out) {
        if (boundaries == null) {
            return;
        }
        for (BoundaryEventConfig boundary : boundaries) {
            if (boundary == null || !boundary.isIngressNodeSource()) {
                continue;
            }
            out.addAll(collectFromIngressBinding(boundary.getIngressNodeBinding()));
        }
    }
}
