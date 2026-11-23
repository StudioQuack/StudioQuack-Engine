package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParameterDto implements Serializable {
    private String id;
    private String shortCode;
    private String label;
    private String type;
    private String description;
    /**
 * Parameter value. Object (not String) since May 2026 because the
 * FE secret picker writes a structured placeholder
 *
 * { "$secretRef": "sec_<24 hex>:v<N>" }
 *
 * directly into this slot when the operator binds a secret to a
 * parameter. Sticking with {@code String} caused workflow saves
 * that referenced secrets to fail with a Jackson 400:
 *
 * "Cannot deserialize value of type `java.lang.String`
 * from Object value (token `JsonToken.START_OBJECT`)"
 *
 * Switching to {@link Object} accepts both shapes: a plain string
 * (legacy parameter values, dropdown picks, free-form text) and
 * the {@code $secretRef} object. Consumers that need a string
 * representation should go through {@link TemplateDto}'s helper
 * methods (which now stringify map values via the shared
 * {@code Objects.toString} contract) or the helper SDK's
 * {@code _sq_resolve_secret_placeholders} resolver, which
 * understands BOTH the object placeholder AND the
 * {@code ${daemon}} env-var dereference.
 */
    private Object value;
    private boolean required;
    private boolean instanceEditable = true;
    private boolean visible = true;
    @JsonProperty("isPublic")
    private boolean isPublic;
    private List<String> options;
    private Map<String, String> validationRules;
    /**
     * Designer-authored visibility / value-filter rules, stored on the SOURCE
     * parameter. Shape: {@code [{ when, targets: [{ param, visible, allowedValues }] }]}.
     * Execution ignores this field; template and workflow JSON round-trip it so the
     * designer can rehydrate Create/Edit Node and the properties panel.
     */
    private List<ParameterFilterRuleDto> filterRules;
}
