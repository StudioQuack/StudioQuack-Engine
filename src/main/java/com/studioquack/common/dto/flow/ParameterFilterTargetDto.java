package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Target of a {@link ParameterFilterRuleDto}: which parameter to show/hide or
 * whose values to restrict when the source matches {@code when}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParameterFilterTargetDto implements Serializable {
    /** Target parameter shortCode. */
    private String param;
    /**
     * {@code true}: render the target only while the source equals {@code when}.
     * {@code false}: always render; when matched, restrict values to {@code allowedValues}.
     */
    private boolean visible = true;
    private List<String> allowedValues;
}
