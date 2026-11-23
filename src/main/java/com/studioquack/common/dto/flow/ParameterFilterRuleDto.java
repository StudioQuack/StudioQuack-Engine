package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * One designer filter rule stored on a SOURCE {@link ParameterDto}.
 *
 * <p>Shape (also documented on the frontend {@code ParameterDto.filterRules}):
 * {@code { when, targets: [{ param, visible, allowedValues }] }}. Opaque to
 * execution — template and workflow APIs persist it as part of the parameter JSON.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParameterFilterRuleDto implements Serializable {
    /** Trigger value compared with the source parameter's current value. */
    private Object when;
    private List<ParameterFilterTargetDto> targets = new ArrayList<>();
}
