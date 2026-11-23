package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InOutValueDto implements Serializable, Cloneable {
    long elementInstanceKey;
    boolean gateway;

    String targetPortId;
    String targetNodeId;
    String targetPortValue;

    String nodeId;
    String portId;
    String portValue;

    /**
     * Source port type (FUNCTION, OUTPUT, and so on), stamped at function-port dispatch.
     * Null on ordinary data-edge fan-out.
     */
    String sourcePortType;

    /**
     * Canvas label of the node this delivery came from, stamped at function-port dispatch.
     * Null on ordinary data-edge fan-out.
     */
    String sourceNodeLabel;

    /**
     * True when this row came from the operator's apply-inputs override.
     * Those rows skip the inbound port-schema filter. Not serialized.
     */
    @JsonIgnore
    transient boolean operatorOverride;

    Object value;

    /**
     * Payload of {@link #value} before the port-schema filter.
     * The function-return path uses this when a stale schema would otherwise empty the value.
     */
    Object unfilteredValue;

    public InOutValueDto(String portValue, Object value) {
        this.portValue = portValue;
        this.value = value;
    }

    @Override
    public InOutValueDto clone() {
        try {
            return (InOutValueDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
