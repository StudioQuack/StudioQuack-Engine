package com.studioquack.common.dto.flow;

import com.studioquack.common.enums.flow.PortType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionDto implements Serializable,Cloneable {
    private String nodeId;
    private String nodeType;
    private String bpmnType;
    private String jobType;
    private String portName;
    private String portValue;
    private PortType portType;
    private List<ConnectionDetailDto> connectionDetailList = new ArrayList<>();

    @Override
    public ConnectionDto clone() {
        try {
            return (ConnectionDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
