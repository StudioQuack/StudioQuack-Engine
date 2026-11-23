package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionDetailDto implements Serializable {
    private String nodeId;
    private String bpmnType;
    private PortDto portDto;
    private List<ParameterDto> configurationParameters;
}
