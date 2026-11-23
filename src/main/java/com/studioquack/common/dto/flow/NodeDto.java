package com.studioquack.common.dto.flow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.studioquack.common.enums.flow.PortType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeDto implements Serializable, Cloneable {
    private String id;
    private PointDto position;
    /** React Flow node type from the designer canvas. */
    private String type;
    private String parentId;
    private StyleDto style;
    /** Template payload for this canvas node. Named {@code data} to match the designer. */
    private TemplateDto data;
    List<ConnectionDto> connectionList = new ArrayList<>();

    private List<InOutValueDto> inputList;
    private List<InOutValueDto> outputList;

    private List<EdgeTransformDto> edgeTransformListForCurrentNode;
    private List<SpecListForNodeOutputDto> specListForNodeOutput;

    @JsonIgnore
    public String findCurrentNodeLanguage() {
        if (Objects.isNull(data)) return StringUtils.EMPTY;

        TemplateScriptDto templateScript = data.getTemplateScript();
        if (Objects.isNull(templateScript)) return StringUtils.EMPTY;

        return templateScript.getLanguage();
    }

    @JsonIgnore
    public String findCurrentNodeScript() {
        if (Objects.isNull(data)) return StringUtils.EMPTY;

        TemplateScriptDto templateScript = data.getTemplateScript();
        if (Objects.isNull(templateScript)) return StringUtils.EMPTY;

        return templateScript.getScript();
    }

    @JsonIgnore
    public List<ConnectionDto> findConnectionListByPortName(String portName) {
        if (Objects.isNull(portName) || CollectionUtils.isEmpty(connectionList)) {
            return null;
        }

        return connectionList.stream()
                .filter(connectionDto -> connectionDto.getPortName().equals(portName))
                .toList();
    }

    @JsonIgnore
    public List<ConnectionDto> findConnectionListByPortValue(String portValue) {
        if (Objects.isNull(portValue) || CollectionUtils.isEmpty(connectionList)) {
            return null;
        }

        return connectionList.stream()
                .filter(connectionDto -> connectionDto.getPortValue().equals(portValue))
                .toList();
    }

    @JsonIgnore
    public String findPortIdByPortName(String portName) {
        if (Objects.isNull(data) || StringUtils.isEmpty(portName)) return StringUtils.EMPTY;

        PortDto portDto = data.findPortByPortName(portName);
        if (Objects.isNull(portDto)) return StringUtils.EMPTY;

        return portDto.getId();
    }

    @JsonIgnore
    public String findPortIdByPortValue(String portValue) {
        if (Objects.isNull(data) || StringUtils.isEmpty(portValue)) return StringUtils.EMPTY;

        PortDto portDto = data.findPortByPortValue(portValue);
        if (Objects.isNull(portDto)) return StringUtils.EMPTY;

        return portDto.getId();
    }
    
    @JsonIgnore
    public String findPortValueByPortName(String portName) {
        if (Objects.isNull(data) || StringUtils.isEmpty(portName)) return StringUtils.EMPTY;

        PortDto portDto = data.findPortByPortName(portName);
        if (Objects.isNull(portDto)) return StringUtils.EMPTY;

        return portDto.getValue();
    }

    @JsonIgnore
    public ConnectionDto findConnectionByPortType(PortType portType) {
        if (Objects.isNull(portType) || CollectionUtils.isEmpty(connectionList)) {
            return null;
        }
        for (ConnectionDto connection : connectionList) {
            if (connection.getPortType() == portType) {
                return connection;
            }
        }
        return null;
    }

    @JsonIgnore
    public ConnectionDto findConnectionByNodeIdAndNodeType(String nodeId, String nodeType) {
        if (StringUtils.isEmpty(nodeId) || StringUtils.isEmpty(nodeType) || CollectionUtils.isEmpty(connectionList)) {
            return null;
        }
        for (ConnectionDto connection : connectionList) {
            if(StringUtils.isNotEmpty(connection.getNodeId()) || StringUtils.isEmpty(connection.getNodeType())) continue;
            if (connection.getNodeId().equals(nodeId) && connection.getNodeType().equals(nodeType)) {
                return connection;
            }
        }
        return null;
    }


    @Override
    public NodeDto clone() {
        try {
            return (NodeDto) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

}
