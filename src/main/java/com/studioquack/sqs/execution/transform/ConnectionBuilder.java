package com.studioquack.sqs.execution.transform;

import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.EdgeDataDto;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.SchemaItemDto;
import com.studioquack.common.dto.flow.SpecListForNodeOutputDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Builds the current node's {@code connectionList} (the wiring the helper channel walks to
 * resolve {@code studioquack.execute(port)}) and collects each edge's transforms. Ported
 * from the control plane {@code ConnectionBuilder} with the {@code commons-collections3 MapUtils}
 * / {@code Strings.CI/CS} calls swapped for {@code commons-lang3} + JDK equivalents to keep
 * the daemon dependency surface lean.
 *
 * <p><b>No in-process JOLT here (Mapping Invariant 14).</b> Like the control plane, this builder
 * only <em>collects</em> the per-output JOLT spec list onto {@code specListForNodeOutput};
 * it never applies JOLT to values. Inter-node JOLT is the downstream node's inbound leg.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConnectionBuilder {

    private final JoltTransformService joltTransformService;

    public void buildConnections(NodeDto currentNode, List<NodeDto> nodes, List<EdgeDto> edges) {
        if (Objects.isNull(currentNode) || edges == null) {
            return;
        }
        List<ConnectionDto> connectionList = new ArrayList<>();
        for (EdgeDto edge : edges) {
            if (Objects.nonNull(edge.getData())) {
                if (currentNode.getId().equals(edge.getTarget())) {
                    addEdgeTransformListForCurrentNode(currentNode, edge);
                } else if (currentNode.getId().equals(edge.getSource())) {
                    addEdgeTransformListForCurrentNode(currentNode, edge);
                    transformedOutputValue(currentNode, edge);
                }
            }

            if (!currentNode.getId().equals(edge.getSource())) {
                continue;
            }

            String sourceHandle = edge.getSourceHandle();
            String targetHandle = edge.getTargetHandle();
            if (StringUtils.isEmpty(sourceHandle) || StringUtils.isEmpty(targetHandle)) {
                continue;
            }

            String sourcePortName = getPortNameFromHandle(sourceHandle);
            PortType sourcePortType = getPortTypeFromHandle(sourceHandle);
            String targetPortName = getPortNameFromHandle(targetHandle);
            if (StringUtils.isEmpty(sourcePortName) || Objects.isNull(sourcePortType)
                    || StringUtils.isEmpty(targetPortName)) {
                continue;
            }

            String finalSourcePortName = sourcePortName;
            ConnectionDto connection = connectionList.stream()
                    .filter(conn -> StringUtils.equalsIgnoreCase(conn.getPortName(), finalSourcePortName))
                    .findFirst()
                    .orElse(new ConnectionDto());

            NodeDto targetNode = nodes.stream()
                    .filter(item -> edge.getTarget().equals(item.getId()))
                    .findFirst()
                    .orElse(null);
            if (Objects.isNull(targetNode)) {
                continue;
            }

            ConnectionDetailDto connectionDetail = new ConnectionDetailDto();
            TemplateDto targetTemplate = targetNode.getData();
            connectionDetail.setNodeId(targetNode.getId());
            List<ParameterDto> configurationParameters = targetTemplate.getConfigurationParameters();
            List<ParameterDto> publicParameters = new ArrayList<>();
            if (configurationParameters != null) {
                for (ParameterDto configurationParameter : configurationParameters) {
                    if (configurationParameter.isPublic()) {
                        publicParameters.add(configurationParameter);
                    }
                }
            }

            connectionDetail.setConfigurationParameters(publicParameters);
            connectionDetail.setPortDto(targetTemplate.findPortByPortName(targetPortName));
            connectionDetail.setBpmnType(targetTemplate.getBpmnType());
            connection.setNodeId(targetNode.getId());
            connection.setNodeType(targetTemplate.getNodeType());
            connection.setBpmnType(targetTemplate.getBpmnType());
            connection.setJobType(targetTemplate.getJobType());
            connection.setPortName(sourcePortName);

            String sourcePortValue = currentNode.findPortValueByPortName(sourcePortName);
            connection.setPortValue(sourcePortValue);
            connection.setPortType(sourcePortType);
            connection.getConnectionDetailList().add(connectionDetail);

            String nodeType = targetNode.getData().getNodeType();
            boolean isGatewayForTargetNode = StringUtils.isNotEmpty(nodeType) && nodeType.equals("gateway");
            if (isGatewayForTargetNode) {
                buildConnections(targetNode, nodes, edges);
                List<ConnectionDto> gatewayConnections = targetNode.getConnectionList();
                connection.getConnectionDetailList().clear();
                for (ConnectionDto gatewayConnection : gatewayConnections) {
                    connection.getConnectionDetailList().addAll(gatewayConnection.getConnectionDetailList());
                }
            }

            boolean hasNotPortType = connectionList.stream()
                    .noneMatch(conn -> StringUtils.equalsIgnoreCase(conn.getPortName(), finalSourcePortName));
            if (hasNotPortType) {
                connectionList.add(connection);
            }
        }

        currentNode.setConnectionList(connectionList);
    }

    private void addEdgeTransformListForCurrentNode(NodeDto currentNode, EdgeDto edge) {
        if (Objects.isNull(currentNode.getEdgeTransformListForCurrentNode())) {
            currentNode.setEdgeTransformListForCurrentNode(new ArrayList<>());
        }

        List<EdgeTransformDto> edgeTransformDtoList = edge.getData().getEdgeTransforms();
        if (CollectionUtils.isNotEmpty(edgeTransformDtoList)) {
            edgeTransformDtoList.forEach(edgeTransformDto -> {
                edgeTransformDto.setSourcePortShortCode(extractShortCode(edge.getSourceHandle()));
                edgeTransformDto.setTargetPortShortCode(extractShortCode(edge.getTargetHandle()));
            });
            currentNode.getEdgeTransformListForCurrentNode().addAll(edgeTransformDtoList);
        }

        List<SchemaItemDto> schemaItemDtoList = edge.getData().getSchemaItems();
        if (CollectionUtils.isNotEmpty(schemaItemDtoList)) {
            List<EdgeTransformDto> finalList = CollectionUtils.isEmpty(edgeTransformDtoList)
                    ? new ArrayList<>() : edgeTransformDtoList;
            schemaItemDtoList.forEach(schemaItemDto ->
                    processSchemaItem(currentNode, edge, finalList, schemaItemDto));
        }
    }

    private void processSchemaItem(NodeDto currentNode, EdgeDto edge,
                                   List<EdgeTransformDto> edgeTransformDtoList, SchemaItemDto schemaItemDto) {
        if ("source".equals(schemaItemDto.getType())) {
            handleSchemaItem(currentNode, edge, edgeTransformDtoList, schemaItemDto, true);
        } else if ("target".equals(schemaItemDto.getType())) {
            handleSchemaItem(currentNode, edge, edgeTransformDtoList, schemaItemDto, false);
        }
    }

    private void handleSchemaItem(NodeDto currentNode, EdgeDto edge,
                                  List<EdgeTransformDto> edgeTransformDtoList,
                                  SchemaItemDto schemaItemDto, boolean isSource) {
        EdgeTransformDto existingTransform = edgeTransformDtoList.stream()
                .filter(et -> isSource
                        ? Objects.equals(et.getSourceNodeId(), schemaItemDto.getNodeId())
                                && Objects.equals(et.getSourcePath(), schemaItemDto.getShortCode())
                        : Objects.equals(et.getTargetNodeId(), schemaItemDto.getNodeId())
                                && Objects.equals(et.getTargetPath(), schemaItemDto.getShortCode()))
                .findFirst()
                .orElse(null);

        if (Objects.nonNull(existingTransform)) {
            if (isSource) {
                existingTransform.setSourcePathValue(
                        Objects.nonNull(schemaItemDto.getValue()) ? schemaItemDto.getValue().toString() : null);
                existingTransform.setSourceDescription(schemaItemDto.getDescription());
            } else {
                existingTransform.setTargetPathValue(
                        Objects.nonNull(schemaItemDto.getValue()) ? schemaItemDto.getValue().toString() : null);
                existingTransform.setTargetDescription(schemaItemDto.getDescription());
            }
        } else {
            EdgeTransformDto newEdgeTransformDto = new EdgeTransformDto();
            if (isSource) {
                newEdgeTransformDto.setSourceNodeId(schemaItemDto.getNodeId());
                newEdgeTransformDto.setSourcePath(schemaItemDto.getShortCode());
                newEdgeTransformDto.setSourcePathValue(
                        Objects.nonNull(schemaItemDto.getValue()) ? schemaItemDto.getValue().toString() : null);
                newEdgeTransformDto.setSourceDescription(schemaItemDto.getDescription());
                newEdgeTransformDto.setSourcePortShortCode(extractShortCode(edge.getSourceHandle()));
                newEdgeTransformDto.setTargetPortShortCode(extractShortCode(edge.getTargetHandle()));
                newEdgeTransformDto.setTargetNodeId(edge.getTarget());
            } else {
                newEdgeTransformDto.setTargetNodeId(schemaItemDto.getNodeId());
                newEdgeTransformDto.setTargetPath(schemaItemDto.getShortCode());
                newEdgeTransformDto.setTargetPathValue(
                        Objects.nonNull(schemaItemDto.getValue()) ? schemaItemDto.getValue().toString() : null);
                newEdgeTransformDto.setTargetDescription(schemaItemDto.getDescription());
                newEdgeTransformDto.setTargetPortShortCode(extractShortCode(edge.getTargetHandle()));
                newEdgeTransformDto.setSourcePortShortCode(extractShortCode(edge.getSourceHandle()));
                newEdgeTransformDto.setSourceNodeId(edge.getSource());
            }
            currentNode.getEdgeTransformListForCurrentNode().add(newEdgeTransformDto);
        }
    }

    private String extractShortCode(String handle) {
        return (handle != null && handle.split("-").length == 2) ? handle.split("-")[1] : null;
    }

    /**
     * Collects the per-output JOLT spec list onto {@code currentNode.specListForNodeOutput}
     * WITHOUT applying it. The application (if any) is the downstream node's inbound leg —
     * this method exists only for the function sub-process aggregator's parity shape.
     */
    private void transformedOutputValue(NodeDto currentNode, EdgeDto edge) {
        EdgeDataDto edgeData = edge.getData();
        if (CollectionUtils.isEmpty(edgeData.getEdgeTransforms())) {
            return;
        }
        String currentNodeId = currentNode.getId();
        String nodeType = currentNode.getData().getNodeType();
        boolean isGateway = StringUtils.isNotEmpty(nodeType) && nodeType.equals("gateway");
        Map<String, List<EdgeTransformDto>> edgeTransformsForOutputMap = new HashMap<>();

        if (isGateway) {
            for (ConnectionDto connectionDto : currentNode.getConnectionList()) {
                for (ConnectionDetailDto connectionDetailDto : connectionDto.getConnectionDetailList()) {
                    Map<String, List<EdgeTransformDto>> outputSchema = edgeData.getEdgeTransforms().stream()
                            .filter(item -> "schema".equalsIgnoreCase(item.getTargetSchemaType()))
                            .filter(item -> Objects.equals(item.getTargetNodeId(), connectionDetailDto.getNodeId()))
                            .filter(item -> Objects.nonNull(item.getSourceNodeId()))
                            .collect(Collectors.groupingBy(EdgeTransformDto::getSourceNodeId));
                    edgeTransformsForOutputMap.putAll(outputSchema);
                }
            }
        } else {
            edgeTransformsForOutputMap = edgeData.getEdgeTransforms().stream()
                    .filter(item -> "outputSchema".equalsIgnoreCase(item.getTargetSchemaType()))
                    .filter(item -> Objects.equals(item.getTargetNodeId(), currentNodeId))
                    .filter(item -> Objects.nonNull(item.getSourceNodeId()))
                    .collect(Collectors.groupingBy(EdgeTransformDto::getSourceNodeId));
        }

        if (edgeTransformsForOutputMap.isEmpty()) {
            return;
        }

        List<SpecListForNodeOutputDto> specListForNodeOutput = new ArrayList<>();
        for (Map.Entry<String, List<EdgeTransformDto>> entry : edgeTransformsForOutputMap.entrySet()) {
            List<Object> joltSpecList = joltTransformService.convertJoltSpec(entry.getValue());
            if (CollectionUtils.isEmpty(joltSpecList)) {
                continue;
            }
            specListForNodeOutput.add(new SpecListForNodeOutputDto(entry.getKey(), joltSpecList));
        }

        if (CollectionUtils.isEmpty(specListForNodeOutput)) {
            return;
        }
        if (CollectionUtils.isEmpty(currentNode.getSpecListForNodeOutput())) {
            currentNode.setSpecListForNodeOutput(new ArrayList<>());
        }
        currentNode.getSpecListForNodeOutput().addAll(specListForNodeOutput);
    }

    private String getPortNameFromHandle(String handle) {
        String[] parts = handle.split("-");
        return parts.length == 2 ? parts[1] : null;
    }

    private PortType getPortTypeFromHandle(String handle) {
        String[] parts = handle.split("-");
        if (parts.length == 2) {
            return PortType.valueOf(parts[0].toUpperCase());
        }
        return null;
    }
}
