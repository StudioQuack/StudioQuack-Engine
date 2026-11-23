package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeploymentInfo implements Serializable {
    private long processDefinitionKey;
    private String bpmnProcessId;
    private int version;
    private long processInstanceKey;
    private String tenantId;
    /**
     * Activiti identifiers, stored as strings.
     * Coexist with the legacy {@code long} keys during the strangler window:
     * {@code processDefinitionId} pins the exact deployed version for a Designer Run
     * ({@code runtimeService.startProcessInstanceById}); {@code processInstanceId} is the
     * UUID returned at start (StrongUuidGenerator). Null on the legacy long-key path.
     */
    private String processDefinitionId;
    private String processInstanceId;
    /**
     * SHA-256 of the graph (name, sorted nodes, sorted edges).
     * An unchanged signature reuses the existing deployment.
     */
    private String graphSignature;
}
