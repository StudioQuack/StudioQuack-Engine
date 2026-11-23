package com.studioquack.sqs.command;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.command.store.DeployedDefinitionEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Call.activity short-code targeting: resolves an authored {@code workflowShortCode} parameter to the concrete
 * workflow id of a definition DEPLOYED ON THIS DAEMON, and stamps it into the node's {@code processId}
 * parameter before generation — so the BPMN pipeline ({@code ActivitiCallActivityStrategy} →
 * {@code WorkflowDeploymentService}'s literal-target check) keeps working on plain process keys unchanged. Why
 * a pre-generation rewrite (not a strategy-time expression): the daemon forbids EL in {@code calledElement}
 * (JUEL would execute in the daemon JVM), so the target must be a literal key by the time the strategy runs.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CallActivityShortCodeResolver {

    private static final String BPMN_TYPE = "call.activity";
    private static final String PROCESS_ID_PARAM = "processId";
    private static final String SHORT_CODE_PARAM = "workflowShortCode";

    private final DeployedArtifactStore artifactStore;

    /**
     * Rewrites every call.activity node in the deploy payload: a blank {@code processId} with a set
     * {@code workflowShortCode} is resolved against this daemon's deployed definitions and stamped back as a
     * literal {@code processId} (the bare workflow id — the strategy's own normalisation adds the {@code Process_}
     * prefix and the draft namespace suffix).
     *
     * @throws IllegalArgumentException when a node has neither parameter filled, the short code matches no
     * deployed workflow, or the match stays ambiguous
     */
    public void resolveDeployPayload(DeployCommandPayload payload) {
        if (payload == null || payload.nodes() == null || payload.nodes().isEmpty()) {
            return;
        }
        for (NodeDto node : payload.nodes()) {
            TemplateDto data = node == null ? null : node.getData();
            if (data == null || !BPMN_TYPE.equalsIgnoreCase(data.getBpmnType())) {
                continue;
            }
            String processId = StringUtils.trimToNull(data.findParameterValueByShortCode(PROCESS_ID_PARAM));
            if (processId != null) {
                continue; // Explicit Process Id wins
            }
            String shortCode = normalizeShortCode(data.findParameterValueByShortCode(SHORT_CODE_PARAM));
            if (shortCode == null) {
                throw new IllegalArgumentException("call.activity '" + nodeLabel(node, data)
                        + "': set either Process Id or Workflow Short Code — both are empty");
            }
            String resolvedWorkflowId = resolve(shortCode, payload.projectShortCode(), payload.projectVersion());
            stampProcessId(data, resolvedWorkflowId);
            log.info("call.activity short-code resolved node={} shortCode={} -> workflowId={}",
                    node.getId(), shortCode, resolvedWorkflowId);
        }
    }

    /**
     * Resolves a workflow short code to the workflow id of a deployed definition on this daemon, preferring the
     * given project/version context when it narrows the candidate set. A {@code null} context (the generation-time
     * fallback path) resolves only a globally unique code.
     *
     * @throws IllegalArgumentException on an unknown or ambiguous short code
     */
    public String resolve(String shortCode, String projectShortCode, Integer projectVersion) {
        List<DeployedDefinitionEntity> candidates = artifactStore.findDeployedByWorkflowShortCode(shortCode);
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("call.activity: no deployed workflow with short code '"
                    + shortCode + "' on this daemon — deploy the target workflow first "
                    + "(or address it by Process Id)");
        }
        if (StringUtils.isNotBlank(projectShortCode)) {
            List<DeployedDefinitionEntity> sameProject = candidates.stream()
                    .filter(e -> projectShortCode.equals(e.getProjectShortCode()))
                    .toList();
            if (!sameProject.isEmpty()) {
                candidates = sameProject;
                if (projectVersion != null) {
                    List<DeployedDefinitionEntity> sameVersion = candidates.stream()
                            .filter(e -> projectVersion.equals(e.getProjectVersion()))
                            .toList();
                    if (!sameVersion.isEmpty()) {
                        candidates = sameVersion;
                    }
                }
            }
        }
        List<String> distinctTargets = candidates.stream()
                .map(DeployedDefinitionEntity::getWorkflowId)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        if (distinctTargets.size() == 1) {
            return distinctTargets.get(0);
        }
        throw new IllegalArgumentException("call.activity: short code '" + shortCode + "' matches "
                + distinctTargets.size() + " deployed workflows on this daemon (across projects/"
                + "versions) — address the target by Process Id instead");
    }

    /**
     * Stamps the resolved workflow id into the node's {@code processId} parameter.
     */
    private static void stampProcessId(TemplateDto data, String workflowId) {
        List<ParameterDto> params = data.getConfigurationParameters();
        if (params != null) {
            for (ParameterDto param : params) {
                if (param != null && PROCESS_ID_PARAM.equals(param.getShortCode())) {
                    param.setValue(workflowId);
                    return;
                }
            }
        }
        // A node authored before the template carried the parameter: add it so the strategy's
        // shortCode lookup finds the resolved value.
        ParameterDto param = new ParameterDto();
        param.setShortCode(PROCESS_ID_PARAM);
        param.setValue(workflowId);
        java.util.List<ParameterDto> merged = params == null
                ? new java.util.ArrayList<>() : new java.util.ArrayList<>(params);
        merged.add(param);
        data.setConfigurationParameters(merged);
    }

    private static String nodeLabel(NodeDto node, TemplateDto data) {
        if (StringUtils.isNotBlank(data.getLabel())) {
            return data.getLabel();
        }
        if (StringUtils.isNotBlank(data.getName())) {
            return data.getName();
        }
        return node.getId();
    }

    /**
     * {@code trim()} + lowercase — mirrors the BE's {@code ShortCodeValidator.normalize} (not on the daemon's
     * common-jar classpath). Short codes are already stored normalised; this keeps a raw authored value comparable
     * to them.
     */
    public static String normalizeShortCode(String raw) {
        return raw == null ? null : StringUtils.trimToNull(raw.toLowerCase(Locale.ROOT));
    }
}
