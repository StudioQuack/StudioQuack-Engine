package com.studioquack.sqs.command.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.EdgeDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.sqs.command.DeployCommandPayload;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns the client-side persistence a {@code deploy} command produces: the {@link DeployedDefinitionEntity}
 * deployment cache — the durable "prior" {@code DeploymentInfo} the {@code bpmn-signature:v7} reuse guard
 * compares against; and. the {@link DeployedTemplateEntity} / {@link DeployedScriptEntity} snapshot of the
 * node templates + scripts, so the environment runs self-contained, offline from central the control plane.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeployedArtifactStore {

    private static final String KEY_SEP = "|";

    private final DeployedTemplateRepository templateRepository;
    private final DeployedScriptRepository scriptRepository;
    private final DeployedDefinitionRepository definitionRepository;
    private final ObjectMapper objectMapper;


    // Deployment cache (deploy-reuse guard).


    /**
     * The durable prior {@link DeploymentInfo} for {@code workflowId}, or {@code null} on first deploy. Fed to
     * {@code WorkflowDeploymentService.deploy(graph, prior, false)} so a byte-identical graph reuses the deployed
     * version (no new engine version) instead of redeploying.
     */
    @Transactional(readOnly = true)
    public DeploymentInfo findPriorDefinition(String workflowId) {
        if (StringUtils.isBlank(workflowId)) {
            return null;
        }
        return definitionRepository.findById(workflowId).map(this::toDeploymentInfo).orElse(null);
    }

    /**
     * Upserts the deployment cache row for {@code workflowId} from the just-produced {@code info}.
     */
    @Transactional
    public void saveDefinition(String workflowId, String workflowVersion, DeploymentInfo info) {
        if (StringUtils.isBlank(workflowId) || info == null) {
            return;
        }
        OffsetDateTime now = OffsetDateTime.now();
        DeployedDefinitionEntity entity = definitionRepository.findById(workflowId)
                .orElseGet(() -> DeployedDefinitionEntity.builder().workflowId(workflowId).cdate(now).build());
        entity.setWorkflowVersion(nz(workflowVersion));
        entity.setBpmnProcessId(info.getBpmnProcessId());
        entity.setProcessDefinitionId(info.getProcessDefinitionId());
        entity.setDefinitionVersion(info.getVersion());
        entity.setGraphSignature(info.getGraphSignature());
        entity.setUdate(now);
        if (entity.getCdate() == null) {
            entity.setCdate(now);
        }
        definitionRepository.save(entity);
    }

    private DeploymentInfo toDeploymentInfo(DeployedDefinitionEntity e) {
        DeploymentInfo info = new DeploymentInfo();
        info.setBpmnProcessId(e.getBpmnProcessId());
        info.setProcessDefinitionId(e.getProcessDefinitionId());
        info.setVersion(e.getDefinitionVersion());
        info.setGraphSignature(e.getGraphSignature());
        return info;
    }

    /**
     * {@code undeploy} — drops every deploy-time artifact the workflow owns: the deployment-cache row (which also
     * carries the deployed graph, the variable scopes and the webhook address) plus the template/script snapshots
     * across ALL versions. Idempotent: missing rows simply count as zero.
     *
     * @return {@code [definitionsRemoved, templatesRemoved, scriptsRemoved]}
     */
    @Transactional
    public long[] deleteArtifacts(String workflowId) {
        if (StringUtils.isBlank(workflowId)) {
            return new long[]{0, 0, 0};
        }
        boolean existed = definitionRepository.existsById(workflowId);
        definitionRepository.deleteById(workflowId);
        long templates = templateRepository.deleteByWorkflowId(workflowId);
        long scripts = scriptRepository.deleteByWorkflowId(workflowId);
        return new long[]{existed ? 1 : 0, templates, scripts};
    }

    /**
     * Persists the deployed graph (nodes + edges) onto the workflow's deployment-cache row so a REF-only
     * {@code start-run} can hydrate the topology locally instead of the backend re-shipping it every run (deploy
     * is the authoritative source of the graph). Upsert on the existing row; a no-op when the row is absent
     * (deploy always writes {@link #saveDefinition} first).
     */
    @Transactional
    public void saveDeployedGraph(String workflowId, List<NodeDto> nodes, List<EdgeDto> edges) {
        if (StringUtils.isBlank(workflowId)) {
            return;
        }
        definitionRepository.findById(workflowId).ifPresent(entity -> {
            entity.setNodesJson(writeJson(nodes));
            entity.setEdgesJson(writeJson(edges));
            entity.setUdate(OffsetDateTime.now());
            definitionRepository.save(entity);
        });
    }

    /**
     * The deployed graph for a workflow (from the last deploy), or empty when none was persisted.
     */
    @Transactional(readOnly = true)
    public java.util.Optional<DeployedGraph> loadDeployedGraph(String workflowId) {
        if (StringUtils.isBlank(workflowId)) {
            return java.util.Optional.empty();
        }
        return definitionRepository.findById(workflowId)
                .filter(e -> StringUtils.isNotBlank(e.getNodesJson()))
                .map(e -> new DeployedGraph(
                        readList(e.getNodesJson(), new TypeReference<List<NodeDto>>() {}),
                        readList(e.getEdgesJson(), new TypeReference<List<EdgeDto>>() {})));
    }

    /**
     * Persists workflow global params + project variables onto the deployment-cache row (same row as
     * {@link #saveDeployedGraph}). Upsert; no-op when the row is absent.
     */
    @Transactional
    public void saveDeployVariables(String workflowId, Map<String, Object> globalParams,
                                    Map<String, Object> projectVariables) {
        if (StringUtils.isBlank(workflowId)) {
            return;
        }
        definitionRepository.findById(workflowId).ifPresent(entity -> {
            entity.setGlobalParamsJson(globalParams == null ? null : writeJson(globalParams));
            entity.setProjectVariablesJson(projectVariables == null ? null : writeJson(projectVariables));
            entity.setUdate(OffsetDateTime.now());
            definitionRepository.save(entity);
        });
    }

    /**
     * Loads the deploy-time variable scopes for a workflow, or empty when the row is absent. Blank/null LOBs
     * deserialise to empty maps (legacy rows / older BE payloads).
     */
    @Transactional(readOnly = true)
    public java.util.Optional<VariableScopes> loadDeployVariables(String workflowId) {
        if (StringUtils.isBlank(workflowId)) {
            return java.util.Optional.empty();
        }
        return definitionRepository.findById(workflowId)
                .map(e -> new VariableScopes(
                        readMap(e.getGlobalParamsJson()),
                        readMap(e.getProjectVariablesJson())));
    }

    private <T> List<T> readList(String json, TypeReference<List<T>> type) {
        if (StringUtils.isBlank(json)) {
            return new ArrayList<>();
        }
        try {
            return objectMapper.readValue(json, type);
        } catch (Exception e) {
            log.warn("deploy.graph: failed to read persisted graph JSON ({}); returning empty", e.getMessage());
            return new ArrayList<>();
        }
    }

    private Map<String, Object> readMap(String json) {
        if (StringUtils.isBlank(json)) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Object> map = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            return map == null ? new LinkedHashMap<>() : new LinkedHashMap<>(map);
        } catch (Exception e) {
            log.warn("deploy.variables: failed to read persisted vars JSON ({}); returning empty", e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    /**
     * The daemon-local deployed graph, hydrated for a REF-only run.
     */
    public record DeployedGraph(List<NodeDto> nodes, List<EdgeDto> edges) {
    }

    /**
     * Deploy-authoritative variable scopes loaded for sealing onto {@code seedContext} at start-run (
     * {@code globalParams} → {@code workflowVariables}, {@code projectVariables} → {@code projectVariables}).
     */
    public record VariableScopes(Map<String, Object> globalParams, Map<String, Object> projectVariables) {
    }


    // CORE (webhook addressing) — projectShortCode/projectVersion/workflowShortCode.


    /**
     * Persists the webhook-addressing triple onto {@code workflowId}'s deployment-cache row (upsert onto the row
     * {@link #saveDefinition} already wrote this deploy). A blank {@code projectShortCode} or
     * {@code workflowShortCode} clears the address (the workflow is not — or no longer — webhook-addressable),
     * matching the "blank clears the field" convention used elsewhere.
     */
    @Transactional
    public void saveWebhookAddress(String workflowId, String projectShortCode, Integer projectVersion,
                                   String workflowShortCode) {
        if (StringUtils.isBlank(workflowId)) {
            return;
        }
        definitionRepository.findById(workflowId).ifPresent(entity -> {
            entity.setProjectShortCode(StringUtils.trimToNull(projectShortCode));
            entity.setProjectVersion(projectVersion);
            entity.setWorkflowShortCode(StringUtils.trimToNull(workflowShortCode));
            entity.setUdate(OffsetDateTime.now());
            definitionRepository.save(entity);
        });
    }

    /**
     * Call.activity short-code targeting — every deployed row carrying this workflow short code, regardless of
     * project/version. Blank input answers an empty list; the resolver owns the unknown/ambiguous verdicts.
     */
    @Transactional(readOnly = true)
    public List<DeployedDefinitionEntity> findDeployedByWorkflowShortCode(String workflowShortCode) {
        if (StringUtils.isBlank(workflowShortCode)) {
            return List.of();
        }
        return definitionRepository.findByWorkflowShortCode(workflowShortCode.trim());
    }


    // Template/script snapshot (self-contained offline env).


    /**
     * Persists the node templates/scripts snapshot for {@code (workflowId, version)}. Uses the explicit
     * {@code payload.templates()} when present, otherwise derives the snapshot from {@code payload.nodes()[].data}
     * so a slim BE payload still self-provisions the environment.
     *
     * @return the number of distinct templates persisted.
     */
    @Transactional
    public int saveTemplateSnapshot(DeployCommandPayload payload) {
        if (payload == null || StringUtils.isBlank(payload.workflowId())) {
            return 0;
        }
        String workflowId = payload.workflowId();
        String version = nz(payload.version());
        OffsetDateTime now = OffsetDateTime.now();

        // Dedup templates by their stable key (templateId, else the owning nodeId) so a template
        // reused by several nodes is snapshotted once.
        Map<String, Sourced> byKey = new LinkedHashMap<>();
        for (Sourced sourced : collectSources(payload)) {
            if (sourced.template() == null) {
                continue;
            }
            byKey.putIfAbsent(templateKey(sourced), sourced);
        }

        List<DeployedTemplateEntity> templates = new ArrayList<>();
        List<DeployedScriptEntity> scripts = new ArrayList<>();
        for (Map.Entry<String, Sourced> entry : byKey.entrySet()) {
            String key = entry.getKey();
            Sourced sourced = entry.getValue();
            TemplateDto template = sourced.template();

            templates.add(DeployedTemplateEntity.builder()
                    .id(join(workflowId, version, key))
                    .workflowId(workflowId)
                    .workflowVersion(version)
                    .templateId(template.getId())
                    .nodeId(sourced.nodeId())
                    .name(StringUtils.defaultIfBlank(template.getLabel(), template.getName()))
                    .category(template.getCategory())
                    .nodeType(template.getNodeType())
                    .jobType(template.getJobType())
                    .templateJson(writeJson(template))
                    .cdate(now)
                    .udate(now)
                    .build());

            TemplateScriptDto script = template.getTemplateScript();
            if (script != null && StringUtils.isNotBlank(script.getScript())) {
                String language = StringUtils.defaultIfBlank(script.getLanguage(), "unknown");
                scripts.add(DeployedScriptEntity.builder()
                        .id(join(workflowId, version, key, language))
                        .workflowId(workflowId)
                        .workflowVersion(version)
                        .templateId(template.getId())
                        .nodeId(sourced.nodeId())
                        .language(language)
                        .script(script.getScript())
                        .cdate(now)
                        .udate(now)
                        .build());
            }
        }

        templateRepository.saveAll(templates);
        scriptRepository.saveAll(scripts);
        log.info("deploy.snapshot workflowId={} version={} templates={} scripts={} (self-contained offline store)",
                workflowId, version, templates.size(), scripts.size());
        return templates.size();
    }

    /**
     * Explicit templates take precedence; otherwise fall back to the templates carried on the nodes.
     */
    private List<Sourced> collectSources(DeployCommandPayload payload) {
        List<Sourced> out = new ArrayList<>();
        if (payload.templates() != null && !payload.templates().isEmpty()) {
            for (TemplateDto template : payload.templates()) {
                out.add(new Sourced(null, template));
            }
            return out;
        }
        if (payload.nodes() != null) {
            for (NodeDto node : payload.nodes()) {
                if (node != null && node.getData() != null) {
                    out.add(new Sourced(node.getId(), node.getData()));
                }
            }
        }
        return out;
    }

    private String templateKey(Sourced sourced) {
        if (StringUtils.isNotBlank(sourced.template().getId())) {
            return "tpl_" + sourced.template().getId();
        }
        if (StringUtils.isNotBlank(sourced.nodeId())) {
            return "node_" + sourced.nodeId();
        }
        // Last resort: identity-based so distinct anonymous templates don't collapse into one row.
        return "anon_" + Integer.toHexString(System.identityHashCode(sourced.template()));
    }


    // Read helpers (monitoring / tests).


    @Transactional(readOnly = true)
    public long countTemplates(String workflowId, String version) {
        return templateRepository.countByWorkflowIdAndWorkflowVersion(workflowId, nz(version));
    }

    @Transactional(readOnly = true)
    public long countScripts(String workflowId, String version) {
        return scriptRepository.countByWorkflowIdAndWorkflowVersion(workflowId, nz(version));
    }

    @Transactional(readOnly = true)
    public List<DeployedTemplateEntity> templates(String workflowId, String version) {
        return templateRepository.findByWorkflowIdAndWorkflowVersion(workflowId, nz(version));
    }

    @Transactional(readOnly = true)
    public List<DeployedScriptEntity> scripts(String workflowId, String version) {
        return scriptRepository.findByWorkflowIdAndWorkflowVersion(workflowId, nz(version));
    }


    // Internals.


    private String writeJson(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            log.warn("deploy.snapshot: template JSON serialise failed ({}) — storing null blob", e.getMessage());
            return null;
        }
    }

    private static String join(String... parts) {
        return String.join(KEY_SEP, parts);
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }

    /**
     * A template plus the (optional) node id that referenced it, for the snapshot.
     */
    private record Sourced(String nodeId, TemplateDto template) {
    }
}
