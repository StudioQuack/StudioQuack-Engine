package com.studioquack.sqs.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.BoundaryEventConfig;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.SelectOptionParser;
import com.studioquack.common.dto.flow.SelectOptionRole;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.webhook.WebhookKeyRefHelper;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Deploy-time index of active webhook key bindings across deployed workflows, backed by JPA
 * ({@code deployed_webhook_bindings}) with a bounded in-memory LRU lookup cache (cap: 1,024 keys).
 *
 * <p>100% parameter-driven: inspects only {@link SelectOptionParser#resolveActiveRole(TemplateDto)},
 * {@link WebhookKeyRefHelper} parameter bindings, and {@code boundaryEventConfigs[].config.ingressNodeBinding}
 * (including control-plane-hydrated {@code resolvedTemplate} snapshots).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeployedWebhookBindingStore {

    /** Fan-out amplification DoS guard: max active MESSAGE_START bindings per webhookKeyRef. */
    public static final int MAX_MESSAGE_START_BINDINGS_PER_KEY = 20;

    /** Bounded in-memory LRU cache capacity (distinct webhookKeyRef keys). */
    public static final int CACHE_MAX_KEYS = 1024;

    public static final String KIND_MESSAGE_START = "MESSAGE_START";
    public static final String KIND_MESSAGE_CATCH = "MESSAGE_CATCH";
    public static final String KIND_SEND_AND_WAIT = "SEND_AND_WAIT";
    public static final String KIND_BOUNDARY_MESSAGE = "BOUNDARY_MESSAGE";

    private static final String RESOLVED_TEMPLATE_FIELD = "resolvedTemplate";

    private final DeployedWebhookBindingRepository repository;
    private final ObjectMapper objectMapper;

    private final Object cacheLock = new Object();
    private final Map<String, List<DeployedWebhookBindingEntity>> cache =
            new LinkedHashMap<>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, List<DeployedWebhookBindingEntity>> eldest) {
                    return size() > CACHE_MAX_KEYS;
                }
            };

    /**
     * Read-only pre-deploy check ensuring the incoming graph does not push any {@code webhookKeyRef}
     * past {@link #MAX_MESSAGE_START_BINDINGS_PER_KEY} active {@code MESSAGE_START} bindings.
     */
    @Transactional(readOnly = true)
    public void validateStartCap(String workflowId, String namespaceKey, List<NodeDto> nodes) {
        if (StringUtils.isBlank(workflowId)) {
            return;
        }
        String wfId = workflowId.trim();
        String ns = namespaceKey == null ? "" : namespaceKey.trim();
        List<DeployedWebhookBindingEntity> nextBindings =
                extractBindings(wfId, ns, null, nodes, OffsetDateTime.now(ZoneOffset.UTC));
        assertStartCap(wfId, ns, nextBindings);
    }

    private void assertStartCap(String wfId, String ns, List<DeployedWebhookBindingEntity> nextBindings) {
        Map<String, Integer> newStartsByKey = new HashMap<>();
        for (DeployedWebhookBindingEntity b : nextBindings) {
            if (KIND_MESSAGE_START.equals(b.getBindingKind())) {
                newStartsByKey.merge(b.getWebhookKeyRef(), 1, Integer::sum);
            }
        }
        for (Map.Entry<String, Integer> entry : newStartsByKey.entrySet()) {
            String keyRef = entry.getKey();
            int incomingStarts = entry.getValue();
            long existingOtherStarts = repository.findByWebhookKeyRef(keyRef).stream()
                    .filter(existing -> KIND_MESSAGE_START.equals(existing.getBindingKind()))
                    .filter(existing -> !(wfId.equals(existing.getWorkflowId())
                            && ns.equals(existing.getNamespaceKey())))
                    .count();
            if (existingOtherStarts + incomingStarts > MAX_MESSAGE_START_BINDINGS_PER_KEY) {
                throw new IllegalArgumentException("webhook key " + keyRef + " exceeds maximum of "
                        + MAX_MESSAGE_START_BINDINGS_PER_KEY + " active MESSAGE_START bindings (existing="
                        + existingOtherStarts + ", new=" + incomingStarts + ")");
            }
        }
    }

    /**
     * Atomically replaces all webhook bindings owned by {@code (workflowId, namespaceKey)}.
     * Rejects deploys that would push any {@code webhookKeyRef} past
     * {@link #MAX_MESSAGE_START_BINDINGS_PER_KEY} active {@code MESSAGE_START} bindings.
     */
    @Transactional
    public int replaceForWorkflow(String workflowId, String namespaceKey,
                                  String processDefinitionId, List<NodeDto> nodes) {
        if (StringUtils.isBlank(workflowId)) {
            return 0;
        }
        String wfId = workflowId.trim();
        String ns = namespaceKey == null ? "" : namespaceKey.trim();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        List<DeployedWebhookBindingEntity> nextBindings = extractBindings(wfId, ns, processDefinitionId, nodes, now);

        // Enforce max 20 active MESSAGE_START bindings per webhookKeyRef BEFORE mutating.
        assertStartCap(wfId, ns, nextBindings);

        // Collect all affected keys for cache invalidation (both previous and new).
        Set<String> affectedKeys = new LinkedHashSet<>();
        List<DeployedWebhookBindingEntity> prior = repository.findByWorkflowIdAndNamespaceKey(wfId, ns);
        for (DeployedWebhookBindingEntity old : prior) {
            affectedKeys.add(old.getWebhookKeyRef());
        }
        for (DeployedWebhookBindingEntity b : nextBindings) {
            affectedKeys.add(b.getWebhookKeyRef());
        }

        repository.deleteByWorkflowIdAndNamespaceKey(wfId, ns);
        repository.flush();
        if (!nextBindings.isEmpty()) {
            repository.saveAll(nextBindings);
        }
        evictFromCache(affectedKeys);

        log.info("webhook-bindings.replaced workflowId={} namespace='{}' count={}", wfId, ns, nextBindings.size());
        return nextBindings.size();
    }

    /** Removes every webhook binding belonging to {@code workflowId} across all namespaces. */
    @Transactional
    public long deleteForWorkflow(String workflowId) {
        if (StringUtils.isBlank(workflowId)) {
            return 0L;
        }
        String wfId = workflowId.trim();
        Set<String> affectedKeys = new LinkedHashSet<>();
        for (DeployedWebhookBindingEntity old : repository.findByWorkflowId(wfId)) {
            affectedKeys.add(old.getWebhookKeyRef());
        }
        long deleted = repository.deleteByWorkflowId(wfId);
        evictFromCache(affectedKeys);
        if (deleted > 0) {
            log.info("webhook-bindings.deleted workflowId={} count={}", wfId, deleted);
        }
        return deleted;
    }

    /** Returns all active deployed bindings attached to {@code webhookKeyRef} (bounded LRU cached). */
    @Transactional(readOnly = true)
    public List<DeployedWebhookBindingEntity> findByWebhookKeyRef(String webhookKeyRef) {
        if (StringUtils.isBlank(webhookKeyRef)) {
            return List.of();
        }
        String key = webhookKeyRef.trim();
        synchronized (cacheLock) {
            List<DeployedWebhookBindingEntity> hit = cache.get(key);
            if (hit != null) {
                return hit;
            }
        }
        List<DeployedWebhookBindingEntity> loaded = List.copyOf(repository.findByWebhookKeyRef(key));
        synchronized (cacheLock) {
            cache.put(key, loaded);
        }
        return loaded;
    }

    /** Visible for tests: current number of cached webhookKeyRef buckets. */
    public int cachedKeyCount() {
        synchronized (cacheLock) {
            return cache.size();
        }
    }

    /** Clears the in-memory lookup cache (e.g. after external DB changes or in tests). */
    public void clearCache() {
        synchronized (cacheLock) {
            cache.clear();
        }
    }

    private void evictFromCache(Set<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        synchronized (cacheLock) {
            for (String k : keys) {
                cache.remove(k);
            }
        }
    }

    private List<DeployedWebhookBindingEntity> extractBindings(String workflowId, String ns,
                                                               String processDefinitionId,
                                                               List<NodeDto> nodes, OffsetDateTime now) {
        List<DeployedWebhookBindingEntity> out = new ArrayList<>();
        if (nodes == null || nodes.isEmpty()) {
            return out;
        }
        for (NodeDto node : nodes) {
            if (node == null || StringUtils.isBlank(node.getId()) || node.getData() == null) {
                continue;
            }
            TemplateDto data = node.getData();

            // 1) Node-level parameter-driven ingress roles (LISTEN / MESSAGE_CATCH / SEND_AND_WAIT).
            SelectOptionRole role = SelectOptionParser.resolveActiveRole(data);
            if (role.isIngressRole()) {
                Set<String> nodeKeyRefs = extractDirectNodeWebhookRefs(data);
                if (!nodeKeyRefs.isEmpty()) {
                    String kind = bindingKindForRole(role);
                    String messageName = WebhookMessageNames.forNode(workflowId, node.getId(), ns);
                    String templateId = StringUtils.defaultIfBlank(data.getId(), "node:" + node.getId());
                    String paramsHash = computeParamsHash(templateId, role.name(), data.getConfigurationParameters());
                    String targetJson = writeJson(data);
                    for (String keyRef : nodeKeyRefs) {
                        String bindingId = buildBindingId(workflowId, ns, node.getId(), null, keyRef);
                        out.add(DeployedWebhookBindingEntity.builder()
                                .bindingId(bindingId)
                                .webhookKeyRef(keyRef)
                                .workflowId(workflowId)
                                .namespaceKey(ns)
                                .processDefinitionId(processDefinitionId)
                                .nodeId(node.getId())
                                .boundaryId(null)
                                .bindingKind(kind)
                                .messageName(messageName)
                                .templateId(templateId)
                                .paramsHash(paramsHash)
                                .targetNodeJson(targetJson)
                                .cdate(now)
                                .udate(now)
                                .build());
                    }
                }
            }

            // 2) Boundary-event INGRESS_NODE bindings.
            List<BoundaryEventConfig> boundaries = data.getBoundaryEventConfigs();
            if (boundaries != null) {
                for (BoundaryEventConfig cfg : boundaries) {
                    if (cfg == null || !cfg.isIngressNodeSource() || StringUtils.isBlank(cfg.getId())) {
                        continue;
                    }
                    Map<String, Object> ingressBinding = cfg.getIngressNodeBinding();
                    Set<String> boundaryRefs = WebhookKeyRefHelper.collectFromIngressBinding(ingressBinding);
                    if (boundaryRefs.isEmpty()) {
                        continue;
                    }
                    TemplateDto effectiveTemplate = resolveBoundaryTemplate(ingressBinding);
                    String messageName = ActivitiBoundaryEventHelper.resolveMessageName(
                            workflowId, ns, node.getId(), cfg);
                    String templateId = effectiveTemplate != null && StringUtils.isNotBlank(effectiveTemplate.getId())
                            ? effectiveTemplate.getId()
                            : stringVal(ingressBinding.get("templateId"));
                    if (StringUtils.isBlank(templateId)) {
                        templateId = "boundary:" + node.getId() + ":" + cfg.getId();
                    }
                    String paramsHash = computeParamsHash(templateId, SelectOptionRole.LISTEN.name(),
                            effectiveTemplate == null ? List.of() : effectiveTemplate.getConfigurationParameters());
                    String targetJson = writeJson(effectiveTemplate);
                    for (String keyRef : boundaryRefs) {
                        String bindingId = buildBindingId(workflowId, ns, node.getId(), cfg.getId(), keyRef);
                        out.add(DeployedWebhookBindingEntity.builder()
                                .bindingId(bindingId)
                                .webhookKeyRef(keyRef)
                                .workflowId(workflowId)
                                .namespaceKey(ns)
                                .processDefinitionId(processDefinitionId)
                                .nodeId(node.getId())
                                .boundaryId(cfg.getId())
                                .bindingKind(KIND_BOUNDARY_MESSAGE)
                                .messageName(messageName)
                                .templateId(templateId)
                                .paramsHash(paramsHash)
                                .targetNodeJson(targetJson)
                                .cdate(now)
                                .udate(now)
                                .build());
                    }
                }
            }
        }
        return out;
    }

    private static Set<String> extractDirectNodeWebhookRefs(TemplateDto data) {
        Set<String> refs = new LinkedHashSet<>();
        if (data == null || data.getConfigurationParameters() == null) {
            return refs;
        }
        for (ParameterDto param : data.getConfigurationParameters()) {
            if (param == null) {
                continue;
            }
            WebhookKeyRefHelper.extractWebhookKeyRef(param.getValue()).ifPresent(refs::add);
        }
        return refs;
    }

    private static String bindingKindForRole(SelectOptionRole role) {
        return switch (role) {
            case LISTEN -> KIND_MESSAGE_START;
            case MESSAGE_CATCH -> KIND_MESSAGE_CATCH;
            case SEND_AND_WAIT -> KIND_SEND_AND_WAIT;
            default -> KIND_MESSAGE_START;
        };
    }

    /**
     * Hydrates the effective {@link TemplateDto} for an {@code INGRESS_NODE} boundary by reading
     * {@code ingressNodeBinding.resolvedTemplate} (shipped in the deploy snapshot) and merging the
     * boundary's per-instance {@code configurationParameters} overrides on top.
     */
    @SuppressWarnings("unchecked")
    private TemplateDto resolveBoundaryTemplate(Map<String, Object> ingressBinding) {
        if (ingressBinding == null) {
            return null;
        }
        TemplateDto template = null;
        Object rawResolved = ingressBinding.get(RESOLVED_TEMPLATE_FIELD);
        if (rawResolved instanceof TemplateDto dto) {
            template = dto.clone();
        } else if (rawResolved != null) {
            try {
                template = objectMapper.convertValue(rawResolved, TemplateDto.class);
            } catch (Exception ex) {
                log.warn("webhook-bindings: failed converting resolvedTemplate: {}", ex.getMessage());
            }
        }
        if (template == null) {
            template = new TemplateDto();
            template.setId(stringVal(ingressBinding.get("templateId")));
            template.setVersion(stringVal(ingressBinding.get("templateVersion")));
        }

        // Apply per-instance configurationParameter overrides from ingressNodeBinding.
        Map<String, Object> overrideByShortCode = new LinkedHashMap<>();
        Object rawParams = ingressBinding.get("configurationParameters");
        if (rawParams instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof ParameterDto p && StringUtils.isNotBlank(p.getShortCode())) {
                    overrideByShortCode.put(p.getShortCode(), p.getValue());
                } else if (item instanceof Map<?, ?> m) {
                    String sc = stringVal(m.get("shortCode"));
                    if (StringUtils.isNotBlank(sc)) {
                        overrideByShortCode.put(sc, m.get("value"));
                    }
                }
            }
        }
        // Ensure the listen select parameter is explicitly pinned to listenParamValue.
        String listenShortCode = stringVal(ingressBinding.get("listenParamShortCode"));
        String listenValue = stringVal(ingressBinding.get("listenParamValue"));
        if (StringUtils.isNotBlank(listenShortCode) && StringUtils.isNotBlank(listenValue)) {
            overrideByShortCode.put(listenShortCode, listenValue);
        }

        List<ParameterDto> merged = template.getConfigurationParameters() == null
                ? new ArrayList<>()
                : new ArrayList<>(template.getConfigurationParameters());
        Set<String> seen = new LinkedHashSet<>();
        for (ParameterDto param : merged) {
            if (param != null && StringUtils.isNotBlank(param.getShortCode())) {
                seen.add(param.getShortCode());
                if (overrideByShortCode.containsKey(param.getShortCode())) {
                    param.setValue(overrideByShortCode.get(param.getShortCode()));
                }
            }
        }
        for (Map.Entry<String, Object> entry : overrideByShortCode.entrySet()) {
            if (!seen.contains(entry.getKey())) {
                ParameterDto added = new ParameterDto();
                added.setShortCode(entry.getKey());
                added.setValue(entry.getValue());
                if (WebhookKeyRefHelper.isWebhookKeyRef(entry.getValue())) {
                    added.setType(WebhookKeyRefHelper.PARAM_TYPE_WEBHOOK_KEY);
                }
                merged.add(added);
            }
        }
        template.setConfigurationParameters(merged);
        return template;
    }

    private String computeParamsHash(String templateId, String role, List<ParameterDto> params) {
        try {
            List<Map<String, Object>> canonicalParams = new ArrayList<>();
            if (params != null) {
                for (ParameterDto p : params) {
                    if (p == null || StringUtils.isBlank(p.getShortCode())) {
                        continue;
                    }
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("shortCode", p.getShortCode());
                    row.put("type", p.getType());
                    row.put("value", p.getValue());
                    canonicalParams.add(row);
                }
            }
            canonicalParams.sort(Comparator.comparing(m -> String.valueOf(m.get("shortCode"))));
            String material = templateId + "|" + role + "|" + objectMapper.writeValueAsString(canonicalParams);
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(material.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception ex) {
            return "unhashed_" + Integer.toHexString(templateId.hashCode());
        }
    }

    private static String buildBindingId(String workflowId, String ns, String nodeId,
                                         String boundaryId, String webhookKeyRef) {
        String raw = workflowId + "|" + ns + "|" + nodeId + "|"
                + (boundaryId == null ? "" : boundaryId) + "|" + webhookKeyRef;
        if (raw.length() <= 191) {
            return raw;
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(raw.getBytes(StandardCharsets.UTF_8));
            return "wb_" + HexFormat.of().formatHex(digest);
        } catch (Exception impossible) {
            return raw.substring(0, 191);
        }
    }

    private String writeJson(Object obj) {
        if (obj == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception ex) {
            log.warn("webhook-bindings: failed serializing template snapshot: {}", ex.getMessage());
            return null;
        }
    }

    private static String stringVal(Object v) {
        return v == null ? null : String.valueOf(v).trim();
    }
}
