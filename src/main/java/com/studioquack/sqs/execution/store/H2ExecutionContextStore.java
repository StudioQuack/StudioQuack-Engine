package com.studioquack.sqs.execution.store;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link ExecutionContextStore} on encrypted-file H2. Mints node ref ids, counts revisions,
 * seals rows with AES-GCM (AAD = nodeRefId), and dedups retries. Callers never see ciphertext.
 */
@Slf4j
@Service
public class H2ExecutionContextStore implements ExecutionContextStore {

    /** Wire-schema version stamped onto the emitted {@link ProcessContextRef}. */
    private static final int SCHEMA_VERSION = 2;
    /** Both the row id prefix and the ref prefix (control plane parity: {@code node_<NanoId>}). */
    private static final String REF_PREFIX = "node_";
    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_COMPLETED = "COMPLETED";

    /**
     * Jackson's stock 20 MB max-string cap rejects legitimately large context blobs at rehydrate
     * time — an agentic prompt carrying node-library search results amplifies across the blob's
     * input/output copies past 20 MB and the run dies with SANDBOX_FAILED even though the row was
     * written fine. Bounded headroom instead of unlimited so a true runaway still fails fast.
     */
    private static final int MAX_CONTEXT_STRING_LENGTH = 250_000_000;

    private final ExecutionContextRepository repository;
    private final ObjectMapper objectMapper;
    private final ExecutionContextCrypto crypto;
    private final SubPiInvocationRepository subPiInvocationRepository;

    /** Test / slice constructor — sub-PI index is a no-op when the repository is absent. */
    public H2ExecutionContextStore(ExecutionContextRepository repository,
                                   ObjectMapper objectMapper,
                                   ExecutionContextCrypto crypto) {
        this(repository, objectMapper, crypto, null);
    }

    @Autowired
    public H2ExecutionContextStore(ExecutionContextRepository repository,
                                   ObjectMapper objectMapper,
                                   ExecutionContextCrypto crypto,
                                   SubPiInvocationRepository subPiInvocationRepository) {
        this.repository = repository;
        // copy() gets its own JsonFactory, so the raised cap stays local to this store and the
        // app-wide mapper keeps the stock constraint.
        this.objectMapper = objectMapper.copy();
        this.objectMapper.getFactory().setStreamReadConstraints(StreamReadConstraints.builder()
                .maxStringLength(MAX_CONTEXT_STRING_LENGTH)
                .build());
        this.crypto = crypto;
        this.subPiInvocationRepository = subPiInvocationRepository;
    }

    @Override
    public boolean encryptionEnabled() {
        return crypto != null && crypto.enabled();
    }

    @Override
    public ProcessContextRef snapshot(ExecutionContext ctx, Long jobKey) {
        if (ctx == null) {
            throw new IllegalArgumentException("ExecutionContext must not be null");
        }
        String nodeRefId = REF_PREFIX + NanoIdUtils.randomNanoId();
        String executionId = ctx.getWorkflowExecutionId();
        String bpmnFlowNodeId = ctx.getCurrentNodeIdSafe();
        long revision = repository.countByExecutionIdAndBpmnFlowNodeId(executionId, bpmnFlowNodeId);

        List<InOutValueDto> inputList = ctx.getCurrentNode() == null
                ? null : ctx.getCurrentNode().getInputList();

        OffsetDateTime now = OffsetDateTime.now();
        ExecutionContextEntity entity = ExecutionContextEntity.builder()
                .nodeRefId(nodeRefId)
                .executionId(executionId)
                .workflowExecutionId(ctx.getWorkflowExecutionId())
                .bpmnFlowNodeId(bpmnFlowNodeId)
                .envId(ctx.getEnvId())
                .ownerId(ctx.getUserId())
                .workflowId(ctx.getWorkflowId())
                .revision(revision)
                .status(STATUS_RUNNING)
                .inputsJson(sealListToJson(inputList, nodeRefId))
                .outputsJson(sealListToJson(new ArrayList<>(), nodeRefId))
                .contextBlobJson(sealMapToJson(toContextBlob(ctx), nodeRefId))
                .jobKey(jobKey)
                .cdate(now)
                .udate(now)
                .build();
        repository.save(entity);
        log.debug("Snapshotted execution context: executionId={} nodeRefId={} revision={}",
                executionId, nodeRefId, revision);

        return ProcessContextRef.builder()
                .executionId(executionId)
                .workflowExecutionId(ctx.getWorkflowExecutionId())
                .currentNodeRefId(nodeRefId)
                .envId(ctx.getEnvId())
                .schemaVersion(SCHEMA_VERSION)
                .build();
    }

    @Override
    public Optional<ExecutionContext> loadByRef(ProcessContextRef ref) {
        if (ref == null || ref.getCurrentNodeRefId() == null) {
            return Optional.empty();
        }
        return repository.findByNodeRefId(ref.getCurrentNodeRefId()).map(this::rehydrate);
    }

    @Override
    public Optional<ProcessContextRef> seedRefOf(String executionId) {
        if (StringUtils.isBlank(executionId)) {
            return Optional.empty();
        }
        // The seed row is the run's first snapshot (start-run); every dispatch row of the same
        // run orders after it. REF-ONLY: only the tiny handle is rebuilt, the blob stays sealed.
        return repository.findFirstByExecutionIdOrderByCdateAscRevisionAsc(executionId)
                .map(row -> ProcessContextRef.builder()
                        .executionId(row.getExecutionId())
                        .workflowExecutionId(row.getWorkflowExecutionId())
                        .currentNodeRefId(row.getNodeRefId())
                        .envId(row.getEnvId())
                        .schemaVersion(SCHEMA_VERSION)
                        .build());
    }

    @Override
    public List<InOutValueDto> loadInputsTargetingNode(ProcessContextRef ref, String currentNodeId) {
        if (ref == null || StringUtils.isBlank(ref.getExecutionId()) || StringUtils.isBlank(currentNodeId)) {
            return new ArrayList<>();
        }
        // revision ASC → latest attempt's value wins per binding key (retry idempotency).
        List<ExecutionContextEntity> attempts =
                repository.findByExecutionIdOrderByRevisionAsc(ref.getExecutionId());
        if (attempts == null || attempts.isEmpty()) {
            return new ArrayList<>();
        }
        LinkedHashMap<String, InOutValueDto> deduped = new LinkedHashMap<>();
        for (ExecutionContextEntity doc : attempts) {
            List<InOutValueDto> dtos = openOutputs(doc);
            if (dtos.isEmpty()) {
                continue;
            }
            for (InOutValueDto dto : dtos) {
                String targetNodeId = dto.getTargetNodeId();
                if (targetNodeId == null || !targetNodeId.equalsIgnoreCase(currentNodeId)) {
                    continue;
                }
                // A node's own outputs can't be its own inputs. Judged on the PRODUCER, not the row
                // label: the dispatch chain saves a node's outputs onto the row its predecessor minted.
                if (currentNodeId.equalsIgnoreCase(producerOf(dto, doc))) {
                    continue;
                }
                deduped.put(bindingKey(dto), dto);
            }
        }
        return new ArrayList<>(deduped.values());
    }

    /**
     * The node that PRODUCED an output row. Every fan-out / sender writer stamps
     * {@link InOutValueDto#getNodeId()}; the row label ({@code bpmnFlowNodeId}) is only a fallback for
     * legacy rows without it. The two differ by design on the dispatch chain: a node's success path
     * mints the next hop's row while {@code currentNode} is still itself, and the NEXT node saves
     * its outputs onto that row — so the row labelled "A" carries B's outputs. Keying on the label
     * handed a pinned Add-Node tile for A the payload of A's successor (and merged two producers
     * into one bucket).
     */
    private static String producerOf(InOutValueDto dto, ExecutionContextEntity doc) {
        return StringUtils.defaultIfBlank(dto.getNodeId(), doc.getBpmnFlowNodeId());
    }

    @Override
    public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId) {
        return collectByProducer(executionId, null);
    }

    @Override
    public Map<String, List<InOutValueDto>> collectOutputsByNode(String executionId, Set<String> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return new LinkedHashMap<>();
        }
        return collectByProducer(executionId, nodeIds);
    }

    /**
     * Groups every completed attempt's outputs by {@link #producerOf producer}; {@code onlyNodeIds}
     * (nullable = all) keeps just those producers. Every row of the execution is opened: the row
     * label cannot pre-filter because a producer's outputs sit on the row its predecessor minted.
     * Rows are one per hop, so the scan stays O(nodes) per dispatch.
     */
    private Map<String, List<InOutValueDto>> collectByProducer(String executionId, Set<String> onlyNodeIds) {
        LinkedHashMap<String, List<InOutValueDto>> grouped = new LinkedHashMap<>();
        if (StringUtils.isBlank(executionId)) {
            return grouped;
        }
        List<ExecutionContextEntity> attempts = repository.findByExecutionIdOrderByRevisionAsc(executionId);
        if (attempts == null || attempts.isEmpty()) {
            return grouped;
        }
        Map<String, LinkedHashMap<String, InOutValueDto>> perNodeDedup = new LinkedHashMap<>();
        for (ExecutionContextEntity doc : attempts) {
            List<InOutValueDto> dtos = openOutputs(doc);
            if (dtos.isEmpty()) {
                continue;
            }
            for (InOutValueDto dto : dtos) {
                if (dto == null) {
                    continue;
                }
                String producer = producerOf(dto, doc);
                if (StringUtils.isBlank(producer) || (onlyNodeIds != null && !onlyNodeIds.contains(producer))) {
                    continue;
                }
                perNodeDedup.computeIfAbsent(producer, k -> new LinkedHashMap<>()).put(bindingKey(dto), dto);
            }
        }
        for (Map.Entry<String, LinkedHashMap<String, InOutValueDto>> e : perNodeDedup.entrySet()) {
            grouped.put(e.getKey(), new ArrayList<>(e.getValue().values()));
        }
        return grouped;
    }

    @Override
    public void saveOutputs(ProcessContextRef ref, List<InOutValueDto> outputs) {
        if (ref == null || ref.getCurrentNodeRefId() == null) {
            return;
        }
        repository.findByNodeRefId(ref.getCurrentNodeRefId()).ifPresent(doc -> {
            doc.setOutputsJson(sealListToJson(outputs, doc.getNodeRefId()));
            doc.setStatus(STATUS_COMPLETED);
            doc.setUdate(OffsetDateTime.now());
            repository.save(doc);
        });
    }

    @Override
    public void recordSubPiInvocation(String parentExecutionId, String childExecutionId,
                                      String callerNodeId, String subPiRootNodeId) {
        if (subPiInvocationRepository == null
                || StringUtils.isBlank(parentExecutionId)
                || StringUtils.isBlank(childExecutionId)
                || StringUtils.isBlank(callerNodeId)
                || StringUtils.isBlank(subPiRootNodeId)) {
            return;
        }
        SubPiInvocationEntity entity = SubPiInvocationEntity.builder()
                .parentExecutionId(parentExecutionId)
                .childExecutionId(childExecutionId)
                .callerNodeId(callerNodeId)
                .subPiRootNodeId(subPiRootNodeId)
                .cdate(OffsetDateTime.now())
                .build();
        subPiInvocationRepository.save(entity);
    }

    @Override
    public List<SubPiInvocation> subPiInvocationsOf(String parentExecutionId) {
        if (subPiInvocationRepository == null || StringUtils.isBlank(parentExecutionId)) {
            return List.of();
        }
        List<SubPiInvocationEntity> rows =
                subPiInvocationRepository.findByParentExecutionIdOrderByIdAsc(parentExecutionId);
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }
        List<SubPiInvocation> out = new ArrayList<>(rows.size());
        for (SubPiInvocationEntity row : rows) {
            out.add(new SubPiInvocation(
                    row.getChildExecutionId(), row.getCallerNodeId(), row.getSubPiRootNodeId()));
        }
        return out;
    }

    /* --------------------------------------------------------------- */
    /* Seal / open + JSON (de)serialisation helpers                     */
    /* --------------------------------------------------------------- */

    private String sealListToJson(List<InOutValueDto> values, String aad) {
        List<Map<String, Object>> sealed = crypto.sealList(toMaps(values), aad);
        return writeJson(sealed);
    }

    private String sealMapToJson(Map<String, Object> map, String aad) {
        Map<String, Object> sealed = crypto.sealMap(map, aad);
        return writeJson(sealed);
    }

    private List<InOutValueDto> openOutputs(ExecutionContextEntity doc) {
        List<Map<String, Object>> stored = readListMaps(doc.getOutputsJson());
        List<Map<String, Object>> opened = crypto.openList(stored, doc.getNodeRefId());
        if (opened == null || opened.isEmpty()) {
            return new ArrayList<>();
        }
        return objectMapper.convertValue(opened, new TypeReference<List<InOutValueDto>>() {});
    }

    private ExecutionContext rehydrate(ExecutionContextEntity doc) {
        Map<String, Object> contextBlob = crypto.openMap(readMap(doc.getContextBlobJson()), doc.getNodeRefId());
        List<Map<String, Object>> inputs = crypto.openList(readListMaps(doc.getInputsJson()), doc.getNodeRefId());

        ExecutionContext ctx;
        if (contextBlob != null && !contextBlob.isEmpty()) {
            try {
                ctx = objectMapper.convertValue(contextBlob, ExecutionContext.class);
            } catch (Exception e) {
                log.warn("Failed to deserialise contextBlob for nodeRefId={}; narrow rehydrate: {}",
                        doc.getNodeRefId(), e.getMessage());
                ctx = narrowRehydrate(doc);
            }
        } else {
            ctx = narrowRehydrate(doc);
        }
        if (inputs != null && !inputs.isEmpty()) {
            ctx.setNextNodeInputList(objectMapper.convertValue(
                    inputs, new TypeReference<List<InOutValueDto>>() {}));
        }
        return ctx;
    }

    private ExecutionContext narrowRehydrate(ExecutionContextEntity doc) {
        ExecutionContext ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId(doc.getWorkflowExecutionId());
        ctx.setEnvId(doc.getEnvId());
        ctx.setCurrentNodeId(doc.getBpmnFlowNodeId());
        ctx.setWorkflowId(doc.getWorkflowId());
        ctx.setUserId(doc.getOwnerId());
        return ctx;
    }

    private List<Map<String, Object>> toMaps(List<InOutValueDto> values) {
        if (values == null || values.isEmpty()) {
            return new ArrayList<>();
        }
        return objectMapper.convertValue(values, new TypeReference<List<Map<String, Object>>>() {});
    }

    private Map<String, Object> toContextBlob(ExecutionContext ctx) {
        if (ctx == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(ctx, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.warn("Failed to serialise ExecutionContext for ref store; empty blob: {}", e.getMessage());
            return null;
        }
    }

    private String writeJson(Object o) {
        if (o == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("execution_context JSON serialise failed: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> readListMaps(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            throw new IllegalStateException("execution_context JSON parse (list) failed: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> readMap(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {});
        } catch (Exception e) {
            throw new IllegalStateException("execution_context JSON parse (map) failed: " + e.getMessage(), e);
        }
    }

    /**
     * Stable identity for an input/output wire — collisions across retry attempts resolve
     * to the same key so the loader keeps the latest value only. {@code value} is
     * deliberately excluded (retrying refreshes the value for the same wire).
     */
    private static String bindingKey(InOutValueDto dto) {
        return dto.getNodeId() + "|" + dto.getPortId() + "|" + dto.getPortValue() + "|"
                + dto.getTargetNodeId() + "|" + dto.getTargetPortId();
    }
}
