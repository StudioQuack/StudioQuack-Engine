package com.studioquack.sqs.execution.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The operator input-override store — one sealed row per (run, node), written by
 * {@code apply-inputs} and read at every dispatch of that node in that run so the operator's
 * edited parameters win on EVERY re-run path:
 * <ul>
 *   <li><b>Retry</b> — the held {@code hold_<node>} job requeues and the node re-dispatches in
 *       the same instance; the dispatch merge folds this row LAST.</li>
 *   <li><b>Replay</b> — the new process instance runs under the SAME {@code workflowExecutionId}
 *       business key, so the (run, node) key still resolves even though every
 *       {@code execution_context} row is freshly minted.</li>
 *   <li><b>Skip &amp; Continue</b> — no sandbox runs, but the SKIPPED sandbox-log row mirrors
 *       these edited inputs instead of nothing.</li>
 * </ul>
 * The same row also seals the operator's OUTPUT override (DuckTales output edit). On Skip &amp;
 * Continue those outputs become the skipped node's outputs; on a successful Retry / Replay each
 * edited port's saved message replaces what the node produced for it. Either way they fan out to
 * the next node(s) exactly as if the node had produced them. The store/class names keep the original
 * "input override" wording — the row now holds inputs and outputs.
 *
 * <p><b>One store for every tier</b> — plain Spring Data JPA on the same DB-agnostic entity
 * mapping as {@code execution_context} on H2. The
 * sealed envelope uses the same {@link ExecutionContextCrypto} (AAD = {@code run|node}), so an
 * override row is exactly as protected at rest as an {@code execution_context} row. This class
 * is the single owner of the repository: no caller ever sees ciphertext, and override VALUES are
 * never logged (PHI) — only the (run, node, count) shape.
 */
@Slf4j
@Service
public class OperatorInputOverrideStore {

    private final OperatorInputOverrideRepository repository;
    private final ObjectMapper objectMapper;
    private final ExecutionContextCrypto crypto;

    public OperatorInputOverrideStore(OperatorInputOverrideRepository repository,
                                      ObjectMapper objectMapper,
                                      ExecutionContextCrypto crypto) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.crypto = crypto;
    }

    /**
     * Seals and stores {@code inputs} as THE override for {@code (workflowExecutionId, nodeId)};
     * a later call overwrites an earlier one (latest wins). An EMPTY list is a valid override —
     * the operator cleared the node's inbound list, and the dispatch merge honours the clear.
     */
    public void saveOverride(String workflowExecutionId, String nodeId, List<InOutValueDto> inputs) {
        if (StringUtils.isBlank(workflowExecutionId) || StringUtils.isBlank(nodeId) || inputs == null) {
            throw new IllegalArgumentException("workflowExecutionId / nodeId / inputs must not be null/blank");
        }
        OperatorInputOverrideKey key = new OperatorInputOverrideKey(workflowExecutionId, nodeId);
        OperatorInputOverrideEntity entity = repository.findById(key)
                .orElse(OperatorInputOverrideEntity.builder()
                        .workflowExecutionId(workflowExecutionId)
                        .nodeId(nodeId)
                        .cdate(OffsetDateTime.now())
                        .build());
        entity.setInputsJson(sealListToJson(inputs, aad(workflowExecutionId, nodeId)));
        entity.setUdate(OffsetDateTime.now());
        repository.save(entity);
        log.info("operator-override.saved run={} node={} inputCount={}",
                workflowExecutionId, nodeId, inputs.size());
    }

    /**
     * Seals and stores {@code outputs} as THE output override for
     * {@code (workflowExecutionId, nodeId)} — the DuckTales edit of the failed node's OUTPUT
     * parameters: the skipped node's outputs on Skip &amp; Continue, and the edited ports' messages
     * after a successful Retry / Replay. Same latest-wins row as the inputs: writing this lane
     * keeps any previously stored inputs untouched (merge at the row level).
     */
    public void saveOutputsOverride(String workflowExecutionId, String nodeId,
                                    List<InOutValueDto> outputs) {
        if (StringUtils.isBlank(workflowExecutionId) || StringUtils.isBlank(nodeId) || outputs == null) {
            throw new IllegalArgumentException("workflowExecutionId / nodeId / outputs must not be null/blank");
        }
        OperatorInputOverrideKey key = new OperatorInputOverrideKey(workflowExecutionId, nodeId);
        OperatorInputOverrideEntity entity = repository.findById(key)
                .orElse(OperatorInputOverrideEntity.builder()
                        .workflowExecutionId(workflowExecutionId)
                        .nodeId(nodeId)
                        .cdate(OffsetDateTime.now())
                        .build());
        entity.setOutputsJson(sealListToJson(outputs, aadOut(workflowExecutionId, nodeId)));
        entity.setUdate(OffsetDateTime.now());
        repository.save(entity);
        log.info("operator-override.saved-outputs run={} node={} outputCount={}",
                workflowExecutionId, nodeId, outputs.size());
    }

    /**
     * The override for {@code (workflowExecutionId, nodeId)}, opened (never ciphertext). Empty
     * when no override exists; a PRESENT empty list means the operator cleared the node's inputs.
     */
    public Optional<List<InOutValueDto>> loadOverride(String workflowExecutionId, String nodeId) {
        if (StringUtils.isBlank(workflowExecutionId) || StringUtils.isBlank(nodeId)) {
            return Optional.empty();
        }
        return repository.findById(new OperatorInputOverrideKey(workflowExecutionId, nodeId))
                // An OUTPUTS-only row must NOT read back as "operator cleared the inputs".
                .filter(e -> StringUtils.isNotBlank(e.getInputsJson()))
                .map(this::openInputs);
    }

    /**
     * The OUTPUT override for {@code (workflowExecutionId, nodeId)}, opened (never ciphertext);
     * empty when no row exists OR the row carries inputs only. Consumed by the Skip &amp;
     * Continue short-circuit only.
     */
    public Optional<List<InOutValueDto>> loadOutputOverride(String workflowExecutionId, String nodeId) {
        if (StringUtils.isBlank(workflowExecutionId) || StringUtils.isBlank(nodeId)) {
            return Optional.empty();
        }
        return repository.findById(new OperatorInputOverrideKey(workflowExecutionId, nodeId))
                .filter(e -> StringUtils.isNotBlank(e.getOutputsJson()))
                .map(this::openOutputs);
    }

    /** Retention sweep hook — same guarded batch as the run's {@code execution_context} purge. */
    public long deleteOverridesForRuns(List<String> workflowExecutionIds) {
        if (workflowExecutionIds == null || workflowExecutionIds.isEmpty()) {
            return 0;
        }
        return repository.deleteByWorkflowExecutionIdIn(workflowExecutionIds);
    }

    /* --------------------------------------------------------------- */
    /* Seal / open + JSON (de)serialisation helpers (mirror the        */
    /* execution_context stores — the PHI rules are identical)         */
    /* --------------------------------------------------------------- */

    /** AAD binds the ciphertext to exactly this (run, node) — a spliced row refuses to open. */
    private static String aad(String workflowExecutionId, String nodeId) {
        return workflowExecutionId + "|" + nodeId;
    }

    /**
     * The OUTPUTS lane seals under its own AAD ({@code run|node|out}) so an outputs blob moved
     * into the inputs column (or vice versa) fails authentication instead of opening as garbage.
     */
    private static String aadOut(String workflowExecutionId, String nodeId) {
        return aad(workflowExecutionId, nodeId) + "|out";
    }

    private String sealListToJson(List<InOutValueDto> values, String aad) {
        List<Map<String, Object>> sealed = crypto.sealList(toMaps(values), aad);
        return writeJson(sealed);
    }

    private List<InOutValueDto> openInputs(OperatorInputOverrideEntity entity) {
        List<Map<String, Object>> stored = readListMaps(entity.getInputsJson());
        List<Map<String, Object>> opened = crypto.openList(stored, aad(
                entity.getWorkflowExecutionId(), entity.getNodeId()));
        if (opened == null) {
            return new ArrayList<>();
        }
        return objectMapper.convertValue(opened, new TypeReference<List<InOutValueDto>>() {});
    }

    private List<InOutValueDto> openOutputs(OperatorInputOverrideEntity entity) {
        List<Map<String, Object>> stored = readListMaps(entity.getOutputsJson());
        List<Map<String, Object>> opened = crypto.openList(stored, aadOut(
                entity.getWorkflowExecutionId(), entity.getNodeId()));
        if (opened == null) {
            return new ArrayList<>();
        }
        return objectMapper.convertValue(opened, new TypeReference<List<InOutValueDto>>() {});
    }

    private List<Map<String, Object>> toMaps(List<InOutValueDto> values) {
        if (values == null || values.isEmpty()) {
            return new ArrayList<>();
        }
        return objectMapper.convertValue(values, new TypeReference<List<Map<String, Object>>>() {});
    }

    private String writeJson(Object o) {
        if (o == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("operator_input_override JSON serialise failed: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> readListMaps(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            throw new IllegalStateException(
                    "operator_input_override JSON parse (list) failed: " + e.getMessage(), e);
        }
    }
}
