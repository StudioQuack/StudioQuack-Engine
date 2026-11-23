package com.studioquack.sqs.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.runtime.WorkflowReplayService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.ProcessInstance;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Repair — {@code replay-pi}: cancels a run and restarts it from its reconstructed seed against the latest
 * deployed definition, keyed by process-instance identity (: the repair uses the process-instance id, never an
 * incidentKey). The seed is reconstructed vithe {@code ReplaySeedRefGuard} inside
 * {@link WorkflowReplayService#replay}, which fails fast when the sealed seed context is unrecoverable — so a
 * replay can never start the latest process with an empty variable map.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReplayPiCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (must be added to the control plane).
     */
    public static final String TYPE = "replay-pi";

    /**
     * The delete reason stamped on the instance a replay cancels. Activiti hands it to the
     * {@code PROCESS_CANCELLED} event as the cancel cause, so the run-status bridge keys off this exact constant
     * to NOT report a terminal CANCELED for a run the replay immediately restarts under the same business key.
     */
    public static final String REPLAY_DELETE_REASON = "replay-pi repair";

    private final ObjectMapper objectMapper;
    private final SqsEngine sqsEngine;
    private final WorkflowReplayService replayService;
    private final ExecutionContextStore contextStore;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        RepairCommandPayload payload;
        try {
            payload = objectMapper.readValue(command.payload(), RepairCommandPayload.class);
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed("replay-pi: malformed payload: " + message(parse));
        }
        if (payload == null || !payload.hasProcessInstance()) {
            return DaemonCommandOutcome.failed("replay-pi: payload requires processInstanceId");
        }

        // The BE may address the run by its workflowExecutionId (business key); map it to the live PI.
        String pid = sqsEngine.resolveProcessInstanceId(payload.processInstanceId());
        try {
            ProcessInstance instance = sqsEngine.findProcessInstance(pid);
            return instance != null
                    ? replayRunning(pid, instance)
                    : replayEnded(pid);
        } catch (Exception ex) {
            log.warn("repair.replay-pi.failed pi={}: {}", pid, ex.toString());
            return DaemonCommandOutcome.failed("replay-pi failed: " + message(ex));
        }
    }

    /**
     * Replays a RUNNING instance: resolve the seed, cancel, restart.
     */
    private DaemonCommandOutcome replayRunning(String pid, ProcessInstance instance) throws Exception {
        String workflowExecutionId = instance.getBusinessKey();
        String processKey = resolveProcessKey(instance);
        ProcessContextRef seedRef = resolveSeedRef(workflowExecutionId, () -> readSeedRef(pid));
        if (StringUtils.isBlank(workflowExecutionId) || StringUtils.isBlank(processKey)) {
            return DaemonCommandOutcome.failed(
                    "replay-pi: could not resolve businessKey/processKey for processInstanceId=" + pid);
        }

        // Cancel the old run, then replay from the reconstructed seed (guard fails fast if unrecoverable).
        sqsEngine.deleteProcessInstance(pid, REPLAY_DELETE_REASON);
        ProcessInstance replayed = replayService.replay(processKey, workflowExecutionId, seedRef);

        log.info("repair.replay-pi.done oldPi={} newPi={} wfeId={} processKey={}",
                pid, replayed.getId(), workflowExecutionId, processKey);
        return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                new ReplayResult(pid, replayed.getId(), workflowExecutionId, processKey)));
    }

    /**
     * Replays an ENDED run from its history rows — nothing is deleted (the run already ended). The seed comes from
     * the run's first {@code execution_context} row (which outlives the run); the HISTORIC {@code studioquackRef}
     * variable is only the fallback.
     */
    private DaemonCommandOutcome replayEnded(String idOrBusinessKey) throws Exception {
        HistoricProcessInstance historic = sqsEngine.findLatestHistoricProcessInstance(idOrBusinessKey);
        if (historic == null) {
            return DaemonCommandOutcome.failed(
                    "replay-pi: no process instance (running or ended) for processInstanceId="
                            + idOrBusinessKey);
        }
        String workflowExecutionId = historic.getBusinessKey();
        ProcessDefinition def = sqsEngine.findProcessDefinition(historic.getProcessDefinitionId());
        String processKey = def == null ? null : def.getKey();
        ProcessContextRef seedRef = resolveSeedRef(workflowExecutionId, () -> toRef(
                sqsEngine.getHistoricProcessVariable(historic.getId(), EngineVariableNames.STUDIOQUACK_REF)));
        if (StringUtils.isBlank(workflowExecutionId) || StringUtils.isBlank(processKey)) {
            return DaemonCommandOutcome.failed(
                    "replay-pi: could not resolve businessKey/processKey for processInstanceId="
                            + idOrBusinessKey);
        }

        ProcessInstance replayed = replayService.replay(processKey, workflowExecutionId, seedRef);

        log.info("repair.replay-pi.done oldPi={} (ended) newPi={} wfeId={} processKey={}",
                historic.getId(), replayed.getId(), workflowExecutionId, processKey);
        return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(
                new ReplayResult(historic.getId(), replayed.getId(), workflowExecutionId, processKey)));
    }

    /**
     * Resolves the process definition KEY (e.g. {@code Process_<workflowId>}) for the instance.
     */
    private String resolveProcessKey(ProcessInstance instance) {
        ProcessDefinition def = sqsEngine.findProcessDefinition(instance.getProcessDefinitionId());
        return def == null ? null : def.getKey();
    }

    /**
     * Resolves the ref the replay restarts from: the run's seed context — the first {@code execution_context} row
     * of the business key, minted at start-run. Reading the instance's CURRENT {@code studioquackRef} instead was
     * wrong on a run held at {@code hold_<X>}: that ref is X's failed-dispatch context, so the replayed trigger
     * would load X's context ({@code refNodeId = X ≠ trigger} → carried seed inputs dropped in the dispatch merge)
     * and the replay would not be a faithful restart.
     */
    private ProcessContextRef resolveSeedRef(String workflowExecutionId,
                                             Supplier<ProcessContextRef> legacyRead) {
        if (StringUtils.isNotBlank(workflowExecutionId)) {
            var seed = contextStore.seedRefOf(workflowExecutionId);
            if (seed.isPresent()) {
                return seed.get();
            }
        }
        log.warn("repair.replay-pi.seed-fallback wfeId={}: no seed execution_context row identifiable; "
                + "falling back to the instance's current studioquackRef", workflowExecutionId);
        return legacyRead.get();
    }

    /**
     * Reads the {@code studioquackRef} handle off the running instance (REF-ONLY) — the FALLBACK when no seed row
     * is identifiable. Activiti may hand back the deserialised {@link ProcessContextRef} or (if stored as JSON) a
     * Map — normalise both, exactly as {@code ScriptDispatchDelegate} does.
     */
    private ProcessContextRef readSeedRef(String pid) {
        return toRef(sqsEngine.getProcessVariable(pid, EngineVariableNames.STUDIOQUACK_REF));
    }

    /**
     * Normalises a {@code studioquackRef} variable value (runtime or historic): Activiti may hand back the
     * deserialised {@link ProcessContextRef} or (if stored as JSON) a Map.
     */
    private ProcessContextRef toRef(Object v) {
        if (v instanceof ProcessContextRef ref) {
            return ref;
        }
        if (v instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, ProcessContextRef.class);
        }
        return null; // The guard turns a null/unrecoverable ref into a deterministic repair failure
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }

    /**
     * Success output shipped to the BE — the NEW process-instance id (repairs stay pi-identity keyed).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record ReplayResult(String previousProcessInstanceId, String processInstanceId,
                        String workflowExecutionId, String processKey) {
    }
}
