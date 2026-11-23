package com.studioquack.sqs.runtime;

import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.runtime.ProcessInstance;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Replays a workflow run against the <b>latest</b> deployed definition, using process-instance
 * identity (the stable {@code workflowExecutionId} business key), never an incident key (* invariant). The seed variables are reconstructed from the encrypted {@code execution_context} via
 * {@link ReplaySeedRefGuard}, which fails fast (deterministic repair error) when the seed ref/context
 * is unrecoverable — so a replay can never start the latest process with an empty variable map.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WorkflowReplayService {

    private final ReplaySeedRefGuard seedGuard;
    private final SqsEngine sqsEngine;

    /**
     * Reconstructs the REF-ONLY seed and starts the latest version of {@code processDefinitionKey}
     * with {@code businessKey == workflowExecutionId}.
     *
     * @throws ReplaySeedRefGuard.SeedRefUnrecoverableException when the seed ref/context is unrecoverable
     */
    public ProcessInstance replay(String processDefinitionKey, String workflowExecutionId,
                                  ProcessContextRef seedRef) {
        Map<String, Object> seed = seedGuard.reconstructSeedVariables(seedRef); // fail-fast if unrecoverable
        log.info("workflow.replay processKey={} wfeId={} seedKeys={}",
                processDefinitionKey, workflowExecutionId, seed.keySet());
        return sqsEngine.startLatest(processDefinitionKey, workflowExecutionId, seed);
    }
}
