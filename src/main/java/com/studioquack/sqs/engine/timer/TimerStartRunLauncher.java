package com.studioquack.sqs.engine.timer;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.engine.runtime.BeRuntimeEpoch;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVariableGuard;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.engine.ActivitiException;
import org.activiti.engine.delegate.event.ActivitiEventType;
import org.activiti.engine.delegate.event.impl.ActivitiEventBuilder;
import org.activiti.engine.impl.ProcessInstanceCreationOptions;
import org.activiti.engine.impl.cmd.StartProcessInstanceCmd;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.jobexecutor.TimerEventHandler;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.activiti.engine.impl.persistence.entity.ProcessDefinitionEntity;
import org.activiti.engine.impl.util.ProcessDefinitionUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Starts the process instance for one timer-start fire as a first-class run. Activiti's own
 * {@code TimerStartEventJobHandler} starts every fire without a businessKey, so a timer-started run had
 * no run id: no run-index row, no node logs and no context for its first dispatchable node. Each fire
 * here mints its own {@code tm_…} run id as the businessKey (the second, third … fire of a cycle each
 * get a new one) and seeds the graph-hydrated {@code studioquackRef} exactly as a message start does,
 * so the run-status bridge reports it RUNNING and the control plane creates its Monitoring row.
 *
 * <p>Otherwise the same as Activiti's handler (re-read definition, suspended skip, {@code TIMER_FIRED},
 * start at the timer's own start event); the repeating schedule lives on the job, not here.
 */
@Slf4j
@Component
public class TimerStartRunLauncher {

    /** Activity id prefix the strategies mint: designer node {@code X} → element {@code node_X}. */
    private static final String NODE_ELEMENT_PREFIX = "node_";

    // ObjectProvider: the launcher is wired into the engine configuration through the timer job
    // handler, so eager store/epoch dependencies would cycle the context.
    private final ObjectProvider<ExecutionContextStore> executionContextStore;
    private final ObjectProvider<DeployedArtifactStore> artifactStore;
    private final ObjectProvider<BeRuntimeEpoch> beRuntimeEpoch;
    private final String ownEnvId;

    public TimerStartRunLauncher(ObjectProvider<ExecutionContextStore> executionContextStore,
                                 ObjectProvider<DeployedArtifactStore> artifactStore,
                                 ObjectProvider<BeRuntimeEpoch> beRuntimeEpoch,
                                 @Value("${sqs.channel.envId:}") String ownEnvId) {
        this.executionContextStore = executionContextStore;
        this.artifactStore = artifactStore;
        this.beRuntimeEpoch = beRuntimeEpoch;
        this.ownEnvId = ownEnvId;
    }

    public void start(JobEntity job, String configuration, CommandContext commandContext) {
        ProcessDefinitionEntity definition = ProcessDefinitionUtil.getProcessDefinitionFromDatabase(
                job.getProcessDefinitionId());
        if (definition == null) {
            throw new ActivitiException("Could not find process definition needed for timer start event");
        }
        if (definition.isSuspended()) {
            log.debug("timer-start: ignoring timer of suspended process definition {}", definition.getKey());
            return;
        }
        if (commandContext.getEventDispatcher().isEnabled()) {
            commandContext.getEventDispatcher()
                    .dispatchEvent(ActivitiEventBuilder.createEntityEvent(ActivitiEventType.TIMER_FIRED, job));
        }
        String runId = "tm_" + NanoIdUtils.randomNanoId();
        String activityId = TimerEventHandler.getActivityIdFromConfiguration(configuration);
        Map<String, Object> variables = runVariables(definition.getKey(), runId, activityId);

        if (activityId != null) {
            org.activiti.bpmn.model.Process process = ProcessDefinitionUtil.getProcess(job.getProcessDefinitionId());
            FlowElement startEvent = process.getFlowElement(activityId, true);
            if (startEvent == null) {
                throw new ActivitiException("Could not find matching FlowElement for activityId " + activityId);
            }
            ProcessInstanceCreationOptions options = ProcessInstanceCreationOptions.builder(definition)
                    .businessKey(runId)
                    .variables(variables)
                    .build();
            commandContext.getProcessEngineConfiguration().getProcessInstanceHelper()
                    .createAndStartProcessInstanceWithInitialFlowElement(options, startEvent, process, true);
        } else {
            new StartProcessInstanceCmd(definition.getKey(), null, runId, variables, job.getTenantId())
                    .execute(commandContext);
        }
        log.info("timer-start.run processKey={} runId={} startEvent={}", definition.getKey(), runId, activityId);
    }

    /** REF-only start variables: the seeded context handle and the daemon boot epoch, like start-run. */
    private Map<String, Object> runVariables(String processKey, String runId, String startActivityId) {
        Map<String, Object> variables = new HashMap<>();
        ProcessContextRef seed = seedStartContext(processKey, runId, startActivityId);
        if (seed != null) {
            variables.put(EngineVariableNames.STUDIOQUACK_REF, seed);
        }
        BeRuntimeEpoch epoch = beRuntimeEpoch.getIfAvailable();
        if (epoch != null) {
            variables.put(BeRuntimeEpoch.VARIABLE_NAME, epoch.current());
        }
        RefOnlyVariableGuard.assertRefOnly(variables);
        return variables;
    }

    /**
     * Same seed as a message start: without a graph-hydrated {@code studioquackRef} the run's first
     * dispatchable node parks forever. {@code null} when the graph or the store is unavailable — the
     * run then starts unseeded and the dispatch path surfaces the parked node.
     */
    private ProcessContextRef seedStartContext(String processKey, String runId, String startActivityId) {
        String workflowId = WorkflowGraph.workflowIdOf(processKey);
        ExecutionContextStore store = executionContextStore.getIfAvailable();
        DeployedArtifactStore artifacts = artifactStore.getIfAvailable();
        if (workflowId == null || store == null || artifacts == null) {
            return null;
        }
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowExecutionId(runId);
        seed.setWorkflowId(workflowId);
        seed.setCurrentNodeId(StringUtils.removeStart(startActivityId, NODE_ELEMENT_PREFIX));
        seed.setEnvId(ownEnvId);
        artifacts.loadDeployedGraph(workflowId + WorkflowGraph.namespaceSuffixOf(processKey)).ifPresentOrElse(graph -> {
            seed.setNodes(graph.nodes());
            seed.setEdges(graph.edges());
        }, () -> log.warn("timer-start.seed-no-graph workflowId={} — the deploy graph row is missing; the run's "
                + "first dispatchable node will park", workflowId));
        return store.snapshot(seed, null);
    }
}
