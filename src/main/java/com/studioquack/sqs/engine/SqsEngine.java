package com.studioquack.sqs.engine;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.converter.BpmnXMLConverter;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.EventDefinition;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.MessageEventDefinition;
import org.activiti.bpmn.model.Process;
import org.activiti.bpmn.model.StartEvent;
import org.activiti.engine.HistoryService;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RepositoryService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.TaskService;
import org.activiti.engine.history.HistoricActivityInstanceQuery;
import org.activiti.engine.history.HistoricProcessInstance;
import org.activiti.engine.history.HistoricProcessInstanceQuery;
import org.activiti.engine.history.HistoricVariableInstance;
import org.activiti.engine.history.HistoricVariableInstanceQuery;
import org.activiti.engine.repository.Deployment;
import org.activiti.engine.repository.DeploymentBuilder;
import org.activiti.engine.repository.ProcessDefinition;
import org.activiti.engine.runtime.DeadLetterJobQuery;
import org.activiti.engine.runtime.Execution;
import org.activiti.engine.runtime.ExecutionQuery;
import org.activiti.engine.runtime.ProcessInstance;
import org.activiti.engine.runtime.ProcessInstanceBuilder;
import org.activiti.engine.runtime.ProcessInstanceQuery;
import org.activiti.engine.task.Task;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Facade over the embedded Activiti engine.
 * Callers pass only the encrypted context handle and non-payload routing tokens as variables.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SqsEngine {

    private final RuntimeService runtimeService;
    private final RepositoryService repositoryService;
    private final ManagementService managementService;
    private final HistoryService historyService;
    private final TaskService taskService;

    // ---------------------------------------------------------------------
    // Deploy — one daemon = one environment, so a definition is simply "the" definition.
    // ---------------------------------------------------------------------

    /**
     * Deploys one or more BPMN resources in a single deployment (main + every sub-process BPMN
     * together, so a Designer Run publishes an atomic version).
     *
     * @param deploymentName human-readable name (e.g. {@code workflowId + "-v" + n})
     * @param resources      map of {@code resourceName -> bpmnXml}
     * @return the created {@link Deployment}
     */
    public Deployment deploy(String deploymentName, Map<String, String> resources) {
        return deploy(deploymentName, null, resources);
    }

    /**
     * As {@link #deploy(String, Map)}, but stamps a deployment <b>category</b>.
     *
     * <p>The category is how a Designer Run's throwaway deployment is recognised later
     * ({@link #EPHEMERAL_CATEGORY}). Storing the marker on the deployment itself — rather than in a
     * side table — means it cannot drift from reality and survives a daemon crash: whatever the engine
     * holds IS the truth, so the reaper never has to reconcile two sources.
     */
    public Deployment deploy(String deploymentName, String category, Map<String, String> resources) {
        if (resources == null || resources.isEmpty()) {
            throw new IllegalArgumentException("No BPMN resources supplied for deployment " + deploymentName);
        }
        guardDuplicateMessageStartSubscriptions(resources);
        DeploymentBuilder builder = repositoryService.createDeployment().name(deploymentName);
        if (category != null) {
            builder.category(category);
        }
        for (Map.Entry<String, String> res : resources.entrySet()) {
            builder.addString(res.getKey(), res.getValue());
        }
        Deployment deployment = builder.deploy();
        log.debug("Deployed '{}' ({} resources, category={}) -> deploymentId={}",
                deploymentName, resources.size(), category, deployment.getId());
        return deployment;
    }

    /**
     * Two DIFFERENT process keys subscribing the same message-start name make every inbound
     * start ambiguous — the engine resolves one definition for the message name and rejects
     * (or mis-resolves) when two match. A new version of the SAME key is fine (the latest
     * supersedes); a different key with the same subscription is an authoring error, so the
     * deploy fails loudly here instead of at the first inbound message.
     */
    private void guardDuplicateMessageStartSubscriptions(Map<String, String> resources) {
        Map<String, String> subscribingKeysByMessageName = new HashMap<>();
        for (Map.Entry<String, String> res : resources.entrySet()) {
            BpmnModel model;
            try {
                byte[] bytes = res.getValue().getBytes(StandardCharsets.UTF_8);
                model = new BpmnXMLConverter().convertToBpmnModel(
                        () -> new ByteArrayInputStream(bytes), false, false);
            } catch (Exception ex) {
                continue; // an unparseable resource fails properly at builder.deploy()
            }
            if (model == null) {
                continue;
            }
            for (Process process : model.getProcesses()) {
                for (StartEvent start : process.findFlowElementsOfType(StartEvent.class)) {
                    for (EventDefinition ed : start.getEventDefinitions()) {
                        if (!(ed instanceof MessageEventDefinition med)) {
                            continue;
                        }
                        Message message = med.getMessageRef() == null
                                ? null : model.getMessage(med.getMessageRef());
                        String subscribedName = message != null ? message.getName() : med.getMessageRef();
                        if (StringUtils.isBlank(subscribedName)) {
                            continue;
                        }
                        String previousKey = subscribingKeysByMessageName.putIfAbsent(
                                subscribedName, process.getId());
                        if (previousKey != null && !previousKey.equals(process.getId())) {
                            throw new IllegalArgumentException(
                                    "message start '" + subscribedName + "' is subscribed by both "
                                            + previousKey + " and " + process.getId()
                                            + " in this deployment — one message name may have only "
                                            + "one subscribing process");
                        }
                        long others = repositoryService.createProcessDefinitionQuery()
                                .messageEventSubscriptionName(subscribedName)
                                .latestVersion()
                                .list().stream()
                                .filter(d -> !d.getKey().equals(process.getId()))
                                .count();
                        if (others > 0) {
                            throw new IllegalArgumentException(
                                    "message start '" + subscribedName + "' is already subscribed by "
                                            + "another deployed workflow on this environment — "
                                            + "undeploy it or choose a different message name");
                        }
                    }
                }
            }
        }
    }

    /**
 * Category stamped on deployments created for a Designer Run. These are throwaway by definition:
 * the control plane re-pushes the graph on every designer run, so the deployment is superseded the moment
 * the next run starts and must not accumulate in {@code ACT_RE_DEPLOYMENT}.
 */
    public static final String EPHEMERAL_CATEGORY = "sq-ephemeral";

    /** All deployments stamped with {@code category}, oldest first. */
    public List<Deployment> findDeploymentsByCategory(String category) {
        return repositoryService.createDeploymentQuery()
                .deploymentCategory(category)
                .orderByDeploymenTime().asc()
                .list();
    }

    /**
     * Number of process instances still RUNNING against any definition in {@code deploymentId}. A
     * non-zero count vetoes deletion — cascading a live run would strand it mid-flight.
     */
    public long runningInstanceCount(String deploymentId) {
        return repositoryService.createProcessDefinitionQuery()
                .deploymentId(deploymentId)
                .list()
                .stream()
                .mapToLong(def -> runtimeService.createProcessInstanceQuery()
                        .processDefinitionId(def.getId())
                        .count())
                .sum();
    }

    /**
     * Number of process instances that have EVER been started against any definition in
     * {@code deploymentId}, finished or not.
     *
     * <p>Combined with {@link #runningInstanceCount(String)} this distinguishes the two states a
     * throwaway deployment can be in when it has no live run: "its run finished, delete it now" versus
     * "its run has not started yet, leave it alone". Without the distinction the reaper can only wait
     * out a timer, which either deletes deployments before their run starts or keeps finished ones
     * around long enough for a published trigger to pick the draft up as the latest version.
     */
    public long historicInstanceCount(String deploymentId) {
        return repositoryService.createProcessDefinitionQuery()
                .deploymentId(deploymentId)
                .list()
                .stream()
                .mapToLong(def -> historyService.createHistoricProcessInstanceQuery()
                        .processDefinitionId(def.getId())
                        .count())
                .sum();
    }

    /**
 * Deletes a deployment and the history derived from it.
 * Cascade is required once the run has a historic process instance.
 *
 * @return {@code true} when the deployment was deleted
 */
    public boolean deleteDeployment(String deploymentId) {
        try {
            repositoryService.deleteDeployment(deploymentId, true);
            log.debug("activiti.deployment.deleted deploymentId={}", deploymentId);
            return true;
        } catch (Exception e) {
            // Never propagate: reaping is housekeeping and must not fail a deploy or a run report.
            log.warn("activiti.deployment.delete.failed deploymentId={} cause={}", deploymentId, e.toString());
            return false;
        }
    }

    /**
     * The Activiti deployment id owning {@code processDefinitionId}, or {@code null} when the
     * definition is gone (already undeployed / reaped). The {@code undeploy} command resolves its
     * target through this so it can count live instances and delete the whole deployment.
     */
    public String deploymentIdOf(String processDefinitionId) {
        if (processDefinitionId == null || processDefinitionId.isBlank()) {
            return null;
        }
        ProcessDefinition def = repositoryService.createProcessDefinitionQuery()
                .processDefinitionId(processDefinitionId)
                .singleResult();
        return def == null ? null : def.getDeploymentId();
    }

    /** Resolves the main {@link ProcessDefinition} (by key) created by a deployment — pins a Designer Run version. */
    public ProcessDefinition findDeployedDefinition(String deploymentId, String processDefinitionKey) {
        return repositoryService.createProcessDefinitionQuery()
                .deploymentId(deploymentId)
                .processDefinitionKey(processDefinitionKey)
                .singleResult();
    }

    /** Every {@link ProcessDefinition} a deployment created — main process plus function sub-processes. */
    public List<ProcessDefinition> findDeployedDefinitions(String deploymentId) {
        return repositoryService.createProcessDefinitionQuery()
                .deploymentId(deploymentId)
                .list();
    }

    /** True when a process definition with {@code processDefinitionKey} is deployed (any version). */
    public boolean latestDefinitionExists(String processDefinitionKey) {
        return repositoryService.createProcessDefinitionQuery()
                .processDefinitionKey(processDefinitionKey)
                .latestVersion()
                .count() > 0;
    }

    /**
     * The latest deployed definition id for {@code processDefinitionKey}, or null when the key is
     * not deployed. {@code CallActivityIoListener} resolves a {@code call.activity}'s
     * {@code calledElement} through this to read the child model.
     */
    public String findLatestDefinitionId(String processDefinitionKey) {
        if (processDefinitionKey == null) {
            return null;
        }
        ProcessDefinition def = repositoryService.createProcessDefinitionQuery()
                .processDefinitionKey(processDefinitionKey)
                .latestVersion()
                .singleResult();
        return def == null ? null : def.getId();
    }

    /** {@link ProcessDefinition} by its (String) id — monitoring name/version resolution. */
    public ProcessDefinition findProcessDefinition(String processDefinitionId) {
        if (processDefinitionId == null) {
            return null;
        }
        return repositoryService.createProcessDefinitionQuery()
                .processDefinitionId(processDefinitionId)
                .singleResult();
    }

    /**
     * Business key for a running or historic process instance. Used off the engine thread by
     * {@code BpmnNodeLogListener} when an activity event is not an entity event and the in-command
     * lookup missed. Returns null when the instance is unknown. Never logs payload values.
     */
    public String findBusinessKey(String processInstanceId) {
        if (processInstanceId == null || processInstanceId.isBlank()) {
            return null;
        }
        ProcessInstance running = findProcessInstance(processInstanceId);
        if (running != null && running.getBusinessKey() != null && !running.getBusinessKey().isBlank()) {
            return running.getBusinessKey();
        }
        var historic = historicProcessInstances().processInstanceId(processInstanceId).singleResult();
        if (historic == null) {
            return null;
        }
        String key = historic.getBusinessKey();
        return (key == null || key.isBlank()) ? null : key;
    }

    /**
 * The parsed {@link BpmnModel} of a deployed definition — the exact BPMN the engine runs, read
 * back out of the repository. Tests use it to prove the generator and the runtime still agree on
 * what was deployed; it is read-only and changes nothing.
 */
    public BpmnModel readDeployedModel(String processDefinitionId) {
        if (processDefinitionId == null) {
            return null;
        }
        return repositoryService.getBpmnModel(processDefinitionId);
    }

    // ---------------------------------------------------------------------
    // Start — Designer Run pins a version; triggers use latest.
    // ---------------------------------------------------------------------

    /** Starts an <b>exact</b> process definition (version pinned) — the Designer Run path. */
    public ProcessInstance startByDefinitionId(String processDefinitionId,
                                               String businessKey,
                                               Map<String, Object> variables) {
        ProcessInstanceBuilder builder = runtimeService.createProcessInstanceBuilder()
                .processDefinitionId(processDefinitionId)
                .businessKey(businessKey);
        if (variables != null && !variables.isEmpty()) {
            builder.variables(variables);
        }
        return builder.start();
    }

    /** Starts the <b>latest</b> version of {@code processDefinitionKey} (trigger / start-run path). */
    public ProcessInstance startLatest(String processDefinitionKey,
                                       String businessKey,
                                       Map<String, Object> variables) {
        ProcessInstanceBuilder builder = runtimeService.createProcessInstanceBuilder()
                .processDefinitionKey(processDefinitionKey)
                .businessKey(businessKey);
        if (variables != null && !variables.isEmpty()) {
            builder.variables(variables);
        }
        return builder.start();
    }

    // ---------------------------------------------------------------------
    // Wait-state completion + lifecycle.
    // ---------------------------------------------------------------------

    /** Completes a {@code ReceiveTask} wait-state (async sandbox job result). */
    public void trigger(String executionId, Map<String, Object> variables) {
        Objects.requireNonNull(executionId, "executionId");
        if (variables == null || variables.isEmpty()) {
            runtimeService.trigger(executionId);
        } else {
            runtimeService.trigger(executionId, variables);
        }
    }

    // ---------------------------------------------------------------------
    // User-task wait-state (the TASK eventKind resume seam).
    // ---------------------------------------------------------------------

    /** The BPMN element-id prefix every node strategy mints for a designer node id. */
    public static final String NODE_ELEMENT_ID_PREFIX = "node_";

    /**
     * Resolves the single ACTIVE user task for {@code (businessKey, activityId)} — the run-scoped
     * lookup for callers that know the node but not the task id. {@code activityId} may be the bare
     * designer node id or the minted {@code node_<id>} element id: the as-given form is tried
     * first (so a non-node element id like a {@code btimer_…} boundary is never rewritten), and a
     * bare id that missed is retried with the {@code node_} prefix. Returns {@code null} when no
     * active task matches (benign no-match); an ambiguous match surfaces as the underlying
     * {@code singleResult} exception, deliberately NOT a silent wrong completion.
     */
    public Task findUserTask(String businessKey, String activityId) {
        if (businessKey == null || businessKey.isBlank() || activityId == null || activityId.isBlank()) {
            return null;
        }
        Task task = taskService.createTaskQuery()
                .processInstanceBusinessKey(businessKey)
                .taskDefinitionKey(activityId)
                .active()
                .singleResult();
        if (task == null && !activityId.startsWith(NODE_ELEMENT_ID_PREFIX)) {
            task = taskService.createTaskQuery()
                    .processInstanceBusinessKey(businessKey)
                    .taskDefinitionKey(NODE_ELEMENT_ID_PREFIX + activityId)
                    .active()
                    .singleResult();
        }
        return task;
    }

    /**
     * Resolves the single ACTIVE user task for {@code (processInstanceId, activityId)} — the
     * instance-scoped lookup for callers that hold the instance but not its businessKey (e.g. a
     * {@code call.activity} child that started keyless). Same bare/{@code node_}-prefixed
     * {@code activityId} tolerance as {@link #findUserTask}.
     */
    public Task findUserTaskByInstance(String processInstanceId, String activityId) {
        if (processInstanceId == null || processInstanceId.isBlank()
                || activityId == null || activityId.isBlank()) {
            return null;
        }
        Task task = taskService.createTaskQuery()
                .processInstanceId(processInstanceId)
                .taskDefinitionKey(activityId)
                .active()
                .singleResult();
        if (task == null && !activityId.startsWith(NODE_ELEMENT_ID_PREFIX)) {
            task = taskService.createTaskQuery()
                    .processInstanceId(processInstanceId)
                    .taskDefinitionKey(NODE_ELEMENT_ID_PREFIX + activityId)
                    .active()
                    .singleResult();
        }
        return task;
    }

    /**
     * The ACTIVE user task with exactly this id, or {@code null} once it completed or was removed
     * (an interrupting boundary, a cancelled run). The task id is unique per visit, so a decision
     * addressed to an earlier visit of the same node can never complete a later one.
     */
    public Task findActiveTask(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return null;
        }
        return taskService.createTaskQuery().taskId(taskId).active().singleResult();
    }

    /** Up to {@code max} parked user tasks, oldest first — the approval reporter's reconcile sweep. */
    public List<Task> listActiveUserTasks(int max) {
        return taskService.createTaskQuery().active().orderByTaskCreateTime().asc().listPage(0, max);
    }

    /** Completes the given user task, advancing the token past it. */
    public void completeUserTask(String taskId) {        Objects.requireNonNull(taskId, "taskId");
        taskService.complete(taskId);
    }

    /** Writes an execution-local, namespaced, non-PHI routing token for a gateway. */
    public void setRouteToken(String executionId, String nodeId, Object scalarValue) {
        Objects.requireNonNull(executionId, "executionId");
        // JUEL-safe name: the generated ${route__<id> == '…'} conditions read exactly this name.
        runtimeService.setVariableLocal(
                executionId, EngineVariableNames.routeTokenVariableName(nodeId), scalarValue);
    }

    /** Cancels a running instance. */
    public void deleteProcessInstance(String processInstanceId, String reason) {
        runtimeService.deleteProcessInstance(processInstanceId, reason);
    }

    /** Correlates a BPMN message to a specific execution. */
    public void messageEventReceived(String messageName, String executionId, Map<String, Object> variables) {
        if (variables == null || variables.isEmpty()) {
            runtimeService.messageEventReceived(messageName, executionId);
        } else {
            runtimeService.messageEventReceived(messageName, executionId, variables);
        }
    }

    // ---------------------------------------------------------------------
    // Signal and message-start seams. They mirror message-catch correlation so an inbound
    // message event can deliver every BPMN event kind, not only an intermediate catch.
    // ---------------------------------------------------------------------

    /**
     * Delivers a BPMN <b>signal</b> to a specific waiting execution (scoped
     * delivery — the caller resolved the parked execution by run + signal name).
     */
    public void signalEventReceived(String signalName, String executionId, Map<String, Object> variables) {
        if (variables == null || variables.isEmpty()) {
            runtimeService.signalEventReceived(signalName, executionId);
        } else {
            runtimeService.signalEventReceived(signalName, executionId, variables);
        }
    }

    /**
     * Broadcasts a BPMN <b>signal</b> by name to every execution in THIS env's
     * engine currently subscribed to it (Activiti signal broadcast semantics).
     * Used only when the caller supplied no correlation key — a signal is, by
     * spec, a broadcast; there is no single execution id to report back.
     */
    public void signalEventReceived(String signalName, Map<String, Object> variables) {
        if (variables == null || variables.isEmpty()) {
            runtimeService.signalEventReceived(signalName);
        } else {
            runtimeService.signalEventReceived(signalName, variables);
        }
    }

    /**
     * Delivers {@code signalName} to ONE parked execution — the escalation bridge's lane.
     * Unlike the broadcast overloads this never leaves the process instance: the caller
     * enumerates the instance's own subscriptions (via {@link #findSignalEventExecutions})
     * and fires each directly, which is how an escalation stays inside its scope.
     */
    public void signalEventReceived(String signalName, String executionId) {
        if (signalName == null || signalName.isBlank() || executionId == null || executionId.isBlank()) {
            return;
        }
        runtimeService.signalEventReceived(signalName, executionId);
    }

    /**
     * Resolves the single execution parked on a BPMN <b>signal catch</b>
     * subscription for {@code (businessKey, signalName)} — the signal analogue of
     * {@link #findMessageEventExecution(String, String)}. Returns {@code null}
     * when nothing is parked (benign no-match), letting the caller report cleanly.
     */
    public Execution findSignalEventExecution(String businessKey, String signalName) {
        if (businessKey == null || businessKey.isBlank() || signalName == null || signalName.isBlank()) {
            return null;
        }
        return executions()
                // Same child-execution rationale as findMessageEventExecution: the
                // businessKey lives on the PI root, the subscription on the child.
                .processInstanceBusinessKey(businessKey, true)
                .signalEventSubscriptionName(signalName)
                .singleResult();
    }

    /**
     * Every execution parked on a BPMN <b>signal catch</b> subscription for
     * {@code signalName}, across ALL running instances on this engine — broadcast
     * addressing for the intra-engine signal bridge (a signal is, by spec, a
     * broadcast; there is no single target execution).
     */
    public List<Execution> findSignalEventExecutions(String signalName) {
        if (signalName == null || signalName.isBlank()) {
            return List.of();
        }
        return executions()
                .signalEventSubscriptionName(signalName)
                .list();
    }

    /**
     * Whether any deployed process definition on this engine holds a <b>message start event</b>
     * subscription for {@code messageName}. Check-then-act guard for
     * {@link #startProcessInstanceByMessage}: Activiti answers a no-subscription start with
     * {@code ActivitiObjectNotFoundException}, and when that happens on a NESTED command (e.g. the
     * sender listener's fanout inside the sender's own running command) the shared command context
     * remembers the exception and rolls the caller's transaction back at close — even when the
     * caller caught it. Pre-checking turns the no-subscription case into a clean benign no-match.
     */
    public boolean hasMessageStartSubscription(String messageName) {
        if (messageName == null || messageName.isBlank()) {
            return false;
        }
        return repositoryService.createProcessDefinitionQuery()
                .messageEventSubscriptionName(messageName)
                .count() > 0;
    }

    /**
     * The designer node id of the <b>message start event</b> subscribing {@code messageName} on
     * the latest deployed definition — the receiver-side node the C1 start envelope seals against
     * and fans out from. The intra-engine sender bridge cannot supply it (its request carries the
     * SENDER's node id), so it is resolved from the subscription itself; {@code null} when nothing
     * subscribes (mirrors {@link #hasMessageStartSubscription}).
     */
    public String findMessageStartNodeId(String messageName) {
        StartEvent start = findMessageStartEvent(messageName);
        if (start == null || start.getId() == null) {
            return null;
        }
        return start.getId().startsWith(NODE_ELEMENT_ID_PREFIX)
                ? start.getId().substring(NODE_ELEMENT_ID_PREFIX.length())
                : start.getId();
    }

    /**
     * The latest {@link ProcessDefinition} holding a <b>message start</b> subscription for
     * {@code messageName} — the definition a message-started run launches from, read so the
     * start path can seed the run's graph-hydrated {@code studioquackRef} off the deploy store.
     * {@code null} when nothing subscribes.
     */
    public ProcessDefinition findMessageStartDefinition(String messageName) {
        if (messageName == null || messageName.isBlank()) {
            return null;
        }
        // Tolerant lookup: two DIFFERENT process keys may subscribe the same message name
        // (the deploy guard rejects that going forward, but pre-guard deployments exist).
        // singleResult() would throw here — on the bridge path that exception escapes inside
        // the SENDER's command and poisons its transaction, the exact hazard the
        // check-then-act design of hasMessageStartSubscription engineered around.
        List<ProcessDefinition> defs = repositoryService.createProcessDefinitionQuery()
                .messageEventSubscriptionName(messageName)
                .latestVersion()
                .orderByProcessDefinitionVersion().desc()
                .list();
        if (defs.isEmpty()) {
            return null;
        }
        if (defs.size() > 1) {
            log.warn("message start '{}': {} definitions subscribe the same name — reading "
                    + "the receiver config / seed definition from {} v{}. Undeploy or rename one "
                    + "subscription.",
                    messageName, defs.size(), defs.get(0).getKey(), defs.get(0).getVersion());
        }
        return defs.get(0);
    }

    /**
     * The {@link StartEvent} element holding the <b>message start</b> subscription for
     * {@code messageName} on the latest deployed definition — carries the authored
     * {@code sq:receiverConfig} extension element ({@code correlationKey} /
     * {@code businessKeyExpression}) the start path enforces (audit C7). {@code null} when
     * nothing subscribes.
     */
    public StartEvent findMessageStartEvent(String messageName) {
        ProcessDefinition def = findMessageStartDefinition(messageName);
        if (def == null) {
            return null;
        }
        BpmnModel model = repositoryService.getBpmnModel(def.getId());
        if (model == null || model.getMainProcess() == null) {
            return null;
        }
        for (StartEvent start : model.getMainProcess().findFlowElementsOfType(StartEvent.class)) {
            for (EventDefinition ed : start.getEventDefinitions()) {
                if (!(ed instanceof MessageEventDefinition med)) {
                    continue;
                }
                Message message = med.getMessageRef() == null ? null : model.getMessage(med.getMessageRef());
                String subscribedName = message != null ? message.getName() : med.getMessageRef();
                if (messageName.equals(subscribedName)) {
                    return start;
                }
            }
        }
        return null;
    }

    /**
     * Starts a NEW process instance whose BPMN <b>message start event</b>
     * subscribes to {@code messageName} (a message-start webhook). Optionally
     * stamps the run's {@code businessKey} (= workflowExecutionId) so downstream
     * correlation addresses it like any other run. REF-only seed {@code variables}
     * only (guarded upstream). Returns the new process-instance id.
     */
    public String startProcessInstanceByMessage(String messageName, String businessKey, Map<String, Object> variables) {
        Objects.requireNonNull(messageName, "messageName");
        ProcessInstance pi;
        if (businessKey != null && !businessKey.isBlank()) {
            pi = (variables == null || variables.isEmpty())
                    ? runtimeService.startProcessInstanceByMessage(messageName, businessKey)
                    : runtimeService.startProcessInstanceByMessage(messageName, businessKey, variables);
        } else {
            pi = (variables == null || variables.isEmpty())
                    ? runtimeService.startProcessInstanceByMessage(messageName)
                    : runtimeService.startProcessInstanceByMessage(messageName, variables);
        }
        return pi == null ? null : pi.getId();
    }

    // ---------------------------------------------------------------------
    // Runtime lookups used by the dispatch / skip / message paths.
    // ---------------------------------------------------------------------

    /** A fresh {@link ExecutionQuery}. */
    public ExecutionQuery executions() {
        return runtimeService.createExecutionQuery();
    }

    /** A fresh {@link ProcessInstanceQuery}. */
    public ProcessInstanceQuery processInstances() {
        return runtimeService.createProcessInstanceQuery();
    }

    /** Resolves the single waiting execution for {@code (businessKey, activityId)}. */
    public Execution findWaitingExecution(String businessKey, String activityId) {
        return executions()
                .processInstanceBusinessKey(businessKey)
                .activityId(activityId)
                .singleResult();
    }

    /**
 * Resolves the single execution parked on a BPMN <b>message catch</b> subscription for
 * {@code (businessKey, messageName)} — the seam the {@code correlate-message}
 * reverse-channel command uses to deliver an external (webhook) intermediate message event to a
 * running instance. Correlation is by the run's {@code businessKey} (= {@code workflowExecutionId})
 * plus the message name that {@code ActivitiMessageCatchStrategy} registered as the event
 * subscription name, so it addresses exactly the instance the caller named and never a foreign run.
 *
 * <p>Unlike {@link #findWaitingExecution(String, String)} (which keys on the BPMN
 * {@code activityId}), this keys on the message-event <b>subscription</b> name — the durable
 * marker Activiti attaches to the waiting execution for an intermediate message catch event, and
 * the exact token {@link org.activiti.engine.RuntimeService#messageEventReceived} consumes. That
 * sidesteps the well-known {@code ExecutionQuery.activityId} single-token gotcha noted on
 * {@link #findUnfinishedWaitExecution(String, String)}: the subscription rides on whichever
 * execution actually holds it (root or child).
 *
 * <p>Returns {@code null} when nothing is parked for that key (unknown/finished instance, or the
 * instance is waiting on a different message) so the caller can report a benign no-match rather
 * than a failure. An ambiguous match (two executions parked on the same message name in one run)
 * is likewise a benign no-match (WARN-logged): the query must NEVER let {@code singleResult}
 * throw here — the query runs as a nested command on the caller's SHARED command context, and a
 * recorded exception rolls the caller's own transaction back at close even when the caller
 * catches it (the st19 manual-test failure: sender run rolled back by an ambiguous target).
 *
 * @param businessKey the run's {@code workflowExecutionId} (the message correlation key)
 * @param messageName the BPMN message name the instance is waiting on
 * @return the parked {@link Execution}, or {@code null} when none matches
 */
    public Execution findMessageEventExecution(String businessKey, String messageName) {
        if (businessKey == null || businessKey.isBlank() || messageName == null || messageName.isBlank()) {
            return null;
        }
        List<Execution> matches = executions()
                // includeChildExecutions=true is REQUIRED: the businessKey lives on the process-instance
                // root execution, but the message-event subscription rides on the CHILD execution holding
                // the waiting token. The single-arg processInstanceBusinessKey() matches only the root and
                // would miss the subscription-bearing child (returning nothing) — the two-arg form widens
                // the businessKey match to the child so the subscription filter resolves it.
                .processInstanceBusinessKey(businessKey, true)
                .messageEventSubscriptionName(messageName)
                .list();
        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() > 1) {
            log.warn("findMessageEventExecution: ambiguous message catch businessKey={} messageName={} matches={} — treating as no-match",
                    businessKey, messageName, matches.size());
            return null;
        }
        return matches.get(0);
    }

    /**
     * Correlation fallback by the receiver-AUTHORED shared correlation key: finds the execution
     * parked on {@code messageName} whose correlation-pin variable
     * ({@code __sq_msgcorr_<messageName>}, written at park time by
     * {@code MessageReceiverCorrelationListener}) equals {@code correlationKey} — engine-wide,
     * across every process definition and run on this engine.
     *
     * <p>This is what makes BPMN-style shared-key correlation work: a sender in another flow
     * cannot know the receiver run's businessKey at authoring time, but both sides can author the
     * SAME correlation key. The pin is execution-local on the park point (the catch execution,
     * the timeout-shape gateway, or the boundary's HOST activity) and survives a daemon restart
     * in {@code ACT_RU_VARIABLE} exactly like the subscription itself.
     *
     * <p>Precedence stays with the businessKey lookup: callers invoke this ONLY when
     * {@link #findMessageEventExecution(String, String)} missed, so a run addressed by its real
     * businessKey always wins. An ambiguous match (two parked catches pinned with the same
     * {@code (messageName, correlationKey)} — e.g. leftover runs from repeated manual tests) is a
     * benign no-match (WARN-logged), mirroring {@link #findMessageEventExecution}: the query must
     * NEVER let {@code singleResult} throw into the caller's shared command context, because the
     * recorded exception rolls the SENDER's transaction back at close even when caught.
     *
     * <p>The pin match is done in Java, not in SQL: Activiti 9's non-local
     * {@code variableValueEquals} join only considers ROOT-scope rows
     * ({@code ACT_RU_VARIABLE.EXECUTION_ID_ = PROC_INST_ID_}), which would miss a pin on any
     * child scope — and a message boundary's host activity (user task / call activity) IS a
     * child execution of the root. The subscription side stays in SQL
     * ({@code messageEventSubscriptionName} joins on the subscription-bearing execution); each
     * candidate is then re-checked through its execution SCOPE CHAIN ({@code getVariable}, not
     * {@code getVariableLocal}): the pin is found when it rides the candidate itself
     * (intermediate catch) or an ANCESTOR — the boundary subscription sits on a child execution
     * of the pinned host — while a pin on a SIBLING branch stays invisible (scope chains never
     * cross sideways). The candidate set is bounded by the number of runs parked on the same
     * message name on this single-tenant engine, so the per-candidate read stays cheap.
     *
     * @param messageName    the BPMN message name the instance is waiting on
     * @param correlationKey the receiver-authored shared correlation key
     * @return the parked {@link Execution}, or {@code null} when none matches
     */
    public Execution findMessageEventExecutionByCorrelationKey(String messageName, String correlationKey) {
        return findMessageEventExecutionByScopedVariable(messageName,
                EngineVariableNames.messageCorrelationVariableName(messageName), correlationKey);
    }

    /**
     * Third correlation fallback: resolves the single execution parked on {@code messageName}
     * whose run was message-STARTED with {@code startKey} as its correlation key (the
     * {@link EngineVariableNames#START_CORRELATION_KEY} seed). A message-started run's businessKey
     * is a minted unique id the caller never learns, so this is how a sender addressing the key it
     * started the run with reaches that run's later catches. Same ambiguity rule as the alias pin:
     * several runs sharing the key is a benign no-match.
     */
    public Execution findMessageEventExecutionByStartKey(String messageName, String startKey) {
        return findMessageEventExecutionByScopedVariable(messageName,
                EngineVariableNames.START_CORRELATION_KEY, startKey);
    }

    /**
     * Shared scan behind the correlation fallbacks: executions subscribed to {@code messageName},
     * filtered by a scope-chain variable read. Ambiguous ({@code > 1}) matches are treated as a
     * no-match — a non-unique key must never deliver to an arbitrary run.
     */
    private Execution findMessageEventExecutionByScopedVariable(String messageName, String variableName,
                                                                String value) {
        if (messageName == null || messageName.isBlank() || value == null || value.isBlank()) {
            return null;
        }
        List<Execution> matches = executions()
                .messageEventSubscriptionName(messageName)
                .list().stream()
                .filter(candidate -> Objects.equals(value,
                        safeScopedVariable(candidate.getId(), variableName)))
                .toList();
        if (matches.isEmpty()) {
            return null;
        }
        if (matches.size() > 1) {
            log.warn("findMessageEventExecutionByScopedVariable: ambiguous correlation messageName={} "
                    + "variable={} matches={} — treating as no-match", messageName, variableName, matches.size());
            return null;
        }
        return matches.get(0);
    }

    /**
     * Scope-chain variable read that never throws: the candidate may be gone between reads.
     * {@code getVariable} (not {@code getVariableLocal}) so a pin written on an ANCESTOR scope —
     * the boundary host activity — is visible to the subscription-bearing child execution.
     */
    private Object safeScopedVariable(String executionId, String name) {
        try {
            return runtimeService.getVariable(executionId, name);
        } catch (Exception ex) {
            return null;
        }
    }

    /**
 * Resolves the live execution id currently parked at {@code waitActivityId} within a process
 * instance, via the UNFINISHED {@link org.activiti.engine.history.HistoricActivityInstance}
 * rather than {@link ExecutionQuery#activityId(String)}. For a linear single-token wait the
 * parked token sits on the process-instance execution, which {@code ExecutionQuery.activityId}
 * does not return (a well-known Activiti gotcha); the unfinished historic activity instance
 * always carries the live execution id to trigger. Returns {@code null} when nothing is parked
 * there (the PI never reached the wait, already resumed, or is gone/terminal).
 *
 * <p>This is the single seam 's {@code DispatchResultCoordinator} + boot-hydration +
 * orphan-sweeper use to answer "is this ReceiveTask still parked?", so no runtime-durability
 * class injects a raw Activiti service (the {@code SqsEngineSingleTenantArchTest} stays green).
 */
    public String findUnfinishedWaitExecution(String processInstanceId, String waitActivityId) {
        if (processInstanceId == null || waitActivityId == null) {
            return null;
        }
        try {
            for (var a : historicActivityInstances(processInstanceId)
                    .activityId(waitActivityId).unfinished().list()) {
                if (a.getExecutionId() != null) {
                    return a.getExecutionId();
                }
            }
            return null;
        } catch (RuntimeException probe) {
            if (isStoreFatal(probe)) {
                log.error("findUnfinishedWaitExecution: engine store failed pi={} wait={}: {}",
                        processInstanceId, waitActivityId, probe.toString());
            } else {
                log.trace("findUnfinishedWaitExecution probe pi={} wait={}: {}",
                        processInstanceId, waitActivityId, probe.getMessage());
            }
        }
        return null;
    }

    /** @return true when a RUNTIME process instance with this id still exists (not ended/removed). */
    public boolean isProcessInstanceActive(String processInstanceId) {
        if (processInstanceId == null) {
            return false;
        }
        return processInstances().processInstanceId(processInstanceId).count() > 0;
    }

    /** @return true when an execution with this id still exists in the runtime (not ended/removed). */
    public boolean isExecutionActive(String executionId) {
        if (executionId == null) {
            return false;
        }
        return executions().executionId(executionId).count() > 0;
    }

    /**
     * The current BPMN activity id of a LIVE execution — how a repair matches a dead-letter job to
     * the {@code hold_<nodeId>} task its token is parked at ({@code job.getExecutionId()} carries
     * only the execution id). Returns {@code null} when the execution vanished between the
     * dead-letter listing and this probe (the run ended concurrently) — a benign no-match.
     */
    public String findExecutionActivityId(String executionId) {
        if (executionId == null) {
            return null;
        }
        try {
            Execution execution = executions().executionId(executionId).singleResult();
            return execution == null ? null : execution.getActivityId();
        } catch (RuntimeException ex) {
            log.debug("findExecutionActivityId executionId={}: {}", executionId, ex.getMessage());
            return null;
        }
    }

    /**
     * Sets process variables on a RUNNING instance (monitoring "edit variables" / "add variable").
     * REF-ONLY invariant still holds — the caller must pass only non-PHI routing/reference values.
     */
    public void setProcessVariables(String processInstanceId, Map<String, Object> variables) {
        if (variables != null && !variables.isEmpty()) {
            runtimeService.setVariables(processInstanceId, variables);
        }
    }

    /**
 * Removes variables from an execution's scope chain. This is the NON-local
 * {@link RuntimeService#removeVariables(String, Collection)} on purpose: the failure sentinels
 * a retry scrubs may sit on the process-instance scope while the dead-letter job's execution is
 * a child of it, and only the non-local walk reaches them. An execution that vanished between
 * the dead-letter listing and the scrub (the run ended concurrently) is logged at debug and
 * swallowed — the repair must still move the job back, never fail on a stale scope.
 */
    public void removeVariables(String executionId, Collection<String> names) {
        if (executionId == null || names == null || names.isEmpty()) {
            return;
        }
        try {
            runtimeService.removeVariables(executionId, names);
        } catch (RuntimeException ex) {
            log.debug("removeVariables executionId={} names={}: {}", executionId, names, ex.getMessage());
        }
    }

    /**
     * Reads a single variable off a RUNNING instance (REF-ONLY: only the encrypted
     * {@code studioquackRef} handle + non-PHI tokens ever live here). Used by the replay repair to
     * recover the seed ref by process-instance identity. Returns {@code null} when the instance or
     * variable is gone.
     */
    public Object getProcessVariable(String processInstanceId, String name) {
        if (processInstanceId == null || name == null) {
            return null;
        }
        try {
            return runtimeService.getVariable(processInstanceId, name);
        } catch (RuntimeException ex) {
            log.debug("getProcessVariable pi={} name={}: {}", processInstanceId, name, ex.getMessage());
            return null;
        }
    }

    /** The single RUNNING {@link ProcessInstance} for an id, or {@code null} when ended/removed. */
    public ProcessInstance findProcessInstance(String processInstanceId) {
        if (processInstanceId == null) {
            return null;
        }
        return processInstances().processInstanceId(processInstanceId).singleResult();
    }

    /**
 * Resolves a repair target to its RUNTIME process-instance id. the control plane addresses
 * client-orchestrated runs by their {@code workflowExecutionId} (business key) — the only handle it
 * keeps in the run index — so a pushed repair payload may carry either a real
 * process-instance id (legacy/the control plane-known) or that business key. This maps both to the live
 * process-instance id: a direct RUNTIME id match wins; otherwise the RUNNING instance carrying that
 * business key is used. Returns the input unchanged when nothing resolves, so the caller reports a
 * clean "no running instance" repair failure rather than acting on the wrong run.
 */
    public String resolveProcessInstanceId(String idOrBusinessKey) {
        if (idOrBusinessKey == null || idOrBusinessKey.isBlank()) {
            return idOrBusinessKey;
        }
        String candidate = idOrBusinessKey.trim();
        if (isProcessInstanceActive(candidate)) {
            return candidate;
        }
        ProcessInstance byBusinessKey = processInstances()
                .processInstanceBusinessKey(candidate)
                .list().stream().findFirst().orElse(null);
        return byBusinessKey != null ? byBusinessKey.getId() : candidate;
    }

    // ---------------------------------------------------------------------
    // Monitoring reads (ACT_HI_* / dead-letter) — read directly, no projection.
    // ---------------------------------------------------------------------

    /** Historic process-instance query; caller adds filters + list/count. */
    public HistoricProcessInstanceQuery historicProcessInstances() {
        return historyService.createHistoricProcessInstanceQuery();
    }

    /**
 * Resolves a repair target to its LATEST HISTORIC process instance (replay of an ended
 * run). Mirrors {@link #resolveProcessInstanceId}'s id-or-business-key duality for runs that no
 * longer have a runtime row: a direct id match wins; otherwise the most recently STARTED
 * instance carrying that business key is used (a replayed run reuses its business key, so the
 * newest start is the incarnation the operator means). {@code null} when history knows neither.
 */
    public HistoricProcessInstance findLatestHistoricProcessInstance(String idOrBusinessKey) {
        if (idOrBusinessKey == null || idOrBusinessKey.isBlank()) {
            return null;
        }
        String candidate = idOrBusinessKey.trim();
        HistoricProcessInstance byId = historicProcessInstances()
                .processInstanceId(candidate).singleResult();
        if (byId != null) {
            return byId;
        }
        return historicProcessInstances()
                .processInstanceBusinessKey(candidate)
                .orderByProcessInstanceStartTime().desc()
                .list().stream().findFirst().orElse(null);
    }

    /**
     * Reads a single HISTORIC variable of an (ended) instance — the history-tier counterpart of
     * {@link #getProcessVariable}, used by the replay repair to recover the seed ref after the
     * runtime variable rows are gone. Returns {@code null} when the instance or variable is absent.
     */
    public Object getHistoricProcessVariable(String processInstanceId, String name) {
        if (processInstanceId == null || name == null) {
            return null;
        }
        HistoricVariableInstance variable = historicVariables(processInstanceId)
                .variableName(name).singleResult();
        return variable == null ? null : variable.getValue();
    }

    /**
 * retention-safety signal — have ALL process instances for {@code businessKey} TERMINATED
 * (completed / failed / cancelled) with their latest end strictly before {@code time}?
 *
 * <p>The authoritative source is Activiti history ({@link org.activiti.engine.history.HistoricProcessInstance#getEndTime},
 * {@code null} ⇒ still running), which is fully populated because the daemon runs at
 * {@code historyLevel=FULL}. {@code businessKey} is the run's {@code workflowExecutionId}
 * (start-run sets {@code businessKey == workflowExecutionId}), which is also the key on the
 * {@code execution_context} rows the log-retention reaper deletes — so this answers "is it safe
 * to purge this run's live per-node context yet?".
 *
 * <p>Returns {@code false} (NOT safe to purge) when any of the following hold, so a live run's
 * context is never reaped no matter how old its rows are:
 * <ul>
 * <li>an instance for the key is still running/parked (an <em>unfinished</em> historic instance
 * exists) — even a single live replay of a reused business key keeps the whole key;</li>
 * <li>an instance for the key ended at/after {@code time} (its history is still within the
 * retention window); or</li>
 * <li>there is no finished instance at all for the key (no positive evidence of termination —
 * we never purge an unknown/orphan key on a guess).</li>
 * </ul>
 *
 * @param businessKey the run's {@code workflowExecutionId}
 * @param time the retention cutoff; only runs whose latest termination is before this are terminal-and-aged
 * @return {@code true} iff every instance for the key has terminated and none ended at/after {@code time}
 */
    public boolean allInstancesTerminatedBefore(String businessKey, java.util.Date time) {
        if (businessKey == null || businessKey.isBlank() || time == null) {
            return false;
        }
        // Still running/parked (endTime == null) → never reap.
        if (historicProcessInstances().processInstanceBusinessKey(businessKey).unfinished().count() > 0) {
            return false;
        }
        // Terminated but still within the retention window (ended at/after the cutoff) → keep.
        if (historicProcessInstances().processInstanceBusinessKey(businessKey).finishedAfter(time).count() > 0) {
            return false;
        }
        // Positive terminal evidence: at least one finished instance for the key.
        return historicProcessInstances().processInstanceBusinessKey(businessKey).finished().count() > 0;
    }

    /** Historic activity instances (flow-node view) for a process instance. */
    public HistoricActivityInstanceQuery historicActivityInstances(String processInstanceId) {
        return historyService.createHistoricActivityInstanceQuery().processInstanceId(processInstanceId);
    }

    /** Historic variables (REF-only tokens) for a process instance. */
    public HistoricVariableInstanceQuery historicVariables(String processInstanceId) {
        return historyService.createHistoricVariableInstanceQuery().processInstanceId(processInstanceId);
    }

    /**
 * Live timer jobs scheduled for a process instance — one per parked timer boundary / timer catch.
 * A count above zero is the durable proof that a boundary timer survives an engine restart
 *the row lives in {@code ACT_RU_TIMER_JOB}, not in memory.
 */
    public long countTimerJobsForProcessInstance(String processInstanceId) {
        if (processInstanceId == null) {
            return 0;
        }
        return managementService.createTimerJobQuery()
                .processInstanceId(processInstanceId)
                .count();
    }

    /** Dead-letter (incident) query. */
    public DeadLetterJobQuery deadLetterJobs() {
        return managementService.createDeadLetterJobQuery();
    }

    /** Dead-letter jobs for a process instance. */
    public DeadLetterJobQuery deadLetterJobsForProcessInstance(String processInstanceId) {
        return managementService.createDeadLetterJobQuery().processInstanceId(processInstanceId);
    }

    /** Full exception stacktrace behind a dead-letter (incident) job — for the incident detail. */
    public String deadLetterJobStacktrace(String jobId) {
        try {
            return managementService.getDeadLetterJobExceptionStacktrace(jobId);
        } catch (RuntimeException ex) {
            log.debug("No dead-letter stacktrace for job {}: {}", jobId, ex.getMessage());
            return null;
        }
    }

    // ---------------------------------------------------------------------
    // Incidents (dead-letter jobs) — retry path.
    // ---------------------------------------------------------------------

    /** Moves a dead-letter job back to executable so the async executor re-runs it. */
    public void retryDeadLetterJob(String jobId, int retries) {
        managementService.moveDeadLetterJobToExecutableJob(jobId, retries);
    }

    public void deleteDeadLetterJob(String jobId) {
        managementService.deleteDeadLetterJob(jobId);
    }

    public void setJobRetries(String jobId, int retries) {
        managementService.setJobRetries(jobId, retries);
    }

    /** True when the throwable chain is an H2/MVStore close — not a benign "not parked" miss. */
    private static boolean isStoreFatal(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String name = c.getClass().getName();
            String message = c.getMessage() == null ? "" : c.getMessage();
            if (name.contains("MVStore")
                    || name.contains("PersistenceException")
                    || name.contains("ClosedByInterrupt")
                    || name.contains("ClosedChannel")
                    || message.contains("database has been closed")) {
                return true;
            }
        }
        return false;
    }
}
