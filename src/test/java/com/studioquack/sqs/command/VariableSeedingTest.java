package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.DeploymentInfo;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.command.store.DeployedArtifactStore;
import com.studioquack.sqs.command.store.DeployedDefinitionEntity;
import com.studioquack.sqs.command.store.DeployedDefinitionRepository;
import com.studioquack.sqs.command.store.DeployedScriptRepository;
import com.studioquack.sqs.command.store.DeployedTemplateRepository;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowDeploymentService;
import com.studioquack.sqs.engine.runtime.BeRuntimeEpoch;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.files.DeployFileIngest;
import com.studioquack.sqs.files.StartRunFileIngest;
import com.studioquack.sqs.status.RunStatusCallback;
import org.activiti.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Deploy persists {@code globalParams}/{@code projectVariables} on the artifact row; start-run
 * seals them onto {@code seedContext} before the REF snapshot. Precedence: the caller stamps FRESH
 * variable maps onto the start-run seedContext, so a non-empty seed map wins and the deploy-stored
 * map is only the fallback for a seed that carries none.
 */
@ExtendWith(MockitoExtension.class)
class VariableSeedingTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Mock private DeployedDefinitionRepository definitionRepository;
    @Mock private DeployedTemplateRepository templateRepository;
    @Mock private DeployedScriptRepository scriptRepository;
    @Mock private WorkflowDeploymentService workflowDeploymentService;
    @Mock private DeployFileIngest deployFileIngest;
    @Mock private com.studioquack.sqs.engine.EphemeralDeploymentReaper ephemeralDeploymentReaper;
    @Mock private SqsEngine sqsEngine;
    @Mock private ExecutionContextStore executionContextStore;
    @Mock private BeRuntimeEpoch beRuntimeEpoch;
    @Mock private RunStatusCallback runStatusCallback;
    @Mock private StartRunFileIngest startRunFileIngest;

    private DeployedArtifactStore artifactStore;
    private DeployCommandHandler deployHandler;
    private StartRunCommandHandler startRunHandler;

    @BeforeEach
    void setUp() {
        artifactStore = new DeployedArtifactStore(
                templateRepository, scriptRepository, definitionRepository, mapper);
        deployHandler = new DeployCommandHandler(
                mapper, workflowDeploymentService, artifactStore,
                ephemeralDeploymentReaper, deployFileIngest,
                new CallActivityShortCodeResolver(artifactStore));
        startRunHandler = new StartRunCommandHandler(
                mapper, sqsEngine, executionContextStore, beRuntimeEpoch,
                runStatusCallback, workflowDeploymentService, artifactStore, startRunFileIngest,
                new com.studioquack.sqs.status.RunLogModeRegistry(), null);
    }

    @Test
    void deploy_persists_vars_and_start_run_seals_them_onto_seed_context() throws Exception {
        String workflowId = "wfVarSeed";
        Map<String, Object> globalParams = Map.of("maxRetries", 3, "label", "wf-a");
        Map<String, Object> projectVariables = Map.of("region", "eu", "tier", "standard");

        deployWithVars(workflowId, globalParams, projectVariables);

        // Round-trip through entity LOBs (what ddl-auto persists).
        DeployedArtifactStore.VariableScopes loaded = artifactStore.loadDeployVariables(workflowId)
                .orElseThrow(() -> new AssertionError("deploy vars missing from artifact store"));
        assertEquals(globalParams, loaded.globalParams());
        assertEquals(projectVariables, loaded.projectVariables());

        // Start-run: the caller stamps FRESH wf/project vars onto the seedContext — those win over
        // the deploy-stored fallback before the snapshot.
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowId(workflowId);
        seed.setEnvId("env-var-seed");
        seed.setCurrentNodeId("taskN");
        seed.setWorkflowVariables(new LinkedHashMap<>(Map.of("maxRetries", 99, "stale", true)));
        seed.setProjectVariables(new LinkedHashMap<>(Map.of("region", "us")));

        ExecutionContext sealed = startAndCaptureSealed(workflowId, seed);
        assertEquals(Map.of("maxRetries", 99, "stale", true), sealed.getWorkflowVariables(),
                "fresh seed workflowVariables must win over the deploy-stored map");
        assertEquals(Map.of("region", "us"), sealed.getProjectVariables(),
                "fresh seed projectVariables must win over the deploy-stored map");
        assertEquals("env-var-seed", sealed.getEnvId(),
                "unrelated seedContext fields must not be wiped");
    }

    @Test
    void start_run_falls_back_to_deploy_vars_when_seed_carries_none() throws Exception {
        String workflowId = "wfVarSeedFallback";
        Map<String, Object> globalParams = Map.of("maxRetries", 3);
        Map<String, Object> projectVariables = Map.of("region", "eu");

        deployWithVars(workflowId, globalParams, projectVariables);

        // Seed carries NO variable maps: the deploy-stored scopes are the only source.
        ExecutionContext seed = new ExecutionContext();
        seed.setWorkflowId(workflowId);
        seed.setEnvId("env-var-seed");
        seed.setCurrentNodeId("taskN");

        ExecutionContext sealed = startAndCaptureSealed(workflowId, seed);
        assertEquals(globalParams, sealed.getWorkflowVariables(),
                "deploy-stored globalParams seal when the seed carries no workflowVariables");
        assertEquals(projectVariables, sealed.getProjectVariables(),
                "deploy-stored projectVariables seal when the seed carries no projectVariables");
    }

    /** Deploy path: upsert definition row, capture LOB writes, run the deploy command. */
    private void deployWithVars(String workflowId, Map<String, Object> globalParams,
                                Map<String, Object> projectVariables) throws Exception {
        DeployedDefinitionEntity entity = DeployedDefinitionEntity.builder()
                .workflowId(workflowId).build();
        AtomicReference<DeployedDefinitionEntity> stored = new AtomicReference<>(entity);
        when(definitionRepository.findById(workflowId)).thenAnswer(inv -> Optional.of(stored.get()));
        when(definitionRepository.save(any(DeployedDefinitionEntity.class))).thenAnswer(inv -> {
            DeployedDefinitionEntity e = inv.getArgument(0);
            stored.set(e);
            return e;
        });

        DeploymentInfo info = new DeploymentInfo();
        info.setProcessDefinitionId("pd-1");
        info.setBpmnProcessId("Process_" + workflowId);
        info.setVersion(1);
        info.setGraphSignature("bpmn-signature:v7|abc");
        when(workflowDeploymentService.deploy(any(), any(), anyBoolean(), anyBoolean())).thenReturn(info);

        DeployCommandPayload deployPayload = new DeployCommandPayload(
                workflowId, "1", "Var Seed Flow",
                java.util.List.of(minimalNode("startN")), java.util.List.of(),
                null, null, false, false,
                null, null, null, null, globalParams, projectVariables);
        DaemonCommandOutcome deployOutcome = deployHandler.handle(
                new DaemonCommand("c-var-deploy", DeployCommandHandler.TYPE, null,
                        mapper.writeValueAsString(deployPayload)));
        assertTrue(deployOutcome.succeeded(), () -> "deploy error: " + deployOutcome.error());
    }

    /** Start-run path: run the command and return the context exactly as sealed for the snapshot. */
    private ExecutionContext startAndCaptureSealed(String workflowId, ExecutionContext seed) throws Exception {
        ProcessContextRef ref = new ProcessContextRef();
        ref.setCurrentNodeRefId("ref-1");
        when(executionContextStore.snapshot(any(ExecutionContext.class), isNull())).thenReturn(ref);
        when(beRuntimeEpoch.current()).thenReturn("1");
        ProcessInstance pi = mock(ProcessInstance.class);
        when(pi.getId()).thenReturn("pi-1");
        when(pi.getProcessDefinitionId()).thenReturn("pd-1");
        when(sqsEngine.latestDefinitionExists("Process_" + workflowId)).thenReturn(true);
        when(sqsEngine.startLatest(eq("Process_" + workflowId), anyString(), anyMap())).thenReturn(pi);

        StartRunCommandPayload startPayload = new StartRunCommandPayload(
                "wfe-1", "Process_" + workflowId, null, null, seed);
        DaemonCommandOutcome startOutcome = startRunHandler.handle(
                new DaemonCommand("c-var-start", StartRunCommandHandler.TYPE, null,
                        mapper.writeValueAsString(startPayload)));
        assertTrue(startOutcome.succeeded(), () -> "start-run error: " + startOutcome.error());

        ArgumentCaptor<ExecutionContext> sealed = ArgumentCaptor.forClass(ExecutionContext.class);
        verify(executionContextStore).snapshot(sealed.capture(), isNull());
        return sealed.getValue();
    }

    private static com.studioquack.common.dto.flow.NodeDto minimalNode(String id) {
        com.studioquack.common.dto.flow.TemplateDto template =
                new com.studioquack.common.dto.flow.TemplateDto();
        template.setBpmnType("start.event");
        template.setNodeType("start");
        com.studioquack.common.dto.flow.NodeDto node = new com.studioquack.common.dto.flow.NodeDto();
        node.setId(id);
        node.setData(template);
        return node;
    }
}
