package com.studioquack.sqs.webhook;

import com.aventrix.jnanoid.jnanoid.NanoIdUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ExecutionRequirementsDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.dto.flow.TemplateScriptDto;
import com.studioquack.sqs.execution.transform.GenerateScriptService;
import com.studioquack.sqs.sandbox.SandboxExecutionPort;
import com.studioquack.sqs.sandbox.SandboxJobRequest;
import com.studioquack.sqs.sandbox.SandboxJobResponse;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.SandboxLimits;
import com.studioquack.sqs.builtinlib.BuiltinNodeLibraryManager;
import com.studioquack.sqs.sandbox.secrets.SecretResolver;
import com.studioquack.sqs.sandbox.stateful.StatefulCaps;
import com.studioquack.sqs.sandbox.stateful.StatefulInstance;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import com.studioquack.sqs.sandbox.stateful.StatefulSandboxExecution;
import com.studioquack.sqs.sandbox.stateful.StatefulStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Runs a bound webhook node (Python/JS/TS warm-pool script, custom Java/BYO container, or
 * stateful built-in library container) in {@code invocationMode = "ingress"} through the daemon's
 * existing sandbox and stateful-instance pipelines.
 *
 * <p>Resolves {@code $secretRef} handles just-in-time onto a private per-invocation map via
 * {@link SecretResolver#resolveInline(Object)} and supplies the resolved literals to the redactor
 * so secrets never touch logs or persisted stores.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SandboxWebhookNodeInvoker implements WebhookNodeInvoker {

    private static final long DEFAULT_INGRESS_WALL_MS = 12_000L;

    private final ObjectMapper objectMapper;
    private final SecretResolver secretResolver;
    private final GenerateScriptService generateScriptService;
    private final SandboxExecutionPort sandboxExecutionPort;
    private final StatefulInstanceManager statefulInstances;
    private final StatefulSandboxExecution statefulSandbox;
    private final Environment environment;

    @Override
    public WebhookInvocationResult invoke(WebhookInvocationInput input) {
        if (input == null || input.template() == null) {
            return WebhookInvocationResult.failure("MISSING_TEMPLATE", "bound webhook template snapshot is missing");
        }
        TemplateDto template = input.template();
        String jobId = "whing_" + NanoIdUtils.randomNanoId();
        String envId = StringUtils.defaultString(input.envId());
        String nodeId = StringUtils.defaultIfBlank(input.nodeId(), "ingress");

        // Private per-invocation map: resolve $secretRef handles in memory ONLY (never mutates stored snapshot).
        Map<String, Object> templateMap = objectMapper.convertValue(
                template, new TypeReference<Map<String, Object>>() {});
        Set<String> secretLiterals = new LinkedHashSet<>(secretResolver.resolveInline(templateMap));

        Map<String, Object> currentNodeMap = new LinkedHashMap<>();
        currentNodeMap.put("id", nodeId);
        currentNodeMap.put("data", templateMap);
        currentNodeMap.put("inputList", List.of());
        currentNodeMap.put("outputList", List.of());
        currentNodeMap.put("connectionList", List.of());

        Map<String, Object> sandboxContext = new LinkedHashMap<>();
        sandboxContext.put("invocationMode", "ingress");
        sandboxContext.put("activeSelectRole", StringUtils.defaultIfBlank(input.activeSelectRole(), "listen"));
        sandboxContext.put("webhookRequest", input.webhookRequest() == null ? Map.of() : input.webhookRequest());
        sandboxContext.put("envId", envId);
        sandboxContext.put("workflowId", StringUtils.defaultString(input.workflowId()));
        sandboxContext.put("workflowExecutionId", jobId);
        sandboxContext.put("currentNodeId", nodeId);
        sandboxContext.put("currentNode", currentNodeMap);
        Object configParams = templateMap.get("configurationParameters");
        sandboxContext.put("parameters", configParams == null ? List.of() : configParams);

        ExecutionRequirementsDto req = template.getExecutionRequirementsDto();
        String ociImageRef = req == null ? null : StringUtils.trimToNull(req.getOciImageRef());
        TemplateScriptDto scriptDto = template.getTemplateScript();
        String rawLang = scriptDto == null ? null : StringUtils.trimToNull(scriptDto.getLanguage());
        SandboxLanguage language = SandboxLanguage.fromWire(rawLang);

        SandboxJobResponse response;
        if (ociImageRef != null) {
            String contextBase64 = generateScriptService.bakeContextBase64(sandboxContext);
            SandboxJobRequest job = SandboxJobRequest.builder(jobId, language, "")
                    .inputVariables(Collections.emptyMap())
                    .executionContext(sandboxContext)
                    .envId(envId)
                    .workflowExecutionId(jobId)
                    .currentNodeId(nodeId)
                    .templateId(template.getId())
                    .contextBase64(contextBase64)
                    .limits(ingressLimits(req))
                    .ociImageRef(ociImageRef)
                    .stateful(true)
                    .secretLiterals(secretLiterals)
                    .build();
            response = dispatchStateful(job);
        } else {
            if (language == null) {
                return WebhookInvocationResult.failure(
                        SandboxJobResponse.ERROR_UNSUPPORTED_LANGUAGE,
                        "webhook ingress node has neither ociImageRef nor supported script language");
            }
            String userScript = scriptDto == null ? "" : StringUtils.defaultString(scriptDto.getScript());
            GenerateScriptService.BuildSourceResult built =
                    generateScriptService.buildScriptSourceFromContext(sandboxContext, rawLang, userScript);
            SandboxJobRequest job = SandboxJobRequest.builder(jobId, language, built.source())
                    .inputVariables(Collections.emptyMap())
                    .executionContext(sandboxContext)
                    .envId(envId)
                    .workflowExecutionId(jobId)
                    .currentNodeId(nodeId)
                    .templateId(template.getId())
                    .contextBase64(built.contextBase64())
                    .limits(ingressLimits(req))
                    .secretLiterals(secretLiterals)
                    .build();
            response = sandboxExecutionPort.execute(job);
        }

        if (response == null) {
            return WebhookInvocationResult.failure("SANDBOX_FAILED", "webhook ingress execution returned null");
        }
        if (response.failed()) {
            return WebhookInvocationResult.failure(response.errorCode(), response.errorMessage());
        }
        return WebhookInvocationResult.success(response.webhookOutcome());
    }

    private SandboxJobResponse dispatchStateful(SandboxJobRequest job) {
        String instanceEnv = instanceEnvFor(job);
        StatefulInstance instance = statefulInstances
                .resolveRunning(instanceEnv, job.ociImageRef()).orElse(null);
        if (instance == null) {
            instance = coldStartImageInstance(job, instanceEnv);
        }
        if (instance == null) {
            return SandboxJobResponse.failure(-1, "", "", Map.of(),
                    SandboxJobResponse.ERROR_SANDBOX_FAILED,
                    "stateful image unavailable for webhook ingress: " + job.ociImageRef(),
                    0L, null, SandboxJobResponse.FailureKind.CONTAINER);
        }
        return statefulSandbox.execute(job, instance);
    }

    private StatefulInstance coldStartImageInstance(SandboxJobRequest job, String envId) {
        boolean library = isBuiltinLibraryImage(job.ociImageRef());
        StatefulInstanceManager.StartResult res;
        try {
            if (library) {
                statefulInstances.reconcilePlatformSingleton(envId, job.ociImageRef(),
                        BuiltinNodeLibraryManager.INSTANCE_ID);
                res = statefulInstances.start(envId, job.ociImageRef(), true,
                        StatefulCaps.uncapped(),
                        BuiltinNodeLibraryManager.INSTANCE_ID, null, null, (Map<String, String>) null);
            } else {
                res = statefulInstances.start(envId, job.ociImageRef(), true, StatefulCaps.uncapped());
            }
        } catch (RuntimeException e) {
            log.warn("webhook-ingress: cold start threw for ref='{}': {}", job.ociImageRef(), e.toString());
            return null;
        }
        if (res == null || !StatefulStatus.RUNNING.equals(res.status())) {
            return null;
        }
        return statefulInstances.resolveRunning(envId, job.ociImageRef()).orElse(null);
    }

    private String instanceEnvFor(SandboxJobRequest job) {
        if (environment != null && isBuiltinLibraryImage(job.ociImageRef())) {
            String daemonEnv = environment.getProperty("sqs.channel.env-id", "").trim();
            if (!daemonEnv.isEmpty()) {
                return daemonEnv;
            }
        }
        return job.envId();
    }

    private boolean isBuiltinLibraryImage(String ref) {
        String libraryRef = environment == null
                ? BuiltinNodeLibraryManager.IMAGE_REF
                : environment.getProperty("sqs.builtin-library.image", BuiltinNodeLibraryManager.IMAGE_REF);
        return ref != null && ref.equals(libraryRef);
    }

    private static SandboxLimits ingressLimits(ExecutionRequirementsDto req) {
        int ramMb = req != null && req.getRam() > 0 ? req.getRam() : 0;
        return SandboxLimits.of(ramMb, DEFAULT_INGRESS_WALL_MS);
    }
}
