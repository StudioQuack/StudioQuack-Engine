package com.studioquack.sqs.docling;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.runtime.EngineVariableNames;
import com.studioquack.sqs.engine.strategy.helper.ActivitiBoundaryEventHelper;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.store.OperatorInputOverrideStore;
import com.studioquack.sqs.files.ClientFileStore;
import com.studioquack.sqs.execution.transform.ConnectionBuilder;
import com.studioquack.sqs.execution.transform.EdgeTransformPreStep;
import com.studioquack.sqs.execution.transform.ForeignEdgeInputResolver;
import com.studioquack.sqs.execution.transform.JoltTransformService;
import com.studioquack.sqs.execution.transform.TransformationCollector;
import com.studioquack.sqs.execution.pipeline.PreparedInbound;
import com.studioquack.sqs.execution.pipeline.TransformPipeline;
import com.studioquack.common.dto.flow.TransformationSpecDto;
import com.studioquack.sqs.runtime.DispatchExecutionRegistry;
import com.studioquack.sqs.runtime.DispatchResultCoordinator;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bean name {@code doclingDelegate} matches the {@code ${doclingDelegate}} expression
 * {@link com.studioquack.sqs.engine.strategy.helper.ActivitiBpmnElementHelper} emits for a
 * {@code docling-converter} node. This delegate is registered UNCONDITIONALLY, so a
 * {@code docling-converter} node is deployable on every tier.
 */
@Slf4j
@Component("doclingDelegate")
public class DoclingDelegate implements JavaDelegate {

    private static final String NODE_ACTIVITY_PREFIX = "node_";
    private static final String WAIT_ACTIVITY_PREFIX = "wait_";
    /** Fallback port value used only when the deployed graph has no resolvable wiring for this node. */
    private static final String DEFAULT_OUTPUT_PORT = "out";
    /**
     * The {@code docling_output} port's schema field per {@code activiti_bpmn_node_catalog.md}
     * (OUTPUT {@code docling_output} schema {@code texts}, list, required) — the shape a JOLT spec
     * authored against this node's output port expects to shift from.
     */
    private static final String OUTPUT_SCHEMA_FIELD = "texts";
    /**
     * Second output schema field next to {@link #OUTPUT_SCHEMA_FIELD}: the same converted content
     * as a FILE item ({@code .md}/{@code .txt} per {@code output_format}, base64 + store handle),
     * for consumers that need a file rather than the resolved text.
     */
    private static final String FILES_SCHEMA_FIELD = "files";
    /** Typed error code raised to the error gate when conversion cannot proceed. */
    private static final String ERROR_DOCLING_FAILED = "DOCLING_FAILED";

    /** The node's output-format parameter (matches the template seeder): markdown (default) | text. */
    private static final String OUTPUT_FORMAT_PARAM = "output_format";
    private static final String FORMAT_TEXT = "text";
    /** docling-serve {@code to_formats} value for the default markdown output. */
    private static final String DOCLING_FORMAT_MD = "md";

    /**
     * The {@code language} field on the node's sandbox log rows. A converter runs no user script, but
     * the field is what the log panels label the step with, so it names the engine doing the work.
     */
    private static final String LOG_LANGUAGE = "docling";


    private static final String ERROR_DOCLING_DISABLED = "DOCLING_DISABLED";

    /**
     * The node's engine-selection parameter (matches {@code DoclingTemplateSeeder.TARGET_ENV_SHORT_CODE}):
     * {@code "host"}/blank → this engine; any other value → a same-owner sibling engine id whose
     * own Docling side-car converts the document over the WireGuard overlay.
     */
    private static final String TARGET_ENV_PARAM = "target_env";
    /** {@code target_env} value meaning "the engine this workflow is deployed to". */
    private static final String TARGET_ENV_HOST = "host";

    private final ExecutionContextStore contextStore;
    private final RefOnlyVarBuilder refOnlyVarBuilder;
    private final DispatchExecutionRegistry dispatchRegistry;
    private final DispatchResultCoordinator resultCoordinator;
    private final DoclingBackend backend;
    private final com.studioquack.sqs.entitlement.EntitlementsHolder entitlementsHolder;
    private final ObjectMapper objectMapper;
    private final ConnectionBuilder connectionBuilder;
    private final JoltTransformService joltTransformService;
    private final TransformationCollector transformationCollector;
    private final EdgeTransformPreStep edgeTransformPreStep;
    private final TransformPipeline transformPipeline;
    private final ClientFileStore clientFileStore;
    private final com.studioquack.sqs.status.SandboxLogCallback sandboxLogCallback;
    private final com.studioquack.sqs.runtime.CancelledRunGuard cancelledRunGuard;
    private final DoclingDiagnostics diagnostics;
    /** Nullable: when absent (slice tests) no posture note is attached. */
    private final com.studioquack.sqs.sandbox.egress.SandboxEgressManager sandboxEgressManager;
    /** Cross-env sibling-engine conversions — nullable only in slice tests. */
    private final CrossEnvDoclingBackend crossEnvBackend;
    /**
     * Lazy engine facade for the operator skip pin ({@code __sq_skip_node}) — an
     * {@link ObjectProvider} for the same engine-bootstrap cycle-avoidance
     * {@code ScriptDispatchDelegate} documents; {@code null} only in slice tests.
     */
    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    /** Client HTTP read budget named in timeout failure messages ({@code sqs.docling.convert-timeout-seconds}). */
    private final int convertTimeoutSeconds;
    /**
     * Operator input overrides ({@code apply-inputs}), keyed by (run, node) — field-injected so the
     * constructor seam keeps its signature; {@code null} in bare slice tests, where no override
     * exists anyway.
     */
    private OperatorInputOverrideStore operatorInputOverrideStore;

    private final AtomicLong invocationCount = new AtomicLong();
    private ExecutorService dispatchPool;

    public DoclingDelegate(ExecutionContextStore contextStore,
                           RefOnlyVarBuilder refOnlyVarBuilder,
                           DispatchExecutionRegistry dispatchRegistry,
                           DispatchResultCoordinator resultCoordinator,
                           DoclingBackend backend,
                           com.studioquack.sqs.entitlement.EntitlementsHolder entitlementsHolder,
                           ObjectMapper objectMapper,
                           ConnectionBuilder connectionBuilder,
                           JoltTransformService joltTransformService,
                           TransformationCollector transformationCollector,
                           EdgeTransformPreStep edgeTransformPreStep,
                           TransformPipeline transformPipeline,
                           ClientFileStore clientFileStore,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           com.studioquack.sqs.status.SandboxLogCallback sandboxLogCallback,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           com.studioquack.sqs.runtime.CancelledRunGuard cancelledRunGuard,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           DoclingClient doclingClient,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           DoclingDiagnostics diagnostics,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           com.studioquack.sqs.sandbox.egress.SandboxEgressManager sandboxEgressManager,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           CrossEnvDoclingBackend crossEnvBackend,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           ObjectProvider<SqsEngine> sqsEngineProvider,
                           @Value("${sqs.docling.convert-timeout-seconds:900}") int convertTimeoutSeconds) {
        this.contextStore = contextStore;
        this.refOnlyVarBuilder = refOnlyVarBuilder;
        this.dispatchRegistry = dispatchRegistry;
        this.resultCoordinator = resultCoordinator;
        this.backend = backend;
        this.entitlementsHolder = entitlementsHolder;
        this.objectMapper = objectMapper;
        this.connectionBuilder = connectionBuilder;
        this.joltTransformService = joltTransformService;
        this.transformationCollector = transformationCollector;
        this.edgeTransformPreStep = edgeTransformPreStep;
        this.transformPipeline = transformPipeline;
        this.clientFileStore = clientFileStore;
        this.sandboxLogCallback = sandboxLogCallback;
        this.cancelledRunGuard = cancelledRunGuard;
        this.diagnostics = diagnostics;
        this.sandboxEgressManager = sandboxEgressManager;
        this.crossEnvBackend = crossEnvBackend;
        this.sqsEngineProvider = sqsEngineProvider;
        // Prefer the live RestTemplate budget when the HTTP client bean is present (same source of truth).
        this.convertTimeoutSeconds = doclingClient != null
                ? doclingClient.convertTimeoutSeconds()
                : convertTimeoutSeconds;
    }

    /** Optional override store — absent in slice tests (package-visible for the test seam). */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    void setOperatorInputOverrideStore(OperatorInputOverrideStore operatorInputOverrideStore) {
        this.operatorInputOverrideStore = operatorInputOverrideStore;
    }

    @PostConstruct
    void init() {
        // Virtual thread per conversion: the task is a blocking HTTP wait on the side-car, and a fixed
        // pool here capped concurrent document nodes daemon-wide (a slow conversion stalled every
        // sibling). The real bounds live on the side-car itself (DOCLING_NUM_THREADS /
        // DOCLING_SERVE_ENG_LOC_NUM_WORKERS) and on convertTimeoutSeconds.
        dispatchPool = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("docling-dispatch-", 0).factory());
    }

    @PreDestroy
    void shutdown() {
        if (dispatchPool != null) {
            dispatchPool.shutdownNow();
        }
    }

    @Override
    public void execute(DelegateExecution execution) {
        invocationCount.incrementAndGet();
        String activityId = execution.getCurrentActivityId();
        String nodeId = stripPrefix(activityId);
        String businessKey = execution.getProcessInstanceBusinessKey();
        String processInstanceId = execution.getProcessInstanceId();
        String candidateExecutionId = execution.getId();
        String waitActivityId = WAIT_ACTIVITY_PREFIX + nodeId;
        ProcessContextRef ref = readRef(execution);

        // No dispatchable context → park like the sandbox stub. Never throw (would dead-letter the job).
        if (ref == null || ref.getCurrentNodeRefId() == null) {
            log.info("doclingDelegate: no dispatchable context for node={} (ref={}); parking the ReceiveTask.",
                    nodeId, ref == null ? "<null>" : "no-node-ref");
            return;
        }

        String dispatchId = UUID.randomUUID().toString();
        dispatchRegistry.register(dispatchId, candidateExecutionId, processInstanceId, businessKey,
                nodeId, waitActivityId, ref.getEnvId());
        boolean retryAttempt = consumeRetryNodePin(execution, nodeId);
        log.info("doclingDelegate.dispatch node={} dispatchId={} businessKey={} retry={} (async → docling backend)",
                nodeId, dispatchId, businessKey, retryAttempt);
        // businessKey IS the run id. It is carried explicitly because the step's log rows are dropped
        // by the transport when the run id is blank, and a sealed context is not guaranteed to still
        // carry one — which is exactly how a failed conversion reached the daemon log and nothing else.
        dispatchPool.submit(() -> runDispatch(dispatchId, nodeId, ref, businessKey, retryAttempt));
    }

    private static boolean consumeRetryNodePin(DelegateExecution execution, String nodeId) {
        if (execution == null || nodeId == null || nodeId.isBlank()) {
            return false;
        }
        String varName = EngineVariableNames.retryNodeVariableName(nodeId);
        Object val = execution.getVariable(varName);
        if (val == null) {
            return false;
        }
        execution.removeVariable(varName);
        return Boolean.TRUE.equals(val) || "true".equalsIgnoreCase(String.valueOf(val));
    }

    /**
     * Reads the node's Docling Engine selection ({@code target_env}) straight from the deployed graph
     * — before {@code createCurrentNode} runs, so the routing decision is available ahead of the
     * entitlement gate. Returns the trimmed value, or {@code null} when the node/parameter is absent
     * or blank (older deployed graphs predate the parameter — they mean "host").
     */
    private String resolveTargetEnv(ExecutionContext ctx, String nodeId) {
        if (ctx.getNodes() == null) {
            return null;
        }
        for (NodeDto node : ctx.getNodes()) {
            if (!nodeId.equals(node.getId()) || node.getData() == null
                    || node.getData().getConfigurationParameters() == null) {
                continue;
            }
            for (ParameterDto p : node.getData().getConfigurationParameters()) {
                if (TARGET_ENV_PARAM.equals(p.getShortCode())
                        && p.getValue() instanceof String s && !s.isBlank()) {
                    return s.trim();
                }
            }
        }
        return null;
    }

    /**
     * The node's {@code output_format} configuration: {@code markdown} (default) or {@code text}.
     */
    private String resolveOutputFormat(ExecutionContext ctx, String nodeId) {
        if (ctx.getNodes() == null) {
            return null;
        }
        for (NodeDto node : ctx.getNodes()) {
            if (!nodeId.equals(node.getId()) || node.getData() == null
                    || node.getData().getConfigurationParameters() == null) {
                continue;
            }
            for (ParameterDto p : node.getData().getConfigurationParameters()) {
                if (OUTPUT_FORMAT_PARAM.equals(p.getShortCode())
                        && p.getValue() instanceof String s && !s.isBlank()) {
                    String v = s.trim().toLowerCase(java.util.Locale.ROOT);
                    // "md" is the docling-serve spelling of the same format; both map to markdown.
                    return (FORMAT_TEXT.equals(v) || "txt".equals(v) || "plain".equals(v))
                            ? FORMAT_TEXT : null;
                }
            }
        }
        return null;
    }

    /**
     * {@code GET /v1/result/{taskId}} returns a {@code ConvertDocumentResponse} JSON envelope — the
     * content sits in {@code document.md_content} / {@code document.text_content} depending on the
     * requested {@code to_formats}. A body that is not a JSON object (older servers, test doubles) is
     * already the content and passes through untouched.
     */
    private String extractConvertedText(String body, String outputFormat) {
        if (body == null) {
            return null;
        }
        String trimmed = body.strip();
        if (!trimmed.startsWith("{")) {
            return body;
        }
        try {
            com.fasterxml.jackson.databind.JsonNode document =
                    objectMapper.readTree(trimmed).path("document");
            if (document.isMissingNode() || document.isNull()) {
                return body; // an envelope we do not recognise — keep the raw body rather than nothing
            }
            String preferred = FORMAT_TEXT.equals(outputFormat) ? "text_content" : "md_content";
            com.fasterxml.jackson.databind.JsonNode content = document.path(preferred);
            if (content.isTextual()) {
                return content.asText();
            }
            // The requested flavour missing but the other present (e.g. an audio transcription only
            // fills text_content): take what exists rather than sealing an empty output.
            for (String fallback : new String[] {"md_content", "text_content"}) {
                com.fasterxml.jackson.databind.JsonNode alt = document.path(fallback);
                if (alt.isTextual()) {
                    return alt.asText();
                }
            }
        } catch (Exception notJson) {
            log.debug("doclingDelegate: response body is not a JSON envelope; using it verbatim");
        }
        return body;
    }

    /** Off-engine: load context, convert via the backend, seal the output, resume the parked token. */
    private void runDispatch(String dispatchId, String nodeId, ProcessContextRef ref, String runId) {
        runDispatch(dispatchId, nodeId, ref, runId, false);
    }

    private void runDispatch(String dispatchId, String nodeId, ProcessContextRef ref, String runId,
                             boolean retryAttempt) {
        long startedNanos = System.nanoTime();
        ExecutionContext ctx = null;
        try {
            ctx = contextStore.loadByRef(ref).orElse(null);
            if (ctx == null) {
                log.warn("doclingDelegate: context vanished for node={} ref={}; failing to error end.",
                        nodeId, ref.getCurrentNodeRefId());
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_FAILED, "context unavailable");
                return;
            }
            ctx.setCurrentNodeId(nodeId);
            // LoadByRef rehydrates currentNode from the context blob; createCurrentNode then early-returns on
            // currentNode != null and NEVER moves nextNodeInputList onto currentNode.inputList.
            ctx.setCurrentNode(null);

            // Operator "Skip & Continue" on a run held at hold_<nodeId> — the same short-circuit
            // ScriptDispatchDelegate runs: the skip-step command pinned __sq_skip_node on the instance and
            // requeued the hold, which loops the token back here. Consume the pin and skip BEFORE the
            // entitlement gate / any input load — no conversion runs, no outputs persist.
            if (consumeSkipNodePin(dispatchId, nodeId)) {
                handleSkippedNode(dispatchId, nodeId, ref, ctx, runId, startedNanos);
                return;
            }

            // Cross-env routing FIRST, before the local entitlement gate: a node pinned to a sibling engine
            // converts THERE, so this environment's own Docling toggle is irrelevant to it. host/blank/this-
            // env all mean local.
            String targetEnv = resolveTargetEnv(ctx, nodeId);
            boolean remoteTarget = targetEnv != null && !TARGET_ENV_HOST.equalsIgnoreCase(targetEnv)
                    && !targetEnv.equals(ref.getEnvId());
            if (remoteTarget && crossEnvBackend == null) {
                String unavailable = "cross-environment Docling is unavailable on this daemon build";
                reportFinished(ctx, runId, nodeId, "FAILED", startedNanos, unavailable, null, null,
                        com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.ENGINE, retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_FAILED, unavailable);
                return;
            }

            // Fail-closed on the operator's setting, and fail LOUDLY — BEFORE any input load, transform work,
            // or the inbound retry, so a disabled environment fails fast and never spends a read or a
            // transform-sandbox hop. Converting anyway would ignore an explicit choice; passing the document
            // through unconverted would produce silently wrong output.
            if (!remoteTarget && !entitlementsHolder.doclingEnabled()) {
                log.warn("doclingDelegate: node={} needs Docling but it is disabled for this environment "
                        + "(Engine Settings → General)", nodeId);
                String disabled = "Docling is disabled for this environment — enable it in Engine Settings → General";
                reportFinished(ctx, runId, nodeId, "FAILED", startedNanos, disabled, null, null,
                        com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.ENGINE, retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_DISABLED, disabled);
                return;
            }

            // Same order as ScriptDispatchDelegate: load upstream inputs onto nextNodeInputList BEFORE
            // createCurrentNode so that method moves them onto currentNode.inputList — otherwise
            // reportFinished reads empty IO and sandbox logs show blank in/out. The producing node's
            // persistOutputs and this read run on DIFFERENT threads (the producer's sandbox-dispatch vs this
            // docling-dispatch), so a fast producer can still be committing when we read.
            List<InOutValueDto> upstreamInputs = loadInputsWithRetry(ref, nodeId);
            // DuckTape Add-Node: pinned foreign-source outputs ride along onto this node's input
            // list (same contract as ScriptDispatchDelegate) so their mapping rows resolve.
            ForeignEdgeInputResolver.Resolution foreign = ForeignEdgeInputResolver.resolveWithCarriers(
                    contextStore, ref, nodeId, ctx.getEdges(), ctx.getWorkflowVariables(), ctx.getProjectVariables());
            List<InOutValueDto> mergedInputs = mergeForeignInputs(
                    upstreamInputs == null ? List.of() : upstreamInputs, foreign.rows());
            // Repair: the operator's apply-inputs override for (run, node) folds LAST, so it wins on every re-
            // run path (retry, replay); an empty override means the operator cleared the inbound list. null
            // when no override was stored.
            mergedInputs = foldOperatorOverride(mergedInputs, loadOperatorOverride(ref, ctx, nodeId), nodeId);
            // One DuckTape screen → one message: remembered so each pinned row folds into the
            // message of the connection line it was pinned on once the inbound JOLT below has shaped it.
            ctx.setPinnedInputCarriers(ForeignEdgeInputResolver.pinnedCarriers(mergedInputs, foreign));
            ctx.setNextNodeInputList(mergedInputs);
            // Resolve currentNode + its connection/edge-transform wiring from the deployed graph — the same
            // Phase-1 step DefaultTransformPipeline#prepareInbound runs for a sandbox node — so saveOutputs
            // below can address the converted text to whatever.
            ctx.createCurrentNode();
            NodeDto currentNode = ctx.getCurrentNode();
            if (currentNode != null && CollectionUtils.isNotEmpty(ctx.getNodes())
                    && CollectionUtils.isNotEmpty(ctx.getEdges())) {
                connectionBuilder.buildConnections(currentNode, ctx.getNodes(), ctx.getEdges());
            }

            // Inbound leg of "transform before AND after Docling" — collect the inbound specs ONCE for both
            // legs. DuckTape "Simple" (jolt): pre-bake onto currentNode.inputList BEFORE the document
            // resolves.
            List<TransformationSpecDto> inboundSpecs = collectInboundSpecs(ctx);
            applyInboundEdgeJolt(currentNode, inboundSpecs);
            foldPinnedRows(ctx, currentNode);
            // Any OTHER DuckTape language (script): a real transformation SANDBOX runs per wire — the same
            // EdgeTransformPreStep a sandbox node runs. Docling has no node language, so every script edge is
            // a language mismatch → a dedicated EgressPolicy.NONE hop whose result replaces the inputList
            // entry before conversion.
            if (!runInboundScriptTransformSandbox(ctx, currentNode, inboundSpecs,
                    dispatchId, runId, nodeId, startedNanos, retryAttempt)) {
                return;
            }
            applyInboundPortSchemaFilter(ctx);

            DocumentInput doc = resolveDocument(ctx, ref, nodeId);
            if (doc == null) {
                // No document: a files-named port left empty, OR a file-shaped wrapper with no bytes ({files:[]},
                // blank {name,data}, Duckbot {msg, files:[]}) even when the port is named output. Skip BEFORE
                // backend.convert (that is what starts the sidecar).
                log.info("doclingDelegate: node={} files input is empty — skipping conversion ({})",
                        nodeId, describeInbound(ctx));
                List<InOutValueDto> skippedOutputs = buildOutputs(currentNode, nodeId, "", null,
                        resolveOutputFormat(ctx, nodeId), runId);
                if (currentNode != null) {
                    currentNode.setOutputList(skippedOutputs);
                }
                saveOutputs(ref, skippedOutputs);
                Map<String, Object> engineVars = refOnlyVarBuilder.toEngineVariables(ctx);
                reportFinished(ctx, runId, nodeId, "COMPLETED", startedNanos, null,
                        "no file input — conversion skipped", null,
                        com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.NONE, retryAttempt);
                resultCoordinator.deliverSuccess(dispatchId, engineVars, Map.of());
                return;
            }
            if (doc.bytes() == null || doc.bytes().length == 0) {
                log.warn("doclingDelegate: node={} has no resolvable document bytes — failing closed ({})",
                        nodeId, describeInbound(ctx));
                String empty = "no document input (file missing or empty)";
                reportFinished(ctx, runId, nodeId, "FAILED", startedNanos, empty, null, null,
                        com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.SCRIPT, retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_FAILED, empty);
                return;
            }
            // Audio/video pre-flight: docling's extension fallback (_mime_from_extension) has NO audio/video
            // branches, so content-sniffing is the ONLY way those formats are admitted — bytes that don't
            // carry the container magic are rejected upstream with. Say what actually arrived (a text payload
            // from a mis-wired port, a truncated browser recording, ...) while the evidence is still local.
            String avMismatch = avContainerMismatch(doc.filename(), doc.bytes());
            if (avMismatch != null) {
                log.warn("doclingDelegate: node={} {} ({})", nodeId, avMismatch, describeInbound(ctx));
                String msg = avMismatch + " — Docling sniffs audio/video by content and would "
                        + "reject the upload as 'File format not allowed'; no conversion attempted";
                reportFinished(ctx, runId, nodeId, "FAILED", startedNanos, msg, null, null,
                        com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.SCRIPT, retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_FAILED, msg);
                return;
            }
            // The node only becomes visibly RUNNING once a STARTED row reaches the backend. Emitted
            // here rather than at dispatch time so the row carries the resolved document, and so a
            // conversion that dies mid-flight has a start to pair its failure with.
            reportStarted(ctx, runId, nodeId);
            if (remoteTarget) {
                log.info("doclingDelegate: node={} converting on sibling engine {} — daemon→daemon "
                        + "over the WireGuard overlay (target's Network Access Control rule on port "
                        + "5001 gates the traffic)", nodeId, targetEnv);
            }
            // The node's output_format (markdown default / text) drives BOTH the side-car's
            // to_formats and the sealed file's extension + MIME (.md/text/markdown vs
            // .txt/text/plain). Blank/unknown values fall back to markdown.
            String outputFormat = resolveOutputFormat(ctx, nodeId);
            String toFormats = FORMAT_TEXT.equals(outputFormat) ? FORMAT_TEXT : DOCLING_FORMAT_MD;
            String rawBody = remoteTarget
                    ? crossEnvBackend.convert(targetEnv, doc.bytes(), doc.filename(), doc.mimeType(), toFormats)
                    : backend.convert(doc.bytes(), doc.filename(), doc.mimeType(), ref, toFormats);
            // The side-car answers a ConvertDocumentResponse JSON envelope; the converted content
            // lives in document.md_content / text_content. A bare-text body (older servers, mocks)
            // passes through unchanged.
            String markdown = extractConvertedText(rawBody, outputFormat);

            // Also set currentNode.outputList so reportFinished's sandbox-log IO is populated (still
            // redacted).
            List<InOutValueDto> outputs = buildOutputs(currentNode, nodeId, markdown, doc.filename(),
                    outputFormat, runId);
            try {
                // A retried / replayed converter with saved operator output edits (apply-inputs)
                // hands those on instead of the conversion — as Skip & Continue does.
                if (!applyOperatorOutputOverride(ref, ctx, runId, nodeId)) {
                    if (currentNode != null) {
                        currentNode.setOutputList(outputs);
                    }
                    saveOutputs(ref, outputs);
                }
            } catch (Exception sealFailure) {
                // NOT best-effort: an unsealed output is unreachable downstream while the run
                // reports COMPLETED — the exact silent loss this delegate's IO mirror exists to
                // make visible. Fail the node loudly (ENGINE: conversion worked, the store did not).
                String sealMessage = "converted output could not be sealed for node=" + nodeId
                        + ": " + sealFailure;
                log.warn("doclingDelegate: {}", sealMessage);
                reportFinished(ctx, runId, nodeId, "FAILED", startedNanos, sealMessage, null, null,
                        com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.ENGINE, retryAttempt);
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_FAILED, sealMessage);
                return;
            }

            // Resume the parked ReceiveTask with a fresh REF-ONLY carry (no gateway routing on a converter).
            Map<String, Object> engineVars = refOnlyVarBuilder.toEngineVariables(ctx);
            reportFinished(ctx, runId, nodeId, "COMPLETED", startedNanos, null,
                    "converted " + (markdown == null ? 0 : markdown.length()) + " character(s) of text",
                    null, com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.NONE, retryAttempt);
            resultCoordinator.deliverSuccess(dispatchId, engineVars, Map.of());
            log.info("doclingDelegate.done node={} dispatchId={} converted-chars={}",
                    nodeId, dispatchId, markdown == null ? 0 : markdown.length());
        } catch (Exception e) {
            log.warn("doclingDelegate.dispatch-error node={} dispatchId={}: {}", nodeId, dispatchId, e.toString());
            // "Unexpected end of file from server" means the side-car died, not that the document was
            // rejected. Ask the runtime which it was, so the node log names the real cause instead of
            // leaving the user with a transport symptom they cannot act on.
            DoclingDiagnostics.Diagnosis diagnosis = safeDiagnose();
            String message = failureMessage(e, diagnosis);
            var kind = diagnosis != null && diagnosis.resourceExhaustion()
                    ? com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.CONTAINER
                    : com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.INFRA;
            try {
                reportFinished(ctx, runId, nodeId, "FAILED", startedNanos, message, null,
                        diagnosis == null ? null : diagnosis.logTail(), kind, retryAttempt);
            } catch (Exception logFailed) {
                log.debug("doclingDelegate: failure-log mirror skipped node={} ({})", nodeId, logFailed.toString());
            }
            try {
                resultCoordinator.deliverFailure(dispatchId, ERROR_DOCLING_FAILED, message);
            } catch (Exception ignore) {
                // token stays parked → boot hydration / orphan sweeper owns it
            }
        }
    }

    /**
     * Operator "Skip &amp; Continue" short-circuit for a held-then-requeued converter node: no
     * conversion, a SKIPPED log row, then resume the parked token. An authored {@code SQ_SKIP} error
     * boundary on the node gets the typed failure the error gate would have raised — the token routes
     * to the step's own skip handler.
     */
    private void handleSkippedNode(String dispatchId, String nodeId, ProcessContextRef ref,
                                   ExecutionContext ctx, String runId, long startedNanos) {
        // Repair: a SKIPPED row must still show the operator-edited parameters from apply-inputs (no
        // conversion ran, so nothing else would surface them). Only when an override exists — otherwise
        // the row keeps its honest no-IO shape.
        mirrorOperatorOverrideOnSkippedRow(ctx, runId, nodeId);
        if (ActivitiBoundaryEventHelper.hasAuthoredErrorBoundary(
                findNode(ctx == null ? null : ctx.getNodes(), nodeId),
                EngineVariableNames.SKIP_ERROR_CODE)) {
            // The authored skip handler must see the operator's output override too: fan out +
            // persist BEFORE delivering the SQ_SKIP failure (same as ScriptDispatchDelegate).
            applyOperatorOutputOverride(ref, ctx, runId, nodeId);
            reportFinished(ctx, runId, nodeId, "SKIPPED", startedNanos,
                    "skipped by operator — routed to the step's skip handler", null, null,
                    com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.NONE);
            resultCoordinator.deliverFailure(dispatchId, EngineVariableNames.SKIP_ERROR_CODE,
                    "skipped by operator");
            return;
        }
        // Repair (output overrides): the operator may have edited/added the failed node's OUTPUT
        // parameters via apply-inputs. Those become the skipped node's outputs — fanned out per wire and
        // sealed onto the dispatch row exactly like a successful conversion, so the next node(s) read them
        // from loadInputsTargetingNode.
        boolean outputsApplied = applyOperatorOutputOverride(ref, ctx, runId, nodeId);
        reportFinished(ctx, runId, nodeId, "SKIPPED", startedNanos,
                outputsApplied
                        ? "skipped by operator — continued with operator-edited outputs"
                        : "skipped by operator — continued with no outputs", null, null,
                com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.NONE);
        Map<String, Object> engineVars = refOnlyVarBuilder.toEngineVariables(ctx);
        resultCoordinator.deliverSuccess(dispatchId, engineVars, Map.of());
        log.info("doclingDelegate.skipped node={} dispatchId={} — operator Skip & Continue; "
                + "no conversion ran", nodeId, dispatchId);
    }

    /**
     * Fans out + persists the operator's apply-inputs OUTPUT override as the skipped node's outputs
     * (exactly like a successful conversion: finalizeOutputs → saveOutputs), so the next node(s) read
     * them from {@code loadInputsTargetingNode}. The whole map is operator-supplied, so exactly those
     * ports are exempted from the output port-schema filter for this fan-out (transient ctx flag —
     * never persisted).
     *
     * @return {@code true} only when outputs actually fanned out and persisted (stays honest: an
     * override that produced no wire rows reads back as "no outputs").
     */
    private boolean applyOperatorOutputOverride(ProcessContextRef ref, ExecutionContext ctx,
                                                String runId, String nodeId) {
        List<InOutValueDto> outputOverride = loadOperatorOutputOverride(ctx, runId, nodeId);
        if (outputOverride == null || outputOverride.isEmpty() || transformPipeline == null || ctx == null) {
            return false;
        }
        resolveSkippedNodeForOutputFanOut(ctx, nodeId);
        Map<String, Object> operatorOutputs = toPortValueMap(outputOverride);
        ctx.setOperatorOverrideOutputPorts(new java.util.LinkedHashSet<>(operatorOutputs.keySet()));
        transformPipeline.finalizeOutputs(ctx, operatorOutputs);
        NodeDto skippedNode = ctx.getCurrentNode();
        if (skippedNode == null || CollectionUtils.isEmpty(skippedNode.getOutputList())) {
            return false; // nothing fanned out → stay honest: no outputs
        }
        saveOutputs(ref, skippedNode.getOutputList());
        return true;
    }

    /**
     * Gives the skip path a fan-out-capable current node (same Phase-1 step the normal dispatch
     * runs before {@code saveOutputs}): resolve it from the loaded graph and build its connection
     * list so {@code finalizeOutputs} addresses the operator's outputs to downstream targets.
     */
    private void resolveSkippedNodeForOutputFanOut(ExecutionContext ctx, String nodeId) {
        if (ctx.getCurrentNode() == null) {
            ctx.setCurrentNodeId(nodeId);
            ctx.createCurrentNode();
        }
        NodeDto node = ctx.getCurrentNode();
        // NodeDto initialises connectionList EMPTY (never null) — rebuild whenever unwired;
        // buildConnections replaces the list wholesale, so a re-build is idempotent.
        if (node != null && CollectionUtils.isEmpty(node.getConnectionList())
                && connectionBuilder != null
                && CollectionUtils.isNotEmpty(ctx.getNodes()) && CollectionUtils.isNotEmpty(ctx.getEdges())) {
            connectionBuilder.buildConnections(node, ctx.getNodes(), ctx.getEdges());
        }
    }

    /**
     * The operator's output override as the {@code portValue → value} map
     * {@code finalizeOutputs} consumes. Rows without a port name cannot bind to a port and are
     * dropped.
     */
    private static Map<String, Object> toPortValueMap(List<InOutValueDto> outputOverride) {
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        for (InOutValueDto row : outputOverride) {
            if (row == null) {
                continue;
            }
            String port = StringUtils.firstNonEmpty(row.getPortValue(), row.getTargetPortValue());
            if (StringUtils.isNotEmpty(port)) {
                out.put(port, row.getValue());
            }
        }
        return out;
    }

    /**
     * The operator's sealed OUTPUT override for {@code (runId, nodeId)}, or {@code null} when no store
     * is wired (slice tests) or none was stored. Read by the skip short-circuit and by a successful
     * re-run, where the saved outputs replace the conversion.
     */
    private List<InOutValueDto> loadOperatorOutputOverride(ExecutionContext ctx, String runId,
                                                           String nodeId) {
        if (operatorInputOverrideStore == null) {
            return null;
        }
        String effectiveRunId = StringUtils.isNotBlank(runId)
                ? runId
                : (ctx == null ? null : ctx.getWorkflowExecutionId());
        if (effectiveRunId == null || nodeId == null) {
            return null;
        }
        return operatorInputOverrideStore.loadOutputOverride(effectiveRunId, nodeId).orElse(null);
    }

    /**
     * Reads and CONSUMES the operator skip pin ({@link EngineVariableNames#SKIP_NODE}) for this
     * dispatch's process instance: {@code true} only when the pin names exactly {@code nodeId}. The
     * pin is removed BEFORE the short-circuit runs (best-effort — {@link SqsEngine#removeVariables}
     * already swallows a stale-scope race) so a dispatch that dies midway can never leave a stale pin
     * behind to skip a LATER run of the same node.
     */
    private boolean consumeSkipNodePin(String dispatchId, String nodeId) {
        SqsEngine engine = sqsEngineProvider == null ? null : sqsEngineProvider.getIfAvailable();
        if (engine == null || nodeId == null) {
            return false;
        }
        String pid = dispatchRegistry.find(dispatchId)
                .map(DispatchExecutionRegistry.WaitState::processInstanceId)
                .orElse(null);
        if (pid == null) {
            return false;
        }
        Object pinned = engine.getProcessVariable(pid, EngineVariableNames.SKIP_NODE);
        if (!nodeId.equals(pinned)) {
            return false;
        }
        engine.removeVariables(pid, Set.of(EngineVariableNames.SKIP_NODE));
        return true;
    }

    /** The graph node with {@code nodeId}, or {@code null} when the loaded context does not carry it. */
    private static NodeDto findNode(List<NodeDto> nodes, String nodeId) {
        if (nodes == null || nodeId == null) {
            return null;
        }
        for (NodeDto node : nodes) {
            if (node != null && nodeId.equals(node.getId())) {
                return node;
            }
        }
        return null;
    }

    private DoclingDiagnostics.Diagnosis safeDiagnose() {
        if (diagnostics == null) {
            return null;
        }
        try {
            return diagnostics.diagnose();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * A non-PHI failure line: the exception class plus either a client-read-timeout explanation or,
     * when the runtime confirmed the side-car died, what actually killed it. Only the class name of
     * the exception is used — a Docling error body can echo document content, which must not reach the
     * backend's log store.
     */
    private String failureMessage(Exception e, DoclingDiagnostics.Diagnosis diagnosis) {
        String base = "docling conversion failed: " + e.getClass().getSimpleName();
        if (DoclingDiagnostics.isClientReadTimeout(e)) {
            return base + " — " + DoclingDiagnostics.formatClientTimeoutMessage(convertTimeoutSeconds);
        }
        // The exception message is the real cause (ensureRunning / convert). Diagnosis is only appended
        // when the runtime confirmed a kill — after a failed start the sidecar is.
        String detail = e.getMessage();
        if (diagnosis != null && diagnosis.resourceExhaustion()) {
            return base + " — " + diagnosis.summary();
        }
        if (detail != null && !detail.isBlank()) {
            return base + " — " + detail;
        }
        return diagnosis == null ? base : base + " — " + diagnosis.summary();
    }

    /** RUNNING marker for the live execution view; identical contract to a sandbox node's. */
    private void reportStarted(ExecutionContext ctx, String runId, String nodeId) {
        String reportRunId = reportRunId(ctx, runId);
        if (sandboxLogCallback == null || reportRunId == null
                || isSuppressedByCancellation(reportRunId, nodeId)) {
            return;
        }
        try {
            sandboxLogCallback.reportStarted(new com.studioquack.sqs.status.SandboxLogCallback.SandboxStartRecord(
                    reportRunId, nodeId, nodeName(ctx), LOG_LANGUAGE));
        } catch (Exception e) {
            log.debug("doclingDelegate: started mirror skipped node={} ({})", nodeId, e.toString());
        }
    }

    /**
     * The run id the step's log rows are filed under: the engine's business key, falling back to the
     * loaded context. Blank means the transport drops the row, so this never silently returns blank —
     * an unreportable step is logged, because "no row on the backend" was previously indistinguishable
     * from "the step never ran".
     */
    private String reportRunId(ExecutionContext ctx, String runId) {
        if (runId != null && !runId.isBlank()) {
            return runId;
        }
        String fromContext = ctx == null ? null : ctx.getWorkflowExecutionId();
        if (fromContext != null && !fromContext.isBlank()) {
            return fromContext;
        }
        log.warn("doclingDelegate: no run id available — this step cannot be reported to the backend");
        return null;
    }

    /**
     * Terminal log for the node, in the same shape a sandbox script node posts, so the Monitor
     * echoes/logs panels and the designer overlay treat a Docling step like any other step.
     * {@code stdout} carries a short human summary and {@code stderr} the side-car's log tail on a
     * crash; the converted markdown itself is NOT mirrored — it is the node's output, and the backend
     * log store is not where user document text belongs.
     */
    private void reportFinished(ExecutionContext ctx, String runId, String nodeId, String status,
                               long startedNanos, String errorMessage, String stdout, String stderr,
                               com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind failureKind) {
        reportFinished(ctx, runId, nodeId, status, startedNanos, errorMessage, stdout, stderr, failureKind, false);
    }

    private void reportFinished(ExecutionContext ctx, String runId, String nodeId, String status,
                               long startedNanos, String errorMessage, String stdout, String stderr,
                               com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind failureKind,
                               boolean retryAttempt) {
        String reportRunId = reportRunId(ctx, runId);
        if (reportRunId == null || isSuppressedByCancellation(reportRunId, nodeId)) {
            return;
        }
        if (sandboxLogCallback == null) {
            log.warn("doclingDelegate: no sandbox-log transport — step {} of run {} stays invisible to "
                    + "the backend", nodeId, reportRunId);
            return;
        }
        try {
            NodeDto node = ctx == null ? null : ctx.getCurrentNode();
            sandboxLogCallback.report(new com.studioquack.sqs.status.SandboxLogCallback.SandboxLogRecord(
                    reportRunId, nodeId, nodeName(ctx), status,
                    null, (System.nanoTime() - startedNanos) / 1_000_000L, LOG_LANGUAGE, null,
                    stdout, stderr, errorMessage,
                    node == null ? null : redactHeavyFilePayloads(node.getInputList()),
                    node == null ? null : redactHeavyFilePayloads(node.getOutputList()),
                    null,
                    unknownInputParameters(ctx),
                    (failureKind == null
                            ? com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.NONE
                            : failureKind).name(),
                    egressPostureNote(),
                    null,
                    retryAttempt ? Boolean.TRUE : null));

            log.info("doclingDelegate.reported run={} node={} status={} kind={}",
                    reportRunId, nodeId, status, failureKind);
        } catch (Exception e) {
            log.warn("doclingDelegate: terminal log mirror failed node={} ({})", nodeId, e.toString());
        }
    }

    /**
     * The same run-log sentence script nodes carry when the host's overlay-egress enforcement is
     * DEGRADED. Delegates to
     * {@link com.studioquack.sqs.sandbox.egress.SandboxEgressManager#degradedRunLogNote}.
     */
    private String egressPostureNote() {
        try {
            return sandboxEgressManager == null ? null : sandboxEgressManager.degradedRunLogNote();
        } catch (RuntimeException e) {
            log.debug("doclingDelegate: egress posture note skipped ({})", e.toString());
            return null;
        }
    }

    private boolean isSuppressedByCancellation(String runId, String nodeId) {
        if (cancelledRunGuard != null && cancelledRunGuard.isCancelled(runId)) {
            log.debug("doclingDelegate: suppressing late log for cancelled run wfe={} node={}", runId, nodeId);
            return true;
        }
        return false;
    }

    private static String nodeName(ExecutionContext ctx) {
        NodeDto node = ctx == null ? null : ctx.getCurrentNode();
        return node == null || node.getData() == null ? null : node.getData().getLabel();
    }

    /** Non-empty dropped inbound parameters for the sandbox-log mirror; null when nothing dropped. */
    private static java.util.List<String> unknownInputParameters(ExecutionContext ctx) {
        if (ctx == null || ctx.getDroppedInputParameters() == null || ctx.getDroppedInputParameters().isEmpty()) {
            return null;
        }
        return ctx.getDroppedInputParameters();
    }

    /**
     * Prunes inbound wire values against each target port's schema after JOLT / edge-transform
     * pre-steps — same contract as {@link ScriptDispatchDelegate} before secrets / conversion.
     */
    private void applyInboundPortSchemaFilter(ExecutionContext ctx) {
        if (transformPipeline == null || ctx == null || ctx.getCurrentNode() == null
                || CollectionUtils.isEmpty(ctx.getCurrentNode().getInputList())) {
            return;
        }
        Map<String, Object> inputVars = new LinkedHashMap<>();
        for (InOutValueDto in : ctx.getCurrentNode().getInputList()) {
            String key = StringUtils.firstNonEmpty(in.getTargetPortValue(), in.getPortValue());
            if (StringUtils.isNotEmpty(key)) {
                inputVars.put(key, in.getValue());
            }
        }
        PreparedInbound prepared = new PreparedInbound(inputVars, List.of(), Map.of(), Set.of());
        transformPipeline.filterInboundAgainstPortSchemas(ctx, prepared);
    }

    /**
     * Deep-copy the node's IO and strip file bytes before it is mirrored. A conversion input is a whole
     * document, so mirroring it verbatim would push multi-MB base64 at the log endpoint.
     */
    @SuppressWarnings("unchecked")
    private Object redactHeavyFilePayloads(Object payload) {
        if (payload == null) {
            return null;
        }
        try {
            Object copy = objectMapper.convertValue(payload, Object.class);
            stripDataFields(copy);
            return copy;
        } catch (Exception e) {
            return null; // prefer an empty echo over a timed-out multi-MB push
        }
    }

    @SuppressWarnings("unchecked")
    private static void stripDataFields(Object node) {
        if (node instanceof Map<?, ?> map) {
            Map<Object, Object> m = (Map<Object, Object>) map;
            Object data = m.get("data");
            if (data instanceof String s && s.length() > 512 && m.containsKey("fileId")) {
                m.put("data", "<omitted " + s.length() + " chars; see fileId>");
            } else if (data instanceof String s && s.length() > 64_000) {
                m.put("data", "<omitted " + s.length() + " chars>");
            }
            for (Object value : m.values()) {
                stripDataFields(value);
            }
        } else if (node instanceof List<?> list) {
            for (Object element : list) {
                stripDataFields(element);
            }
        }
    }

    /**
     * Collects the current node's inbound transform specs ONCE for both inbound legs (JOLT pre-bake
     * + script transform sandbox). Best-effort: a collection failure is a warning and an empty list,
     * so a malformed edge can never break the conversion itself.
     */
    private List<TransformationSpecDto> collectInboundSpecs(ExecutionContext ctx) {
        try {
            return transformationCollector == null
                    ? List.of() : transformationCollector.collectForCurrentNode(ctx);
        } catch (Exception e) {
            log.warn("doclingDelegate: inbound transform collection failed node={}: {}",
                    ctx == null ? "<null>" : ctx.getCurrentNodeIdSafe(), e.toString());
            return List.of();
        }
    }

    /**
     * Specs come from the same {@link TransformationCollector} the sandbox path uses, so language
     * resolution (jolt default, script edges excluded) matches every other node. Best-effort: a failed
     * spec is a soft warning and the value stays untouched (Invariant 11).
     */
    private void applyInboundEdgeJolt(NodeDto currentNode, List<TransformationSpecDto> specs) {
        if (currentNode == null || CollectionUtils.isEmpty(currentNode.getInputList())
                || CollectionUtils.isEmpty(specs)) {
            return;
        }
        for (TransformationSpecDto spec : specs) {
            if (spec == null || !"jolt".equalsIgnoreCase(spec.getKind())
                    || CollectionUtils.isEmpty(spec.getJoltSpec())) {
                continue;
            }
            if (StringUtils.isNotEmpty(spec.getDirection()) && !"input".equalsIgnoreCase(spec.getDirection())) {
                continue;
            }
            for (InOutValueDto entry : currentNode.getInputList()) {
                if (!inputListEntryMatchesSpec(entry, spec)) {
                    continue;
                }
                try {
                    entry.setValue(joltTransformService.transform(entry.getValue(), spec.getJoltSpec()));
                } catch (Exception e) {
                    log.warn("doclingDelegate: inbound JOLT failed node={} edge={}->{}: {}",
                            currentNode.getId(), spec.getSourceNodeId(), spec.getTargetNodeId(),
                            e.toString());
                }
            }
        }
    }

    /**
     * Appends DuckTape Add-Node foreign-source messages to the upstream inputs. Existing wires
     * win on key overlap: a node that IS directly connected must not be duplicated by its tile.
     */
    private static List<InOutValueDto> mergeForeignInputs(List<InOutValueDto> upstream, List<InOutValueDto> foreignInputs) {
        if (foreignInputs == null || foreignInputs.isEmpty()) {
            return upstream;
        }
        LinkedHashMap<String, InOutValueDto> byKey = new LinkedHashMap<>();
        for (InOutValueDto in : upstream) {
            if (in != null) {
                byKey.put(bindingKey(in), in);
            }
        }
        for (InOutValueDto in : foreignInputs) {
            if (in != null) {
                byKey.putIfAbsent(bindingKey(in), in);
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /** Single definition lives on the resolver so the pinned fold names rows identically. */
    private static String bindingKey(InOutValueDto dto) {
        return ForeignEdgeInputResolver.bindingKey(dto);
    }

    /**
     * Puts the operator's apply-inputs override for {@code (runId, nodeId)} onto the node the SKIPPED
     * log row reports ({@code reportFinished} reads {@code currentNode.inputList}), so the record
     * shows the edited parameters. No-op when no override was stored; the skip path never snapshots
     * again, so the in-process mutation cannot leak into another node's dispatch.
     */
    private void mirrorOperatorOverrideOnSkippedRow(ExecutionContext ctx, String runId, String nodeId) {
        if (operatorInputOverrideStore == null || ctx == null
                || StringUtils.isBlank(runId) || nodeId == null) {
            return;
        }
        List<InOutValueDto> override = operatorInputOverrideStore.loadOverride(runId, nodeId).orElse(null);
        if (override == null) {
            return;
        }
        NodeDto node = findNode(ctx.getNodes(), nodeId);
        if (node == null) {
            node = new NodeDto();
            node.setId(nodeId);
        }
        node.setInputList(new ArrayList<>(override));
        ctx.setCurrentNode(node);
    }

    /**
     * The operator's sealed input override for {@code (workflowExecutionId, nodeId)}, or
     * {@code null} when no store is wired (slice tests) or no override was stored. Never logs
     * values (PHI); a corrupt/tampered row propagates so the dispatch fails honestly rather than
     * silently re-running the pre-repair inputs.
     */
    private List<InOutValueDto> loadOperatorOverride(ProcessContextRef ref, ExecutionContext ctx,
                                                     String nodeId) {
        if (operatorInputOverrideStore == null) {
            return null;
        }
        String runId = ref != null && StringUtils.isNotBlank(ref.getWorkflowExecutionId())
                ? ref.getWorkflowExecutionId()
                : (ctx == null ? null : ctx.getWorkflowExecutionId());
        if (runId == null || nodeId == null) {
            return null;
        }
        return operatorInputOverrideStore.loadOverride(runId, nodeId).orElse(null);
    }

    /**
     * Folds the operator override LAST onto the merged inputs: the edited values win on a binding-key
     * overlap and newly added parameters land as new keys. An EMPTY override is the operator clearing
     * the node's inbound list and returns an empty list; {@code null} leaves the merge untouched.
     */
    private static List<InOutValueDto> foldOperatorOverride(List<InOutValueDto> merged,
                                                            List<InOutValueDto> operatorOverride,
                                                            String nodeId) {
        if (operatorOverride == null) {
            return merged;
        }
        if (operatorOverride.isEmpty()) {
            return List.of();
        }
        LinkedHashMap<String, InOutValueDto> byKey = new LinkedHashMap<>();
        for (InOutValueDto in : merged) {
            if (in != null) {
                byKey.put(bindingKey(in), in);
            }
        }
        for (InOutValueDto in : operatorOverride) {
            String target = in == null ? null : in.getTargetNodeId();
            if (StringUtils.isNotBlank(target) && !target.equalsIgnoreCase(nodeId)) {
                continue;
            }
            if (in != null) {
                byKey.put(bindingKey(in), in);
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /**
     * One DuckTape screen → one message: after the inbound JOLT shaped every row into this node's
     * port shape, each pinned (Add-Node / Globals) row folds into the wire's row on the same port —
     * the same {@link com.studioquack.sqs.execution.transform.PinnedRowFolder} rules a sandbox node gets.
     */
    private static void foldPinnedRows(ExecutionContext ctx, NodeDto currentNode) {
        if (currentNode == null) {
            return;
        }
        com.studioquack.sqs.execution.transform.PinnedRowFolder.Result result =
                com.studioquack.sqs.execution.transform.PinnedRowFolder.fold(
                        currentNode.getInputList(), ctx.getPinnedInputCarriers(), currentNode.getId());
        if (result.changed()) {
            currentNode.setInputList(result.rows());
        }
    }

    /** Conservative wire-identity match — an unset spec field is "any" (mirrors the pipeline). */
    private static boolean inputListEntryMatchesSpec(InOutValueDto entry, TransformationSpecDto spec) {
        if (entry == null || spec == null) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getSourceNodeId()) && !spec.getSourceNodeId().equals(entry.getNodeId())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getTargetNodeId()) && !spec.getTargetNodeId().equals(entry.getTargetNodeId())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getSourcePortShortCode())
                && !spec.getSourcePortShortCode().equals(entry.getPortValue())) {
            return false;
        }
        if (StringUtils.isNotEmpty(spec.getTargetPortShortCode())
                && !spec.getTargetPortShortCode().equals(entry.getTargetPortValue())) {
            return false;
        }
        return true;
    }

    /**
     * Runs {@code kind=script} inbound edge transforms as a real transformation sandbox — the same
     * {@link EdgeTransformPreStep} a sandbox node runs on its inbound edges. Docling has no node
     * language of its own, so every script edge is a language MISMATCH.
     *
     * @return False when the node was failed and the dispatch must stop
     */
    private boolean runInboundScriptTransformSandbox(ExecutionContext ctx, NodeDto currentNode,
                                                     List<TransformationSpecDto> specs,
                                                     String dispatchId, String runId, String nodeId,
                                                     long startedNanos, boolean retryAttempt) {
        if (edgeTransformPreStep == null || currentNode == null
                || CollectionUtils.isEmpty(currentNode.getInputList()) || CollectionUtils.isEmpty(specs)) {
            return true;
        }
        List<TransformationSpecDto> scriptSpecs = specs.stream()
                .filter(s -> s != null && "script".equalsIgnoreCase(s.getKind()))
                .toList();
        if (scriptSpecs.isEmpty()) {
            return true;
        }
        // Flat port map the pre-step falls back to when matching by port key (Invariant 12 shape).
        Map<String, Object> inputVars = new LinkedHashMap<>();
        for (InOutValueDto in : currentNode.getInputList()) {
            String key = StringUtils.firstNonEmpty(in.getTargetPortValue(), in.getPortValue());
            if (StringUtils.isNotEmpty(key)) {
                inputVars.put(key, in.getValue());
            }
        }
        PreparedInbound prepared = new PreparedInbound(inputVars, new ArrayList<>(specs), Map.of(), Set.of());
        final EdgeTransformPreStep.Result edgeStep;
        try {
            // null node language → every script edge mismatches → sandbox hop (never a combined bake).
            edgeStep = edgeTransformPreStep.run(ctx, prepared, null, 0L, dispatchId);
        } catch (Exception e) {
            log.warn("doclingDelegate: inbound transform sandbox error node={}: {}", nodeId, e.toString());
            reportFinished(ctx, runId, nodeId, "FAILED", startedNanos,
                    "inbound transform sandbox error", null, null,
                    com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.ENGINE, retryAttempt);
            resultCoordinator.deliverFailure(dispatchId, EdgeTransformPreStep.ERROR_FAILED,
                    "inbound transform sandbox error");
            return false;
        }
        if (edgeStep != null && edgeStep.failed()) {
            // The author's transform code failed — SCRIPT kind, same as a sandbox node's pre-step.
            reportFinished(ctx, runId, nodeId, "FAILED", startedNanos,
                    edgeStep.errorMessage(), null, null,
                    com.studioquack.sqs.sandbox.SandboxJobResponse.FailureKind.SCRIPT, retryAttempt);
            resultCoordinator.deliverFailure(dispatchId, edgeStep.errorCode(), edgeStep.errorMessage());
            return false;
        }
        return true;
    }

    /**
     * Loads the node's inbound rows, retrying briefly when none are found. The producing node seals
     * its outputs on its own dispatch thread; on a parallel fan-out (Duckbot start → Docling AND →
     * agent) this converter's dispatch can read the store a beat before the producer's commit is
     * visible.
     */
    private List<InOutValueDto> loadInputsWithRetry(ProcessContextRef ref, String nodeId) {
        List<InOutValueDto> inputs = contextStore.loadInputsTargetingNode(ref, nodeId);
        log.info("doclingDelegate: loadInputs node={} execId={} firstRead={} row(s)",
                nodeId, ref == null ? "<null>" : ref.getExecutionId(),
                inputs == null ? 0 : inputs.size());
        if (inputs != null && !inputs.isEmpty()) {
            return inputs;
        }
        long deadline = System.nanoTime() + 2_000_000_000L; // 2 s
        int attempt = 0;
        while (System.nanoTime() < deadline) {
            attempt++;
            try {
                Thread.sleep(50L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
            inputs = contextStore.loadInputsTargetingNode(ref, nodeId);
            if (inputs != null && !inputs.isEmpty()) {
                log.info("doclingDelegate: inbound rows for node={} arrived after {} attempt(s) "
                        + "(producer commit raced this read)", nodeId, attempt);
                return inputs;
            }
        }
        log.warn("doclingDelegate: loadInputs node={} execId={} exhausted {} attempt(s), still empty",
                nodeId, ref == null ? "<null>" : ref.getExecutionId(), attempt);
        return inputs == null ? List.of() : inputs;
    }

    /**
     * Best-effort resolution of the document to convert from the node's inbound values: the first
     * non-blank input value is treated as the document content (String → UTF-8 bytes; anything else →
     * its JSON form). The full designer input contract (base64 bytes / mime / filename ports) is a
     * later refinement; this keeps the delegate self-contained and the wire call correct.
     *
     * @return The document to convert; {@code null} when there is no document — a files/document port
     * left empty, OR a file-shaped wrapper with no bytes ({@code {files:[]}}, blank
     * {@code {name,data}}, Duckbot {@code {msg, files:[]}}) even when the PORT.
     */
    private DocumentInput resolveDocument(ExecutionContext ctx, ProcessContextRef ref, String nodeId) {
        NodeDto currentNode = ctx == null ? null : ctx.getCurrentNode();
        boolean usedFallback = currentNode == null || currentNode.getInputList() == null;
        List<InOutValueDto> inputs = !usedFallback
                ? currentNode.getInputList()
                : contextStore.loadInputsTargetingNode(ref, nodeId);
        log.info("doclingDelegate: resolveDocument node={} currentNode={} fallback={} inputs={} row(s)",
                nodeId, currentNode != null ? "resolved" : "null", usedFallback,
                inputs == null ? 0 : inputs.size());
        if (inputs != null) {
            boolean sawFilesPort = false;
            boolean sawFileShaped = false;
            // 1) Inline { name, data } items (Duckbot files port) or sq-file: handles.
            for (InOutValueDto in : inputs) {
                if (in == null) {
                    continue;
                }

                if (isFilesPort(in)) {
                    sawFilesPort = true;
                }
                Object raw = wireValue(in);
                if (raw == null) {
                    continue;
                }
                DocumentInput inline = firstNamedBase64(raw);
                if (inline != null) {
                    return inline;
                }
                String handle = firstHandle(raw);
                if (handle != null) {
                    Optional<ClientFileStore.StoredFile> stored = clientFileStore.resolve(handle);
                    if (stored.isPresent()) {
                        ClientFileStore.StoredFile file = stored.get();
                        String filename = StringUtils.defaultIfBlank(file.fileName(),
                                StringUtils.defaultIfBlank(in.getPortValue(), "document"));
                        return new DocumentInput(file.content(), filename, file.mimeType());
                    }
                    log.warn("doclingDelegate: resolveDocument node={} handle={} did not resolve",
                            nodeId, handle);
                } else {
                    log.info("doclingDelegate: resolveDocument node={} port={} value has no sq-file handle "
                            + "(shape={})", nodeId, in.getPortValue(), shapeOf(in.getValue()));
                }
                // File-shaped wrappers (empty or not) are never inline text — stringify would
                // send {"files":[]} JSON to Docling and start the sidecar. Port name is NOT
                // required: Duckbot-start fans out on port=output as {files:[]} / {msg, files:[]}.
                if (isFileShaped(raw)) {
                    sawFileShaped = true;
                }
            }
            // A files-named port OR any file-shaped wrapper with no resolvable bytes: skip.
            // Never fall through to the chat prompt / wrapper JSON as a stand-in document.
            if (sawFilesPort || sawFileShaped) {
                return null;
            }
            // 2) No file-shaped value: first non-blank value is pasted inline text/document.
            for (InOutValueDto in : inputs) {
                Object raw = wireValue(in);
                if (in == null || raw == null) {
                    continue;
                }
                String filename = StringUtils.defaultIfBlank(in.getPortValue(), "document");
                return new DocumentInput(toBytes(raw), filename, null);
            }
        }
        return new DocumentInput(new byte[0], "document", null);
    }

    /**
     * Inbound payload the node actually received. Schema-filter may have emptied {@code value}
     * after the file-type children landed; {@code unfilteredValue} is the Activiti-delivered
     * envelope (same contract as helper harvest — not a bypass).
     */
    private static Object wireValue(InOutValueDto in) {
        if (in == null) {
            return null;
        }
        return in.getUnfilteredValue() != null ? in.getUnfilteredValue() : in.getValue();
    }

    private static boolean isFilesPort(InOutValueDto in) {
        String port = StringUtils.defaultString(in.getTargetPortValue());
        if (port.isBlank()) {
            port = StringUtils.defaultString(in.getPortValue());
        }
        return "files".equalsIgnoreCase(port) || "document".equalsIgnoreCase(port)
                || "file".equalsIgnoreCase(port);
    }

    /**
     * True when {@code value} is a file wrapper: a map with a {@code files} key, a
     * {@code {name, data}} file object, a list of those, or an empty list. File-shaped
     * values must never be stringified as inline text for Docling.
     */
    static boolean isFileShaped(Object value) {
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                return true;
            }
            for (Object element : list) {
                if (isFileShaped(element)) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof Map<?, ?> map) {
            if (map.containsKey("files")) {
                return true;
            }
            // {name, data} or a handle-bearing file object — even when data is blank.
            return map.containsKey("data") || map.containsKey("fileId");
        }
        return false;
    }

    /**
     * True when a file-shaped value carries no resolvable document bytes: {@code {files:[]}},
     * {@code files: null}, an empty list, or a file object with blank {@code data} and no
     * {@code sq-file:} handle. Used as the empty-container contract; skip also treats any unresolved
     * file-shaped value as no-document (see {@link #isFileShaped}).
     */
    static boolean isEmptyFileContainer(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                return true;
            }
            for (Object element : list) {
                if (!isEmptyFileContainer(element)) {
                    return false;
                }
            }
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            if (map.containsKey("files")) {
                return isEmptyFileContainer(map.get("files"));
            }
            if (!isFileShaped(map)) {
                return false;
            }
            Object data = map.get("data");
            boolean blankData = data == null || (data instanceof String s && StringUtils.isBlank(s));
            boolean noHandle = !ClientFileStore.isRef(data) && !ClientFileStore.isRef(map.get("fileId"));
            return blankData && noHandle;
        }
        return false;
    }

    /**
     * The audio/video extensions Docling supports (its docs' audio + video rows). These are exactly
     * the formats docling's own {@code _mime_from_extension} fallback CANNOT map (it has no
     * audio/video branches), so upstream admission depends on content-sniffing alone.
     */
    private static final Set<String> AV_EXTENSIONS = Set.of(
            "webm", "mkv", "mp4", "mov", "avi", "wav", "mp3", "m4a", "aac", "ogg", "flac");

    /** Below this many bytes no container magic is decidable and no converter could do anything. */
    private static final int AV_MIN_BYTES = 12;

    /**
     * Uses the same signatures docling's sniffer (the {@code filetype} library) keys on, so a mismatch
     * here is a guaranteed upstream "File format not allowed" — reported locally with WHAT arrived
     * (size, head bytes, printable-text hint) instead.
     *
     * @return The mismatch description, or {@code null} when the bytes are consistent with the
     * filename (or the name is not an audio/video container — those formats have a working extension
     * fallback upstream and need no guard)
     */
    static String avContainerMismatch(String filename, byte[] bytes) {
        String ext = extensionOf(filename);
        if (ext == null || !AV_EXTENSIONS.contains(ext)) {
            return null;
        }
        if (bytes == null || bytes.length < AV_MIN_BYTES) {
            return filename + ": " + (bytes == null ? 0 : bytes.length)
                    + " byte(s) — too small to be a " + ext + " container";
        }
        if (matchesContainerMagic(ext, bytes)) {
            return null;
        }
        StringBuilder head = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            head.append(String.format("%02x", bytes[i]));
        }
        String printable = printablePrefix(bytes);
        return filename + ": the " + bytes.length + " byte(s) received are not " + ext + " content "
                + "(first bytes " + head + (printable != null
                        ? " — printable text starting with \"" + printable + "\"" : "")
                + ")";
    }

    /** Lowercased extension of {@code filename}, or null when it carries none. */
    private static String extensionOf(String filename) {
        if (StringUtils.isBlank(filename)) {
            return null;
        }
        int dot = filename.lastIndexOf('.');
        if (dot < 1 || dot == filename.length() - 1) {
            return null;
        }
        return filename.substring(dot + 1).toLowerCase(Locale.ROOT);
    }

    /** EBML container magic (WebM and Matroska share it). */
    private static final byte[] EBML_MAGIC = {0x1A, 0x45, (byte) 0xDF, (byte) 0xA3};
    /** EBML DocType element for WebM — filetype requires it within the sniffed window. */
    private static final byte[] EBML_DOCTYPE_WEBM = {0x42, (byte) 0x82, (byte) 0x84, 'w', 'e', 'b', 'm'};
    /** EBML DocType element for Matroska. */
    private static final byte[] EBML_DOCTYPE_MKV = {0x42, (byte) 0x82, (byte) 0x88,
            'm', 'a', 't', 'r', 'o', 's', 'k', 'a'};
    /** docling sniffs the first 8192 bytes of a stream — the window the DocType must appear in. */
    private static final int SNIFF_WINDOW = 8192;

    /** The container signature {@code ext} must start with for docling's sniffer to admit it. */
    private static boolean matchesContainerMagic(String ext, byte[] b) {
        return switch (ext) {
            // EBML magic AND the DocType element — filetype's Webm/Mkv matchers need both,
            // so a header stub that stops after the magic still fails upstream.
            case "webm" -> startsWith(b, EBML_MAGIC) && containsWithin(b, EBML_DOCTYPE_WEBM);
            case "mkv" -> startsWith(b, EBML_MAGIC) && containsWithin(b, EBML_DOCTYPE_MKV);
            // ISO BMFF / QuickTime: "....ftyp"
            case "mp4", "m4a", "mov" -> asciiAt(b, 4, "ftyp");
            case "avi" -> asciiAt(b, 0, "RIFF") && asciiAt(b, 8, "AVI ");
            case "wav" -> asciiAt(b, 0, "RIFF") && asciiAt(b, 8, "WAVE");
            case "ogg" -> asciiAt(b, 0, "OggS");
            case "flac" -> asciiAt(b, 0, "fLaC");
            // ID3 tag or MPEG frame sync
            case "mp3" -> asciiAt(b, 0, "ID3") || (b[0] == (byte) 0xFF && (b[1] & 0xE0) == 0xE0);
            // ADTS sync
            case "aac" -> b[0] == (byte) 0xFF && (b[1] & 0xF0) == 0xF0;
            default -> true;
        };
    }

    /** Whether {@code b} starts with {@code magic}. */
    private static boolean startsWith(byte[] b, byte[] magic) {
        if (b.length < magic.length) {
            return false;
        }
        for (int i = 0; i < magic.length; i++) {
            if (b[i] != magic[i]) {
                return false;
            }
        }
        return true;
    }

    /** Whether {@code needle} appears in {@code b} within docling's sniff window. */
    private static boolean containsWithin(byte[] b, byte[] needle) {
        int limit = Math.min(b.length, SNIFF_WINDOW) - needle.length;
        for (int i = 0; i <= limit; i++) {
            boolean hit = true;
            for (int j = 0; j < needle.length; j++) {
                if (b[i + j] != needle[j]) {
                    hit = false;
                    break;
                }
            }
            if (hit) {
                return true;
            }
        }
        return false;
    }

    /** Whether {@code b} at {@code offset} starts with the ASCII string {@code s}. */
    private static boolean asciiAt(byte[] b, int offset, String s) {
        if (b.length < offset + s.length()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (b[offset + i] != (byte) s.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The first bytes as a quoted string when they are all printable ASCII (a JSON/base64/text
     * payload delivered where a recording was promised), otherwise null. Capped at 16 chars.
     */
    private static String printablePrefix(byte[] b) {
        int probe = Math.min(8, b.length);
        for (int i = 0; i < probe; i++) {
            int v = b[i] & 0xFF;
            if (v < 0x20 || v > 0x7E) {
                return null;
            }
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(16, b.length); i++) {
            int v = b[i] & 0xFF;
            sb.append(v >= 0x20 && v <= 0x7E ? (char) v : '.');
        }
        return sb.toString();
    }

    /**
     * Redaction-safe diagnosis for an empty document: which inbound ports arrived and the SHAPE of
     * each payload (keys / list size / type) — never values — so "the document never reached
     * Docling" is answerable from the daemon log without opening the sealed store.
     */
    private static String describeInbound(ExecutionContext ctx) {
        NodeDto node = ctx == null ? null : ctx.getCurrentNode();
        List<InOutValueDto> inputs = node == null ? null : node.getInputList();
        if (CollectionUtils.isEmpty(inputs)) {
            return "inbound=none (no upstream rows addressed to this node)";
        }
        List<String> parts = new ArrayList<>();
        for (InOutValueDto in : inputs) {
            if (in == null) {
                continue;
            }
            String port = StringUtils.firstNonEmpty(in.getTargetPortValue(), in.getPortValue(), "?");
            Object value = in.getValue();
            String shape = value == null ? "null"
                    : value instanceof Map<?, ?> map ? "keys=" + map.keySet()
                    : value instanceof List<?> list ? "list[" + list.size() + "]"
                    : value.getClass().getSimpleName();
            parts.add(port + ":" + shape);
        }
        return "inbound=[" + String.join(", ", parts) + "]";
    }

    /** Redaction-safe one-line shape of an inbound value (keys / list size / type — never values). */
    private static String shapeOf(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Map<?, ?> map) {
            return "keys=" + map.keySet();
        }
        if (value instanceof List<?> list) {
            return "list[" + list.size() + "]";
        }
        if (value instanceof String s) {
            return "String[" + s.length() + "]";
        }
        return value.getClass().getSimpleName();
    }

    /**
     * First {@code { name, data }} (base64) payload in an inbound value — Duckbot's files port shape.
     * {@code output_files} is searched alongside {@code files}: the AI Agent / LLM return path
     * publishes generated documents under that key.
     */
    private DocumentInput firstNamedBase64(Object value) {
        if (value instanceof List<?> list) {
            for (Object element : list) {
                DocumentInput found = firstNamedBase64(element);
                if (found != null) {
                    return found;
                }
            }
            return null;
        }
        if (value instanceof String s) {
            // A transform can deliver a file block STRINGIFIED (an authored toString on a file
            // leaf, or a backend-pre-applied spec): unwrap one JSON level so the bytes stay
            // reachable instead of falling through to "no document".
            Object unwrapped = unwrapJsonString(s);
            return unwrapped == null ? null : firstNamedBase64(unwrapped);
        }
        if (value instanceof Map<?, ?> map) {
            Object data = map.get("data");
            if (data instanceof String s && StringUtils.isNotBlank(s) && !ClientFileStore.isRef(s)) {
                try {
                    byte[] bytes = Base64.getDecoder().decode(s);
                    if (bytes.length > 0) {
                        String name = map.get("name") instanceof String n && StringUtils.isNotBlank(n)
                                ? n : "document";
                        String mime = map.get("mime_type") instanceof String m1 ? m1
                                : (map.get("mimeType") instanceof String m2 ? m2 : null);
                        return new DocumentInput(bytes, name, mime);
                    }
                } catch (IllegalArgumentException ignore) {
                    // not base64 — fall through to handle / nested walk
                }
            }
            DocumentInput fromFiles = firstNamedBase64(map.get("files"));
            if (fromFiles == null) {
                fromFiles = firstNamedBase64(map.get("output_files"));
            }
            if (fromFiles != null) {
                return fromFiles;
            }
            for (Object element : map.values()) {
                DocumentInput found = firstNamedBase64(element);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /** The daemon's
{@link ClientFileStore} resolves it to bytes. */
    private String firstHandle(Object value) {
        if (ClientFileStore.isRef(value)) {
            return (String) value;
        }
        if (value instanceof String s) {
            // Same stringified-block tolerance as firstNamedBase64: the handle rides inside the
            // JSON string ("fileId":"sq-file://…") where the plain walk cannot see it.
            Object unwrapped = unwrapJsonString(s);
            return unwrapped == null ? null : firstHandle(unwrapped);
        }
        if (value instanceof List<?> list) {
            for (Object element : list) {
                String handle = firstHandle(element);
                if (handle != null) {
                    return handle;
                }
            }
        } else if (value instanceof Map<?, ?> map) {
            Object fileId = map.get("fileId");
            if (ClientFileStore.isRef(fileId)) {
                return (String) fileId;
            }
            Object source = map.get("source");
            if (ClientFileStore.isRef(source)) {
                return (String) source;
            }
            String fromFiles = firstHandle(map.get("files"));
            if (fromFiles == null) {
                fromFiles = firstHandle(map.get("output_files"));
            }
            if (fromFiles != null) {
                return fromFiles;
            }
            for (Object element : map.values()) {
                String handle = firstHandle(element);
                if (handle != null) {
                    return handle;
                }
            }
        }
        return null;
    }

    /**
     * One level only; the walkers recurse if the parsed tree holds another stringified layer. Plain
     * text (a chat prompt, pasted document) never starts/ends with JSON braces, so it is left
     * untouched.
     */
    private Object unwrapJsonString(String s) {
        String trimmed = s.trim();
        boolean looksJson = (trimmed.startsWith("[") && trimmed.endsWith("]"))
                || (trimmed.startsWith("{") && trimmed.endsWith("}"));
        if (!looksJson) {
            return null;
        }
        try {
            return objectMapper.readValue(trimmed, Object.class);
        } catch (Exception notJson) {
            return null;
        }
    }

    /**
     * Boundary delegates (Docling, Duckbot) must address outputs themselves — nothing else in the
     * pipeline turns a raw value into {@code InOutValueDto}s for them. Payload keys come only from
     * each row's {@code sourcePath} (default {@code texts}) — never from {@code targetPath}, target
     * {@code PortDto} schema, or producer-side JOLT.
     */
    private List<InOutValueDto> buildOutputs(NodeDto currentNode, String nodeId,
                                             String markdown, String filename,
                                             String outputFormat, String runId) {
        List<InOutValueDto> out = new ArrayList<>();
        String md = markdown == null ? "" : markdown;
        // Empty markdown (the {files:[]} skip) publishes texts=[] / files=[] — schema-shaped empty lists,
        // never a raw string a downstream JOLT would flatten into garbage keys.
        List<?> textsPayload = textsPayload(md, filename, outputFormat);
        List<?> filesPayload = filesPayload(md, filename, outputFormat, runId);

        List<EdgeTransformDto> edgeTransforms = currentNode == null
                ? null : currentNode.getEdgeTransformListForCurrentNode();
        if (CollectionUtils.isNotEmpty(edgeTransforms)) {
            // edgeTransformListForCurrentNode carries BOTH inbound AND outbound edges; only rows
            // where WE are the source publish. Group by wire so multi-attribute edges seal as ONE dto.
            List<EdgeTransformDto> outbound = new ArrayList<>();
            for (EdgeTransformDto edge : edgeTransforms) {
                if (edge == null || !nodeId.equalsIgnoreCase(edge.getSourceNodeId())
                        || StringUtils.isBlank(edge.getTargetNodeId())) {
                    continue;
                }
                outbound.add(edge);
            }
            for (Map.Entry<String, List<EdgeTransformDto>> wire : groupByWire(outbound).entrySet()) {
                List<EdgeTransformDto> rows = wire.getValue();
                EdgeTransformDto first = rows.get(0);
                InOutValueDto dto = new InOutValueDto();
                dto.setNodeId(nodeId);
                dto.setPortId(first.getSourcePortId());
                dto.setPortValue(StringUtils.defaultIfBlank(first.getSourcePortShortCode(), OUTPUT_SCHEMA_FIELD));
                dto.setTargetNodeId(first.getTargetNodeId());
                dto.setTargetPortId(first.getTargetPortId());
                dto.setTargetPortValue(first.getTargetPortShortCode());
                Map<String, Object> sourcePayload = new HashMap<>();
                for (EdgeTransformDto edge : rows) {
                    String sourceAttr = rootSchemaAttribute(edge.getSourcePath());
                    sourcePayload.put(sourceAttr, FILES_SCHEMA_FIELD.equalsIgnoreCase(sourceAttr)
                            ? filesPayload : textsPayload);
                }
                // Source-shaped only — do NOT call convertJoltSpec/transform; consumer owns JOLT.
                dto.setValue(sourcePayload);
                out.add(dto);
            }
        }

        if (out.isEmpty() && currentNode != null && currentNode.getConnectionList() != null) {
            for (ConnectionDto conn : currentNode.getConnectionList()) {
                if (conn == null || (conn.getPortType() != null && conn.getPortType() != PortType.OUTPUT)) {
                    continue;
                }
                String sourcePortValue = StringUtils.defaultIfBlank(conn.getPortValue(), OUTPUT_SCHEMA_FIELD);
                if (conn.getConnectionDetailList() == null) {
                    continue;
                }
                for (ConnectionDetailDto detail : conn.getConnectionDetailList()) {
                    if (detail == null || StringUtils.isBlank(detail.getNodeId())) {
                        continue;
                    }
                    InOutValueDto dto = new InOutValueDto();
                    dto.setNodeId(nodeId);
                    dto.setPortValue(sourcePortValue);
                    dto.setTargetNodeId(detail.getNodeId());
                    PortDto target = detail.getPortDto();
                    if (target != null) {
                        dto.setTargetPortId(target.getId());
                        dto.setTargetPortValue(target.getValue());
                    }
                    // Connection path has no authored edge/JOLT — source attributes only (never PortDto schema).
                    Map<String, Object> payload = new HashMap<>();
                    payload.put(OUTPUT_SCHEMA_FIELD, textsPayload);
                    payload.put(FILES_SCHEMA_FIELD, filesPayload);
                    dto.setValue(payload);
                    log.info("doclingDelegate.buildOutputs.noAuthoredMapping: sourceAttr={} "
                                    + "sourcePortName={} sourcePortValue={} "
                                    + "targetPortName={} targetPortValue={} targetPortId={}",
                            OUTPUT_SCHEMA_FIELD,
                            conn.getPortName(),
                            sourcePortValue,
                            target != null ? target.getName() : null,
                            target != null ? target.getValue() : null,
                            target != null ? target.getId() : null);
                    out.add(dto);
                }
            }
        }

        if (out.isEmpty()) {
            InOutValueDto dto = new InOutValueDto();
            dto.setNodeId(nodeId);
            dto.setPortId(DEFAULT_OUTPUT_PORT);
            dto.setPortValue(DEFAULT_OUTPUT_PORT);
            // Isolated / skip fallback: a successful conversion publishes the texts list; the skip
            // stays schema-shaped ({texts:[], files:[]}) — never a raw empty string, which a
            // downstream JOLT flattens into leaf keys (texts.data[, files.name[, …).
            dto.setValue(md.isEmpty()
                    ? Map.of(OUTPUT_SCHEMA_FIELD, List.of(), FILES_SCHEMA_FIELD, List.of())
                    : textsPayload);
            out.add(dto);
        }
        return out;
    }

    /**
     * The payload key for an authored row's {@code sourcePath}: the ROOT schema attribute (
     * {@code texts} / {@code files}), never a dotted leaf. A row mapping {@code texts.data} asks the
     * CONSUMER's inbound JOLT to extract the data leaf — its shift spec, built from the same row,
     * navigates {@code texts[*].data} off the root.
     */
    private static String rootSchemaAttribute(String sourcePath) {
        String attr = StringUtils.defaultIfBlank(sourcePath, OUTPUT_SCHEMA_FIELD);
        int dot = attr.indexOf('.');
        return dot < 0 ? attr : attr.substring(0, dot);
    }

    /**
     * Source-shaped {@code texts} list: one text ContentBlock on success, an EMPTY list when
     * conversion was skipped — the skip stays schema-shaped ({@code {texts:[], files:[]}}) so a
     * downstream edge JOLT sees the declared attributes instead of flattening a raw empty string
     * into leaf keys like {@code texts.data[}.
     */
    private static List<?> textsPayload(String markdown, String filename, String outputFormat) {
        if (markdown.isEmpty()) {
            return List.of();
        }
        return List.of(textContentBlock(markdown, filename, outputFormat));
    }

    /**
     * {@code texts} item: the same ContentBlock fields a file block carries, but {@code data} is the
     * PLAIN converted text — the resolved content a prompt/mapping consumes directly, never base64.
     * Key order matches Duckbot file blocks: {@code kind}, {@code mime_type}, {@code source},
     * {@code name}, {@code data}.
     */
    private static Map<String, Object> textContentBlock(String text, String filename, String outputFormat) {
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "document");
        block.put("mime_type", outputMimeType(outputFormat));
        block.put("source", "text");
        block.put("name", outputName(filename, outputFormat));
        block.put("data", text);
        return block;
    }

    /**
     * Source-shaped {@code files} list: the converted content as a FILE item — base64 under
     * {@code data} plus a run-scoped store handle under {@code fileId} when the seal succeeds, so file
     * consumers (sandbox materialise, DuckBot End attachments). Empty on the skip path; base64-only
     * (never a failure) when the store rejects.
     */
    private List<?> filesPayload(String markdown, String filename, String outputFormat, String runId) {
        if (markdown.isEmpty()) {
            return List.of();
        }
        String name = outputName(filename, outputFormat);
        String mimeType = outputMimeType(outputFormat);
        byte[] bytes = markdown.getBytes(StandardCharsets.UTF_8);
        String handle = null;
        try {
            handle = clientFileStore == null || StringUtils.isBlank(runId) ? null
                    : clientFileStore.store(ClientFileStore.SCOPE_RUN, runId, name, mimeType, bytes);
        } catch (RuntimeException storeFailed) {
            log.warn("doclingDelegate: converted file {} could not be sealed into the run store ({}); "
                    + "publishing it base64-only", name, storeFailed.getMessage());
        }
        Map<String, Object> block = new LinkedHashMap<>();
        block.put("kind", "document");
        block.put("mime_type", mimeType);
        block.put("source", handle != null ? "sq-file" : "base64");
        block.put("name", name);
        if (handle != null) {
            block.put("fileId", handle);
        }
        block.put("data", Base64.getEncoder().encodeToString(bytes));
        return List.of(block);
    }

    /** {@code text/markdown} for the default format, {@code text/plain} for {@code text}. */
    private static String outputMimeType(String outputFormat) {
        return FORMAT_TEXT.equals(outputFormat) ? "text/plain" : "text/markdown";
    }

    /**
     * {@code report.pdf} → {@code report.md} (markdown) or {@code report.txt} (text);
     * blank/missing → {@code document.md}/{@code document.txt}. Last suffix only.
     */
    private static String outputName(String filename, String outputFormat) {
        String extension = FORMAT_TEXT.equals(outputFormat) ? ".txt" : ".md";
        if (StringUtils.isBlank(filename)) {
            return "document" + extension;
        }
        String trimmed = filename.trim();
        int lastDot = trimmed.lastIndexOf('.');
        String base = lastDot > 0 ? trimmed.substring(0, lastDot) : trimmed;
        if (StringUtils.isBlank(base)) {
            return "document" + extension;
        }
        return base + extension;
    }

    /**
     * Groups outbound edge transforms by the WIRE they belong to, preserving authoring order.
     *
     * <p>Key mirrors bindingKey in the execution-context store and the inbound merge —
     * source port plus target node/port — because that is the identity those two dedup on.
     */
    private static java.util.LinkedHashMap<String, List<EdgeTransformDto>> groupByWire(
            List<EdgeTransformDto> edgeTransforms) {
        java.util.LinkedHashMap<String, List<EdgeTransformDto>> byWire = new java.util.LinkedHashMap<>();
        for (EdgeTransformDto edge : edgeTransforms) {
            if (edge == null || StringUtils.isBlank(edge.getTargetNodeId())) {
                continue;
            }
            String key = StringUtils.defaultString(edge.getSourcePortId()) + '|'
                    + StringUtils.defaultString(edge.getSourcePortShortCode()) + '|'
                    + edge.getTargetNodeId() + '|'
                    + StringUtils.defaultString(edge.getTargetPortId());
            byWire.computeIfAbsent(key, k -> new ArrayList<>()).add(edge);
        }
        return byWire;
    }

    private byte[] toBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (Exception e) {
            return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Seals the node's outputs into the context store. Throws on failure — the caller fails the
     * node loudly rather than completing the run with output nothing downstream can read (parity
     * with {@code ScriptDispatchDelegate.persistOutputs}).
     */
    private void saveOutputs(ProcessContextRef ref, List<InOutValueDto> outputs) {
        contextStore.saveOutputs(ref, outputs);
    }

    private ProcessContextRef readRef(DelegateExecution execution) {
        Object v = execution.getVariable(EngineVariableNames.STUDIOQUACK_REF);
        if (v instanceof ProcessContextRef ref) {
            return ref;
        }
        if (v instanceof Map<?, ?> map) {
            return objectMapper.convertValue(map, ProcessContextRef.class);
        }
        return null;
    }

    private static String stripPrefix(String activityId) {
        if (activityId != null && activityId.startsWith(NODE_ACTIVITY_PREFIX)) {
            return activityId.substring(NODE_ACTIVITY_PREFIX.length());
        }
        return activityId;
    }

    /** @return how many times this delegate has been invoked (diagnostics/tests). */
    public long getInvocationCount() {
        return invocationCount.get();
    }

    /** The document bytes + hints handed to {@link DoclingBackend#convert}. */
    private record DocumentInput(byte[] bytes, String filename, String mimeType) {
    }
}
