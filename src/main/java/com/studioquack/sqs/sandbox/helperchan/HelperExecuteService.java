package com.studioquack.sqs.sandbox.helperchan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.common.dto.flow.FunctionPortTransformsDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.sqs.channel.wg.GrantedPeerDirectory;
import com.studioquack.sqs.channel.wg.mesh.CrossEnvAuthorizationClient;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.engine.WorkflowGraph;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.runtime.RefOnlyVarBuilder;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import com.studioquack.sqs.execution.transform.TransformDirectiveApplier;
import com.studioquack.sqs.aimodel.LlmStreamCancellationRegistry;
import com.studioquack.sqs.runtime.DispatchCancellationRegistry;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Daemon-mediated {@code studioquack.execute()} handler. For client-orchestrated environments the
 * sub-workflow runs on the local embedded Activiti engine ({@code Process_SubProc_<root>}); when
 * the requested {@code envId} differs from the daemon's enrolled env the call goes straight to the
 * sibling daemon over the overlay.
 */
@Slf4j
@Service
public class HelperExecuteService {

    private static final String FUNCTION_PORT_TRANSFORMS_KEY = "__sq_function_port_transforms__";

    private final ObjectMapper mapper;
    private final SqsEngine sqsEngine;
    private final SubProcessCompletionRegistry subProcessRegistry;
    /**
     * The legacy same-owner envId → overlay-address view. Cross-env execute no longer resolves through
     * it (resolves via the GRANTED directory); it remains injected because the engineIP/NAC surface
     * shares this bean's wiring and the slice-test constructor shape.
     */
    private final com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory;
    /**
     * The handshake-delivered mesh identity whose token is presented as the Bearer credential on
     * outbound peer calls. A daemon holding no token cannot cross environments.
     */
    private final com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity;
    /**
     * The GRANTED-peers view the SDK {@code env=} value resolves against (id or name). {@code null}
     * only on a daemon assembled without the secure channel: every {@code env=}-targeted call then
     * fails closed with {@code Error_AuthorizationUnavailable}.
     */
    private final GrantedPeerDirectory grantedPeerDirectory;
    /**
     * The caller-side on-miss authorize client. {@code null} when the secure channel is off; a scope
     * miss then denies with {@code Error_AuthorizationUnavailable}.
     */
    private final CrossEnvAuthorizationClient crossEnvAuthorizationClient;

    /** Fallback HTTP for tests without a mesh CA. Production uses {@link #meshPeerHttp}. */
    private final HttpClient overlayHttp = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.channel.net.PeerAddressResolver peerAddressResolver;
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.channel.net.MeshPeerHttp meshPeerHttp;

    @org.springframework.beans.factory.annotation.Value("${sqs.overlay.execute-port:8443}")
    private int overlayExecutePort;
    private final RefOnlyVarBuilder refOnlyVarBuilder;
    private final TransformDirectiveApplier transformApplier;
    private final ExecutionContextStore contextStore;
    /**
     * Live emit-partial fan-in for {@code execute/stream}. Null in slice tests that never
     * exercise streaming; those calls still complete, they just never see child partials.
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private HelperPartialEventBus partialBus;

    /**
     * Cancels a child run whose caller is gone (its sandbox, and through the helper revoke, any
     * call that child is itself parked in). Null in slice tests: the child then runs to its end.
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    DispatchCancellationRegistry dispatchCancellation;

    /** Aborts an orphaned child's in-flight LLM calls. Null in slice tests. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    LlmStreamCancellationRegistry llmCancellation;

    /**
     * Reactive mesh-token refresh: a peer's 401 means the held token expired against the peer's
     * clock, so the daemon re-handshakes and retries the call once. Package-private so slice
     * tests install a lambda directly; {@code null} there (a 401 then surfaces as before).
     */
    java.util.function.BooleanSupplier meshIdentityRefresher;

    /** Spring wiring — lazy in both directions so the bootstrap↔helper pair cannot cycle. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    void setMeshIdentityRefresher(
            org.springframework.beans.factory.ObjectProvider<com.studioquack.sqs.channel.SqsChannelBootstrap>
                    bootstrap) {
        this.meshIdentityRefresher = () -> {
            com.studioquack.sqs.channel.SqsChannelBootstrap b = bootstrap.getIfAvailable();
            return b != null && b.refreshMeshIdentityNow();
        };
    }

    /**
     * Slice/test constructor without the cross-env collaborators: every {@code env=}-targeted call
     * fails closed with {@code Error_AuthorizationUnavailable} instead of resolving.
     */
    public HelperExecuteService(ObjectMapper mapper,
                                SqsEngine sqsEngine,
                                SubProcessCompletionRegistry subProcessRegistry,
                                RefOnlyVarBuilder refOnlyVarBuilder,
                                TransformDirectiveApplier transformApplier,
                                ExecutionContextStore contextStore,
                                com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory,
                                com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity) {
        this(mapper, sqsEngine, subProcessRegistry, refOnlyVarBuilder, transformApplier, contextStore,
                peerDirectory, meshIdentity, null, null);
    }

    @org.springframework.beans.factory.annotation.Autowired
    public HelperExecuteService(ObjectMapper mapper,
                                SqsEngine sqsEngine,
                                SubProcessCompletionRegistry subProcessRegistry,
                                RefOnlyVarBuilder refOnlyVarBuilder,
                                TransformDirectiveApplier transformApplier,
                                ExecutionContextStore contextStore,
                                com.studioquack.sqs.channel.wg.OverlayPeerDirectory peerDirectory,
                                com.studioquack.sqs.channel.wg.mesh.MeshIdentity meshIdentity,
                                GrantedPeerDirectory grantedPeerDirectory,
                                @Nullable CrossEnvAuthorizationClient crossEnvAuthorizationClient) {
        this.mapper = mapper;
        this.sqsEngine = sqsEngine;
        this.subProcessRegistry = subProcessRegistry;
        this.refOnlyVarBuilder = refOnlyVarBuilder;
        this.transformApplier = transformApplier;
        this.contextStore = contextStore;
        this.peerDirectory = peerDirectory;
        this.meshIdentity = meshIdentity;
        this.grantedPeerDirectory = grantedPeerDirectory;
        this.crossEnvAuthorizationClient = crossEnvAuthorizationClient;
    }

    /**
     * @return wire shape {@code {result: <value>}} where {@code <value>} is a list (legacy helper.py
     * semantics) or a single element when exactly one connection detail matched.
     */
    public Object handle(HelperJob job, String portValue, Object payload, String nodeIdFilter, String envId) {
        Map<String, Object> ec = job.executionContext();
        if (ec == null || ec.isEmpty()) {
            log.warn("helper-execute: empty execution context job={}", job.id());
            return Map.of("result", List.of());
        }
        List<ConnectionDetail> details = connectionDetailsForPort(ec, portValue, nodeIdFilter);
        if (details.isEmpty()) {
            log.warn("helper-execute: no connection details job={} port={} filter={} (currentNode.connectionList "
                    + "missing this FUNCTION port — check the baked execution context)", job.id(), portValue, nodeIdFilter);
            return Map.of("result", List.of());
        }
        String daemonEnvId = str(ec.get("envId"));
        // The SDK `env=` value resolves against the GRANTED-peers directory (by env id or by name), with
        // the caller-side scope pre-check inside. Blank, or naming THIS environment (id or name), stays
        // local; an unresolvable value is a typed failure (Error_UnknownEnvironment /
        // Error_CrossEnvNotGranted / Error_AuthorizationUnavailable) — never a silent fallback to a local
        // run.
        Optional<GrantedPeerDirectory.GrantedPeer> peer = CrossEnvPeerResolver.resolvePeer(
                envId, daemonEnvId,
                meshIdentity == null ? null : meshIdentity.ownEnvName().orElse(null),
                grantedPeerDirectory, crossEnvAuthorizationClient, "execute");
        boolean crossEnv = peer.isPresent();
        String effectiveEnvId = crossEnv ? peer.get().envId() : daemonEnvId;
        logExecute(portValue, "helper-execute.start job={} port={} details={} crossEnv={} effectiveEnv={}",
                job.id(), portValue, details.size(), crossEnv, effectiveEnvId);
        logExecute(portValue, "helper-execute.input job={} port={} {}",
                job.id(), portValue, describePayload(payload));

        List<Object> results = new ArrayList<>();
        for (ConnectionDetail detail : details) {
            Object txPayload = applyRequestTransforms(ec, portValue, payload);
            if (crossEnv) {
                // Daemon → daemon over the overlay. The control plane is not in this path: it holds no workflow
                // engine, and a synchronous call cannot be parked on the heartbeat queue anyway.
                Object raw = forwardToPeer(ec, detail, portValue, txPayload, peer.get(), job);
                results.add(applyResponseTransforms(ec, portValue, raw));
            } else {
                results.add(executeLocal(ec, detail, portValue, txPayload, job.id(), job));
            }
        }
        Object wire = results.size() == 1 ? results.get(0) : results;
        // Response checkpoint: confirms the provider's output is being routed back into the agent's
        // FUNCTION-port response (the "[Provider return-path error] empty transport response" surfaces
        // when this is empty). Logs shape only, never PHI values.
        logExecute(portValue, "helper-execute.done job={} port={} matched={} resultCount={} wireType={} empty={}",
                job.id(), portValue, details.size(), results.size(),
                wire == null ? "null" : wire.getClass().getSimpleName(),
                wire == null || (wire instanceof java.util.List<?> wl && wl.isEmpty()));
        return Map.of("result", wire);
    }

    /**
     * Streaming {@code studioquack.execute(stream=True)}: same resolution as {@link #handle},
     * but child {@code emit-partial} frames are forwarded as they arrive and the final
     * aggregated result is delivered through {@code listener.onComplete}. Cross-env overlay
     * calls stay buffered and are synthesized as a single complete (no live partials).
     */
    public void handleStream(HelperJob job, String portValue, Object payload, String nodeIdFilter,
                             String envId, ExecuteStreamListener listener) {
        if (listener == null) {
            return;
        }
        Map<String, Object> ec = job.executionContext();
        if (ec == null || ec.isEmpty()) {
            log.warn("helper-execute.stream: empty execution context job={}", job.id());
            listener.onComplete(Map.of("result", List.of()));
            return;
        }
        List<ConnectionDetail> details = connectionDetailsForPort(ec, portValue, nodeIdFilter);
        if (details.isEmpty()) {
            log.warn("helper-execute.stream: no connection details job={} port={}", job.id(), portValue);
            listener.onComplete(Map.of("result", List.of()));
            return;
        }
        String daemonEnvId = str(ec.get("envId"));
        Optional<GrantedPeerDirectory.GrantedPeer> peer = CrossEnvPeerResolver.resolvePeer(
                envId, daemonEnvId,
                meshIdentity == null ? null : meshIdentity.ownEnvName().orElse(null),
                grantedPeerDirectory, crossEnvAuthorizationClient, "execute");
        boolean crossEnv = peer.isPresent();
        // The stream path had no start line at all — the first log was the sub-PI start deep in
        // executeLocalStream, so a function-port call that failed before dispatch left no trace.
        logExecute(portValue, "helper-execute.stream.start job={} port={} details={} crossEnv={} {}",
                job.id(), portValue, details.size(), crossEnv, describePayload(payload));
        List<Object> results = new ArrayList<>();
        for (ConnectionDetail detail : details) {
            Object txPayload = applyRequestTransforms(ec, portValue, payload);
            if (crossEnv) {
                Object raw = forwardToPeer(ec, detail, portValue, txPayload, peer.get(), job);
                results.add(applyResponseTransforms(ec, portValue, raw));
            } else {
                results.add(executeLocalStream(ec, detail, portValue, txPayload, job.id(), listener, job));
            }
        }
        Object wire = results.size() == 1 ? results.get(0) : results;
        // Response checkpoint (stream twin of helper-execute.done): proves WHAT the agent's model
        // port is about to receive. Shape only — never payload values.
        logExecute(portValue, "helper-execute.stream.complete job={} port={} wireType={} {}",
                job.id(), portValue, wire == null ? "null" : wire.getClass().getSimpleName(),
                describeWireShape(wire));
        listener.onComplete(Map.of("result", wire));
    }

    /** Parent-side callbacks for one {@code execute/stream} request. */
    public interface ExecuteStreamListener {
        void onPartial(String channel, Object payload);
        void onComplete(Object result);
        void onError(String message);
    }

    /**
     * The BPMN namespace the given run is executing in, so its sub-process calls stay inside it. A
     * Designer Run deploys into a draft namespace of its own, and its sub-processes are keyed {@code
     * Process_SubProc_<node>__draft_<token>}.
     */
    private String namespaceSuffixOfRun(String runId) {
        if (runId == null || runId.isBlank()) {
            return "";
        }
        try {
            var instance = sqsEngine.processInstances().processInstanceBusinessKey(runId).singleResult();
            return instance == null ? "" : WorkflowGraph.namespaceSuffixOf(instance.getProcessDefinitionKey());
        } catch (RuntimeException ex) {
            // Never fail the call over this: an unresolvable namespace means the published one.
            log.debug("helper-execute: could not resolve the namespace of run {} ({})", runId, ex.toString());
            return "";
        }
    }

    private Object executeLocal(Map<String, Object> srcEc, ConnectionDetail detail,
                                String portValue, Object payload, String parentJobId,
                                HelperJob parent) {
        String targetNodeId = detail.targetNodeId();
        String parentRunId = str(srcEc.get("workflowExecutionId"));
        String bpmnProcessId = "Process_SubProc_" + targetNodeId + namespaceSuffixOfRun(parentRunId);
        if (!sqsEngine.latestDefinitionExists(bpmnProcessId)) {
            throw new IllegalStateException("sub-process definition not deployed: " + bpmnProcessId
                    + " (the function-port target node was not emitted as a sub-process at deploy time)");
        }
        logExecute(portValue, "helper-execute.local starting sub-pi bpmn={} targetNode={} parentExec={}",
                bpmnProcessId, targetNodeId, str(srcEc.get("workflowExecutionId")));
        String childWfeId = UUID.randomUUID().toString();
        String parentWfeId = str(srcEc.get("workflowExecutionId"));
        // Index before startLatest so a child that completes on this thread is still findable.
        String callerNodeId = str(srcEc.get("currentNodeId"));
        recordSubPiInvocationSafely(parentWfeId, childWfeId, callerNodeId, targetNodeId);
        Map<String, Object> childEcMap = buildExecuteEc(srcEc, detail, portValue, payload, str(srcEc.get("envId")));
        ExecutionContext childCtx = mapper.convertValue(childEcMap, ExecutionContext.class);
        childCtx.setWorkflowExecutionId(childWfeId);
        childCtx.createCurrentNode();

        // Data-flow checkpoint: the sub-PI can only run the provider node if THIS child context carries
        // the target node's definition (language/script) in `nodes`. Log the resolution up-front so a
        // regression surfaces here (language=null → the agent's execute() context arrived graph-less)
        // instead of as an opaque downstream "unresolved language" DISPATCH_UNRESOLVED failure.
        Object childNodes = childEcMap.get("nodes");
        int childNodeCount = childNodes instanceof java.util.List<?> l ? l.size() : 0;
        String resolvedLang = childCtx.findCurrentNodeLanguage();
        boolean duckbotBoundary = isDuckbotBoundary(childCtx);
        logExecute(portValue, "helper-execute.local child-ctx targetNode={} nodesInCtx={} currentNodeResolved={} language={} reqPayloadType={}",
                targetNodeId, childNodeCount, childCtx.getCurrentNode() != null, resolvedLang,
                payload == null ? "null" : payload.getClass().getSimpleName());
        if (resolvedLang == null && !duckbotBoundary) {
            log.warn("helper-execute.local targetNode={} has NO resolvable language — the sub-PI will fail "
                    + "'unresolved language'. The agent's carried executionContext is missing the target "
                    + "node in `nodes` (nodesInCtx={}).", targetNodeId, childNodeCount);
        }

        // Hold the future the registry hands back rather than looking it up again after the start. A
        // provider node with no async sandbox dispatch runs to completion INSIDE startLatest below, on
        // this same thread; the completion resolves and removes the registry entry before we could look it
        // up, and the result would be dropped ("no pending sub-process future").
        CompletableFuture<Map<String, Object>> completion =
                subProcessRegistry.register(new SubProcessCompletionRegistry.SubPiLink(
                        childWfeId, parentWfeId, parentJobId, str(srcEc.get("envId")), bpmnProcessId, null));
        Map<String, Object> startVars = refOnlyVarBuilder.toEngineVariables(childCtx);
        try {
            // Start the sub-PI under the app (fat-jar) classloader as TCCL. This call runs on the
            // helper-channel thread; its transaction commit may spawn an Activiti async worker that inherits
            // this TCCL, and evaluating ${scriptDispatchDelegate} there needs to resolve
            // org.apache.el.ExpressionFactoryImpl via jakarta.el's TCCL lookup.
            Thread current = Thread.currentThread();
            ClassLoader prevCl = current.getContextClassLoader();
            try {
                current.setContextClassLoader(getClass().getClassLoader());
                sqsEngine.startLatest(bpmnProcessId, childWfeId, startVars);
            } finally {
                current.setContextClassLoader(prevCl);
            }
            Map<String, Object> rawVars = awaitSubPi(completion, parent, childWfeId);
            List<Object> aggregated = aggregateSubPiResult(rawVars, childWfeId, targetNodeId, portValue);
            if (aggregated.size() == 1) {
                return applyResponseTransforms(srcEc, portValue, aggregated.get(0));
            }
            return applyResponseTransforms(srcEc, portValue, aggregated);
        } catch (Exception ex) {
            subProcessRegistry.cancel(childWfeId, ex.getMessage());
            throw new IllegalStateException("local sub-process failed: " + ex.getMessage(), ex);
        }
    }

    /**
     * Waits for a sub-PI (streaming or not) for as long as it takes — there is deliberately no limit:
     * a delegated step may run for hours, and a fixed 30-minute cap (plus a 10-minute silence cutoff
     * on streams) used to fail such calls mid-flight. The thread parks on the child's completion and
     * wakes exactly once, nothing polls: when the child finishes, fails or is cancelled, or when the
     * parent's dispatch is over ({@link HelperJob#onRevoke} completes the wait).
     */
    private Map<String, Object> awaitSubPi(CompletableFuture<Map<String, Object>> completion,
                                           HelperJob parent, String childWfeId) throws Exception {
        if (parent == null) {
            return completion.get();
        }
        Runnable callerGone = () -> completion.completeExceptionally(new CancellationException(
                "the calling node ended before sub-process " + childWfeId + " finished"));
        parent.onRevoke(callerGone);
        try {
            return completion.get();
        } catch (CancellationException | ExecutionException e) {
            if (parent.revoked()) {
                cancelOrphanedChild(childWfeId);
            }
            throw e;
        } finally {
            parent.removeRevokeListener(callerGone);
        }
    }

    /**
     * Cancels a child run whose caller is gone: its in-flight LLM calls, its sandbox (whose helper
     * revoke in turn releases any call the child itself is parked in) and its process instance.
     * Best-effort — the caller is already gone, so a failure here is only logged.
     */
    private void cancelOrphanedChild(String childWfeId) {
        try {
            int abortedLlm = llmCancellation == null ? 0 : llmCancellation.abortRun(childWfeId);
            String pid = sqsEngine.resolveProcessInstanceId(childWfeId);
            int reaped = dispatchCancellation == null || pid == null ? 0 : dispatchCancellation.cancelRun(pid);
            boolean deleted = false;
            if (pid != null && sqsEngine.isProcessInstanceActive(pid)) {
                sqsEngine.deleteProcessInstance(pid, "caller ended before the sub-process finished");
                deleted = true;
            }
            log.info("helper-execute: cancelled orphaned sub-process child={} pi={} abortedLlm={} reaped={} deleted={}",
                    childWfeId, pid, abortedLlm, reaped, deleted);
        } catch (RuntimeException e) {
            log.warn("helper-execute: could not cancel orphaned sub-process child={}: {}", childWfeId, e.toString());
        }
    }

    /**
     * Local sub-PI for {@code execute/stream}: registers this parent as the child's emit-partial
     * listener keyed by {@code childWfeId} BEFORE start, so a fast child cannot miss the slot.
     */
    private Object executeLocalStream(Map<String, Object> srcEc, ConnectionDetail detail,
                                      String portValue, Object payload, String parentJobId,
                                      ExecuteStreamListener listener, HelperJob parent) {
        String targetNodeId = detail.targetNodeId();
        String parentRunId = str(srcEc.get("workflowExecutionId"));
        String bpmnProcessId = "Process_SubProc_" + targetNodeId + namespaceSuffixOfRun(parentRunId);
        if (!sqsEngine.latestDefinitionExists(bpmnProcessId)) {
            throw new IllegalStateException("sub-process definition not deployed: " + bpmnProcessId
                    + " (the function-port target node was not emitted as a sub-process at deploy time)");
        }
        String childWfeId = UUID.randomUUID().toString();
        String parentWfeId = str(srcEc.get("workflowExecutionId"));
        // Index before startLatest so a child that completes on this thread is still findable.
        String callerNodeId = str(srcEc.get("currentNodeId"));
        recordSubPiInvocationSafely(parentWfeId, childWfeId, callerNodeId, targetNodeId);
        logExecute(portValue, "helper-execute.stream starting sub-pi bpmn={} targetNode={} child={}",
                bpmnProcessId, targetNodeId, childWfeId);
        Map<String, Object> childEcMap = buildExecuteEc(srcEc, detail, portValue, payload, str(srcEc.get("envId")));
        ExecutionContext childCtx = mapper.convertValue(childEcMap, ExecutionContext.class);
        childCtx.setWorkflowExecutionId(childWfeId);
        childCtx.createCurrentNode();

        CompletableFuture<Map<String, Object>> completion =
                subProcessRegistry.register(new SubProcessCompletionRegistry.SubPiLink(
                        childWfeId, parentWfeId, parentJobId, str(srcEc.get("envId")), bpmnProcessId, null));
        if (partialBus != null && listener != null) {
            // Key = child run id, which the child's HelperJob copies into executionContext.workflowExecutionId.
            partialBus.register(childWfeId, listener::onPartial);
        }
        Map<String, Object> startVars = refOnlyVarBuilder.toEngineVariables(childCtx);
        try {
            Thread current = Thread.currentThread();
            ClassLoader prevCl = current.getContextClassLoader();
            try {
                current.setContextClassLoader(getClass().getClassLoader());
                sqsEngine.startLatest(bpmnProcessId, childWfeId, startVars);
            } finally {
                current.setContextClassLoader(prevCl);
            }
            Map<String, Object> rawVars = awaitSubPi(completion, parent, childWfeId);
            List<Object> aggregated = aggregateSubPiResult(rawVars, childWfeId, targetNodeId, portValue);
            if (aggregated.size() == 1) {
                return applyResponseTransforms(srcEc, portValue, aggregated.get(0));
            }
            return applyResponseTransforms(srcEc, portValue, aggregated);
        } catch (Exception ex) {
            subProcessRegistry.cancel(childWfeId, ex.getMessage());
            throw new IllegalStateException("local sub-process failed: " + ex.getMessage(), ex);
        } finally {
            if (partialBus != null) {
                partialBus.unregister(childWfeId);
            }
        }
    }

    /**
     * Soft-fail index of a function-port sub-PI so {@code ForeignEdgeInputResolver} can later find the
     * child's outputs. Never throws — a store miss must not abort the execution.
     */
    private void recordSubPiInvocationSafely(String parentWfeId, String childWfeId,
                                             String callerNodeId, String subPiRootNodeId) {
        try {
            if (contextStore == null) return;
            contextStore.recordSubPiInvocation(parentWfeId, childWfeId, callerNodeId, subPiRootNodeId);
        } catch (RuntimeException ex) {
            log.warn("helper-execute: sub-pi invocation index failed parent={} child={} caller={} root={}: {}",
                    parentWfeId, childWfeId, callerNodeId, subPiRootNodeId, ex.toString());
        }
    }

    /**
     * Places a cross-environment call directly on the sibling daemon over the overlay. Fails closed,
     * with a distinguishable code.
     */
    private Object forwardToPeer(Map<String, Object> srcEc, ConnectionDetail detail,
                                 String portValue, Object payload,
                                 GrantedPeerDirectory.GrantedPeer peer, HelperJob parent) {
        String targetEnvId = peer.envId();
        String address = resolvePeerIp(peer);
        if (address == null || address.isBlank()) {
            throw new CrossEnvCallException(CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env execute: granted peer " + targetEnvId + " ('" + peer.name()
                            + "') is absent or offline in the local mesh map. "
                            + grantedPeerDirectory.size() + " granted peer(s) known.");
        }
        // The callee authenticates this daemon by the backend-signed mesh token. Without one there is no
        // cross-env identity to present — fail closed BEFORE dialing.
        String meshToken = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
        if (meshToken == null) {
            throw new CrossEnvCallException(CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY,
                    "cross-env execute to " + targetEnvId + " cannot be placed: this daemon holds no "
                            + "backend-signed mesh identity (the handshake delivered no meshToken — a "
                            + "backend that cannot sign cannot cross environments). Re-handshake "
                            + "against a signing backend and retry.");
        }
        Map<String, Object> childEc = buildExecuteEc(srcEc, detail, portValue, payload, targetEnvId);
        HttpClient http = peerHttp(targetEnvId);
        String baseUrl = peerScheme() + "://" + address + ":" + overlayExecutePort;
        String url = baseUrl + "/sqs/v1/overlay/execute";
        // Lets this side cancel the peer's child run if our caller goes away mid-call.
        String callId = com.studioquack.sqs.channel.overlay.PeerCallRegistry.newCallId();
        Runnable cancelOnPeer = () -> cancelPeerCall(http, baseUrl, callId);
        log.info("helper-execute.cross-env → peer env={} addr={} targetNode={}",
                targetEnvId, address, detail.targetNodeId());
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("executionContext", childEc);
            body.put("targetNodeId", detail.targetNodeId());
            body.put("portValue", portValue);
            body.put("payload", payload);
            String bodyJson = mapper.writeValueAsString(body);
            HttpResponse<String> response = awaitPeer(http, peerExecuteRequest(url, meshToken, bodyJson, callId), parent, cancelOnPeer);
            if (response.statusCode() == 401 && refreshMeshIdentity()) {
                // The callee authenticates BEFORE parsing the body or touching execution
                // (MeshPeerAuthenticator), so a 401 means nothing ran on the peer — one retry
                // with the fresh token cannot double-execute.
                String fresh = meshIdentity.currentToken().orElse(null);
                if (fresh != null && !fresh.equals(meshToken)) {
                    log.info("helper-execute.cross-env: peer {} refused the mesh token (401) — "
                            + "re-handshook; retrying once with the fresh identity", targetEnvId);
                    response = awaitPeer(http, peerExecuteRequest(url, fresh, bodyJson, callId), parent, cancelOnPeer);
                }
            }
            if (response.statusCode() == 401) {
                // The callee refused the mesh token even after the re-handshake + retry (: a systematically skewed
                // peer clock is the usual remaining cause).
                throw new CrossEnvCallException(CrossEnvCallException.ERROR_PEER_REJECTED_IDENTITY,
                        "peer " + targetEnvId + " refused this daemon's mesh identity (401) — this "
                                + "daemon re-handshook and retried once; check the clock skew "
                                + "between the two daemons. " + trimBody(response.body()));
            }
            if (response.statusCode() == 403) {
                // The callee's inbound grant re-check denied the call: pass ITS error code through
                // (Error_CrossEnvNotGranted / Error_AuthorizationUnavailable) so the script reads
                // the real reason rather than a generic transport failure.
                throw new CrossEnvCallException(
                        peerErrorCode(response.body(), CrossEnvCallException.ERROR_NOT_GRANTED),
                        "peer " + targetEnvId + " denied the cross-env execute (403): "
                                + trimBody(response.body()));
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("peer " + targetEnvId + " returned "
                        + response.statusCode() + ": " + trimBody(response.body()));
            }
            Map<?, ?> parsed = mapper.readValue(response.body(), Map.class);
            return parsed.get("result");
        } catch (CrossEnvCallException | IllegalStateException rethrow) {
            throw rethrow;
        } catch (IOException e) {
            // Connect refused / connect or read timeout / reset: the granted peer could not be reached on the
            // overlay at dial time (Error_PeerUnreachable) — the message carries the tunnel/directory context
            // an operator needs to tell "tunnel down" apart from "sub-process failed".
            throw new CrossEnvCallException(CrossEnvCallException.ERROR_PEER_UNREACHABLE,
                    "cross-env execute: granted peer " + targetEnvId + " ('" + peer.name() + "') at "
                            + address + " is not reachable on the overlay ("
                            + e.getClass().getSimpleName() + ") — the tunnel is down or the gateway "
                            + "drops the flow. " + grantedPeerDirectory.size()
                            + " granted peer(s) known.", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("cross-env execute to " + targetEnvId
                    + " interrupted", e);
        } catch (Exception e) {
            throw new IllegalStateException("cross-env execute to " + targetEnvId + " failed: "
                    + e.getMessage(), e);
        }
    }

    /**
     * Sends one cross-env execute and waits for the peer's answer for as long as the remote
     * sub-process takes, parked on the response (nothing polls). When the parent's dispatch ends
     * first, the call is cancelled here and the peer is told to cancel its side (its child run),
     * so neither engine keeps working for a caller that is gone.
     */
    private HttpResponse<String> awaitPeer(HttpClient http, HttpRequest request, HelperJob parent,
                                           Runnable cancelOnPeer) throws IOException, InterruptedException {
        CompletableFuture<HttpResponse<String>> call = http.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        Runnable callerGone = () -> {
            call.cancel(true);
            cancelOnPeer.run();
        };
        if (parent != null) {
            parent.onRevoke(callerGone);
        }
        try {
            return call.get();
        } catch (CancellationException | ExecutionException e) {
            if (parent != null && parent.revoked()) {
                throw new CancellationException("the calling node ended before the cross-env call returned");
            }
            if (e instanceof ExecutionException ee && ee.getCause() instanceof IOException io) {
                throw io;
            }
            throw new IllegalStateException(e.getCause() == null ? e : e.getCause());
        } catch (InterruptedException e) {
            call.cancel(true);
            throw e;
        } finally {
            if (parent != null) {
                parent.removeRevokeListener(callerGone);
            }
        }
    }

    /** One cross-env execute request against the peer's overlay endpoint with {@code token}. */
    private HttpRequest peerExecuteRequest(String url, String token, String bodyJson, String callId) {
        return HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + token)
                .header(com.studioquack.sqs.channel.overlay.PeerCallRegistry.CALL_ID_HEADER, callId)
                .POST(HttpRequest.BodyPublishers.ofString(bodyJson))
                .build();
    }

    /**
     * Tells the peer to cancel call {@code callId} (its child run). Fire-and-forget: the caller is
     * already gone, nothing waits on the answer, and an older peer that has no cancel route simply
     * answers 404.
     */
    private void cancelPeerCall(HttpClient http, String baseUrl, String callId) {
        String token = meshIdentity == null ? null : meshIdentity.currentToken().orElse(null);
        if (token == null) {
            return;
        }
        http.sendAsync(com.studioquack.sqs.channel.overlay.PeerCallRegistry.cancelRequest(baseUrl, token, callId),
                        HttpResponse.BodyHandlers.discarding())
                .exceptionally(failure -> {
                    log.debug("helper-execute: cancel of cross-env call {} not delivered: {}", callId, failure.toString());
                    return null;
                });
    }

    /**
     * Re-handshake once to mint a fresh mesh token after a peer rejected the held one (401).
     * {@code false} when the secure channel is off or the refresh itself failed.
     */
    private boolean refreshMeshIdentity() {
        try {
            return meshIdentityRefresher != null && meshIdentityRefresher.getAsBoolean();
        } catch (Exception ex) {
            return false;
        }
    }

    /** Reads the callee's {@code errorCode} out of a denial body; {@code fallback} when absent. */
    private String peerErrorCode(String body, String fallback) {
        try {
            Map<?, ?> parsed = mapper.readValue(body, Map.class);
            Object code = parsed == null ? null : parsed.get("errorCode");
            return code == null ? fallback : String.valueOf(code);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static String trimBody(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 300 ? body : body.substring(0, 300) + "…";
    }

    /**
     * Runs a function-port target on THIS daemon on behalf of a sibling (the far side of a cross-env
     * call, entered from {@code OverlayPeerListener} on the overlay-bound peer endpoint). Deliberately
     * the same {@link #executeLocal} the caller would have used had the target been in its own
     * environment: crossing an environment boundary changes who runs the sub-process, not how.
     */
    public Object executeForPeer(Map<String, Object> executionContext, String targetNodeId,
                                 String portValue, Object payload, HelperJob caller) {
        // The caller daemon stamped the source port type onto the forwarded nextNodeInputList
        // (buildExecuteEc on its side). Recover it so executeLocal's rebuild does not wipe the
        // stamp and reclassify FUNCTION-sourced thinking as an answer on this side.
        ConnectionDetail detail = new ConnectionDetail(targetNodeId, portValue,
                carriedSourcePortType(executionContext, targetNodeId));
        return executeLocal(executionContext, detail, portValue, payload,
                "peer:" + str(executionContext.get("workflowExecutionId")), caller);
    }

    private Object applyRequestTransforms(Map<String, Object> ec, String portValue, Object payload) {
        FunctionPortTransformsDto transforms = lookupTransforms(ec, portValue);
        if (transforms == null || CollectionUtils.isEmpty(transforms.getRequest())) {
            return payload;
        }
        TransformDirectiveApplier.ValueResult vr = transformApplier.applyToValue(transforms.getRequest(), payload);
        return vr.value();
    }

    private Object applyResponseTransforms(Map<String, Object> ec, String portValue, Object raw) {
        FunctionPortTransformsDto transforms = lookupTransforms(ec, portValue);
        if (transforms == null || CollectionUtils.isEmpty(transforms.getResponse())) {
            return raw;
        }
        TransformDirectiveApplier.ValueResult vr = transformApplier.applyToValue(transforms.getResponse(), raw);
        return vr.value();
    }

    private FunctionPortTransformsDto lookupTransforms(Map<String, Object> ec, String portValue) {
        Object raw = ec.get(FUNCTION_PORT_TRANSFORMS_KEY);
        if (!(raw instanceof Map<?, ?> map)) {
            return null;
        }
        Object entry = map.get(portValue);
        if (entry == null) {
            return null;
        }
        return mapper.convertValue(entry, FunctionPortTransformsDto.class);
    }

    List<Object> aggregateSubPiResult(Map<String, Object> rawVars, String childWfeId,
                                     String currentNodeId, String portValue) {
        List<Object> aggregated = new ArrayList<>();
        if (rawVars != null) {
            for (Map.Entry<String, Object> e : rawVars.entrySet()) {
                if (e.getKey() == null || !e.getKey().contains("output_")) {
                    continue;
                }
                List<InOutValueDto> outputList = mapper.convertValue(e.getValue(), new TypeReference<>() {});
                aggregated.addAll(filterEndInputValues(outputList, portValue));
            }
        }
        if (aggregated.isEmpty()) {
            Map<String, List<InOutValueDto>> grouped = contextStore.collectOutputsByNode(childWfeId);
            // Data-flow checkpoint (shape only): the completion vars are REF-only by design, so this
            // sealed-store read is the only real return path. Rows=0 means the sub-PI's persist.outputs never
            // landed under the child run id; rows>0 with count=0 means the harvest filter dropped them.
            StringBuilder rowShape = new StringBuilder();
            for (Map.Entry<String, List<InOutValueDto>> e : grouped.entrySet()) {
                if (rowShape.length() > 0) {
                    rowShape.append(',');
                }
                rowShape.append(e.getKey()).append(':')
                        .append(e.getValue() == null ? -1 : e.getValue().size());
            }
            logExecute(portValue, "helper-execute: sub-pi store child={} rows={} outputsByNode=[{}]",
                    childWfeId, grouped.size(), rowShape);
            for (List<InOutValueDto> outputs : grouped.values()) {
                aggregated.addAll(filterEndInputValues(outputs, portValue));
            }
        }
        logExecute(portValue, "helper-execute: sub-pi aggregate child={} node={} count={} rawKeys={}",
                childWfeId, currentNodeId, aggregated.size(),
                rawVars == null ? "null" : rawVars.keySet());
        return aggregated;
    }

    /**
     * Harvests child outputs that belong on this helper return path: unwired / {@code end_input}
     * (legacy) plus values the designer wired onto the invoked function port (e.g. {@code llm_output →
     * model}) plus the sub-PI's internal terminal wire ({@code end_sub_*}).
     */
    static List<Object> filterEndInputValues(List<InOutValueDto> outputList, String invokedPort) {
        List<Object> values = new ArrayList<>();
        if (outputList == null) {
            return values;
        }
        for (InOutValueDto dto : outputList) {
            if (dto == null) {
                continue;
            }
            String target = dto.getTargetPortValue();
            boolean onReturnPath = StringUtils.isEmpty(target)
                    || "end_input".equalsIgnoreCase(target)
                    || isInternalEndSubWire(target)
                    || (StringUtils.isNotBlank(invokedPort) && invokedPort.equalsIgnoreCase(target));
            if (!onReturnPath) {
                // A dropped row is a candidate "provider produced output but the agent saw
                // nothing" — name it (shape only) instead of vanishing it.
                log.info("helper-execute: harvest drop port={} target={} invoked={} valueEmpty={}",
                        dto.getPortValue(), target, invokedPort, isEmptyPayload(dto.getValue()));
                continue;
            }
                // Function-port return is a transport envelope back to the caller sandbox. The snapshot schema
                // (Invariant 14) still filters graph fan-out {@code value}; the caller owns the return contract,
                // so unfilteredValue wins whenever present — including when the filter left a non-empty but
                // file-stripped map.
                Object value = dto.getUnfilteredValue() != null ? dto.getUnfilteredValue() : dto.getValue();
                if (isEmptyPayload(value)) {
                    log.info("helper-execute: harvest skip empty port={} target={} invoked={}",
                            dto.getPortValue(), target, invokedPort);
                    continue;
                }
                log.info("helper-execute: harvest keep port={} target={} invoked={} usedUnfiltered={} {}",
                        dto.getPortValue(), target, invokedPort,
                        dto.getUnfilteredValue() != null, describeWireShape(value));
                values.add(value);
        }
        return values;
    }

    /**
     * Shape-only description of a harvested/returned wire value: top-level keys, list sizes,
     * and for an LLM envelope the content text length + output_files count. Never values.
     */
    static String describeWireShape(Object wire) {
        StringBuilder sb = new StringBuilder("shape=");
        if (wire == null) {
            return sb.append("null").toString();
        }
        if (wire instanceof List<?> list) {
            return sb.append("list[").append(list.size()).append("]").toString();
        }
        if (!(wire instanceof Map<?, ?> map)) {
            return sb.append(wire.getClass().getSimpleName()).toString();
        }
        sb.append("keys=").append(map.keySet());
        Object result = map.get("result");
        if (result instanceof Map<?, ?> rm) {
            sb.append(" result.keys=").append(rm.keySet());
            Object content = rm.get("content");
            if (content instanceof List<?> cl) {
                sb.append(" contentBlocks=").append(cl.size());
            }
            Object files = rm.get("output_files");
            if (files instanceof List<?> fl) {
                sb.append(" output_files=").append(fl.size());
            }
        }
        Object files = map.get("output_files");
        if (files instanceof List<?> fl) {
            sb.append(" output_files=").append(fl.size());
        }
        return sb.toString();
    }

    /** Internal terminal wire of a sub-PI ({@code end_sub_*} node ports) — always a return path. */
    private static boolean isInternalEndSubWire(String targetPortValue) {
        return targetPortValue != null && targetPortValue.toLowerCase(Locale.ROOT).startsWith("end_sub");
    }

    /** True when a schema filter reduced the payload to nothing usable. */
    private static boolean isEmptyPayload(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        if (value instanceof List<?> list) {
            return list.isEmpty();
        }
        return value instanceof String s && s.isBlank();
    }

    private Map<String, Object> buildExecuteEc(Map<String, Object> srcEc, ConnectionDetail detail,
                                               String portValue, Object payload, String effectiveEnvId) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : srcEc.entrySet()) {
            // Drop the CALLER's resolved node/request. `currentNode` holds the agent node (with the agent's
            // own inputList); carrying it makes the child ExecutionContext.createCurrentNode() short-circuit
            // (`currentNode!= null` → returns), so the TARGET (provider) node is never resolved and the
            // {llm_input} payload set below is never moved onto the target node's inputList. The sub-PI then
            // boots with the CALLER's inputs (wrong targetPortValue) and the provider reports "No input
            // received on 'llm_input' port".
            if ("currentNodeRequest".equals(e.getKey()) || "currentNode".equals(e.getKey())) {
                continue;
            }
            // A Create Node trial's temporary container (and the workspace staged for it) is for
            // the node under test only: the function-port callee runs where it runs in any other run.
            if ("temporaryContainer".equals(e.getKey()) || "stagedWorkspaceId".equals(e.getKey())) {
                continue;
            }
            out.put(e.getKey(), e.getValue());
        }
        out.put("currentNodeId", detail.targetNodeId());
        List<Map<String, Object>> inbound = new ArrayList<>();
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("targetNodeId", detail.targetNodeId());
        item.put("targetPortValue", detail.targetPortValue());
        item.put("portValue", portValue);
        if (detail.sourcePortType() != null) {
            item.put("sourcePortType", detail.sourcePortType());
        }
        // Stamp the CALLER's identity so the target can attribute the delivery — Duckbot end keys/labels a
        // thinking fallback by the source agent, not by its own node id. The child EC overwrites
        // currentNodeId with the TARGET below, so the caller's id and label must be captured from srcEc
        // here (the label also covers targets whose own graph does not contain the caller, e.g.
        // cross-environment calls).
        String callerNodeId = str(srcEc.get("currentNodeId"));
        if (!callerNodeId.isBlank()) {
            item.put("nodeId", callerNodeId);
        }
        String callerLabel = callerNodeLabel(srcEc);
        if (callerLabel != null) {
            item.put("sourceNodeLabel", callerLabel);
        }
        item.put("value", payload);
        inbound.add(item);
        out.put("nextNodeInputList", inbound);
        if (srcEc.containsKey("specListForNodeOutput")) {
            out.put("specListForNodeOutput", srcEc.get("specListForNodeOutput"));
        }
        if (StringUtils.isNotBlank(effectiveEnvId)) {
            out.put("envId", effectiveEnvId);
        }
        return out;
    }

    private List<ConnectionDetail> connectionDetailsForPort(Map<String, Object> ec,
                                                            String portValue, String filterNodeId) {
        if (ec == null || StringUtils.isBlank(portValue)) {
            return List.of();
        }
        Object curNodeRaw = ec.get("currentNode");
        if (!(curNodeRaw instanceof Map<?, ?> curNode)) {
            return List.of();
        }
        Object connListRaw = curNode.get("connectionList");
        if (!(connListRaw instanceof List<?> connList)) {
            return List.of();
        }
        for (Object rawConn : connList) {
            if (!(rawConn instanceof Map<?, ?> conn)) {
                continue;
            }
            if (!portValue.equals(conn.get("portValue"))) {
                continue;
            }
            // The caller's own connection carries the SOURCE port type — the graph fact the
            // target side uses to tell ephemeral stream deliveries (FUNCTION) from answers.
            String sourcePortType = str(conn.get("portType"));
            Object detailsRaw = conn.get("connectionDetailList");
            if (!(detailsRaw instanceof List<?> details)) {
                return List.of();
            }
            List<ConnectionDetail> out = new ArrayList<>();
            for (Object dRaw : details) {
                ConnectionDetail parsed = parseDetail(dRaw, sourcePortType);
                if (parsed == null) {
                    continue;
                }
                if (StringUtils.isNotBlank(filterNodeId) && !filterNodeId.equals(parsed.targetNodeId())) {
                    continue;
                }
                out.add(parsed);
            }
            return out;
        }
        return List.of();
    }

    /** Reads the caller-stamped source port type off a forwarded EC's nextNodeInputList. */
    private static String carriedSourcePortType(Map<String, Object> ec, String targetNodeId) {
        Object raw = ec == null ? null : ec.get("nextNodeInputList");
        if (raw instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> m
                        && (targetNodeId == null || targetNodeId.equals(str(m.get("targetNodeId"))))) {
                    String type = str(m.get("sourcePortType"));
                    if (!type.isBlank()) {
                        return type;
                    }
                }
            }
        }
        return null;
    }

    private ConnectionDetail parseDetail(Object raw, String sourcePortType) {
        if (!(raw instanceof Map<?, ?> d)) {
            return null;
        }
        String nodeId = str(d.get("nodeId"));
        String targetPortValue = "";
        Object portDtoRaw = d.get("portDto");
        if (portDtoRaw instanceof Map<?, ?> portDto) {
            targetPortValue = str(portDto.get("value"));
        } else if (portDtoRaw instanceof PortDto portDto) {
            targetPortValue = portDto.getValue();
        }
        if (StringUtils.isBlank(nodeId)) {
            return null;
        }
        return new ConnectionDetail(nodeId, targetPortValue,
                StringUtils.isBlank(sourcePortType) ? null : sourcePortType);
    }

    private static String str(Object v) {
        return v == null ? "" : String.valueOf(v);
    }

    /** The caller node's canvas label off its EC {@code currentNode} map; null when absent. */
    private static String callerNodeLabel(Map<String, Object> srcEc) {
        Object curNodeRaw = srcEc == null ? null : srcEc.get("currentNode");
        if (curNodeRaw instanceof Map<?, ?> curNode && curNode.get("data") instanceof Map<?, ?> data) {
            String label = str(data.get("label"));
            if (!label.isBlank()) {
                return label;
            }
        }
        return null;
    }

    /**
     * Per-delta {@code execute("thoughts"|"thinking"|"reasoning")} is a log flood at INFO
     * (one sub-PI per token). Session open/close, errors, and non-thinking executes stay INFO.
     */
    private void logExecute(String portValue, String format, Object... args) {
        if (isThinkingPort(portValue)) {
            log.debug(format, args);
        } else {
            log.info(format, args);
        }
    }

    /**
     * One-line, content-free description of a function-port payload for the execute logs. The run-log
     * row ({@code sandbox_log}, mirrored to the control plane) already carries the full input for the
     * Monitoring details view; this line exists so the daemon log shows WHAT was addressed to a port
     * without writing user content into stdout — keys, counts, roles and sizes only, never message
     * text (the same shape-only contract the response checkpoint above documents).
     */
    static String describePayload(Object payload) {
        if (payload == null) {
            return "payload=null";
        }
        if (payload instanceof String s) {
            return "payload=string[" + s.length() + "]";
        }
        if (payload instanceof List<?> l) {
            return "payload=list[" + l.size() + "]";
        }
        if (!(payload instanceof Map<?, ?> m)) {
            return "payload=" + payload.getClass().getSimpleName();
        }
        StringBuilder sb = new StringBuilder("keys=").append(m.keySet());
        if (m.get("messages") instanceof List<?> msgs) {
            Map<String, Integer> roles = new LinkedHashMap<>();
            long chars = 0;
            for (Object msg : msgs) {
                if (msg instanceof Map<?, ?> mm) {
                    Object role = mm.get("role");
                    roles.merge(role == null ? "?" : String.valueOf(role), 1, Integer::sum);
                    chars += contentChars(mm.get("content"));
                }
            }
            sb.append(" messages=").append(msgs.size())
                    .append(" roles=").append(roles)
                    .append(" contentChars=").append(chars);
        }
        if (m.get("inputFiles") instanceof List<?> files) {
            sb.append(" inputFiles=").append(files.size());
        }
        if (m.get("tools") instanceof List<?> tools) {
            sb.append(" tools=").append(tools.size());
            List<String> names = new ArrayList<>();
            for (Object t : tools) {
                if (t instanceof Map<?, ?> tm) {
                    Object n = tm.get("name");
                    if (n != null && !String.valueOf(n).isBlank()) {
                        names.add(String.valueOf(n));
                    }
                }
                if (names.size() >= 8) {
                    break;
                }
            }
            if (!names.isEmpty()) {
                sb.append(" toolNames=").append(names).append(tools.size() > 8 ? "…" : "");
            }
        }
        String out = sb.toString();
        return out.length() <= 400 ? out : out.substring(0, 400) + "…";
    }

    /** Text length of a message {@code content} value (string, or text blocks); never the text. */
    private static long contentChars(Object content) {
        if (content instanceof String s) {
            return s.length();
        }
        if (content instanceof List<?> blocks) {
            long n = 0;
            for (Object b : blocks) {
                if (b instanceof Map<?, ?> bm) {
                    Object t = bm.get("text");
                    if (t instanceof String ts) {
                        n += ts.length();
                    }
                } else if (b instanceof String bs) {
                    n += bs.length();
                }
            }
            return n;
        }
        // Maps / unknown shapes must never stringify — Map.toString includes values (file bytes, text).
        return 0;
    }

    private static boolean isThinkingPort(String portValue) {
        if (portValue == null || portValue.isBlank()) {
            return false;
        }
        String c = portValue.toLowerCase(java.util.Locale.ROOT);
        return c.contains("think") || c.contains("thought") || c.contains("reason");
    }

    /** Duckbot start/end carry no script language — a missing language is not a failure. */
    private static boolean isDuckbotBoundary(ExecutionContext ctx) {
        if (ctx == null || ctx.getCurrentNode() == null || ctx.getCurrentNode().getData() == null) {
            return false;
        }
        String category = ctx.getCurrentNode().getData().getCategory();
        String nodeType = ctx.getCurrentNode().getData().getNodeType();
        if (category == null || nodeType == null) {
            return false;
        }
        boolean duck = "conversion".equalsIgnoreCase(category) || "application".equalsIgnoreCase(category);
        return duck && ("end".equalsIgnoreCase(nodeType) || "start".equalsIgnoreCase(nodeType));
    }

    private String resolvePeerIp(GrantedPeerDirectory.GrantedPeer peer) {
        if (peer == null) {
            return null;
        }
        if (peerAddressResolver != null) {
            return peerAddressResolver.resolve(peer.envId()).orElse(null);
        }
        return null;
    }

    private String peerScheme() {
        return meshPeerHttp != null && meshPeerHttp.meshCaPem() != null ? "https" : "http";
    }

    private HttpClient peerHttp(String targetEnvId) {
        if (meshPeerHttp != null && meshPeerHttp.meshCaPem() != null) {
            return meshPeerHttp.clientFor(targetEnvId);
        }
        return overlayHttp;
    }

    /** Test seam. */
    void setPeerAddressResolver(com.studioquack.sqs.channel.net.PeerAddressResolver resolver) {
        this.peerAddressResolver = resolver;
    }

    void setMeshPeerHttp(com.studioquack.sqs.channel.net.MeshPeerHttp http) {
        this.meshPeerHttp = http;
    }

    private record ConnectionDetail(String targetNodeId, String targetPortValue, String sourcePortType) {
    }
}
