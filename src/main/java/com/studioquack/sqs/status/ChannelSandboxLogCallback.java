package com.studioquack.sqs.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.status.log.SandboxLogFileHydrator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

/**
 * {@link SandboxLogCallback} that pushes one node's sandbox result to the control plane over the
 * secure channel. Active only when {@code sqs.channel.enabled=true}; otherwise
 * {@link LoggingSandboxLogCallback} is used so nothing dials a BE.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelSandboxLogCallback implements SandboxLogCallback {

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    // Lazy: the registry pulls in SqsEngine, which is downstream of the dispatch/sandbox graph this
    // callback is wired into — ObjectProvider defers resolution to call time so bean construction
    // can never deadlock on a cycle.
    private final ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry;
    private final com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder;
    // Shared file-handle → inline-base64 hydration (also used by the BATCH flush / on-demand pull).
    private final SandboxLogFileHydrator fileHydrator;
    // Shared with reportStarted: one lane per (run, node) so STARTED is posted before terminal.
    private final AsyncSandboxLogDispatcher dispatcher;

    public ChannelSandboxLogCallback(SqsChannelProperties props,
                                     @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                     ChannelHttpClients httpClients,
                                     ObjectMapper mapper,
                                     ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry,
                                     com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder,
                                     SandboxLogFileHydrator fileHydrator,
                                     AsyncSandboxLogDispatcher dispatcher) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.subProcessRegistry = subProcessRegistry;
        this.outboundBaseHolder = outboundBaseHolder;
        this.fileHydrator = fileHydrator;
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

    /** Null in unit tests that construct this callback by hand. */
    private volatile com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setExecutionHaltGuard(com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard) {
        this.executionHaltGuard = executionHaltGuard;
    }

    @Override
    public void report(SandboxLogRecord record) {
        if (record == null || record.workflowExecutionId() == null || record.workflowExecutionId().isBlank()
                || record.nodeId() == null || record.nodeId().isBlank()) {
            // Loud, because the consequence is a node that never appears in the run's logs at all, and
            // a silent return here is indistinguishable from a step that never executed.
            log.warn("sandbox-log[channel]: dropping a terminal record with no run id / node id "
                            + "(run={} node={})",
                    record == null ? null : record.workflowExecutionId(),
                    record == null ? null : record.nodeId());
            return;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.debug("sandbox-log[channel]: no sqs.backend.base-url — skipping push for {}",
                    record.workflowExecutionId());
            return;
        }
        // Hydration + 60s POST leave the dispatch thread here; same lane as reportStarted.
        try {
            dispatcher.enqueue(record.workflowExecutionId(), record.nodeId(),
                    () -> postTerminal(record));
        } catch (Exception ex) {
            log.warn("sandbox-log[channel]: enqueue failed for workflowExecutionId={} node={}: {}",
                    record.workflowExecutionId(), record.nodeId(), ex.toString());
        }
    }

    /**
     * Existing terminal-report body: remap a function-port child run id, hydrate {@code sq-file:}
     * handles, then POST (60 s). Never throws — a hung or failing BE must not surface to dispatch.
     */
    private void postTerminal(SandboxLogRecord record) {
        // A function-port sub-PI runs under a fresh child run id the control plane never registered an
        // ExecutionHistory for, so posting its per-node log under that id 404s and the provider node's
        // sandbox log never reaches. Attribute it to the top-level run the control plane knows so the
        // Monitoring per-node panels populate (the record still carries the real nodeId).
        String reportRunId = resolveReportRunId(record.workflowExecutionId());
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(reportRunId, StandardCharsets.UTF_8)
                    + "/sandbox-logs";
            // Hydrate sq-file: handles to inline base64 so the control plane can store the file
            // (inline when small, stored-file chunked past its threshold). Body must stay under
            // SandboxLogIngestLimits (16 MiB reject); slim if needed.
            Object hydratedInput = hydrate(record.input());
            Object hydratedOutput = hydrate(record.output());
            String body = fitTerminalBody(record, hydratedInput, hydratedOutput);
            if (body == null) {
                dispatcher.markLossy(record.workflowExecutionId());
                log.error("sandbox-log[channel]: refusing oversize terminal body run={} node={} "
                                + "(exceeds {} bytes even after slim)",
                        record.workflowExecutionId(), record.nodeId(),
                        SandboxLogIngestLimits.MAX_REQUEST_BYTES);
                return;
            }
            HttpResponse<String> resp = ChannelHttp.postJson(
                    httpClients.get(), url, props.getBearer(), body, Duration.ofSeconds(60));
            if (!ChannelHttp.isOk(resp.statusCode())) {
                // Not stored in BE: the local rows stay the only copy — never purge-eligible.
                dispatcher.markLossy(record.workflowExecutionId());
                if (executionHaltGuard != null) {
                    executionHaltGuard.observeLogRefusal(resp.statusCode(), resp.body());
                }
                if (SandboxLogIngestLimits.isOversizeRefusal(resp.statusCode(), resp.body())) {
                    log.error("sandbox-log[channel]: control plane 413 oversize for run={} node={} — "
                                    + "engine must keep POST under {} bytes",
                            record.workflowExecutionId(), record.nodeId(),
                            SandboxLogIngestLimits.MAX_REQUEST_BYTES);
                }
                log.warn("sandbox-log[channel]: BE {} for workflowExecutionId={} reportAs={} node={} body={}",
                        resp.statusCode(), record.workflowExecutionId(), reportRunId, record.nodeId(),
                        ChannelHttp.trim(resp.body()));
            } else {
                String trimmed = ChannelHttp.trim(resp.body());
                if (trimmed != null && trimmed.contains("\"persisted\":false")) {
                    // 200 + persisted=false is the backend declining to store the row (a run it already
                    // considers terminal, most often). That is a legitimate answer, but silently
                    // treating it as success is why a step could vanish between the two sides.
                    dispatcher.markLossy(record.workflowExecutionId());
                    log.warn("sandbox-log[channel]: BE accepted but did NOT persist run={} reportAs={} node={}: {}",
                            record.workflowExecutionId(), reportRunId, record.nodeId(), trimmed);
                } else {
                    log.info("sandbox-log[channel] run={} reportAs={} node={} status={} -> BE stored",
                            record.workflowExecutionId(), reportRunId, record.nodeId(), record.status());
                }
            }
        } catch (Exception ex) {
            // Best-effort mirror: never let a log push break a dispatch or engine event. The run is
            // marked lossy so self-hosted retention keeps its local copy.
            dispatcher.markLossy(record.workflowExecutionId());
            log.warn("sandbox-log[channel]: push failed for workflowExecutionId={} node={}: {}",
                    record.workflowExecutionId(), record.nodeId(), ex.toString());
        }
    }

    @Override
    public void reportStarted(SandboxStartRecord record) {
        if (record == null || record.workflowExecutionId() == null || record.workflowExecutionId().isBlank()
                || record.nodeId() == null || record.nodeId().isBlank()) {
            return;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            return;
        }
        // Same dispatcher as report(): enqueue STARTED first on this lane so it POSTs before terminal.
        // The BE maps status="STARTED" to a PROCESSING sandbox-log row (intent SANDBOX_EXECUTION_STARTED).
        try {
            dispatcher.enqueue(record.workflowExecutionId(), record.nodeId(),
                    () -> postStarted(record));
        } catch (Exception ex) {
            log.debug("sandbox-log[channel] STARTED enqueue failed for workflowExecutionId={} node={}: {}",
                    record.workflowExecutionId(), record.nodeId(), ex.toString());
        }
    }

    /** Existing STARTED HTTP body (10 s timeout). Never throws. */
    private void postStarted(SandboxStartRecord record) {
        String reportRunId = resolveReportRunId(record.workflowExecutionId());
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(reportRunId, StandardCharsets.UTF_8)
                    + "/sandbox-logs";
            String body = mapper.writeValueAsString(new Body(
                    record.nodeId(), record.nodeName(), "STARTED", null, null,
                    record.language(), null, null, null, null, null, null, null, null, null, null,
                    record.logId(), null));
            HttpResponse<String> resp = ChannelHttp.postJson(
                    httpClients.get(), url, props.getBearer(), body, Duration.ofSeconds(10));
            if (!ChannelHttp.isOk(resp.statusCode())) {
                if (executionHaltGuard != null) {
                    executionHaltGuard.observeLogRefusal(resp.statusCode(), resp.body());
                }
                log.debug("sandbox-log[channel] STARTED: BE {} for workflowExecutionId={} node={}",
                        resp.statusCode(), record.workflowExecutionId(), record.nodeId());
            }
        } catch (Exception ex) {
            log.debug("sandbox-log[channel] STARTED push failed for workflowExecutionId={} node={}: {}",
                    record.workflowExecutionId(), record.nodeId(), ex.toString());
        }
    }

    /**
     * Maps a function-port sub-PI child run id to the top-level run the BE registered, so the node's
     * log is attributed to a run that actually exists. Falls back to the raw id for top-level runs or
     * when the registry is unavailable — best-effort, never blocks a log.
     */
    private String resolveReportRunId(String workflowExecutionId) {
        try {
            SubProcessCompletionRegistry registry = subProcessRegistry.getIfAvailable();
            if (registry != null) {
                return registry.resolveReportableRunId(workflowExecutionId);
            }
        } catch (RuntimeException ignored) {
            // fall through to the raw id
        }
        return workflowExecutionId;
    }

    /**
     * Builds a terminal POST body under {@link SandboxLogIngestLimits#MAX_CHUNK_BYTES}.
     * Drops hydrated IO first, then caps streams; returns {@code null} only when even a
     * status stub would exceed the ingest reject ceiling.
     */
    private String fitTerminalBody(SandboxLogRecord record, Object hydratedInput, Object hydratedOutput)
            throws Exception {
        Body full = new Body(
                record.nodeId(), record.nodeName(), record.status(), record.exitCode(),
                record.durationMs(), record.language(), record.containerId(),
                record.stdout(), record.stderr(), record.errorMessage(),
                hydratedInput, hydratedOutput, record.appliedInputs(), record.unknownInputParameters(),
                record.failureKind(), record.postureNote(), record.logId(),
                Boolean.TRUE.equals(record.retry()) ? Boolean.TRUE : null);
        String json = mapper.writeValueAsString(full);
        if (json.getBytes(StandardCharsets.UTF_8).length <= SandboxLogIngestLimits.MAX_CHUNK_BYTES) {
            return json;
        }
        log.warn("sandbox-log[channel]: terminal body run={} node={} over chunk budget — "
                        + "dropping hydrated IO / capping streams",
                record.workflowExecutionId(), record.nodeId());
        Body slim = new Body(
                record.nodeId(), record.nodeName(), record.status(), record.exitCode(),
                record.durationMs(), record.language(), record.containerId(),
                capStream(record.stdout()), capStream(record.stderr()), capStream(record.errorMessage()),
                null, null, null, record.unknownInputParameters(),
                record.failureKind(), record.postureNote(), record.logId(),
                Boolean.TRUE.equals(record.retry()) ? Boolean.TRUE : null);
        json = mapper.writeValueAsString(slim);
        if (json.getBytes(StandardCharsets.UTF_8).length <= SandboxLogIngestLimits.MAX_CHUNK_BYTES) {
            return json;
        }
        Body stub = new Body(
                record.nodeId(), record.nodeName(), record.status(), record.exitCode(),
                record.durationMs(), record.language(), record.containerId(),
                "[omitted: exceeded sandbox-log ingest budget]", null,
                capStream(record.errorMessage()),
                null, null, null, null,
                record.failureKind(), record.postureNote(), record.logId(),
                Boolean.TRUE.equals(record.retry()) ? Boolean.TRUE : null);
        json = mapper.writeValueAsString(stub);
        if (json.getBytes(StandardCharsets.UTF_8).length > SandboxLogIngestLimits.MAX_REQUEST_BYTES) {
            return null;
        }
        return json;
    }

    private static String capStream(String s) {
        if (s == null) {
            return null;
        }
        int cap = Math.min(s.length(), 1024 * 1024);
        if (cap == s.length()) {
            return s;
        }
        return s.substring(0, cap) + "\n…[truncated by engine ingest budget]";
    }

    /**
     * Hydrates {@code sq-file:} handles to inline base64 via the shared {@link SandboxLogFileHydrator}
     * (also used by the BATCH flush and on-demand pull), so the backend sees the original port schema
     * shape and can offer a download. Null-safe: a missing hydrator (unit test) passes the payload
     * through unchanged.
     */
    private Object hydrate(Object payload) {
        return fileHydrator == null ? payload : fileHydrator.hydrate(payload);
    }

    /** Optional — absent when the local write failed. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Body(String nodeId, String nodeName, String status, Integer exitCode, Long durationMs,
                String language, String containerId, String stdout, String stderr, String errorMessage,
                Object input, Object output, Object appliedInputs, java.util.List<String> unknownInputParameters,
                String failureKind, String postureNote, String logId, Boolean retry) {
    }
}
