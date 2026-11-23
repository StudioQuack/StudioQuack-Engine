package com.studioquack.sqs.status;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

/**
 * {@link RunStatusCallback} that pushes the run-index status to the control plane over the secure
 * channel. Active only when {@code sqs.channel.enabled=true}; otherwise
 * {@link LoggingRunStatusCallback} is used so nothing dials a BE.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelRunStatusCallback implements RunStatusCallback {

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    // Lazy (see ChannelSandboxLogCallback): defers the SqsEngine-backed registry to call time so
    // bean construction can't cycle.
    private final ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry;
    private final com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder;
    // Optional. When present, an acked terminal status is recorded so retention can purge local history.
    @Autowired(required = false)
    private com.studioquack.sqs.retention.ReportedRunRegistry reportedRegistry;
    // Optional for the same reason the registry is: the narrow unit tests around this callback
    // construct it directly. Files are released on BOTH tiers when a run ends.
    @Autowired(required = false)
    private com.studioquack.sqs.files.ClientFileStore clientFileStore;

    public ChannelRunStatusCallback(SqsChannelProperties props,
                                    @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                    ChannelHttpClients httpClients,
                                    ObjectMapper mapper,
                                    ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry,
                                    com.studioquack.sqs.channel.OutboundBaseHolder outboundBaseHolder) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.subProcessRegistry = subProcessRegistry;
        this.outboundBaseHolder = outboundBaseHolder;
    }

    /**
     * Test seam for the reported-purge gate (production uses the optional field injection above).
     */
    public void wireReportedRegistryForTests(
            com.studioquack.sqs.retention.ReportedRunRegistry reportedRegistry) {
        this.reportedRegistry = reportedRegistry;
    }

    @Override
    public void report(String workflowExecutionId, Status status, String detail) {
        report(workflowExecutionId, status, detail, null);
    }

    @Override
    public void report(String workflowExecutionId, Status status, String detail, String workflowId) {
        report(workflowExecutionId, status, detail, workflowId, true);
    }

    @Override
    public void report(String workflowExecutionId, Status status, String detail, String workflowId,
                       boolean logsDurable) {
        report(workflowExecutionId, status, detail, workflowId, logsDurable, null);
    }

    @Override
    public void report(String workflowExecutionId, Status status, String detail, String workflowId,
                       boolean logsDurable, String correlationKey) {
        if (workflowExecutionId == null || workflowExecutionId.isBlank() || status == null) {
            return;
        }
        // A function-port sub-PI is an internal child execution the BE never registered as a run;
        // pushing its status would 404 (Error_RunNotFound), and remapping it to the parent run would
        // wrongly finalize that run when the sub-PI (not the whole workflow) completes. Only the
        // top-level run's own status callback reports to the BE.
        if (isSubPiChild(workflowExecutionId)) {
            log.debug("run-status[channel]: skipping sub-PI child status wfe={} status={}",
                    workflowExecutionId, status);
            return;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.debug("run-status[channel]: no sqs.backend.base-url — skipping push for {}", workflowExecutionId);
            return;
        }
        try {
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(workflowExecutionId, StandardCharsets.UTF_8) + "/status";
            String body = mapper.writeValueAsString(new StatusBody(
                    workflowExecutionId, status.name(), detail, props.getAgentVersion(), Instant.now().toString(),
                    blankToNull(workflowId), blankToNull(correlationKey)));
            HttpResponse<String> resp = ChannelHttp.postJson(
                    httpClients.get(), url, props.getBearer(), body, Duration.ofSeconds(15));
            if (!ChannelHttp.isOk(resp.statusCode())) {
                log.warn("run-status[channel]: BE {} for workflowExecutionId={} status={} body={}",
                        resp.statusCode(), workflowExecutionId, status, ChannelHttp.trim(resp.body()));
            } else {
                log.debug("run-status[channel] workflowExecutionId={} status={} -> BE ok",
                        workflowExecutionId, status);
                // BE now holds this run's terminal state — mark it reported so SQS can drop the local
                // execution_context on the next retention sweep (no-op when the registry is absent). A BATCH run
                // whose log flush was refused (logsDurable=false) is NOT marked: the status ack says nothing about
                // the logs, and the local rows are their only surviving copy.
                if (reportedRegistry != null && isTerminal(status) && logsDurable) {
                    reportedRegistry.markReported(workflowExecutionId);
                } else if (reportedRegistry != null && isTerminal(status)) {
                    log.warn("run-status[channel]: run={} terminal status acked but logs NOT durable — "
                            + "withholding reported mark so retention keeps the local log rows",
                            workflowExecutionId);
                }
            }
        } catch (Exception ex) {
            // Best-effort mirror: never let a status push break a handler or engine event.
            log.warn("run-status[channel]: push failed for workflowExecutionId={} status={}: {}",
                    workflowExecutionId, status, ex.toString());
        }
        if (isTerminal(status)) {
            // Files held for this run are inputs to its nodes, not history: once the run is over nothing can
            // dispatch against them. Reclaimed when the run ends.
            releaseRunFiles(workflowExecutionId);
        }
    }

    /**
     * @return {@code true} when the id is a known function-port sub-PI child (best-effort — treats an
     * unavailable registry as "not a child" so a top-level status is never dropped).
     */
    private boolean isSubPiChild(String workflowExecutionId) {
        try {
            SubProcessCompletionRegistry registry = subProcessRegistry.getIfAvailable();
            return registry != null && registry.isSubPiChild(workflowExecutionId);
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    /** Drops this run's held files. Never throws — housekeeping must not fail a status report. */
    private void releaseRunFiles(String workflowExecutionId) {
        if (clientFileStore == null || workflowExecutionId == null || workflowExecutionId.isBlank()) {
            return;
        }
        try {
            clientFileStore.deleteScope(
                    com.studioquack.sqs.files.ClientFileStore.SCOPE_RUN, workflowExecutionId);
        } catch (Exception ex) {
            // The age-based sweep will pick these up; a failure here must not surface to the engine.
            log.warn("run-status[channel]: file cleanup failed for workflowExecutionId={}: {}",
                    workflowExecutionId, ex.toString());
        }
    }

    /** Terminal run statuses whose logs the BE now durably holds (RUNNING is not terminal). */
    private static boolean isTerminal(Status status) {
        return status == Status.COMPLETED || status == Status.FAILED || status == Status.CANCELED;
    }

    /** Blank / null workflowId is omitted from the wire (NON_NULL) so existing BE parsers stay valid. */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /** Run-index status body (camelCase, nulls omitted). BE-side route owns the exact schema. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record StatusBody(String workflowExecutionId, String status, String detail, String agentVersion, String at,
                      String workflowId, String correlationKey) {
    }
}
