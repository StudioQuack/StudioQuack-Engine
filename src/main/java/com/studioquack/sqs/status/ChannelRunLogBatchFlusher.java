package com.studioquack.sqs.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.ChannelHttp;
import com.studioquack.sqs.channel.ChannelHttpClients;
import com.studioquack.sqs.channel.OutboundBaseHolder;
import com.studioquack.sqs.channel.config.SqsChannelProperties;
import com.studioquack.sqs.runtime.SubProcessCompletionRegistry;
import com.studioquack.sqs.status.log.SandboxLogEntity;
import com.studioquack.sqs.status.log.SandboxLogRepository;
import com.studioquack.sqs.status.log.SandboxLogRowMapper;
import com.studioquack.sqs.status.log.SandboxLogWireRow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URLEncoder;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

/**
 * {@link RunLogBatchFlusher} that POSTs a BATCH run's locally-buffered per-node rows to the
 * control plane in one (chunked) shot over the secure channel.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelRunLogBatchFlusher implements RunLogBatchFlusher {

    /** Rows per POST — bounds the body so a many-node run cannot exceed the backend's JSON cap. */
    static final int CHUNK_SIZE = 200;

    /** Per-chunk POST budget: generous, because a chunk may carry multi-MB hydrated file bytes. */
    private static final Duration CHUNK_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Per-chunk attempts: the flush happens once at the terminal boundary, so a transient refusal (BE
     * restart, overlay blip) must not cost the run its logs. Backoff sleeps between attempts; a hard
     * refusal (e.g. 401 while an older backend runs) exhausts quickly and the rows stay local — the
     * run is then withheld from the reported-purge sweep by the caller.
     */
    static final int MAX_CHUNK_ATTEMPTS = 3;
    private static final long[] RETRY_BACKOFF_MS = {1_000L, 3_000L};

    private final SqsChannelProperties props;
    private final String backendBaseUrl;
    private final ChannelHttpClients httpClients;
    private final ObjectMapper mapper;
    private final OutboundBaseHolder outboundBaseHolder;
    private final SandboxLogRepository repository;
    private final SandboxLogRowMapper rowMapper;
    private final ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry;

    public ChannelRunLogBatchFlusher(SqsChannelProperties props,
                                     @Value("${sqs.backend.base-url:}") String backendBaseUrl,
                                     ChannelHttpClients httpClients,
                                     ObjectMapper mapper,
                                     OutboundBaseHolder outboundBaseHolder,
                                     SandboxLogRepository repository,
                                     SandboxLogRowMapper rowMapper,
                                     ObjectProvider<SubProcessCompletionRegistry> subProcessRegistry) {
        this.props = props;
        this.backendBaseUrl = backendBaseUrl;
        this.httpClients = httpClients;
        this.mapper = mapper;
        this.outboundBaseHolder = outboundBaseHolder;
        this.repository = repository;
        this.rowMapper = rowMapper;
        this.subProcessRegistry = subProcessRegistry;
    }

    /** Null in unit tests that construct this flusher by hand. */
    private volatile com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setExecutionHaltGuard(com.studioquack.sqs.engine.halt.ExecutionHaltGuard executionHaltGuard) {
        this.executionHaltGuard = executionHaltGuard;
    }

    @Override
    public boolean flush(String workflowExecutionId) {
        if (workflowExecutionId == null || workflowExecutionId.isBlank()) {
            return true;
        }
        if (backendBaseUrl == null || backendBaseUrl.isBlank()) {
            log.debug("run-log-batch[channel]: no sqs.backend.base-url — skipping flush for {}",
                    workflowExecutionId);
            return true;
        }
        try {
            List<SandboxLogEntity> entities =
                    repository.findByWorkflowExecutionIdOrderByCdateAsc(workflowExecutionId);
            // HYDRATE: this is the ONE shipment the backend persists (and spills to stored files for the
            // Monitoring download). The on-demand run-logs pull stays REF_ONLY.
            List<SandboxLogWireRow> rows =
                    rowMapper.toWireRows(entities, SandboxLogRowMapper.FileMode.HYDRATE);
            if (rows.isEmpty()) {
                log.debug("run-log-batch[channel]: no buffered rows for run={} — nothing to flush",
                        workflowExecutionId);
                return true;
            }
            // Sub-PI re-key (parity with ChannelSandboxLogCallback's STREAM path): a function-port
            // child runs under a fresh id the BE never registered, so posting under it 404s
            // (Error_RunNotFound) and the provider node's logs never land. Rows stay buffered under
            // the child id locally; only the POST target moves to the top-level run.
            String reportRunId = resolveReportRunId(workflowExecutionId);
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(reportRunId, StandardCharsets.UTF_8)
                    + "/sandbox-logs/batch";
            if (!reportRunId.equals(workflowExecutionId)) {
                log.info("run-log-batch[channel]: run={} is a sub-PI child — posting {} row(s) under top-level run={}",
                        workflowExecutionId, rows.size(), reportRunId);
            }
            int total = rows.size();
            for (int from = 0; from < total; from += CHUNK_SIZE) {
                int to = Math.min(from + CHUNK_SIZE, total);
                if (!postChunk(url, reportRunId, rows.subList(from, to))) {
                    // Do NOT log "flushed": the BE refused at least one chunk, so the local rows
                    // are the only copy. Returning false withholds the run from the reported-purge
                    // sweep (ReportedRunRegistry) — a refused flush must never destroy the logs.
                    log.warn("run-log-batch[channel]: run={} NOT fully flushed ({} row(s) buffered) — "
                            + "rows retained locally; run stays out of the reported-purge sweep",
                            workflowExecutionId, total);
                    return false;
                }
            }
            log.info("run-log-batch[channel] run={} flushed {} row(s) in {} chunk(s)",
                    workflowExecutionId, total, (total + CHUNK_SIZE - 1) / CHUNK_SIZE);
            return true;
        } catch (Exception ex) {
            // Best-effort: the terminal status still goes out; the rows remain in the local store.
            log.warn("run-log-batch[channel]: flush failed for run={}: {}",
                    workflowExecutionId, ex.toString());
            return false;
        }
    }

    /**
     * Resolves the run id the BE actually registered (top-level) for {@code workflowExecutionId} —
     * a function-port sub-PI child maps to its outermost parent. Falls back to the raw id when the
     * registry is unavailable — best-effort, never blocks a flush.
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

    /** POSTs one chunk with bounded retries. */
    private boolean postChunk(String url, String workflowExecutionId, List<SandboxLogWireRow> chunk) {
        for (int attempt = 1; attempt <= MAX_CHUNK_ATTEMPTS; attempt++) {
            try {
                String body = mapper.writeValueAsString(chunk);
                HttpResponse<String> resp = ChannelHttp.postJson(
                        httpClients.get(), url, props.getBearer(), body, CHUNK_TIMEOUT);
                if (ChannelHttp.isOk(resp.statusCode())) {
                    return true;
                }
                log.warn("run-log-batch[channel]: BE {} for run={} chunkSize={} attempt={}/{} body={}",
                        resp.statusCode(), workflowExecutionId, chunk.size(), attempt, MAX_CHUNK_ATTEMPTS,
                        ChannelHttp.trim(resp.body()));
                if (com.studioquack.sqs.engine.halt.ExecutionHaltGuard.isQuotaRefusal(resp.statusCode(), resp.body())) {
                    // The allowance does not renew within the backoff — retrying only repeats the refusal.
                    if (executionHaltGuard != null) {
                        executionHaltGuard.observeLogRefusal(resp.statusCode(), resp.body());
                    }
                    return false;
                }
            } catch (Exception ex) {
                log.warn("run-log-batch[channel]: chunk POST failed for run={} chunkSize={} attempt={}/{}: {}",
                        workflowExecutionId, chunk.size(), attempt, MAX_CHUNK_ATTEMPTS, ex.toString());
            }
            if (attempt < MAX_CHUNK_ATTEMPTS) {
                try {
                    Thread.sleep(RETRY_BACKOFF_MS[attempt - 1]);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }
}
