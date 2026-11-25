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
import java.util.ArrayList;
import java.util.List;

/**
 * {@link RunLogBatchFlusher} that POSTs a BATCH run's locally-buffered per-node rows to the
 * control plane in one (chunked) shot over the secure channel.
 *
 * <p>Chunks respect both {@link SandboxLogIngestLimits#MAX_BATCH_ROWS} and
 * {@link SandboxLogIngestLimits#MAX_CHUNK_BYTES} so a hydrated flush stays under the
 * control-plane 16 MiB reject. A single row that alone exceeds the byte budget is slimmed
 * (IO hydration dropped, streams capped) before POST; a 413 oversize refusal is a hard failure
 * (no retry).
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "sqs.channel", name = "enabled", havingValue = "true")
public class ChannelRunLogBatchFlusher implements RunLogBatchFlusher {

    /** Rows per POST — matches control-plane batch row cap. */
    static final int CHUNK_SIZE = SandboxLogIngestLimits.MAX_BATCH_ROWS;

    /** Per-chunk POST budget: generous, because a chunk may carry multi-MB hydrated file bytes. */
    private static final Duration CHUNK_TIMEOUT = Duration.ofSeconds(60);

    /**
     * Per-chunk attempts: the flush happens once at the terminal boundary, so a transient refusal
     * (restart, overlay blip) must not cost the run its logs. Backoff sleeps between attempts; a
     * hard refusal (413 oversize / quota) exhausts immediately and the rows stay local.
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
            // HYDRATE: this is the ONE shipment the control plane persists (and spills to stored
            // files for the Monitoring download). The on-demand run-logs pull stays REF_ONLY.
            List<SandboxLogWireRow> rows =
                    rowMapper.toWireRows(entities, SandboxLogRowMapper.FileMode.HYDRATE);
            if (rows.isEmpty()) {
                log.debug("run-log-batch[channel]: no buffered rows for run={} — nothing to flush",
                        workflowExecutionId);
                return true;
            }
            // Sub-PI re-key (parity with ChannelSandboxLogCallback's STREAM path): a function-port
            // child runs under a fresh id the control plane never registered, so posting under it
            // 404s (Error_RunNotFound) and the provider node's logs never land. Rows stay buffered
            // under the child id locally; only the POST target moves to the top-level run.
            String reportRunId = resolveReportRunId(workflowExecutionId);
            String url = outboundBaseHolder.currentEnvironmentBase(props.environmentBase(backendBaseUrl))
                    + "/runs/" + URLEncoder.encode(reportRunId, StandardCharsets.UTF_8)
                    + "/sandbox-logs/batch";
            if (!reportRunId.equals(workflowExecutionId)) {
                log.info("run-log-batch[channel]: run={} is a sub-PI child — posting {} row(s) under top-level run={}",
                        workflowExecutionId, rows.size(), reportRunId);
            }
            List<List<SandboxLogWireRow>> chunks = partitionByBudget(rows);
            int chunkIndex = 0;
            for (List<SandboxLogWireRow> chunk : chunks) {
                chunkIndex++;
                if (!postChunk(url, reportRunId, chunk)) {
                    log.warn("run-log-batch[channel]: run={} NOT fully flushed ({} row(s) buffered, "
                                    + "failed at chunk {}/{}) — rows retained locally; run stays out of "
                                    + "the reported-purge sweep",
                            workflowExecutionId, rows.size(), chunkIndex, chunks.size());
                    return false;
                }
            }
            log.info("run-log-batch[channel] run={} flushed {} row(s) in {} chunk(s)",
                    workflowExecutionId, rows.size(), chunks.size());
            return true;
        } catch (Exception ex) {
            // Best-effort: the terminal status still goes out; the rows remain in the local store.
            log.warn("run-log-batch[channel]: flush failed for run={}: {}",
                    workflowExecutionId, ex.toString());
            return false;
        }
    }

    /**
     * Packs rows into chunks that stay under both the row cap and the byte budget.
     * A single oversize row is slimmed so the flush can still progress.
     */
    List<List<SandboxLogWireRow>> partitionByBudget(List<SandboxLogWireRow> rows) throws Exception {
        List<List<SandboxLogWireRow>> chunks = new ArrayList<>();
        List<SandboxLogWireRow> current = new ArrayList<>();
        int currentBytes = 2; // []
        for (SandboxLogWireRow raw : rows) {
            SandboxLogWireRow row = fitRow(raw);
            byte[] encoded = mapper.writeValueAsBytes(row);
            int rowBytes = encoded.length + (current.isEmpty() ? 0 : 1); // comma
            boolean overRows = current.size() >= CHUNK_SIZE;
            boolean overBytes = !current.isEmpty()
                    && currentBytes + rowBytes > SandboxLogIngestLimits.MAX_CHUNK_BYTES;
            if (overRows || overBytes) {
                chunks.add(current);
                current = new ArrayList<>();
                currentBytes = 2;
                rowBytes = encoded.length;
            }
            current.add(row);
            currentBytes += rowBytes;
        }
        if (!current.isEmpty()) {
            chunks.add(current);
        }
        return chunks;
    }

    /**
     * Ensures one wire row serializes under {@link SandboxLogIngestLimits#MAX_CHUNK_BYTES}.
     * Drops hydrated IO first, then caps streams.
     */
    SandboxLogWireRow fitRow(SandboxLogWireRow row) throws Exception {
        if (row == null) {
            return null;
        }
        byte[] encoded = mapper.writeValueAsBytes(row);
        if (encoded.length <= SandboxLogIngestLimits.MAX_CHUNK_BYTES) {
            return row;
        }
        log.warn("run-log-batch[channel]: single row node={} bytes={} exceeds chunk budget — "
                        + "dropping hydrated IO and capping streams",
                row.nodeId(), encoded.length);
        SandboxLogWireRow slim = new SandboxLogWireRow(
                row.nodeId(), row.nodeName(), row.status(), row.exitCode(), row.durationMs(),
                row.language(), row.containerId(),
                capStream(row.stdout()), capStream(row.stderr()), capStream(row.errorMessage()),
                null, null, null, row.unknownInputParameters(),
                row.failureKind(), row.postureNote(), row.cdate(), row.logId(), row.retry());
        encoded = mapper.writeValueAsBytes(slim);
        if (encoded.length <= SandboxLogIngestLimits.MAX_CHUNK_BYTES) {
            return slim;
        }
        log.error("run-log-batch[channel]: row node={} still {} bytes after slim — sending status stub",
                row.nodeId(), encoded.length);
        return new SandboxLogWireRow(
                row.nodeId(), row.nodeName(), row.status(), row.exitCode(), row.durationMs(),
                row.language(), row.containerId(),
                "[omitted: exceeded sandbox-log ingest budget]", null,
                row.errorMessage() == null ? null : capStream(row.errorMessage()),
                null, null, null, null,
                row.failureKind(), row.postureNote(), row.cdate(), row.logId(), row.retry());
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
     * Resolves the run id the control plane registered (top-level) for {@code workflowExecutionId} —
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

    /**
     * POSTs one chunk with bounded retries. Never throws.
     *
     * @return {@code true} only when the control plane acknowledged a chunk with 2xx.
     */
    private boolean postChunk(String url, String workflowExecutionId, List<SandboxLogWireRow> chunk) {
        for (int attempt = 1; attempt <= MAX_CHUNK_ATTEMPTS; attempt++) {
            try {
                String body = mapper.writeValueAsString(chunk);
                if (body.getBytes(StandardCharsets.UTF_8).length > SandboxLogIngestLimits.MAX_REQUEST_BYTES) {
                    log.error("run-log-batch[channel]: local body {} bytes exceeds {} — refusing chunk "
                                    + "(run={} rows={})",
                            body.getBytes(StandardCharsets.UTF_8).length,
                            SandboxLogIngestLimits.MAX_REQUEST_BYTES, workflowExecutionId, chunk.size());
                    return false;
                }
                HttpResponse<String> resp = ChannelHttp.postJson(
                        httpClients.get(), url, props.getBearer(), body, CHUNK_TIMEOUT);
                if (ChannelHttp.isOk(resp.statusCode())) {
                    return true;
                }
                log.warn("run-log-batch[channel]: BE {} for run={} chunkSize={} attempt={}/{} body={}",
                        resp.statusCode(), workflowExecutionId, chunk.size(), attempt, MAX_CHUNK_ATTEMPTS,
                        ChannelHttp.trim(resp.body()));
                if (SandboxLogIngestLimits.isOversizeRefusal(resp.statusCode(), resp.body())) {
                    log.error("run-log-batch[channel]: control plane rejected oversize log chunk run={} rows={} — "
                                    + "engine must chunk under {} bytes",
                            workflowExecutionId, chunk.size(), SandboxLogIngestLimits.MAX_REQUEST_BYTES);
                    return false;
                }
                if (com.studioquack.sqs.engine.halt.ExecutionHaltGuard.isQuotaRefusal(resp.statusCode(), resp.body())) {
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
