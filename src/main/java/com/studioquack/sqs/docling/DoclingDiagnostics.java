package com.studioquack.sqs.docling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.stateful.OciCli;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;

/**
 * Post-mortem for a Docling conversion that died mid-request. The failure the user actually hits
 * is not a Docling bug: the side-car is killed by the host (or by its own memory cap) while it is
 * loading OCR weights, and the HTTP client sees only {@code Unexpected end of file from server}.
 */
@Slf4j
@Component
public class DoclingDiagnostics {

    /** Bounded so a wedged runtime cannot hold the dispatch thread while we ask it questions. */
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(5);

    /** Container-log tail attached to the node's stderr. Enough for the OCR/startup trace, not a dump. */
    private static final int LOG_TAIL_LINES = 80;

    /** 128 = SIGKILL offset; 137 = 128+9, what the kernel OOM killer / runtime stop produces. */
    private static final int EXIT_CODE_SIGKILL = 137;

    private final OciCli cli;
    private final StatefulInstanceManager statefulInstanceManager;
    private final ObjectMapper mapper;
    private final boolean enabled;

    public DoclingDiagnostics(OciCli cli,
                              StatefulInstanceManager statefulInstanceManager,
                              ObjectMapper mapper,
                              @Value("${sqs.docling.diagnostics-enabled:true}") boolean enabled) {
        this.cli = cli;
        this.statefulInstanceManager = statefulInstanceManager;
        this.mapper = mapper;
        this.enabled = enabled;
    }

    /**
     * @param oomKilled
     * @param exitCode
     * @param restartCount How many times the runtime has restarted it (a non-zero value on a
     * just-failed conversion is itself the diagnosis)
     * @param state
     * @param summary One non-PHI line for the node log, never null
     * @param logTail
     */
    public record Diagnosis(boolean oomKilled,
                            Integer exitCode,
                            Integer restartCount,
                            String state,
                            String summary,
                            String logTail) {

        /**
         * True when the evidence points at the side-car being killed rather than rejecting the
         * document — the case an operator has to fix by giving the machine or the cap more memory.
         */
        public boolean resourceExhaustion() {
            return oomKilled
                    || (exitCode != null && exitCode == EXIT_CODE_SIGKILL)
                    || (restartCount != null && restartCount > 0);
        }
    }

    /**
     * Inspects the Docling side-car. Returns null when diagnostics are off or no runtime is
     * available, so callers keep their existing generic message.
     */
    public Diagnosis diagnose() {
        if (!enabled || cli == null || !cli.available() || cli.cliPath() == null) {
            return null;
        }
        String containerName = resolveContainerName();
        try {
            OciCli.Result inspect = cli.run(
                    List.of(cli.cliPath(), "inspect", containerName), PROBE_TIMEOUT);
            if (!inspect.ok()) {
                String err = inspect.firstErrLine();
                boolean missing = err != null && (err.contains("no such container")
                        || err.contains("failed to find") || err.toLowerCase().contains("not found"));
                String summary = missing
                        ? "the Docling side-car container " + containerName + " is not present"
                        : "could not inspect Docling side-car " + containerName
                        + (err == null || err.isBlank() ? "" : " (" + err + ")");
                return new Diagnosis(false, null, null, missing ? "absent" : "unknown", summary, null);
            }
            JsonNode root = mapper.readTree(inspect.stdout());
            JsonNode container = root.isArray() && root.size() > 0 ? root.get(0) : root;
            JsonNode state = container.path("State");
            boolean oom = state.path("OOMKilled").asBoolean(false);
            Integer exitCode = state.has("ExitCode") ? state.path("ExitCode").asInt() : null;
            Integer restarts = container.has("RestartCount") ? container.path("RestartCount").asInt() : null;
            String status = state.path("Status").asText("");

            boolean killed = oom || (exitCode != null && exitCode == EXIT_CODE_SIGKILL)
                    || (restarts != null && restarts > 0);
            return new Diagnosis(oom, exitCode, restarts, status,
                    summarise(oom, exitCode, restarts, status),
                    // Only pull the log when something actually killed it; a healthy side-car's
                    // startup trace is noise on an ordinary conversion error.
                    killed ? logTail(containerName) : null);
        } catch (Exception e) {
            log.debug("docling-diagnostics: inspect of {} failed: {}", containerName, e.toString());
            return null;
        }
    }

    /**
     * Name of the container that is actually serving Docling. The tracked RUNNING instance wins
     * (it may carry a reused non-hint id); the deterministic hint name is the fallback.
     */
    private String resolveContainerName() {
        try {
            return statefulInstanceManager.byInstanceId(DoclingContainer.INSTANCE_ID)
                    .filter(com.studioquack.sqs.sandbox.stateful.StatefulInstance::isRunning)
                    .map(com.studioquack.sqs.sandbox.stateful.StatefulInstance::containerName)
                    .orElseGet(() -> statefulInstanceManager.containerNameFor(DoclingContainer.INSTANCE_ID));
        } catch (Exception e) {
            return statefulInstanceManager.containerNameFor(DoclingContainer.INSTANCE_ID);
        }
    }

    private String summarise(boolean oom, Integer exitCode, Integer restarts, String status) {
        if (oom || (exitCode != null && exitCode == EXIT_CODE_SIGKILL)) {
            return "the Docling side-car was killed for exceeding its memory limit"
                    + " (state=" + status + ", exitCode=" + exitCode + ", restarts=" + restarts + ")."
                    + " Give the machine more free memory or raise sqs.docling.ram-mb.";
        }
        if (restarts != null && restarts > 0) {
            return "the Docling side-car restarted mid-conversion"
                    + " (state=" + status + ", restarts=" + restarts + ", exitCode=" + exitCode + ")."
                    + " This is normally host memory pressure; check free memory on this machine.";
        }
        // Healthy / still-running with no kill evidence: do not imply the converter faulted —
        // the caller may have given up (client read budget) or hit a non-fatal HTTP error.
        return "the Docling side-car is " + (status.isBlank() ? "in an unknown state" : status)
                + " with no kill evidence (exitCode=" + exitCode + ", restarts=" + restarts + ")";
    }

    /**
     * True when the convert wait was exhausted — a per-request HTTP read timeout, a poll-loop budget
     * expiry surfaced as a socket timeout, or an HTTP 504 — not when the side-car died or refused the
     * document. WHY: Spring's {@code SimpleClientHttpRequestFactory} surfaces an expired read as
     * {@link ResourceAccessException}/{@link SocketTimeoutException} ("Read timed out").
     */
    static boolean isClientReadTimeout(Throwable throwable) {
        return isConvertBudgetExceeded(throwable);
    }

    /** @see #isClientReadTimeout(Throwable) */
    static boolean isConvertBudgetExceeded(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (containsReadTimedOut(t.getMessage())) {
                return true;
            }
            if (t instanceof HttpStatusCodeException http
                    && http.getStatusCode().value() == HttpStatus.GATEWAY_TIMEOUT.value()) {
                return true;
            }
            // SocketTimeoutException with a null/empty message under ResourceAccessException is still
            // the read-budget path when the wrapper did not already match above; skip connect timeouts.
            if (t instanceof SocketTimeoutException && !containsConnectTimedOut(t.getMessage())) {
                return true;
            }
            if (t instanceof ResourceAccessException
                    && t.getCause() instanceof SocketTimeoutException
                    && !containsConnectTimedOut(t.getMessage())
                    && !containsConnectTimedOut(t.getCause().getMessage())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Operator-facing line when {@link #isConvertBudgetExceeded(Throwable)} is true.
     *
     * @param budgetSeconds configured {@code sqs.docling.convert-timeout-seconds} (the poll-loop
     *                      wall-clock on {@link DoclingClient})
     */
    static String formatClientTimeoutMessage(long budgetSeconds) {
        return "conversion exceeded the convert budget of " + budgetSeconds
                + "s (sqs.docling.convert-timeout-seconds); the Docling"
                + " side-car is probably still converting — raise that property if documents need longer";
    }

    private static boolean containsReadTimedOut(String message) {
        return message != null && message.contains("Read timed out");
    }

    private static boolean containsConnectTimedOut(String message) {
        return message != null && message.toLowerCase().contains("connect timed out");
    }

    private String logTail(String containerName) {
        try {
            OciCli.Result logs = cli.run(List.of(cli.cliPath(), "logs",
                    "--tail", Integer.toString(LOG_TAIL_LINES), containerName), PROBE_TIMEOUT);
            // The side-car writes its startup/OCR trace to stderr, so both streams matter here.
            String out = logs.stdout() == null ? "" : logs.stdout();
            String err = logs.stderr() == null ? "" : logs.stderr();
            String combined = (err + (err.isBlank() || out.isBlank() ? "" : "\n") + out).trim();
            return combined.isBlank() ? null : combined;
        } catch (Exception e) {
            return null;
        }
    }
}
