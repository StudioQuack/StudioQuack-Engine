package com.studioquack.sqs.sandbox;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The SQS-local job-response record, mirroring the wire shape from the daemon map (again without
 * importing the control plane DTOs). {@code ScriptDispatchDelegate} feeds {@link
 * #outputVariables()} into {@code TransformPipeline.finalizeOutputs}.
 *
 * @param ok True when the container exited 0.
 */
public record SandboxJobResponse(
        boolean ok,
        int exitCode,
        String stdout,
        String stderr,
        Map<String, Object> outputVariables,
        Map<String, Object> appliedInputs,
        boolean sealedOutputs,
        String errorMessage,
        String errorCode,
        long wallMs,
        String containerId,
        FailureKind failureKind) {

    /**
     * Coarse classification of a dispatch failure, carried end-to-end onto the outbound
     * {@code SandboxLogRecord} / channel body and the BPMN boundary payload so an operator (or a
     * downstream error handler) can tell "my script threw" apart from "the sandbox/container broke"
     * apart from "the daemon itself has a problem" without parsing free-text.
     */
    public enum FailureKind {
        /** No failure — the job succeeded. */
        NONE,
        /** The user's own script/process failed: a typed {@code studioquack.fail()} or a non-zero exit. */
        SCRIPT,
        /** The container/runtime execution itself failed or was cut short (timeout, podman setup). */
        CONTAINER,
        /** No usable sandbox infrastructure for this dispatch (no runtime, no image, isolation floor). */
        INFRA,
        /** The daemon's own dispatch/orchestration glue failed — never reached (or ran past) the sandbox. */
        ENGINE
    }

    /** Typed error code raised when the sandbox process failed (non-zero exit / launch error). */
    public static final String ERROR_SANDBOX_FAILED = "SANDBOX_FAILED";
    /** Typed error code raised when the wall-clock deadline was exceeded. */
    public static final String ERROR_DEADLINE = "SANDBOX_DEADLINE";
    /** Typed error code raised when no backend could run the language. */
    public static final String ERROR_UNSUPPORTED_LANGUAGE = "SANDBOX_UNSUPPORTED_LANGUAGE";
    /**
     * Retained error-code token. Dispatch no longer emits this: a missing live
     * instance falls through to the ephemeral sandbox instead of failing closed.
     */
    public static final String ERROR_STATEFUL_INSTANCE_UNAVAILABLE = "STATEFUL_INSTANCE_UNAVAILABLE";

    public SandboxJobResponse {
        outputVariables = outputVariables == null ? Collections.emptyMap() : outputVariables;
        appliedInputs = appliedInputs == null ? Collections.emptyMap() : appliedInputs;
        failureKind = failureKind == null ? (ok ? FailureKind.NONE : FailureKind.SCRIPT) : failureKind;
    }

    /** True when the job did not complete successfully (drives the error gate). */
    public boolean failed() {
        return !ok;
    }

    /**
     * Builds a success response, folding {@code $stdout}/{@code $stderr}/{@code $exitCode} into the
     * output map alongside the parsed per-port outputs.
     */
    public static SandboxJobResponse success(int exitCode, String stdout, String stderr,
                                             Map<String, Object> portOutputs, long wallMs,
                                             String containerId) {
        Map<String, Object> out = withStdStreams(portOutputs, exitCode, stdout, stderr);
        return new SandboxJobResponse(true, exitCode, stdout, stderr, out,
                Collections.emptyMap(), false, null, null, wallMs, containerId, FailureKind.NONE);
    }

    /**
     * Back-compat failure factory (no explicit {@link FailureKind}): defaults to {@link
     * FailureKind#SCRIPT}, the most common untyped-failure case. Callers that know their failure's
     * origin (sandbox infra, container/podman setup, a typed SDK error) MUST use {@link
     * #failure(int, String, String, Map, String, String, long, String, FailureKind)} instead so the
     * outbound log/boundary classification is accurate.
     */
    public static SandboxJobResponse failure(int exitCode, String stdout, String stderr,
                                             Map<String, Object> portOutputs, String errorCode,
                                             String errorMessage, long wallMs, String containerId) {
        return failure(exitCode, stdout, stderr, portOutputs, errorCode, errorMessage, wallMs,
                containerId, FailureKind.SCRIPT);
    }

    /** Builds a classified failure response carrying the typed error code + message for the error gate. */
    public static SandboxJobResponse failure(int exitCode, String stdout, String stderr,
                                             Map<String, Object> portOutputs, String errorCode,
                                             String errorMessage, long wallMs, String containerId,
                                             FailureKind failureKind) {
        // A failed node must enter only the BPMN error path. stdout/stderr remain on the
        // container-log record; never materialise them (or port outputs) on its normal output port.
        return new SandboxJobResponse(false, exitCode, stdout, stderr, Collections.emptyMap(),
                Collections.emptyMap(), false, errorMessage, errorCode, wallMs, containerId, failureKind);
    }

    private static Map<String, Object> withStdStreams(Map<String, Object> portOutputs, int exitCode,
                                                      String stdout, String stderr) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (portOutputs != null) {
            out.putAll(portOutputs);
        }
        out.put("$stdout", stdout == null ? "" : stdout);
        out.put("$stderr", stderr == null ? "" : stderr);
        out.put("$exitCode", exitCode);
        return out;
    }
}
