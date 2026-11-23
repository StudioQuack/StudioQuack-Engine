package com.studioquack.sqs.sandbox;

/**
 * SPI for running a prepared node job in the in-process sandbox. This is the seam that exposes to
 * the rest of the daemon.
 */
public interface SandboxExecutionPort {

    /**
     * Runs {@code job} to completion (blocking) and returns the local job-response record. Never
     * throws for a job-level failure — a non-zero exit, deadline, or unsupported language is reported
     * via {@link SandboxJobResponse#failed()} + {@link SandboxJobResponse#errorCode()} so the caller
     * can route the error gate deterministically.
     */
    SandboxJobResponse execute(SandboxJobRequest job);

    /**
     * @return a human-readable name of the wired backend (e.g. {@code "oci (podman, intermediate)"}),
     * for boot banners / audit.
     */
    String name();

    /** @return true when a live OCI runtime is available so real containers can run. */
    boolean runtimeAvailable();
}
