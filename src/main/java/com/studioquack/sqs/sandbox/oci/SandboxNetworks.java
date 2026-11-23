package com.studioquack.sqs.sandbox.oci;

/**
 * Dedicated Podman network names for job sandboxes vs stateful instances. These names are
 * intentionally distinct: warm/cold job sandboxes attach to {@link #JOBS} while stateful instances
 * attach to {@link #STATEFUL}.
 */
public final class SandboxNetworks {

    /**
     * Warm slots and cold/ephemeral job sandboxes ({@link HardeningArgs} under
     * {@link com.studioquack.sqs.sandbox.EgressPolicy#OPEN} egress).
     */
    public static final String JOBS = "sqs-jobs";

    /** Long-lived stateful instances ({@code StatefulRunArgs}). */
    public static final String STATEFUL = "sqs-stateful";

    /**
     * Image builds ({@code OciImageBuildService}). A build compiles user-authored source and resolves
     * its own dependencies, so it needs internet egress — but it is untrusted content and must not be
     * adjacent to a running job or stateful instance either.
     */
    public static final String BUILD = "sqs-build";

    private SandboxNetworks() {
    }
}
