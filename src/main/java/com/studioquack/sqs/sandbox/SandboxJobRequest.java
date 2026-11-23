package com.studioquack.sqs.sandbox;

import java.util.Collections;
import java.util.Map;

/**
 * Everything {@link SandboxExecutionPort} needs to run one node in the sandbox. This is the
 * daemon's own job-request record mirroring the wire shape documented in the daemon map.
 *
 * @param jobId Unique per dispatch (also the helper-channel bearer suffix).
 * @param language Resolved script language.
 */
public record SandboxJobRequest(
        String jobId,
        SandboxLanguage language,
        String source,
        Map<String, Object> inputVariables,
        Map<String, Object> executionContext,
        String envId,
        String workflowExecutionId,
        String currentNodeId,
        String templateId,
        String bespokeDir,
        SandboxLimits limits,
        EgressPolicy egress,
        IsolationTier isolationTier,
        Map<String, String> env,
        String jobMode,
        Object secretsTree,
        String ociImageRef,
        boolean stateful,
        double cpus,
        int diskMb,
        java.util.Set<String> secretLiterals,
        java.util.List<AttachedFile> attachedFiles,
        String contextBase64,
        boolean streaming,
        boolean temporaryContainer,
        String stagedWorkspaceId) {

    /**
     * A file to materialise inside the sandbox scratch before the interpreter starts. Resolved from a
     * {@code sq-file:} handle by the dispatch delegate, so by the time it reaches here the name is
     * already a sanitised basename and the size is already within the client limit.
     *
     * @param name Sanitised basename the file gets inside the sandbox files directory.
     */
    public record AttachedFile(String name, byte[] content) { }

    /** Canonical constructor normalises nullable collection/enum fields so callers never NPE. */
    public SandboxJobRequest {
        limits = limits == null ? SandboxLimits.unbounded() : limits;
        egress = egress == null ? EgressPolicy.DEFAULT : egress;
        inputVariables = inputVariables == null ? Collections.emptyMap() : inputVariables;
        env = env == null ? Collections.emptyMap() : env;
        jobMode = jobMode == null || jobMode.isBlank() ? "user" : jobMode;
        secretLiterals = secretLiterals == null ? Collections.emptySet() : secretLiterals;
        attachedFiles = attachedFiles == null ? java.util.List.of() : attachedFiles;
    }

    /** True when the job names an OCI image that may have a live environment instance. */
    public boolean hasOciImageRef() {
        return ociImageRef != null && !ociImageRef.isBlank();
    }

    public static Builder builder(String jobId, SandboxLanguage language, String source) {
        return new Builder(jobId, language, source);
    }

    /** Fluent builder — most fields are optional so a record all-args constructor is unwieldy. */
    public static final class Builder {
        private final String jobId;
        private final SandboxLanguage language;
        private final String source;
        private Map<String, Object> inputVariables = Collections.emptyMap();
        private Map<String, Object> executionContext;
        private String envId;
        private String workflowExecutionId;
        private String currentNodeId;
        private String templateId;
        private String bespokeDir;
        private SandboxLimits limits = SandboxLimits.unbounded();
        private EgressPolicy egress = EgressPolicy.DEFAULT;
        private IsolationTier isolationTier;
        private Map<String, String> env = Collections.emptyMap();
        private String jobMode = "user";
        private Object secretsTree;
        private String ociImageRef;
        private boolean stateful;
        private double cpus;
        private int diskMb;
        private java.util.Set<String> secretLiterals = Collections.emptySet();
        private java.util.List<AttachedFile> attachedFiles = java.util.List.of();
        private String contextBase64;
        private boolean streaming;
        private boolean temporaryContainer;
        private String stagedWorkspaceId;

        private Builder(String jobId, SandboxLanguage language, String source) {
            this.jobId = jobId;
            this.language = language;
            this.source = source;
        }

        public Builder inputVariables(Map<String, Object> v) { this.inputVariables = v; return this; }
        public Builder executionContext(Map<String, Object> v) { this.executionContext = v; return this; }
        public Builder envId(String v) { this.envId = v; return this; }
        public Builder workflowExecutionId(String v) { this.workflowExecutionId = v; return this; }
        public Builder currentNodeId(String v) { this.currentNodeId = v; return this; }
        public Builder templateId(String v) { this.templateId = v; return this; }
        public Builder bespokeDir(String v) { this.bespokeDir = v; return this; }
        public Builder limits(SandboxLimits v) { this.limits = v; return this; }
        public Builder egress(EgressPolicy v) { this.egress = v; return this; }
        public Builder isolationTier(IsolationTier v) { this.isolationTier = v; return this; }
        public Builder env(Map<String, String> v) { this.env = v; return this; }
        public Builder jobMode(String v) { this.jobMode = v; return this; }
        public Builder secretsTree(Object v) { this.secretsTree = v; return this; }
        public Builder ociImageRef(String v) { this.ociImageRef = v; return this; }
        public Builder stateful(boolean v) { this.stateful = v; return this; }
        public Builder cpus(double v) { this.cpus = v; return this; }
        public Builder diskMb(int v) { this.diskMb = v; return this; }
        public Builder secretLiterals(java.util.Set<String> v) { this.secretLiterals = v; return this; }
        public Builder attachedFiles(java.util.List<AttachedFile> v) { this.attachedFiles = v; return this; }
        public Builder contextBase64(String v) { this.contextBase64 = v; return this; }
        public Builder streaming(boolean v) { this.streaming = v; return this; }
        public Builder temporaryContainer(boolean v) { this.temporaryContainer = v; return this; }
        public Builder stagedWorkspaceId(String v) { this.stagedWorkspaceId = v; return this; }

        public SandboxJobRequest build() {
            return new SandboxJobRequest(jobId, language, source, inputVariables, executionContext,
                    envId, workflowExecutionId, currentNodeId, templateId, bespokeDir, limits, egress,
                    isolationTier, env, jobMode, secretsTree, ociImageRef, stateful, cpus, diskMb,
                    secretLiterals, attachedFiles, contextBase64, streaming, temporaryContainer,
                    stagedWorkspaceId);
        }
    }
}
