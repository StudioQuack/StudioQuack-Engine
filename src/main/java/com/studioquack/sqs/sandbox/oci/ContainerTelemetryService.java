package com.studioquack.sqs.sandbox.oci;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import com.studioquack.sqs.sandbox.runtime.ContainerRuntime;
import com.studioquack.sqs.sandbox.runtime.OciRuntimeKind;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Lists daemon-owned sandbox and stateful containers from the host's OCI CLIs. The selector is
 * intentionally the same provenance label stamped by {@link HardeningArgs}: {@code
 * studioquack.sqs=1}.
 */
@Slf4j
@Component
public class ContainerTelemetryService {

    public static final String LABEL_SELECTOR = "studioquack.sqs=1";
    /** Stateful instances use their own provenance label because they intentionally skip sandbox flags. */
    public static final String STATEFUL_LABEL_SELECTOR = "studioquack.sqs.stateful=1";
    private static final Duration LIST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration INSPECT_TIMEOUT = Duration.ofSeconds(10);

    private final ObjectMapper objectMapper;
    @Nullable
    private final ContainerRuntime activeRuntime;
    private final RuntimeCommandRunner runner;

    @Autowired
    public ContainerTelemetryService(ObjectMapper objectMapper, @Nullable ContainerRuntime activeRuntime) {
        this(objectMapper, activeRuntime, new ProcessRuntimeCommandRunner());
    }

    /** Injectable command-runner seam for deterministic inventory tests. */
    public ContainerTelemetryService(ObjectMapper objectMapper,
                                     @Nullable ContainerRuntime activeRuntime,
                                     RuntimeCommandRunner runner) {
        this.objectMapper = objectMapper;
        this.activeRuntime = activeRuntime;
        this.runner = runner == null ? new ProcessRuntimeCommandRunner() : runner;
    }

    public ContainerTelemetry snapshot() {
        List<RuntimeCandidate> candidates = candidates();
        List<ContainerInfo> items = new ArrayList<>();
        Map<String, String> errors = new LinkedHashMap<>();
        if (candidates.isEmpty()) {
            errors.put("podman", "Podman CLI not found on PATH; install/start Podman to enable sandbox and warm-pool containers");
        }
        for (RuntimeCandidate candidate : candidates) {
            try {
                items.addAll(list(candidate));
            } catch (RuntimeException e) {
                errors.put(candidate.kind(), e.getMessage() == null ? e.toString() : e.getMessage());
                log.debug("container telemetry failed for {}: {}", candidate.kind(), e.toString());
            }
        }
        return new ContainerTelemetry(
                activeKind(),
                candidates.stream().map(RuntimeCandidate::kind).toList(),
                LABEL_SELECTOR,
                Instant.now().toString(),
                items,
                errors);
    }

    private List<ContainerInfo> list(RuntimeCandidate candidate) {
        List<ContainerInfo> out = new ArrayList<>();
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        for (String selector : List.of(LABEL_SELECTOR, STATEFUL_LABEL_SELECTOR)) {
            CliRunner.Result ps = runner.run(List.of(
                    candidate.cli(), "ps", "-a",
                    "--filter", "label=" + selector,
                    "--format", "{{.ID}}"), LIST_TIMEOUT);
            if (!ps.ok()) {
                throw new IllegalStateException(firstLine(ps.stderr(), "ps failed"));
            }
            for (String id : ps.stdout().split("\\R")) {
                String trimmed = id.trim();
                if (!trimmed.isEmpty()) {
                    ids.add(trimmed);
                }
            }
        }
        for (String id : ids) {
            inspect(candidate, id).ifPresent(out::add);
        }
        return out;
    }

    private java.util.Optional<ContainerInfo> inspect(RuntimeCandidate candidate, String id) {
        CliRunner.Result inspect = runner.run(List.of(candidate.cli(), "inspect", id), INSPECT_TIMEOUT);
        if (!inspect.ok()) {
            log.debug("container telemetry inspect failed runtime={} id={}: {}",
                    candidate.kind(), id, firstLine(inspect.stderr(), "inspect failed"));
            return java.util.Optional.empty();
        }
        try {
            JsonNode root = objectMapper.readTree(inspect.stdout());
            JsonNode node = root.isArray() && root.size() > 0 ? root.get(0) : root;
            JsonNode labels = node.path("Config").path("Labels");
            String language = text(labels.path("studioquack.sqs.lang"));
            String role = text(labels.path(HardeningArgs.LABEL_ROLE));
            String name = stripLeadingSlash(text(node.path("Name")));
            String image = firstText(node.path("Config").path("Image"), node.path("ImageName"));
            String status = firstText(node.path("State").path("Status"), node.path("State").path("Running"));
            return java.util.Optional.of(new ContainerInfo(
                    firstText(node.path("Id"), node.path("ID"), textNode(id)),
                    name,
                    status,
                    image,
                    emptyToNull(language),
                    candidate.kind(),
                    emptyToNull(text(node.path("Created"))),
                    "1".equals(text(labels.path("studioquack.sqs.stateful")))
                            ? ContainerKind.STATEFUL_INSTANCE
                            : ContainerKind.fromRoleLabel(role),
                    null));
        } catch (Exception e) {
            log.debug("container telemetry inspect parse failed runtime={} id={}: {}",
                    candidate.kind(), id, e.toString());
            return java.util.Optional.empty();
        }
    }

    private List<RuntimeCandidate> candidates() {
        Map<String, RuntimeCandidate> byKind = new LinkedHashMap<>();
        if (activeRuntime != null && activeRuntime.cli() != null && activeRuntime.kind() != null) {
            byKind.put(activeRuntime.kind().cli(), new RuntimeCandidate(
                    activeRuntime.kind().cli(), activeRuntime.cli()));
        }
        for (OciRuntimeKind kind : OciRuntimeKind.preferenceOrder()) {
            String cli = runner.lookPath(kind.cli());
            if (cli != null && !cli.isBlank()) {
                byKind.putIfAbsent(kind.cli(), new RuntimeCandidate(kind.cli(), cli));
            }
        }
        // Preserve insertion order while avoiding duplicate absolute paths when
        // the active runtime was also found on PATH.
        LinkedHashSet<String> seenCli = new LinkedHashSet<>();
        List<RuntimeCandidate> out = new ArrayList<>();
        for (RuntimeCandidate candidate : byKind.values()) {
            if (seenCli.add(candidate.cli())) {
                out.add(candidate);
            }
        }
        return out;
    }

    private String activeKind() {
        return activeRuntime != null && activeRuntime.kind() != null
                ? activeRuntime.kind().cli()
                : "";
    }

    private static String firstLine(String value, String fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        int nl = value.indexOf('\n');
        return (nl < 0 ? value : value.substring(0, nl)).trim();
    }

    private static String firstText(JsonNode... nodes) {
        for (JsonNode node : nodes) {
            String value = text(node);
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private static JsonNode textNode(String value) {
        return com.fasterxml.jackson.databind.node.TextNode.valueOf(value == null ? "" : value);
    }

    private static String text(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "";
        }
        return node.isTextual() ? node.asText() : String.valueOf(node.asText());
    }

    private static String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String stripLeadingSlash(String value) {
        if (value == null) {
            return "";
        }
        return value.startsWith("/") ? value.substring(1) : value;
    }

    record RuntimeCandidate(String kind, String cli) {
    }

    public interface RuntimeCommandRunner {
        CliRunner.Result run(List<String> argv, Duration timeout);

        @Nullable
        String lookPath(String name);
    }

    private static final class ProcessRuntimeCommandRunner implements RuntimeCommandRunner {
        @Override
        public CliRunner.Result run(List<String> argv, Duration timeout) {
            return CliRunner.run(argv, timeout);
        }

        @Override
        public String lookPath(String name) {
            return CliRunner.lookPath(name);
        }
    }

    public record ContainerTelemetry(
            String activeRuntime,
            List<String> queriedRuntimes,
            String labelSelector,
            String reportedAt,
            List<ContainerInfo> items,
            Map<String, String> errors) {
    }

    /**
     * One item in the daemon's status container inventory. Generic on purpose: the same shape
     * covers a live Podman-inspected sandbox/warm-pool container (this class) AND a registered
     * {@link com.studioquack.sqs.sandbox.stateful.StatefulInstance} merged in by
     * {@link com.studioquack.sqs.command.StatusCommandHandler} — {@code kind} is what tells them
     * apart, {@code image} is always the actual image reference (never a container name).
     */
    public record ContainerInfo(
            String id,
            String name,
            String status,
            String image,
            String language,
            String runtime,
            String created,
            ContainerKind kind,
            Integer warmPoolTarget) {

        /** Adds the configured warm-pool target without changing Podman-derived fields. */
        public ContainerInfo withWarmPoolTarget(Integer target) {
            return new ContainerInfo(id, name, status, image, language, runtime, created, kind, target);
        }
    }

    /**
     * The generic role of a status-inventory item. {@link #WARM_POOL} / {@link #RUN_SANDBOX} come
     * from the ephemeral-sandbox {@code studioquack.sqs.role} label ({@link HardeningArgs#ROLE_WARM}
     * / {@link HardeningArgs#ROLE_JOB}); {@link #STATEFUL_INSTANCE} is a registered stateful
     * instance merged in from {@link com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager}.
     */
    public enum ContainerKind {
        WARM_POOL,
        RUN_SANDBOX,
        STATEFUL_INSTANCE,
        UNKNOWN;

        static ContainerKind fromRoleLabel(String role) {
            if (HardeningArgs.ROLE_WARM.equals(role)) {
                return WARM_POOL;
            }
            if (HardeningArgs.ROLE_JOB.equals(role)) {
                return RUN_SANDBOX;
            }
            return UNKNOWN;
        }
    }
}
