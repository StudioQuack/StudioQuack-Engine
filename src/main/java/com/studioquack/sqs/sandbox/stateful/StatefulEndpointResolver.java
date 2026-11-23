package com.studioquack.sqs.sandbox.stateful;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Resolves a stateful instance NAME to the address a daemon-side socket can dial, plus the
 * provenance a caller must be checked against. This is what lets a user script name a container
 * instead of an IP: {@code studioquack.connect("my-postgres", 5432)} resolves here, and the
 * sandbox itself never learns (or needs) the address.
 */
@Slf4j
@Component
public class StatefulEndpointResolver {

    /** Label {@link StatefulRunArgs} stamps with the owning environment id. */
    static final String ENV_LABEL = "studioquack.sqs.env";
    /** Label marking a container as a daemon-managed stateful instance. */
    static final String STATEFUL_LABEL = "studioquack.sqs.stateful";
    /** Label carrying the instance id the name was derived from. */
    static final String INSTANCE_LABEL = "studioquack.sqs.instance";

    /**
     * Container names are passed to {@code podman} as an argv element (never a shell string), so this
     * is a sanity/typo fence rather than an injection one — but it also keeps an absurd name out of
     * the daemon log.
     */
    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}");

    private static final Duration INSPECT_TIMEOUT = Duration.ofSeconds(10);

    private final OciCli cli;
    private final ObjectMapper mapper;
    private final StatefulRuntimeProperties props;

    public StatefulEndpointResolver(OciCli cli, ObjectMapper mapper, StatefulRuntimeProperties props) {
        this.cli = cli;
        this.mapper = mapper;
        this.props = props;
    }

    /**
     * One resolved stateful container.
     *
     * @param containerName The real container name that was inspected.
     * @param instanceId The instance id from the container's own label, or {@code null}.
     * @param envId The owning environment from the container's own label, or {@code null}.
     */
    public record Endpoint(String containerName, String instanceId, String envId, String ip,
                           boolean running, boolean stateful) {
    }

    /**
     * The container name a caller-supplied instance name maps to. Both spellings are accepted, so a
     * script may use the short instance id it created the instance with ({@code "my-postgres"}) or
     * the name it sees in {@code podman ps} ({@code "sq-stateful-my-postgres"}).
     */
    public String containerNameFor(String nameOrInstanceId) {
        String raw = nameOrInstanceId == null ? "" : nameOrInstanceId.trim();
        String prefix = props.getContainerNamePrefix();
        return raw.startsWith(prefix) ? raw : prefix + raw;
    }

    /**
     * Resolves a stateful instance name, or empty when the runtime is unavailable, the name is
     * malformed, or no such container exists.
     */
    public Optional<Endpoint> resolve(String nameOrInstanceId) {
        String containerName = containerNameFor(nameOrInstanceId);
        if (!SAFE_NAME.matcher(containerName).matches()) {
            log.debug("stateful-resolve: rejected malformed container name");
            return Optional.empty();
        }
        if (!cli.available()) {
            return Optional.empty();
        }
        OciCli.Result r = cli.run(List.of(cli.cliPath(), "inspect", containerName), INSPECT_TIMEOUT);
        if (!r.ok()) {
            // "no such container" is the normal negative outcome, not an error worth a WARN.
            log.debug("stateful-resolve: inspect {} failed: {}", containerName, r.firstErrLine());
            return Optional.empty();
        }
        try {
            JsonNode root = mapper.readTree(r.out());
            JsonNode c = root.isArray() && !root.isEmpty() ? root.get(0) : root;
            if (c == null || c.isMissingNode()) {
                return Optional.empty();
            }
            JsonNode labels = c.path("Config").path("Labels");
            return Optional.of(new Endpoint(
                    containerName,
                    text(labels.path(INSTANCE_LABEL)),
                    text(labels.path(ENV_LABEL)),
                    bridgeIp(c.path("NetworkSettings")),
                    c.path("State").path("Running").asBoolean(false),
                    "1".equals(text(labels.path(STATEFUL_LABEL)))));
        } catch (Exception e) {
            log.debug("stateful-resolve: unparseable inspect for {}: {}", containerName, e.toString());
            return Optional.empty();
        }
    }

    /**
     * Podman reports the per-network address under {@code NetworkSettings.Networks.<net>.IPAddress}
     * and leaves the top-level {@code IPAddress} empty on the default bridge, so the nested form is
     * tried first and the flat one kept as the fallback for runtimes that only fill that.
     */
    private static String bridgeIp(JsonNode networkSettings) {
        JsonNode networks = networkSettings.path("Networks");
        if (networks.isObject()) {
            for (JsonNode net : networks) {
                String ip = text(net.path("IPAddress"));
                if (ip != null) {
                    return ip;
                }
            }
        }
        return text(networkSettings.path("IPAddress"));
    }

    private static String text(JsonNode node) {
        String v = node == null || node.isMissingNode() || node.isNull() ? "" : node.asText("").trim();
        return v.isEmpty() ? null : v;
    }
}
