package com.studioquack.sqs.sandbox.stateful;

import com.studioquack.sqs.sandbox.helperchan.StreamInstanceLocator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * Production {@link StreamInstanceLocator}: the live {@code (env, imageRef)} stateful instance's
 * bridge address + the in-instance app port/path. Same resolution dispatch uses ({@link
 * StatefulInstanceManager#resolveRunning}), then {@link StatefulEndpointResolver} for a
 * daemon-dialable IP.
 */
@Slf4j
@Component
public class StatefulStreamInstanceLocator implements StreamInstanceLocator {

    private final StatefulInstanceManager instances;
    private final StatefulEndpointResolver endpoints;
    private final StatefulRuntimeProperties props;

    public StatefulStreamInstanceLocator(@Nullable StatefulInstanceManager instances,
                                         @Nullable StatefulEndpointResolver endpoints,
                                         @Nullable StatefulRuntimeProperties props) {
        this.instances = instances;
        this.endpoints = endpoints;
        this.props = props;
    }

    @Override
    public Optional<Location> locate(String envId, Map<String, Object> targetNode) {
        if (instances == null || endpoints == null || props == null) {
            return Optional.empty();
        }
        if (envId == null || envId.isBlank() || targetNode == null) {
            return Optional.empty();
        }
        String imageRef = ociImageRef(targetNode);
        if (imageRef.isEmpty()) {
            return Optional.empty();
        }
        StatefulInstance inst = instances.resolveRunning(envId, imageRef).orElse(null);
        if (inst == null || !envId.equals(inst.envId())) {
            return Optional.empty();
        }
        StatefulEndpointResolver.Endpoint ep = endpoints.resolve(inst.instanceId()).orElse(null);
        if (ep == null || !ep.running() || ep.ip() == null || ep.ip().isBlank()) {
            return Optional.empty();
        }
        if (ep.envId() != null && !envId.equals(ep.envId())) {
            log.warn("stream-through: instance env mismatch caller={} owner={} — refusing",
                    envId, ep.envId());
            return Optional.empty();
        }
        String base = props.getAppPath();
        int port = props.getAppPort();
        if (port < 1 || port > 65535) {
            return Optional.empty();
        }
        return Optional.of(new Location(ep.ip(), port, base, inst.imageRef(), inst.containerId()));
    }

    /** {@code data.executionRequirementsDto.ociImageRef} from the serialized node map. */
    static String ociImageRef(Map<String, Object> node) {
        if (node == null || !(node.get("data") instanceof Map<?, ?> data)) {
            return "";
        }
        if (!(data.get("executionRequirementsDto") instanceof Map<?, ?> req)) {
            return "";
        }
        Object ref = req.get("ociImageRef");
        return ref == null ? "" : String.valueOf(ref).trim();
    }
}
