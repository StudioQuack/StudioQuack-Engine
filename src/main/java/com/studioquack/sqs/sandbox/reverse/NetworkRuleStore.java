package com.studioquack.sqs.sandbox.reverse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The daemon's projected copy of the Network Access Control rules, in BOTH directions. The
 * backend's Postgres table (V76) is the source of truth; this store is replaced wholesale by every
 * {@code set-network-rules} command and by the handshake snapshot — a projection is never a delta,
 * so a rule deleted while the daemon was down cannot linger.
 */
@Slf4j
@Component
public class NetworkRuleStore {

    /** One projected inbound rule: "sourceEnvId may dial my published reverse port {@code port}". */
    public record Rule(String sourceEnvId, int port) {
    }

    /** One projected outbound rule: "my containers may dial targetEnvId's reverse port {@code port}". */
    public record OutboundRule(String targetEnvId, int port) {
    }

    private final Path file;
    private final ObjectMapper objectMapper;
    private final AtomicReference<Set<Rule>> rules = new AtomicReference<>(Set.of());
    private final AtomicReference<Set<OutboundRule>> outbound = new AtomicReference<>(Set.of());

    /** Single subscriber (the sandbox egress manager) reprogramming live containers on change. */
    private volatile Runnable changeListener = () -> {
    };

    public NetworkRuleStore(
            @Value("${sqs.sandbox.network-rules.state-path:${SQS_DATA_DIR:./data}/network-rules.json}") String path,
            ObjectMapper objectMapper) {
        this.file = Path.of(path);
        this.objectMapper = objectMapper;
        Set<Rule> loaded = loadFromDisk();
        if (!loaded.isEmpty()) {
            rules.set(loaded);
            log.info("network-rules: restored {} rule(s) from {}", loaded.size(), file);
        }
        Set<OutboundRule> loadedOutbound = loadOutboundFromDisk();
        if (!loadedOutbound.isEmpty()) {
            outbound.set(loadedOutbound);
            log.info("network-rules: restored {} outbound rule(s) from {}", loadedOutbound.size(), file);
        }
    }

    /** Test seam — in-memory only. */
    public static NetworkRuleStore inMemory() {
        return new NetworkRuleStore("/nonexistent-network-rules.json", new ObjectMapper());
    }

    /**
     * Registers the single change subscriber (the sandbox egress manager). Fired after every
     * wholesale replacement so live containers' egress rules re-derive from the new projection.
     */
    public void setChangeListener(Runnable listener) {
        this.changeListener = listener == null ? () -> {
        } : listener;
    }

    /**
     * Replaces BOTH views of the projection wholesale and persists them. Never throws (best-effort
     * IO).
     */
    public void replaceAll(List<Rule> next, List<OutboundRule> nextOutbound) {
        Set<Rule> clean = new LinkedHashSet<>();
        if (next != null) {
            for (Rule r : next) {
                if (r == null || r.sourceEnvId() == null || r.sourceEnvId().isBlank()
                        || r.port() < 1 || r.port() > 65535) {
                    continue;
                }
                clean.add(r);
            }
        }
        Set<OutboundRule> cleanOutbound = new LinkedHashSet<>();
        if (nextOutbound != null) {
            for (OutboundRule r : nextOutbound) {
                if (r == null || r.targetEnvId() == null || r.targetEnvId().isBlank()
                        || r.port() < 1 || r.port() > 65535) {
                    continue;
                }
                cleanOutbound.add(r);
            }
        }
        rules.set(Set.copyOf(clean));
        outbound.set(Set.copyOf(cleanOutbound));
        persist(clean, cleanOutbound);
        log.info("network-rules: projection replaced ({} inbound, {} outbound rule(s))",
                clean.size(), cleanOutbound.size());
        changeListener.run();
    }

    /** @return the source engine ids allowed to dial {@code port} (empty = deny all). */
    public Set<String> allowedSourceEngineIds(int port) {
        Set<String> out = new LinkedHashSet<>();
        for (Rule r : rules.get()) {
            if (r.port() == port) {
                out.add(r.sourceEnvId());
            }
        }
        return out;
    }

    /** @return the current inbound projection (diagnostics / status surface). */
    public Set<Rule> snapshot() {
        return rules.get();
    }

    /** @return the current outbound projection — what THIS engine's containers may dial. */
    public Set<OutboundRule> outboundSnapshot() {
        return outbound.get();
    }

    private void persist(Set<Rule> set, Set<OutboundRule> outboundSet) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("version", 2);
            ArrayNode arr = root.putArray("rules");
            for (Rule r : set) {
                ObjectNode row = arr.addObject();
                row.put("sourceEnvId", r.sourceEnvId());
                row.put("port", r.port());
            }
            ArrayNode outArr = root.putArray("outbound");
            for (OutboundRule r : outboundSet) {
                ObjectNode row = outArr.addObject();
                row.put("targetEnvId", r.targetEnvId());
                row.put("port", r.port());
            }
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(file, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(root));
        } catch (Exception e) {
            log.warn("network-rules: persist to {} failed (in-memory copy stands): {}", file, e.toString());
        }
    }

    private Set<Rule> loadFromDisk() {
        try {
            if (!Files.isRegularFile(file)) {
                return Set.of();
            }
            return Set.copyOf(parse(objectMapper.readTree(Files.readAllBytes(file))));
        } catch (Exception e) {
            log.warn("network-rules: could not read {} (starting empty): {}", file, e.toString());
            return Set.of();
        }
    }

    private Set<OutboundRule> loadOutboundFromDisk() {
        try {
            if (!Files.isRegularFile(file)) {
                return Set.of();
            }
            // A v1 file carries no outbound array — parses as empty (fail closed, never wider).
            return Set.copyOf(parseOutbound(objectMapper.readTree(Files.readAllBytes(file))));
        } catch (Exception e) {
            log.warn("network-rules: could not read outbound from {} (starting empty): {}", file, e.toString());
            return Set.of();
        }
    }

    /** Parses the inbound half of the {@code set-network-rules} payload / handshake snapshot. */
    public static List<Rule> parse(JsonNode root) {
        List<Rule> out = new ArrayList<>();
        JsonNode arr = root == null ? null : root.get("rules");
        if (arr == null || !arr.isArray()) {
            return out;
        }
        for (JsonNode row : arr) {
            String source = row.path("sourceEnvId").asText("").trim();
            int port = row.path("port").asInt(0);
            if (!source.isEmpty() && port >= 1 && port <= 65535) {
                out.add(new Rule(source, port));
            }
        }
        return out;
    }

    /** Parses the outbound half ({@code outbound:[{targetEnvId, port}]}); absent means empty. */
    public static List<OutboundRule> parseOutbound(JsonNode root) {
        List<OutboundRule> out = new ArrayList<>();
        JsonNode arr = root == null ? null : root.get("outbound");
        if (arr == null || !arr.isArray()) {
            return out;
        }
        for (JsonNode row : arr) {
            String target = row.path("targetEnvId").asText("").trim();
            int port = row.path("port").asInt(0);
            if (!target.isEmpty() && port >= 1 && port <= 65535) {
                out.add(new OutboundRule(target, port));
            }
        }
        return out;
    }
}
