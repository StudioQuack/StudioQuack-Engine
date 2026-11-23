package com.studioquack.sqs.sandbox.helperchan;

import com.studioquack.sqs.execution.transform.HelperScriptProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The real {@link HelperScriptProvider} — supplies the language helper templates that define the
 * in-sandbox {@code studioquack} SDK binding (context reads, {@code return_result}, {@code
 * execute}, metric/log).
 */
@Slf4j
@Component
public class SandboxHelperScriptProvider implements HelperScriptProvider {

    private static final Map<String, String> RESOURCE_BY_LANGUAGE = Map.ofEntries(
            Map.entry("python", "/sandbox/helpers/python.py"),
            Map.entry("py", "/sandbox/helpers/python.py"),
            Map.entry("python3", "/sandbox/helpers/python.py"),
            Map.entry("javascript", "/sandbox/helpers/node.js"),
            Map.entry("js", "/sandbox/helpers/node.js"),
            Map.entry("node", "/sandbox/helpers/node.js"),
            Map.entry("nodejs", "/sandbox/helpers/node.js"),
            Map.entry("typescript", "/sandbox/helpers/node.js"),
            Map.entry("ts", "/sandbox/helpers/node.js"),
            Map.entry("bash", "/sandbox/helpers/bash.sh"),
            Map.entry("sh", "/sandbox/helpers/bash.sh"),
            Map.entry("shell", "/sandbox/helpers/bash.sh"));

    /** Restricted transform helpers (source/target only). */
    private static final Map<String, String> TRANSFORM_RESOURCE_BY_LANGUAGE = Map.ofEntries(
            Map.entry("python", "/sandbox/helpers/transform_python.py"),
            Map.entry("py", "/sandbox/helpers/transform_python.py"),
            Map.entry("python3", "/sandbox/helpers/transform_python.py"),
            Map.entry("javascript", "/sandbox/helpers/transform_node.js"),
            Map.entry("js", "/sandbox/helpers/transform_node.js"),
            Map.entry("node", "/sandbox/helpers/transform_node.js"),
            Map.entry("nodejs", "/sandbox/helpers/transform_node.js"),
            Map.entry("typescript", "/sandbox/helpers/transform_node.js"),
            Map.entry("ts", "/sandbox/helpers/transform_node.js"));

    private final Map<String, String> cache = new ConcurrentHashMap<>();

    @Override
    public Optional<String> helperScriptFor(String language) {
        if (language == null || language.isBlank()) {
            return Optional.empty();
        }
        String resource = RESOURCE_BY_LANGUAGE.get(language.trim().toLowerCase());
        if (resource == null) {
            return Optional.empty(); // unknown language → raw user script (backend fallback parity)
        }
        String template = cache.computeIfAbsent(resource, this::loadResource);
        return template.isEmpty() ? Optional.empty() : Optional.of(template);
    }

    @Override
    public Optional<String> transformHelperScriptFor(String language) {
        if (language == null || language.isBlank()) {
            return Optional.empty();
        }
        String resource = TRANSFORM_RESOURCE_BY_LANGUAGE.get(language.trim().toLowerCase());
        if (resource == null) {
            return Optional.empty();
        }
        String template = cache.computeIfAbsent(resource, this::loadResource);
        return template.isEmpty() ? Optional.empty() : Optional.of(template);
    }

    private String loadResource(String resource) {
        try (InputStream in = getClass().getResourceAsStream(resource)) {
            if (in == null) {
                log.warn("helper template {} missing on classpath — dispatching raw user script.", resource);
                return "";
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("helper template {} failed to load ({}); dispatching raw user script.",
                    resource, e.getMessage());
            return "";
        }
    }
}
