package com.studioquack.sqs.deps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Goal B — one entry in the daemon's live dependency manifest, pushed by the
 * backend as part of a {@code set-deps} command. Byte-compatible with the BE
 * {@code EnvironmentDependencyDto} wire shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DependencyEntry(
        @JsonProperty("language") String language,
        @JsonProperty("name") String name,
        @JsonProperty("version") String version,
        @JsonProperty("state") String state,
        @JsonProperty("preload") Boolean preload,
        @JsonProperty("preloadModules") List<String> preloadModules,
        @JsonProperty("digest") String digest,
        @JsonProperty("message") String message,
        @JsonProperty("bytes") Long bytes) {
}
