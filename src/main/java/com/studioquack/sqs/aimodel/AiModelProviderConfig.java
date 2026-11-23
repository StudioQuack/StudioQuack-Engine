package com.studioquack.sqs.aimodel;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;

/**
 * The OpenAI-compatible provider connection this daemon proxies {@code studioquack.llm.*} calls
 * to. Pushed from the backend as a {@code set-ai-model} command; byte-compatible with the control
 * plane {@code OpenAiProviderConfig} DTO wire shape.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AiModelProviderConfig(
        @JsonProperty("server") String server,
        @JsonProperty("apiKey") String apiKey,
        @JsonProperty("chatPath") String chatPath,
        @JsonProperty("modelsPath") String modelsPath,
        @JsonProperty("loadPath") String loadPath,
        @JsonProperty("unloadPath") String unloadPath,
        @JsonProperty("downloadPath") String downloadPath,
        @JsonProperty("downloadStatusPath") String downloadStatusPath,
        @JsonProperty("defaultModel") String defaultModel,
        @JsonProperty("loadedModelsPath") String loadedModelsPath,
        @JsonProperty("modelPayloadField") String modelPayloadField,
        @JsonProperty("variantsPath") String variantsPath,
        @JsonProperty("variantPayloadField") String variantPayloadField) {

    /** Backwards-compatible constructor without {@link #loadedModelsPath} (unmanaged reconciliation). */
    public AiModelProviderConfig(String server, String apiKey, String chatPath, String modelsPath,
                                 String loadPath, String unloadPath, String downloadPath,
                                 String downloadStatusPath, String defaultModel) {
        this(server, apiKey, chatPath, modelsPath, loadPath, unloadPath, downloadPath,
                downloadStatusPath, defaultModel, null, null, null, null);
    }

    /** Backwards-compatible constructor without {@link #modelPayloadField} (defaults to {@code model}). */
    public AiModelProviderConfig(String server, String apiKey, String chatPath, String modelsPath,
                                 String loadPath, String unloadPath, String downloadPath,
                                 String downloadStatusPath, String defaultModel, String loadedModelsPath) {
        this(server, apiKey, chatPath, modelsPath, loadPath, unloadPath, downloadPath,
                downloadStatusPath, defaultModel, loadedModelsPath, null, null, null);
    }

    /** The unconfigured default the holder seeds with before any BE push. */
    public static final AiModelProviderConfig EMPTY =
            new AiModelProviderConfig(null, null, null, null, null, null, null, null, null, null, null, null, null);

    /** True once a non-blank {@link #server} is set. */
    public boolean configured() {
        return server != null && !server.isBlank();
    }

    /** The list-models path, or the OpenAI default {@code /v1/models}. */
    public String modelsPathOrDefault() {
        return (modelsPath == null || modelsPath.isBlank()) ? "/v1/models" : modelsPath;
    }

    /** The chat-completions path, or the OpenAI default {@code /v1/chat/completions}. */
    public String chatPathOrDefault() {
        return (chatPath == null || chatPath.isBlank()) ? "/v1/chat/completions" : chatPath;
    }

    /**
     * The JSON field name the provider's load/unload body keys the model on. Most managed servers take
     * {@code "model"} (the default); some expect a different key — Unsloth Studio uses
     * {@code "model_path"}, text-generation-webui and TabbyAPI use {@code "model_name"}.
     */
    public String modelPayloadFieldOrDefault() {
        return (modelPayloadField == null || modelPayloadField.isBlank()) ? "model" : modelPayloadField;
    }

    /**
     * The JSON field name the provider's load body keys the model VARIANT (quant) on, or {@code null}
     * when the provider has no variant concept. Unsloth Studio uses {@code "gguf_variant"}: a GGUF
     * repo ships several quant files (Q4_K_M, BF16, ...) and loading without the variant makes the
     * server pick a default and DOWNLOAD it even when another quant is already local.
     */
    public String variantPayloadFieldOrDefault() {
        return (variantPayloadField == null || variantPayloadField.isBlank()) ? null : variantPayloadField;
    }

    /** True when the provider exposes a per-variant (quant) listing the daemon can expand from. */
    public boolean hasVariants() {
        return variantsPath != null && !variantsPath.isBlank();
    }

    /**
     * Absolute variant-list URL for one model, or {@code null} when no {@link #variantsPath} is set.
     * The path is a template carrying {@code {id}} and/or {@code {path}} placeholders: {@code {id}} is
     * the model's load identifier, {@code {path}} its local directory.
     */
    public String variantsUrlFor(String id, String path) {
        return hasVariants() ? variantsUrlForTemplate(variantsPath, id, path) : null;
    }

    /**
     * Same substitution against an EXPLICIT template — used for a provider-shape default when the
     * saved config predates {@link #variantsPath} (a config stored before the field existed must
     * still load the exact local quant rather than let the server pick and download one).
     */
    public String variantsUrlForTemplate(String template, String id, String path) {
        if (template == null || template.isBlank() || !configured()) {
            return null;
        }
        String resolved = template.trim()
                .replace("{id}", urlEncode(id == null ? "" : id))
                .replace("{path}", urlEncode(path == null ? "" : path));
        return join(server, resolved);
    }

    /** Minimal query-value encoder (encodes everything except unreserved chars). */
    private static String urlEncode(String value) {
        try {
            return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    /** Absolute list-models URL ({@link #server} + models path), tolerant of slashes. */
    public String modelsUrl() {
        return join(server, modelsPathOrDefault());
    }

    /**
     * Absolute chat-completions URL ({@link #server} + chat path). This is the
     * target the daemon proxies {@code studioquack.llm.completion/chat} calls to.
     */
    public String chatUrl() {
        return join(server, chatPathOrDefault());
    }

    /**
     * Absolute model-load URL, or {@code null} when the provider does not expose a
     * load path (a plain OpenAI-compatible endpoint has no load/unload — only a
     * local model server does). The llm proxy returns a structured "unsupported"
     * error for a null URL rather than guessing a path.
     */
    public String loadUrl() {
        return optionalUrl(loadPath);
    }

    /** Absolute model-unload URL, or {@code null} when the provider defines none. */
    public String unloadUrl() {
        return optionalUrl(unloadPath);
    }

    /** Absolute model-download URL, or {@code null} when the provider defines none. */
    public String downloadUrl() {
        return optionalUrl(downloadPath);
    }

    /** Absolute model-download-status URL, or {@code null} when the provider defines none. */
    public String downloadStatusUrl() {
        return optionalUrl(downloadStatusPath);
    }
    /**
     * When the explicit {@code loadedModelsPath} is blank but the load path ends in {@code /load} (the
     * LM Studio preset shape, {@code /api/v1/models/load}), the loaded-models list lives at the same
     * root minus the suffix. Configs saved before the explicit field existed therefore reconcile with
     * zero operator action; an explicit value always wins for providers with a different layout.
     */
    public String loadedModelsUrl() {
        if (loadedModelsPath != null && !loadedModelsPath.isBlank()) {
            return join(server, loadedModelsPath);
        }
        if (loadPath != null) {
            String p = loadPath.trim();
            if (p.endsWith("/load") && p.length() > "/load".length()) {
                return join(server, p.substring(0, p.length() - "/load".length()));
            }
        }
        return null;
    }

    /** Joins an OPTIONAL per-service path onto {@link #server}, or {@code null} when blank. */
    private String optionalUrl(String path) {
        return (path == null || path.isBlank()) ? null : join(server, path);
    }

    private static String join(String base, String path) {
        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        String p = path.startsWith("/") ? path : "/" + path;
        return b + p;
    }
}
