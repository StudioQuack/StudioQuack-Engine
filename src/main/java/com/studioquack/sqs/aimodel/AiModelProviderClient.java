package com.studioquack.sqs.aimodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Thin HTTP client that reaches the configured OpenAI-compatible provider on behalf of the
 * {@code list-models} / {@code test-connection} command handlers. Uses the JDK {@link HttpClient}
 * (no extra dependency) with short timeouts so an offline provider fails fast rather than hanging
 * the heartbeat command thread.
 */
@Slf4j
@Component
public class AiModelProviderClient {

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Separator joining a model's load id to its variant (quant) in the EXPANDED row id —
     * {@code "owner/repo@Q4_K_M"}. Unsloth ignores the chat {@code model} field for routing (it serves
     * the loaded model), so carrying the variant in the id is safe end to end: picker value,
     * loaded-state key, and load payload all derive from it.
     */
    static final String VARIANT_SEPARATOR = "@";

    /**
     * Unsloth Studio's variant contract, applied when the provider's load route is Unsloth's but the
     * stored config predates the variant fields. Without this an existing provider keeps loading by
     * bare repo id, which makes the server pick its preferred quant and DOWNLOAD it even though
     * another quant is already on disk.
     */
    private static final String UNSLOTH_VARIANT_FIELD = "gguf_variant";
    private static final String UNSLOTH_VARIANTS_PATH =
            "/api/hub/gguf-variants?repo_id={id}&local_path={path}&prefer_local_cache=true";

    private final AiModelProviderHolder holder;
    private final AiModelLoadState loadState;
    private final AiModelCapabilityState capabilityState;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    public AiModelProviderClient(AiModelProviderHolder holder, AiModelLoadState loadState,
                                 AiModelCapabilityState capabilityState) {
        this.holder = holder;
        this.loadState = loadState;
        this.capabilityState = capabilityState;
    }

    /**
     * Probe HOW this provider's model lifecycle is controlled WITHOUT loading a model. Any of those
     * must demote the provider to {@link AiModelCapabilityState.Lifecycle#PROVIDER_MANAGED} so the
     * operator sees "Managed By Provider" instead of a Load button that silently does nothing.
     */
    public AiModelCapabilityState.Capability probeCapability(AiModelProviderConfig config) {
        if (config == null || !config.configured()) {
            return AiModelCapabilityState.Capability.providerManaged("No provider configured");
        }
        String loadUrl = config.loadUrl();
        if (loadUrl == null) {
            return AiModelCapabilityState.Capability.providerManaged(
                    "No load path configured — the provider manages model residency itself.");
        }
        String blocked = ProviderDialGuard.refusal(loadUrl);
        if (blocked != null) {
            log.info("ai-model: capability probe refused load url={} reason={}", loadUrl, blocked);
            return AiModelCapabilityState.Capability.providerManaged(blocked + " (" + loadUrl + ")");
        }
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(loadUrl))
                    .timeout(Duration.ofSeconds(6))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json");
            if (config.apiKey() != null && !config.apiKey().isBlank()) {
                builder.header("Authorization", "Bearer " + config.apiKey());
            }
            builder.POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(JSON.createObjectNode())));
            HttpResponse<byte[]> resp = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            int status = resp.statusCode();
            if (isRouteAbsentStatus(status)) {
                log.info("ai-model: capability probe demoted to provider-managed (HTTP {} from load path)", status);
                return AiModelCapabilityState.Capability.providerManaged(
                        "Load path answered HTTP " + status + " — the provider manages model residency itself.");
            }
            log.info("ai-model: capability probe confirmed managed (HTTP {} from load path)", status);
            return AiModelCapabilityState.Capability.managedCapability(
                    "Load path is live (HTTP " + status + ").");
        } catch (Exception e) {
            String reason = friendlyProbeError(e);
            log.info("ai-model: capability probe demoted to provider-managed (load path unreachable: {})", reason);
            return AiModelCapabilityState.Capability.providerManaged(
                    "Load path could not be reached (" + reason + ") — the provider manages model residency itself.");
        }
    }

    /** Probes the CURRENT provider and stores the verdict in {@link AiModelCapabilityState}. */
    public AiModelCapabilityState.Capability refreshCapability() {
        AiModelCapabilityState.Capability capability = probeCapability(holder.current());
        capabilityState.set(capability);
        return capability;
    }

    /**
     * HTTP statuses that mean the load route is ABSENT / unusable (demote to provider-managed):
     * 401/403 unauthorised, 404 not found, 405 method not allowed, 501 not implemented.
     */
    private static boolean isRouteAbsentStatus(int status) {
        return status == 401 || status == 403 || status == 404 || status == 405 || status == 501;
    }

    /**
     * Fetch the provider's models list, annotated with the daemon's loaded-state record. Returns an
     * empty JSON array when no provider is configured or the call fails, so the "Available models" tab
     * degrades to "none" rather than error.
     */
    public String listModelsRaw() {
        AiModelProviderConfig config = holder.current();
        if (!config.configured()) {
            return "[]";
        }
        reconcileLoadedModels(config);
        try {
            HttpResponse<String> resp = get(config.modelsUrl(), config.apiKey());
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                return annotateLoaded(expandVariants(resp.body(), config), config);
            }
            log.debug("ai-model: list-models non-2xx status={} url={}", resp.statusCode(), config.modelsUrl());
            return "[]";
        } catch (Exception e) {
            log.debug("ai-model: list-models failed: {}", e.toString());
            return "[]";
        }
    }

    /**
     * Best-effort refresh of {@link AiModelLoadState} from the provider's own report of what is
     * resident. No-op when nothing reliable can be read; in every unsure case the tracked record is
     * kept as-is (never blanked on a guess).
     */
    public void reconcileLoadedModels(AiModelProviderConfig config) {
        if (config == null || !config.configured()) {
            return;
        }
        boolean managed = capabilityState.managed();
        String url = config.loadedModelsUrl();
        boolean presenceMeansLoaded = false;
        if (!managed) {
            // Provider-managed: fall back to the plain models endpoint when no dedicated loaded
            // endpoint is set, and treat presence in the list as "resident".
            if (url == null) {
                url = config.modelsUrl();
            }
            presenceMeansLoaded = true;
        }
        if (url == null) {
            return;
        }
        try {
            HttpResponse<String> resp = get(url, config.apiKey());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                log.debug("ai-model: loaded-models reconcile non-2xx status={}", resp.statusCode());
                return;
            }
            java.util.Set<String> ids = parseLoadedIds(resp.body(), presenceMeansLoaded);
            if (ids != null) {
                loadState.replaceAll(ids);
                log.debug("ai-model: reconciled loaded models from provider ({} loaded, presenceRule={})",
                        ids.size(), presenceMeansLoaded);
            }
        } catch (Exception e) {
            log.debug("ai-model: loaded-models reconcile failed: {}", e.toString());
        }
    }

    /** Convenience overload: marker-only parse (no presence rule) — the MANAGED reconcile path. */
    static java.util.Set<String> parseLoadedIds(String body) {
        return parseLoadedIds(body, false);
    }

    /**
     * The ids a provider reports as LOADED, or {@code null} when the reply carries no recognisable
     * loaded marker AND the presence rule is off. Understood shapes: LM Studio —
     * {@code {models:[{key, loaded_instances:[{id}]}]}} (loaded when the instance list is non-empty);.
     */
    static java.util.Set<String> parseLoadedIds(String body, boolean presenceMeansLoaded) {
        if (body == null || body.isBlank()) {
            return null;
        }
        JsonNode root;
        try {
            root = JSON.readTree(body);
        } catch (Exception e) {
            return null;
        }
        java.util.Set<String> loaded = new java.util.LinkedHashSet<>();
        boolean anyMarker = false;

        // Root-level residency reports that are NOT a list of model rows.
        if (root != null && root.isObject()) {
            // Unsloth /api/inference/status: {"active_model": "...", "gguf_variant": "...", "loaded": [...]}.
            String active = firstText(root, "active_model");
            if (active != null) {
                loaded.add(active);
                // A loaded GGUF variant is resident as repo+quant; emit the composite id so the
                // expanded per-variant row ("repo@quant") reads loaded, not just the bare repo.
                String variant = firstText(root, "gguf_variant", "variant");
                if (variant != null && !variant.isBlank()) {
                    loaded.add(active + VARIANT_SEPARATOR + variant);
                }
                anyMarker = true;
            }
            JsonNode loadedArr = root.get("loaded");
            if (loadedArr != null && loadedArr.isArray()) {
                anyMarker = true;
                for (JsonNode n : loadedArr) {
                    String id = n.isTextual() ? n.asText() : firstText(n, "id", "model_path", "model", "name", "key");
                    if (id != null && !id.isBlank()) {
                        loaded.add(id);
                    }
                }
            }
            // LocalAI /system: {"loaded_models": [{"id": "..."}]}.
            JsonNode loadedModels = root.get("loaded_models");
            if (loadedModels != null && loadedModels.isArray()) {
                anyMarker = true;
                for (JsonNode n : loadedModels) {
                    String id = n.isTextual() ? n.asText() : firstText(n, "id", "model", "name", "key");
                    if (id != null && !id.isBlank()) {
                        loaded.add(id);
                    }
                }
            }
            // text-generation-webui /v1/internal/model/info: {"model_name": "..."} ("None" = empty slot).
            String modelName = firstText(root, "model_name");
            if (modelName != null && !"None".equalsIgnoreCase(modelName)) {
                loaded.add(modelName);
                anyMarker = true;
            }
        }

        java.util.List<JsonNode> rows = new java.util.ArrayList<>();
        if (root != null && root.isArray()) {
            root.forEach(rows::add);
        } else if (root != null && root.isObject()) {
            if (root.has("models") && root.get("models").isArray()) {
                root.get("models").forEach(rows::add);
            } else if (root.has("data") && root.get("data").isArray()) {
                root.get("data").forEach(rows::add);
            }
        }
        for (JsonNode row : rows) {
            if (row == null || !row.isObject()) {
                continue;
            }
            JsonNode instances = row.get("loaded_instances");
            if (instances != null && instances.isArray()) {
                anyMarker = true;
                if (!instances.isEmpty()) {
                    String key = firstText(row, "key", "id", "model", "name");
                    if (key != null) {
                        loaded.add(key);
                    }
                    for (JsonNode instance : instances) {
                        String instanceId = firstText(instance, "id");
                        if (instanceId != null) {
                            loaded.add(instanceId);
                        }
                    }
                }
                continue;
            }
            // llama.cpp router: per-model status object, loaded only when value == "loaded".
            JsonNode status = row.get("status");
            if (status != null && status.isObject() && status.get("value") != null
                    && status.get("value").isTextual()) {
                anyMarker = true;
                if ("loaded".equalsIgnoreCase(status.get("value").asText())) {
                    String id = firstText(row, "id", "model", "name", "key");
                    if (id != null) {
                        loaded.add(id);
                    }
                }
                continue;
            }
            JsonNode flag = row.get("loaded") != null ? row.get("loaded") : row.get("isLoaded");
            if (flag != null && flag.isBoolean()) {
                anyMarker = true;
                if (flag.asBoolean()) {
                    String id = firstText(row, "id", "model", "name", "key", "upstream_name");
                    if (id != null) {
                        loaded.add(id);
                    }
                }
                continue;
            }
            // Provider-managed presence rule: a plain listed row is a resident model.
            if (presenceMeansLoaded) {
                String id = firstText(row, "id", "model", "name", "key", "upstream_name");
                if (id != null) {
                    loaded.add(id);
                    anyMarker = true;
                }
            }
        }
        return anyMarker ? loaded : null;
    }

    /**
     * Annotates every model row of a provider list reply with the daemon's lifecycle view. Tolerates
     * the three shapes the backend normalisers accept — a bare array, an OpenAI {@code {data:[...]}}
     * envelope, or a {@code {models:[...]}} envelope.
     */
    private String annotateLoaded(String body, AiModelProviderConfig config) {
        if (body == null || body.isBlank()) {
            return body;
        }
        JsonNode root;
        try {
            root = JSON.readTree(body);
        } catch (Exception e) {
            return body;
        }
        ArrayNode rows = null;
        if (root != null && root.isArray() && root instanceof ArrayNode arr) {
            rows = arr;
        } else if (root instanceof ObjectNode obj) {
            if (obj.get("data") instanceof ArrayNode arr) {
                rows = arr;
            } else if (obj.get("models") instanceof ArrayNode arr) {
                rows = arr;
            }
        }
        if (rows == null) {
            return body;
        }
        boolean managed = capabilityState.managed();
        boolean touched = false;
        for (JsonNode row : rows) {
            if (row instanceof ObjectNode obj) {
                String id = firstText(obj, "upstream_name", "id", "name", "model", "key");
                if (id != null && !id.isBlank()) {
                    obj.put("loaded", loadState.isLoaded(id));
                    if (!managed) {
                        obj.put("providerManaged", true);
                    }
                    touched = true;
                }
            }
        }
        if (!touched) {
            return body;
        }
        try {
            return JSON.writeValueAsString(root);
        } catch (Exception e) {
            return body;
        }
    }

    /**
     * Expands a provider's model list so every multi-variant (GGUF quant) model becomes ONE ROW PER
     * DOWNLOADED variant, each with id {@code repo@quant} and a label carrying the quant. This is what
     * lets the operator see — and pick — the exact version that is already on disk, so a Load forwards
     * that variant and the provider never re-downloads a different quant (Unsloth defaults to Q4 and
     * pulls it even when BF16 is local).
     */
    private String expandVariants(String body, AiModelProviderConfig config) {
        if (body == null || body.isBlank() || config == null) {
            return body;
        }
        // The JSON key the load body carries the quant under. An explicit config wins; an Unsloth load
        // route implies gguf_variant.
        String variantField = variantField(config);
        if (variantField == null) {
            log.info("ai-model: variant expansion skipped — provider declares no variant body field");
            return body;
        }
        JsonNode root;
        try {
            root = JSON.readTree(body);
        } catch (Exception e) {
            return body;
        }
        ArrayNode rows = null;
        ObjectNode objRoot = null;
        String rowsKey = null;
        boolean bare = false;
        if (root instanceof ArrayNode arr) {
            rows = arr;
            bare = true;
        } else if (root instanceof ObjectNode obj) {
            objRoot = obj;
            if (obj.get("models") instanceof ArrayNode a) {
                rows = a;
                rowsKey = "models";
            } else if (obj.get("data") instanceof ArrayNode a) {
                rows = a;
                rowsKey = "data";
            }
        }
        if (rows == null) {
            return body;
        }
        ArrayNode expanded = JSON.createArrayNode();
        boolean changed = false;
        // Providers list the same model twice (a cache snapshot plus its repo row), which would
        // otherwise expand into duplicate per-quant rows.
        java.util.Set<String> emitted = new java.util.LinkedHashSet<>();
        boolean anyVariantRow = false;
        for (JsonNode row : rows) {
            if (!(row instanceof ObjectNode objRow) || !mayHaveVariants(objRow)) {
                addRow(expanded, emitted, row);
                continue;
            }
            String id = firstText(objRow, "load_id", "id", "name", "model", "upstream_name");
            if (id == null) {
                addRow(expanded, emitted, row);
                continue;
            }
            String path = firstText(objRow, "path");
            String display = firstText(objRow, "display_name", "label", "name");
            // The provider already pinned this row to ONE quant (an imported single-quant model, or a
            // standalone .gguf). No lookup needed — carry that quant in the id so the load request
            // names it instead of letting the server fall back to its preferred quant and fetch it.
            String pinned = firstText(objRow, "format_variant");
            if (pinned != null) {
                addRow(expanded, emitted, variantRow(objRow, id, display, pinned));
                anyVariantRow = true;
                continue;
            }
            java.util.List<String> quants = fetchDownloadedVariants(config, id, path);
            if (quants == null) {
                addRow(expanded, emitted, row); // variant list unreadable — keep the row, don't hide it
                continue;
            }
            if (quants.isEmpty()) {
                // Flagged multi-quant with nothing on disk → not loadable, so drop it. Otherwise keep
                // the row: an empty answer can simply mean this model has no quant dimension.
                if (!requiresVariant(objRow)) {
                    addRow(expanded, emitted, row);
                }
                continue;
            }
            for (String quant : quants) {
                addRow(expanded, emitted, variantRow(objRow, id, display, quant));
            }
            anyVariantRow = true;
        }
        changed = anyVariantRow || expanded.size() != rows.size();
        if (!changed) {
            log.info("ai-model: variant expansion found no expandable GGUF row in {} row(s)", rows.size());
            return body;
        }
        log.info("ai-model: variant expansion: {} row(s) in → {} row(s) out", rows.size(), expanded.size());
        try {
            if (bare) {
                return JSON.writeValueAsString(expanded);
            }
            objRoot.set(rowsKey, expanded);
            return JSON.writeValueAsString(objRoot);
        } catch (Exception e) {
            return body;
        }
    }

    /**
     * Whether a row is worth a variant lookup. GGUF is the only format with a quant dimension, but the
     * declared format cannot be the sole test.
     */
    private static boolean mayHaveVariants(JsonNode row) {
        String format = firstText(row, "model_format", "format");
        if (format != null && format.equalsIgnoreCase("gguf")) {
            return true;
        }
        if (format != null && !format.equalsIgnoreCase("unknown")) {
            return false; // safetensors / adapter / checkpoint / diffusers — no quants
        }
        String probe = firstText(row, "id", "name", "model", "path");
        return probe != null && probe.toLowerCase(java.util.Locale.ROOT).contains("gguf");
    }

    /**
     * Appends a row unless its id was already emitted. Returns {@code false} on a skipped duplicate.
     */
    private static boolean addRow(ArrayNode out, java.util.Set<String> emitted, JsonNode row) {
        String id = row.isObject() ? firstText(row, "id") : null;
        if (id != null && !emitted.add(id)) {
            return false;
        }
        out.add(row);
        return true;
    }

    /**
     * True when a GGUF row is a multi-quant repo whose variants must be looked up — Unsloth flags it
     * with {@code capabilities.requires_variant}. Absent the flag, a row whose id/path is not a single
     * {@code .gguf} file is treated as a repo directory holding several quants.
     */
    private static boolean requiresVariant(JsonNode row) {
        JsonNode caps = row.get("capabilities");
        JsonNode flag = caps != null && caps.isObject() ? caps.get("requires_variant") : null;
        if (flag != null && flag.isBoolean()) {
            return flag.asBoolean();
        }
        String probe = firstText(row, "path", "id", "name", "model");
        return probe == null || !probe.toLowerCase(java.util.Locale.ROOT).endsWith(".gguf");
    }

    /** One per-quant row: the composite id {@code loadModel} splits on, plus a quant-suffixed label. */
    private static ObjectNode variantRow(ObjectNode row, String id, String display, String quant) {
        ObjectNode copy = row.deepCopy();
        String label = (display != null ? display : id) + " (" + quant + ")";
        copy.put("id", id + VARIANT_SEPARATOR + quant);
        copy.put("variant", quant);
        copy.put("display_name", label);
        copy.put("label", label);
        return copy;
    }

    /**
     * The quant to load for a model id that carries none, read from the provider's DOWNLOADED variant
     * list. One downloaded quant is unambiguous.
     */
    private String resolveDownloadedVariant(AiModelProviderConfig config, String modelId) {
        java.util.List<String> quants = fetchDownloadedVariants(config, modelId, null);
        if (quants == null || quants.isEmpty()) {
            return null;
        }
        if (quants.size() > 1) {
            log.warn("ai-model: '{}' has {} downloaded quants {} — loading '{}'; pick a version from"
                            + " the model list to load a different one",
                    modelId, quants.size(), quants, quants.get(0));
        } else {
            log.info("ai-model: '{}' carried no quant — loading the on-disk '{}'", modelId, quants.get(0));
        }
        return quants.get(0);
    }

    /**
     * The load body's variant key: the configured field, else Unsloth's {@code gguf_variant} when the
     * load route is Unsloth's, else {@code null} (provider has no variant concept).
     */
    private static String variantField(AiModelProviderConfig config) {
        String configured = config.variantPayloadFieldOrDefault();
        if (configured != null) {
            return configured;
        }
        String loadPath = config.loadPath();
        return loadPath != null && loadPath.contains("/api/inference/") ? UNSLOTH_VARIANT_FIELD : null;
    }

    /**
     * The DOWNLOADED variant (quant) labels for one model, read from the provider's variant endpoint:
     * {@code null} when the call failed or the shape is unrecognisable (caller keeps the row), an
     * empty list when the endpoint answered but nothing. Variants flagged {@code downloaded:false} are
     * skipped so only loadable local quants are offered.
     */
    private java.util.List<String> fetchDownloadedVariants(AiModelProviderConfig config, String id, String path) {
        // With no local path known (a load resolving its own quant), substitute the id for {path} too:
        // a template keyed on {path} then still names the model, and Unsloth ignores a local_path that
        // is not an on-disk path.
        String effectivePath = (path == null || path.isBlank()) ? id : path;
        String url = config.variantsUrlFor(id, effectivePath);
        if (url == null) {
            // No configured template: fall back to Unsloth's cache-preferring lookup so a provider
            // saved before variantsPath existed still lists the quants already on disk.
            url = config.variantsUrlForTemplate(UNSLOTH_VARIANTS_PATH, id, effectivePath);
        }
        if (url == null) {
            return null;
        }
        try {
            HttpResponse<String> resp = get(url, config.apiKey());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                log.info("ai-model: variant listing for '{}' HTTP {} ({})", id, resp.statusCode(), url);
                return null;
            }
            JsonNode root = JSON.readTree(resp.body());
            JsonNode variants = root != null ? root.get("variants") : null;
            if (variants == null || !variants.isArray()) {
                return null;
            }
            java.util.List<String> quants = new java.util.ArrayList<>();
            for (JsonNode v : variants) {
                if (v == null || !v.isObject()) {
                    continue;
                }
                JsonNode downloaded = v.get("downloaded");
                if (downloaded != null && downloaded.isBoolean() && !downloaded.asBoolean()) {
                    continue;
                }
                String quant = firstText(v, "quant", "variant", "label");
                if (quant != null && !quant.isBlank()) {
                    quants.add(quant);
                }
            }
            return quants;
        } catch (Exception e) {
            log.info("ai-model: variant listing failed for '{}' ({}): {}", id, url, e.toString());
            return null;
        }
    }

    /** First non-blank textual field among {@code fields}, or {@code null}. */
    private static String firstText(JsonNode row, String... fields) {
        for (String field : fields) {
            JsonNode n = row.get(field);
            if (n != null && n.isTextual() && !n.asText().isBlank()) {
                return n.asText();
            }
        }
        return null;
    }

    /**
     * Loads a model into the provider's memory via its configured load path (operator action from
     * Engine Settings → AI Model — NEVER on the execution path). The payload is
     * {@code {"model": "<id>"}} — the shape LM Studio's {@code /api/v1/models/load} documents and the
     * de-facto managed-provider contract.
     */
    public ModelActionResult loadModel(String model) {
        AiModelProviderConfig config = holder.current();
        if (config.loadUrl() == null) {
            return new ModelActionResult(false, 0, config.configured()
                    ? "The configured provider does not expose a load path — set one on Engine Settings → AI Model."
                    : "No provider configured");
        }
        ObjectNode body = JSON.createObjectNode();
        // A variant-aware provider (Unsloth GGUF) gets the model id WITHOUT the variant suffix plus the
        // variant under its own key — model_path="owner/repo", gguf_variant="Q4_K_M". The id the operator
        // picked is "repo@quant"; split on the LAST separator so a repo id is never cut.
        String modelId = model;
        String variantField = variantField(config);
        if (variantField != null) {
            int sep = model.lastIndexOf(VARIANT_SEPARATOR);
            if (sep > 0 && sep < model.length() - 1) {
                modelId = model.substring(0, sep);
                body.put(variantField, model.substring(sep + 1));
            } else {
                // No quant in the id — a value picked before per-variant rows existed, or a provider that lists
                // one row per repo. Unsloth answers a variant-less GGUF load from HF mode, which resolves its
                // PREFERRED quant and downloads it even when another quant is already on disk, so resolve the on-
                // disk quant instead of letting that happen.
                String resolved = resolveDownloadedVariant(config, model);
                if (resolved != null) {
                    body.put(variantField, resolved);
                }
            }
        }
        body.put(config.modelPayloadFieldOrDefault(), modelId);
        log.info("ai-model: load '{}' → {}", model, body);
        ModelActionResult result =
                postModelAction(config.loadUrl(), config.apiKey(), body, Duration.ofMinutes(5), "load", model);
        // Auto-demote: a load route that turns out absent/unauthorised (probe can lag a live edit) must
        // flip the provider to provider-managed so the operator stops seeing a dead Load button.
        if (!result.ok() && isRouteAbsentStatus(result.status())) {
            capabilityState.set(AiModelCapabilityState.Capability.providerManaged(
                    "Load returned HTTP " + result.status() + " — the provider manages model residency itself."));
        }
        return result;
    }

    /**
     * LM Studio's unload keys on {@code instance_id}, and a load without an explicit instance id gets
     * a SERVER-GENERATED one (the first instance usually equals the model key; repeat loads, JIT loads
     * and UI loads can differ). Sending the model id as the instance id is therefore a guess that
     * fails with "instance not found" exactly when the operator needs the toggle most.
     */
    public ModelActionResult unloadModel(String model) {
        AiModelProviderConfig config = holder.current();
        if (config.unloadUrl() == null) {
            return new ModelActionResult(false, 0, config.configured()
                    ? "The configured provider does not expose an unload path — set one on Engine Settings → AI Model."
                    : "No provider configured");
        }
        java.util.List<String> instanceIds = resolveInstanceIds(config, model);
        if (instanceIds != null && instanceIds.isEmpty()) {
            // The provider's own loaded list says the model is not in memory — the desired
            // end state already holds, so succeed quietly instead of POSTing a doomed id.
            return new ModelActionResult(true, 0,
                    "Model '" + model + "' is not loaded on the provider.");
        }
        if (instanceIds == null) {
            return demoteIfRouteAbsent(postModelAction(config.unloadUrl(), config.apiKey(),
                    legacyUnloadBody(config, model), Duration.ofSeconds(60), "unload", model));
        }
        ModelActionResult firstFailure = null;
        for (String instanceId : instanceIds) {
            ModelActionResult r = postModelAction(config.unloadUrl(), config.apiKey(),
                    instanceUnloadBody(instanceId), Duration.ofSeconds(60), "unload", model);
            if (!r.ok() && firstFailure == null) {
                firstFailure = r;
            }
        }
        if (firstFailure != null) {
            return demoteIfRouteAbsent(firstFailure);
        }
        return new ModelActionResult(true, 200, instanceIds.size() == 1
                ? "Model '" + model + "' unloaded."
                : "Model '" + model + "' unloaded (" + instanceIds.size() + " instances).");
    }

    /**
     * Resolved-instance unload payload: {@code {"instance_id": "<id>"}} ALONE. LM Studio
     * validates the unload body strictly and 400s on any extra key ("Unrecognized key(s) in
     * object: 'model'"), so the model key must not ride along — the instance id is the whole
     * contract here.
     */
    private static ObjectNode instanceUnloadBody(String instanceId) {
        ObjectNode body = JSON.createObjectNode();
        body.put("instance_id", instanceId);
        return body;
    }

    /**
     * Fallback unload payload when the provider's instance list is unreadable. Generic managed servers
     * key on the model field ({@code model} by default, {@code model_path} for Unsloth, with the
     * variant split exactly as on load).
     */
    private ObjectNode legacyUnloadBody(AiModelProviderConfig config, String model) {
        ObjectNode body = JSON.createObjectNode();
        String unloadPath = config.unloadPath();
        if (unloadPath != null && isLmStudioUnloadRoute(unloadPath.trim())) {
            body.put("instance_id", model);
            return body;
        }
        // Split the variant off exactly as loadModel does. The operator's id is "repo@quant", and a
        // variant-aware provider keys on the bare repo — sending the composite made the server match
        // no model at all, so the toggle reported success while the weights stayed resident.
        String modelId = model;
        String variantField = variantField(config);
        if (variantField != null && model != null) {
            int sep = model.lastIndexOf(VARIANT_SEPARATOR);
            if (sep > 0 && sep < model.length() - 1) {
                modelId = model.substring(0, sep);
                body.put(variantField, model.substring(sep + 1));
            }
        }
        body.put(config.modelPayloadFieldOrDefault(), modelId);
        return body;
    }

    /**
     * LM Studio's native unload routes — the only routes that strict-validate the body and key
     * on {@code instance_id}. Matched exactly (not by suffix) so llama.cpp's same-named
     * {@code /models/unload} keeps its {@code {"model": …}} contract.
     */
    private static boolean isLmStudioUnloadRoute(String unloadPath) {
        return "/api/v1/models/unload".equals(unloadPath) || "/api/v0/models/unload".equals(unloadPath);
    }

    /**
     * The live instance ids the provider reports for {@code model} on its loaded-models list: a
     * non-empty list when instances are reported, an EMPTY list when the list was read and the model
     * is definitively not loaded, or {@code null} when.
     */
    public java.util.List<String> resolveInstanceIds(AiModelProviderConfig config, String model) {
        if (config == null || model == null || model.isBlank() || config.loadedModelsUrl() == null) {
            return null;
        }
        try {
            HttpResponse<String> resp = get(config.loadedModelsUrl(), config.apiKey());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                return null;
            }
            JsonNode root = JSON.readTree(resp.body());
            java.util.List<JsonNode> rows = new java.util.ArrayList<>();
            if (root != null && root.isArray()) {
                root.forEach(rows::add);
            } else if (root != null && root.isObject()) {
                if (root.has("models") && root.get("models").isArray()) {
                    root.get("models").forEach(rows::add);
                } else if (root.has("data") && root.get("data").isArray()) {
                    root.get("data").forEach(rows::add);
                }
            }
            if (rows.isEmpty()) {
                return null;
            }
            for (JsonNode row : rows) {
                if (row == null || !row.isObject()) {
                    continue;
                }
                String key = firstText(row, "key", "id", "model", "name");
                if (!model.equals(key)) {
                    continue;
                }
                JsonNode instances = row.get("loaded_instances");
                if (instances == null || !instances.isArray()) {
                    return null; // a shape without instance detail cannot resolve an id
                }
                java.util.List<String> ids = new java.util.ArrayList<>();
                for (JsonNode instance : instances) {
                    String id = firstText(instance, "id");
                    if (id != null) {
                        ids.add(id);
                    }
                }
                return ids;
            }
            return java.util.List.of(); // the list read fine and the model is not on it
        } catch (Exception e) {
            log.debug("ai-model: instance-id resolution failed for '{}': {}", model, e.toString());
            return null;
        }
    }

    /** Flips the provider to provider-managed when a load/unload route came back absent/unauthorised. */
    private ModelActionResult demoteIfRouteAbsent(ModelActionResult result) {
        if (result != null && !result.ok() && isRouteAbsentStatus(result.status())) {
            capabilityState.set(AiModelCapabilityState.Capability.providerManaged(
                    "Unload returned HTTP " + result.status()
                            + " — the provider manages model residency itself."));
        }
        return result;
    }

    /** Shared POST for {@link #loadModel}/{@link #unloadModel}; never throws, never logs the key. */
    private ModelActionResult postModelAction(String url, String apiKey, ObjectNode body,
                                              Duration readTimeout, String op, String model) {
        String blocked = ProviderDialGuard.refusalIfPresent(url);
        if (blocked != null) {
            log.info("ai-model: {} refused url={} reason={}", op, url, blocked);
            return new ModelActionResult(false, 0, blocked);
        }
        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                    .timeout(readTimeout)
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json");
            if (apiKey != null && !apiKey.isBlank()) {
                builder.header("Authorization", "Bearer " + apiKey);
            }
            builder.POST(HttpRequest.BodyPublishers.ofByteArray(
                    JSON.writeValueAsBytes(body)));
            HttpResponse<byte[]> resp = http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
            boolean ok = resp.statusCode() >= 200 && resp.statusCode() < 300;
            if (ok) {
                log.info("ai-model: {} OK model='{}' status={}", op, model, resp.statusCode());
                return new ModelActionResult(true, resp.statusCode(),
                        "Model '" + model + "' " + ("load".equals(op) ? "loaded" : "unloaded") + ".");
            }
            String detail = trimBody(resp.body());
            log.info("ai-model: {} failed model='{}' status={} body={}", op, model, resp.statusCode(), detail);
            return new ModelActionResult(false, resp.statusCode(),
                    "Provider refused the " + op + " of '" + model + "' (HTTP " + resp.statusCode() + ")"
                            + (detail.isBlank() ? "." : ": " + detail));
        } catch (Exception e) {
            log.info("ai-model: {} unreachable model='{}': {}", op, model, e.toString());
            return new ModelActionResult(false, 0,
                    "The provider could not be reached for the " + op + " of '" + model + "': "
                            + friendlyProbeError(e));
        }
    }

    /** Compact one-line excerpt of a provider error body (never more than 300 chars). */
    private static String trimBody(byte[] body) {
        if (body == null || body.length == 0) {
            return "";
        }
        String text = new String(body, StandardCharsets.UTF_8).replaceAll("\\s+", " ").trim();
        return text.length() <= 300 ? text : text.substring(0, 300) + "…";
    }


    private static final int HEALTH_PROBE_TIMEOUT_MS = 1000;

    /**
     * A TCP connect rather than an HTTP call: it is the cheapest thing that distinguishes "server is
     * listening" from "nothing is there", and it works across every provider without knowing which
     * health path each one exposes. Four states, because a boolean cannot express the two cases that
     * must NOT read as a failure: {@code NOT_CONFIGURED} (this environment was never meant to have a
     * local LLM) and {@code UNKNOWN} (the address is unusable, so we learned nothing).
     */
    public String healthProbe() {
        AiModelProviderConfig config = holder.current();
        if (!config.configured()) {
            return "NOT_CONFIGURED";
        }
        if (ProviderDialGuard.refusalIfPresent(config.server()) != null) {
            return "OFFLINE";
        }
        String host;
        int port;
        try {
            URI uri = URI.create(config.server());
            host = uri.getHost();
            if (host == null) {
                return "UNKNOWN";
            }
            port = uri.getPort() > 0 ? uri.getPort() : ("https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80);
        } catch (RuntimeException badUri) {
            log.debug("aimodel.healthProbe: unusable server URL {}: {}", config.server(), badUri.toString());
            return "UNKNOWN";
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), HEALTH_PROBE_TIMEOUT_MS);
            return "ONLINE";
        } catch (IOException unreachable) {
            return "OFFLINE";
        }
    }

    /**
     * Probe the daemon's STORED provider config (GET its models path). Returns a structured result
     * the {@code test-connection} handler ships back to the operator: {@code ok}, upstream
     * {@code status} (0 when unreachable), and an optional message.
     */
    public ProbeResult probe() {
        return probe(holder.current());
    }

    /**
     * Probe an EXPLICIT provider config (the operator's draft, forwarded by the control plane with the
     * {@code test-connection} command) instead of the stored one, so a just-picked preset can be
     * tested before Save. Same {@code {ok,status,message}} shape as {@link #probe}; an unconfigured
     * config still reports {@code "No provider configured"}.
     */
    public ProbeResult probe(AiModelProviderConfig config) {
        if (config == null || !config.configured()) {
            return new ProbeResult(false, 0, "No provider configured");
        }
        String url = config.modelsUrl();
        String blocked = ProviderDialGuard.refusal(url);
        if (blocked != null) {
            log.info("ai-model: probe refused url={} reason={}", url, blocked);
            return new ProbeResult(false, 0, blocked + " (" + url + ")");
        }
        try {
            HttpResponse<String> resp = get(url, config.apiKey());
            boolean ok = resp.statusCode() >= 200 && resp.statusCode() < 300;
            if (ok) {
                String detail = summariseModels(url, resp.body());
                log.info("ai-model: probe OK url={} status={} {}", url, resp.statusCode(), detail);
                return new ProbeResult(true, resp.statusCode(), detail, resp.body());
            }
            String fail = "HTTP " + resp.statusCode() + " from " + url;
            log.info("ai-model: probe failed url={} status={}", url, resp.statusCode());
            return new ProbeResult(false, resp.statusCode(), fail);
        } catch (Exception e) {
            String reason = friendlyProbeError(e);
            log.info("ai-model: probe unreachable url={} reason={}", url, reason);
            return new ProbeResult(false, 0, reason + " (" + url + ")");
        }
    }

    private HttpResponse<String> get(String url, String apiKey) throws Exception {
        String blocked = ProviderDialGuard.refusalIfPresent(url);
        if (blocked != null) {
            throw new IOException(blocked);
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(6))
                .header("Accept", "application/json")
                .GET();
        if (apiKey != null && !apiKey.isBlank()) {
            builder.header("Authorization", "Bearer " + apiKey);
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Success detail: reached URL + optional model count from an OpenAI-compatible list body. */
    private static String summariseModels(String url, String body) {
        int count = countModels(body);
        if (count >= 0) {
            return "Reached " + url + " (" + count + " model" + (count == 1 ? "" : "s") + ")";
        }
        return "Reached " + url;
    }

    /** Returns the length of {@code data}/{@code models} arrays, or {@code -1} when unknown. */
    private static int countModels(String body) {
        if (body == null || body.isBlank()) {
            return -1;
        }
        try {
            JsonNode root = JSON.readTree(body);
            if (root == null) {
                return -1;
            }
            if (root.isArray()) {
                return root.size();
            }
            if (root.has("data") && root.get("data").isArray()) {
                return root.get("data").size();
            }
            if (root.has("models") && root.get("models").isArray()) {
                return root.get("models").size();
            }
        } catch (Exception e) {
            log.debug("aimodel: probe body was not a models list ({})", e.toString());
        }
        return -1;
    }

    /** Cap on models attached to a test-connection reply (bounded payload). */
    private static final int MODELS_REPLY_CAP = 200;

    /**
     * Parse a provider models-list body into {@code [{id,label,...}]} for the
     * {@code test-connection} reply. Empty when the body is absent or not a list.
     */
    public ArrayNode modelsFromListBody(String body) {
        ArrayNode out = JSON.createArrayNode();
        if (body == null || body.isBlank()) {
            return out;
        }
        try {
            JsonNode root = JSON.readTree(body);
            if (root == null) {
                return out;
            }
            java.util.List<JsonNode> rows = new java.util.ArrayList<>();
            if (root.isArray()) {
                root.forEach(rows::add);
            } else if (root.has("data") && root.get("data").isArray()) {
                root.get("data").forEach(rows::add);
            } else if (root.has("models") && root.get("models").isArray()) {
                root.get("models").forEach(rows::add);
            }
            for (JsonNode row : rows) {
                if (out.size() >= MODELS_REPLY_CAP) {
                    break;
                }
                if (row != null && row.isTextual() && !row.asText().isBlank()) {
                    ObjectNode m = out.addObject();
                    m.put("id", row.asText());
                    m.put("label", row.asText());
                    continue;
                }
                String id = firstText(row, "id", "name", "model", "upstream_name");
                if (id == null || id.isBlank()) {
                    continue;
                }
                String label = firstText(row, "label", "display_name");
                ObjectNode m = out.addObject();
                m.put("id", id);
                m.put("label", label != null && !label.isBlank() ? label : id);
                if (row.has("loaded") && row.get("loaded").isBoolean()) {
                    m.put("loaded", row.get("loaded").asBoolean());
                }
                if (row.has("providerManaged") && row.get("providerManaged").isBoolean()) {
                    m.put("providerManaged", row.get("providerManaged").asBoolean());
                }
            }
        } catch (Exception e) {
            log.debug("aimodel: probe body was not a model-row array ({})", e.toString());
        }
        return out;
    }

    /** Compact, operator-facing reason for a connect/IO failure (no stack traces). */
    private static String friendlyProbeError(Exception e) {
        // JDK ConnectException often has a null/empty message — classify by type first.
        if (e instanceof java.net.ConnectException) {
            return "Connection refused";
        }
        if (e instanceof java.net.http.HttpTimeoutException || e instanceof java.net.SocketTimeoutException) {
            return "Connection timed out";
        }
        if (e instanceof java.net.UnknownHostException) {
            return "Unknown host";
        }
        String raw = e.getMessage() == null || e.getMessage().isBlank() ? e.toString() : e.getMessage();
        String lower = raw.toLowerCase();
        if (lower.contains("connection refused")) {
            return "Connection refused";
        }
        if (lower.contains("timed out") || lower.contains("timeout")) {
            return "Connection timed out";
        }
        if (lower.contains("no route to host") || lower.contains("network is unreachable")) {
            return "Unreachable";
        }
        if (lower.contains("unknown host") || lower.contains("nodename nor servname")) {
            return "Unknown host";
        }
        return raw;
    }

    /**
     * Result of a {@link #probe()} — {@code ok}, upstream {@code status}, optional {@code message},
     * and the raw models-path body on a 2xx so {@code test-connection} can return the catalogue
     * without a second GET.
     */
    public record ProbeResult(boolean ok, int status, String message, String body) {
        public ProbeResult(boolean ok, int status, String message) {
            this(ok, status, message, null);
        }
    }

    /**
     * Result of a {@link #loadModel(String)} / {@link #unloadModel(String)} —
     * {@code ok} on an upstream 2xx, the upstream {@code status} (0 when
     * unreachable / unconfigured), and an operator-facing {@code message}.
     */
    public record ModelActionResult(boolean ok, int status, String message) {
    }
}
