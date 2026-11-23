package com.studioquack.sqs.aimodel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.command.LoadModelCommandHandler;
import com.studioquack.sqs.command.UnloadModelCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The managed-model lifecycle behind Engine Settings → AI Model: {@code load-model} /
 * {@code unload-model} command handlers on top of {@link AiModelProviderClient}'s provider
 * calls, the {@link AiModelLoadState} record they maintain, and the {@code loaded} annotation
 * on the models list the settings panel + designer picker consume. A real loopback HTTP server
 * stands in for the managed provider (LM Studio-shaped paths).
 */
class AiModelLoadManagementTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private HttpServer provider;
    private String providerBase;
    private AiModelProviderHolder holder;
    private AiModelLoadState loadState;
    private AiModelCapabilityState capabilityState;
    private AiModelProviderClient client;
    private final AtomicReference<String> lastPath = new AtomicReference<>();
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicReference<String> lastAuth = new AtomicReference<>();
    private final java.util.List<String> requestedPaths = new java.util.concurrent.CopyOnWriteArrayList<>();
    /** Per-path response overrides; a path absent here (or an empty map) falls back to providerBody. */
    private final java.util.Map<String, String> pathResponses = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile int providerStatus = 200;
    private volatile String providerBody = "{\"instance_id\":\"llama3.1:8b\"}";

    @BeforeEach
    void setUp() throws Exception {
        provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            lastPath.set(path);
            requestedPaths.add(path);
            lastAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            lastBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] out = pathResponses.getOrDefault(path, providerBody).getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(providerStatus, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        provider.start();
        providerBase = "http://127.0.0.1:" + provider.getAddress().getPort();
        holder = new AiModelProviderHolder();
        holder.apply(managedProvider());
        loadState = new AiModelLoadState();
        capabilityState = new AiModelCapabilityState();
        // The default fixture is the LM Studio-shaped MANAGED provider (working load route); mark the
        // probed verdict directly so the annotation/gate/reconcile take the managed path without a
        // capability probe polluting the request-path assertions.
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        client = new AiModelProviderClient(holder, loadState, capabilityState);
    }

    @AfterEach
    void tearDown() {
        provider.stop(0);
    }

    /** LM Studio preset shape: managed load/unload paths under the native API. */
    private AiModelProviderConfig managedProvider() {
        return new AiModelProviderConfig(
                providerBase, "secret-key",
                "/v1/chat/completions", "/v1/models",
                "/api/v1/models/load", "/api/v1/models/unload",
                "/api/v1/models/download", "/api/v1/models/download/status",
                null);
    }

    /** Same managed provider PLUS the loaded-models reconciliation path (LM Studio /api/v1/models). */
    private AiModelProviderConfig reconcilingProvider() {
        return new AiModelProviderConfig(
                providerBase, "secret-key",
                "/v1/chat/completions", "/v1/models",
                "/api/v1/models/load", "/api/v1/models/unload",
                "/api/v1/models/download", "/api/v1/models/download/status",
                null, "/api/v1/models");
    }

    private DaemonCommand cmd(String type, String payload) {
        return new DaemonCommand("cmd-1", type, "2026-08-16T00:00:00Z", payload);
    }

    @Test
    void loadModelPostsToTheLoadPathAndMarksLoaded() throws Exception {
        LoadModelCommandHandler handler =
                new LoadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("load-model", "{\"model\":\"llama3.1:8b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        assertEquals("/api/v1/models/load", lastPath.get());
        assertEquals("Bearer secret-key", lastAuth.get(), "the daemon injects the provider bearer");
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("llama3.1:8b", sent.get("model").asText());
        assertTrue(loadState.isLoaded("llama3.1:8b"));
        JsonNode ack = mapper.readTree(outcome.output());
        assertTrue(ack.get("loaded").asBoolean());
        assertEquals("llama3.1:8b", ack.get("model").asText());
    }

    @Test
    void unloadModelPostsInstanceIdAndDropsTheRecord() throws Exception {
        loadState.markLoaded("llama3.1:8b");
        UnloadModelCommandHandler handler =
                new UnloadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("unload-model", "{\"model\":\"llama3.1:8b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        assertEquals("/api/v1/models/unload", lastPath.get());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("llama3.1:8b", sent.get("instance_id").asText(),
                "LM Studio's unload keys on instance_id");
        assertFalse(sent.has("model"),
                "the strict LM Studio route 400s a dual-key body — instance_id only: " + sent);
        assertFalse(loadState.isLoaded("llama3.1:8b"));
        JsonNode ack = mapper.readTree(outcome.output());
        assertFalse(ack.get("loaded").asBoolean());
    }

    @Test
    void providerRefusalFailsTheCommandAndKeepsTheRecord() {
        providerStatus = 409;
        providerBody = "{\"error\":\"already loaded\"}";
        LoadModelCommandHandler handler =
                new LoadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("load-model", "{\"model\":\"llama3.1:8b\"}"));

        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("409"), "the provider status reaches the operator: " + outcome.error());
        assertFalse(loadState.isLoaded("llama3.1:8b"), "a refused load must not mark the model loaded");
    }

    @Test
    void unreachableProviderFailsCleanly() {
        holder.apply(new AiModelProviderConfig(
                "http://127.0.0.1:1", null, "/v1/chat/completions", "/v1/models",
                "/api/v1/models/load", "/api/v1/models/unload", null, null, null));
        LoadModelCommandHandler handler =
                new LoadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("load-model", "{\"model\":\"llama3.1:8b\"}"));

        assertFalse(outcome.succeeded());
        assertFalse(loadState.isLoaded("llama3.1:8b"));
    }

    /** Managed provider: every catalog row gains an explicit {@code loaded} flag. */
    @Test
    void listModelsAnnotatesLoadedStateForManagedProvider() throws Exception {
        providerBody = "{\"data\":[{\"id\":\"llama3.1:8b\"},{\"id\":\"qwen3:4b\"}]}";
        loadState.markLoaded("llama3.1:8b");

        JsonNode root = mapper.readTree(client.listModelsRaw());

        JsonNode rows = root.get("data");
        assertEquals(2, rows.size());
        assertTrue(rows.get(0).get("loaded").asBoolean(), "the loaded row is flagged");
        assertFalse(rows.get(1).get("loaded").asBoolean(), "the other row is explicitly not loaded");
    }

    /**
     * Provider-managed provider (no working load path): every row is annotated {@code
     * providerManaged:true} so the panel shows a "Managed By Provider" status and the picker keeps
     * it, and each listed model reads as {@code loaded} via the presence rule (the provider only
     * serves what it lists) — this is exactly what surfaces a manually-started vLLM/Ollama model.
     */
    @Test
    void listModelsAnnotatesProviderManagedRows() throws Exception {
        holder.apply(new AiModelProviderConfig(
                providerBase, null, "/v1/chat/completions", "/v1/models",
                null, null, null, null, null));
        capabilityState.set(AiModelCapabilityState.Capability.providerManaged("no load path"));
        providerBody = "{\"data\":[{\"id\":\"gpt-4o-mini\"}]}";

        JsonNode root = mapper.readTree(client.listModelsRaw());

        JsonNode row = root.get("data").get(0);
        assertTrue(row.get("providerManaged").asBoolean(), "row is flagged managed-by-provider");
        assertTrue(row.get("loaded").asBoolean(), "a listed provider-managed model is resident (presence rule)");
    }

    /** The annotation survives the bare-array provider shape too. */
    @Test
    void listModelsAnnotatesBareArrayShape() throws Exception {
        providerBody = "[{\"id\":\"llama3.1:8b\"}]";
        loadState.markLoaded("llama3.1:8b");

        JsonNode root = mapper.readTree(client.listModelsRaw());

        assertTrue(root.isArray());
        assertTrue(root.get(0).get("loaded").asBoolean());
    }

    /**
     * The reported gap: a freshly connected daemon has an EMPTY tracked record, so models the
     * operator already loaded in LM Studio showed as not-loaded. With a loaded-models path
     * configured, list-models reconciles from the provider first — the annotation reflects the
     * provider's truth, not the daemon's boot-time empty set.
     */
    @Test
    void listModelsReconcilesAlreadyLoadedModelsFromTheProvider() throws Exception {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models",
                "{\"models\":["
                        + "{\"key\":\"llama3.1:8b\",\"loaded_instances\":[{\"id\":\"llama3.1:8b\"}]},"
                        + "{\"key\":\"qwen3:4b\",\"loaded_instances\":[]}"
                        + "]}");
        pathResponses.put("/v1/models",
                "{\"data\":[{\"id\":\"llama3.1:8b\"},{\"id\":\"qwen3:4b\"}]}");
        // Deliberately NOT marking anything in the tracked set — reconciliation must do it.

        JsonNode root = mapper.readTree(client.listModelsRaw());

        JsonNode rows = root.get("data");
        assertTrue(rows.get(0).get("loaded").asBoolean(), "provider-loaded model shows loaded");
        assertFalse(rows.get(1).get("loaded").asBoolean(), "provider-unloaded model shows not loaded");
        assertTrue(loadState.isLoaded("llama3.1:8b"), "the tracked record was reconciled");
        assertTrue(requestedPaths.contains("/api/v1/models"), "the reconcile endpoint was queried");
    }

    /**
     * The RAM blow-up guard: clicking Load on a model the provider ALREADY has in memory must not
     * issue a second load (LM Studio would map the weights twice as a new instance). The handler
     * reconciles first and short-circuits with an "already loaded" ack.
     */
    @Test
    void loadModelSkipsTheProviderCallWhenAlreadyLoaded() throws Exception {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models",
                "{\"models\":[{\"key\":\"llama3.1:8b\",\"loaded_instances\":[{\"id\":\"llama3.1:8b\"}]}]}");
        LoadModelCommandHandler handler =
                new LoadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("load-model", "{\"model\":\"llama3.1:8b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        assertFalse(requestedPaths.contains("/api/v1/models/load"),
                "no second load may reach the provider for an already-loaded model");
        JsonNode ack = mapper.readTree(outcome.output());
        assertTrue(ack.get("loaded").asBoolean());
        assertTrue(ack.get("message").asText().contains("already loaded"));
    }

    /** Mirror guard: unload of a model the provider does NOT report as loaded is a quiet no-op. */
    @Test
    void unloadModelSkipsTheProviderCallWhenNotLoaded() throws Exception {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models", "{\"models\":[{\"key\":\"qwen3:4b\",\"loaded_instances\":[]}]}");
        UnloadModelCommandHandler handler =
                new UnloadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("unload-model", "{\"model\":\"qwen3:4b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        assertFalse(requestedPaths.contains("/api/v1/models/unload"),
                "no unload call may reach the provider for a model it does not hold");
    }

    /**
     * The reported failure: LM Studio assigns the instance id (a default load usually gets the
     * model key, but repeat/JIT/UI loads differ), so unloading with instance_id = model id is a
     * guess that the provider refuses with "instance not found". The client resolves the LIVE
     * instance id from the loaded-models list and unloads THAT.
     */
    @Test
    void unloadResolvesTheLiveInstanceIdBeforeUnloading() throws Exception {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models",
                "{\"models\":[{\"key\":\"qwen/qwen3-8b\","
                        + "\"loaded_instances\":[{\"id\":\"qwen/qwen3-8b:2\"}]}]}");
        UnloadModelCommandHandler handler =
                new UnloadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("unload-model", "{\"model\":\"qwen/qwen3-8b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        assertEquals("/api/v1/models/unload", lastPath.get());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("qwen/qwen3-8b:2", sent.get("instance_id").asText(),
                "the provider's live instance id is unloaded, not the model-key guess");
        assertFalse(sent.has("model"),
                "LM Studio validates strictly — the resolved unload carries instance_id ALONE: " + sent);
        assertFalse(loadState.isLoaded("qwen/qwen3-8b"));
    }

    /** A model loaded twice holds TWO instances — the toggle frees all of them. */
    @Test
    void unloadDropsEveryLoadedInstance() throws Exception {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models",
                "{\"models\":[{\"key\":\"llama3.1:8b\",\"loaded_instances\":"
                        + "[{\"id\":\"llama3.1:8b\"},{\"id\":\"llama3.1:8b:2\"}]}]}");
        loadState.markLoaded("llama3.1:8b");
        UnloadModelCommandHandler handler =
                new UnloadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("unload-model", "{\"model\":\"llama3.1:8b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        long unloads = requestedPaths.stream().filter("/api/v1/models/unload"::equals).count();
        assertEquals(2, unloads, "one unload per live instance: " + requestedPaths);
    }

    /** When the loaded list cannot be read, the legacy instance_id = model id payload fires. */
    @Test
    void unloadFallsBackToModelIdWhenInstancesUnresolvable() throws Exception {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models", "not-json");
        loadState.markLoaded("llama3.1:8b");
        UnloadModelCommandHandler handler =
                new UnloadModelCommandHandler(mapper, client, loadState, holder, capabilityState);

        DaemonCommandOutcome outcome = handler.handle(cmd("unload-model", "{\"model\":\"llama3.1:8b\"}"));

        assertTrue(outcome.succeeded(), "got: " + outcome.error());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("llama3.1:8b", sent.get("instance_id").asText(),
                "unresolvable instance detail falls back to the model id");
        assertFalse(sent.has("model"),
                "the LM-Studio-shaped fallback stays single-key (strict route): " + sent);
    }

    /** A definitive empty instance list is a quiet success — no doomed POST. */
    @Test
    void unloadQuietlySucceedsWhenTheProviderReportsNoInstances() {
        holder.apply(reconcilingProvider());
        pathResponses.put("/api/v1/models",
                "{\"models\":[{\"key\":\"llama3.1:8b\",\"loaded_instances\":[]}]}");

        AiModelProviderClient.ModelActionResult result = client.unloadModel("llama3.1:8b");

        assertTrue(result.ok(), "got: " + result.message());
        assertTrue(result.message().contains("not loaded"), result.message());
        assertFalse(requestedPaths.contains("/api/v1/models/unload"),
                "no unload POST when the provider says nothing is loaded");
    }

    /**
     * llama.cpp's router also mounts {@code /models/unload} — but it keys on {@code model} and
     * ignores extra keys, so the LM Studio {@code instance_id} fallback must NOT fire on the
     * shared suffix. The route sniff matches LM Studio's exact native routes only.
     */
    @Test
    void llamaCppUnloadKeepsTheModelFieldContract() throws Exception {
        holder.apply(new AiModelProviderConfig(
                providerBase, "secret-key",
                "/v1/chat/completions", "/models",
                "/models/load", "/models/unload",
                null, null, null));
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/models/unload", "{\"status\":\"ok\"}");

        AiModelProviderClient.ModelActionResult result = client.unloadModel("qwen3:4b");

        assertTrue(result.ok(), "got: " + result.message());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("qwen3:4b", sent.get("model").asText(),
                "llama.cpp unloads by model id: " + sent);
        assertFalse(sent.has("instance_id"),
                "the LM Studio instance guess must not leak onto the llama.cpp route: " + sent);
    }

    /** A provider whose loaded-models reply has no recognisable markers must NOT blank the record. */
    @Test
    void reconcileKeepsTheTrackedRecordWhenTheShapeIsUnsupported() {
        holder.apply(reconcilingProvider());
        loadState.markLoaded("llama3.1:8b");
        pathResponses.put("/api/v1/models", "{\"some\":\"other-shape\"}");

        client.reconcileLoadedModels(holder.current());

        assertTrue(loadState.isLoaded("llama3.1:8b"), "an unparseable reconcile answer never blanks the record");
    }

    /** The LM Studio native shape parses to key + instance ids; empty instances mean not loaded. */
    @Test
    void parseLoadedIdsReadsTheLmStudioShape() {
        java.util.Set<String> ids = AiModelProviderClient.parseLoadedIds(
                "{\"models\":["
                        + "{\"key\":\"a\",\"loaded_instances\":[{\"id\":\"a\"},{\"id\":\"a-custom\"}]},"
                        + "{\"key\":\"b\",\"loaded_instances\":[]}"
                        + "]}");
        assertTrue(ids != null && ids.contains("a") && ids.contains("a-custom") && !ids.contains("b"),
                "got: " + ids);
        assertTrue(AiModelProviderClient.parseLoadedIds("{\"data\":[{\"id\":\"x\"}]}") == null,
                "no loaded markers → null (unknown), never an empty truth");
    }

    /** llama.cpp router: a per-model {@code status.value} of "loaded" is the marker; others are not. */
    @Test
    void parseLoadedIdsReadsTheLlamaCppRouterShape() {
        java.util.Set<String> ids = AiModelProviderClient.parseLoadedIds(
                "{\"data\":["
                        + "{\"id\":\"gemma\",\"status\":{\"value\":\"loaded\"}},"
                        + "{\"id\":\"qwen\",\"status\":{\"value\":\"sleeping\"}},"
                        + "{\"id\":\"phi\",\"status\":{\"value\":\"unloaded\"}}"
                        + "]}");
        assertTrue(ids != null && ids.contains("gemma") && !ids.contains("qwen") && !ids.contains("phi"),
                "got: " + ids);
    }

    /** Unsloth /api/inference/status: root-level active_model + loaded[] are the markers. */
    @Test
    void parseLoadedIdsReadsTheUnslothStatusShape() {
        java.util.Set<String> ids = AiModelProviderClient.parseLoadedIds(
                "{\"active_model\":\"unsloth/gemma\",\"loaded\":[\"unsloth/gemma\"]}");
        assertTrue(ids != null && ids.contains("unsloth/gemma"), "got: " + ids);
    }

    /** text-generation-webui /v1/internal/model/info: {model_name} — "None" means empty slot. */
    @Test
    void parseLoadedIdsReadsTheTextGenWebuiShape() {
        assertTrue(AiModelProviderClient.parseLoadedIds("{\"model_name\":\"llama\"}").contains("llama"));
        assertTrue(AiModelProviderClient.parseLoadedIds("{\"model_name\":\"None\"}") == null,
                "an empty slot reports no loaded model");
    }

    /** LocalAI /system: {loaded_models:[{id}]}. */
    @Test
    void parseLoadedIdsReadsTheLocalAiSystemShape() {
        java.util.Set<String> ids =
                AiModelProviderClient.parseLoadedIds("{\"loaded_models\":[{\"id\":\"mistral\"}]}");
        assertTrue(ids != null && ids.contains("mistral"), "got: " + ids);
    }

    /**
     * Provider-managed presence rule: a plain list carries no loaded markers, but for a provider that
     * only serves what it lists (vLLM /v1/models, Ollama /api/ps) every listed id is resident.
     */
    @Test
    void parseLoadedIdsPresenceRuleTreatsEveryListedRowAsLoaded() {
        assertTrue(AiModelProviderClient.parseLoadedIds("{\"data\":[{\"id\":\"served\"}]}", false) == null,
                "without the presence rule a marker-less list stays unknown");
        java.util.Set<String> ids =
                AiModelProviderClient.parseLoadedIds("{\"data\":[{\"id\":\"served\"}]}", true);
        assertTrue(ids != null && ids.contains("served"), "presence rule marks listed models loaded: " + ids);
    }

    /** A working load route (2xx / 4xx client error) classifies the provider MANAGED. */
    @Test
    void probeCapabilityConfirmsManagedWhenLoadRouteAnswers() {
        providerStatus = 200;
        assertTrue(client.probeCapability(managedProvider()).managed(), "2xx load route → managed");
        providerStatus = 400;
        assertTrue(client.probeCapability(managedProvider()).managed(),
                "a 'missing model' 400 still proves the route exists → managed");
    }

    /** A load route that 404s / 401s (absent or unauthorised) demotes to provider-managed. */
    @Test
    void probeCapabilityDemotesWhenLoadRouteIsAbsentOrUnauthorised() {
        providerStatus = 404;
        assertFalse(client.probeCapability(managedProvider()).managed(), "404 load route → provider-managed");
        providerStatus = 401;
        assertFalse(client.probeCapability(managedProvider()).managed(), "401 load route → provider-managed");
    }

    /** No load path at all is provider-managed without any HTTP call. */
    @Test
    void probeCapabilityIsProviderManagedWhenNoLoadPath() {
        AiModelProviderConfig noLoad = new AiModelProviderConfig(
                providerBase, null, "/v1/chat/completions", "/v1/models",
                null, null, null, null, null);
        assertFalse(client.probeCapability(noLoad).managed(), "no load path → provider-managed");
    }

    /**
     * Configs saved before the explicit loaded-models path existed carry only the LM Studio
     * preset's load path. The {@code /load}-suffix convention derives the loaded-models URL from
     * it, so those installs reconcile with zero operator action — the exact case that showed
     * already-loaded models as unloaded.
     */
    @Test
    void loadedModelsUrlDerivesFromTheLoadPathConvention() {
        AiModelProviderConfig legacy = managedProvider(); // no explicit loadedModelsPath
        assertEquals(providerBase + "/api/v1/models", legacy.loadedModelsUrl(),
                "the /load suffix strips to the native models root");

        AiModelProviderConfig explicit = new AiModelProviderConfig(
                providerBase, null, "/v1/chat/completions", "/v1/models",
                "/api/v1/models/load", "/api/v1/models/unload", null, null, null, "/custom/loaded");
        assertEquals(providerBase + "/custom/loaded", explicit.loadedModelsUrl(),
                "an explicit path always wins over the convention");

        AiModelProviderConfig unconventional = new AiModelProviderConfig(
                providerBase, null, "/v1/chat/completions", "/v1/models",
                "/admin/load-model", null, null, null, null, null);
        assertTrue(unconventional.loadedModelsUrl() == null,
                "a non-/load load path derives nothing — tracked-only record");
    }

    /** End-to-end on the convention: a legacy config reconciles on list-models with no re-save. */
    @Test
    void listModelsReconcilesViaTheConventionWhenNoExplicitPathIsSaved() throws Exception {
        // managedProvider() has loadPath=/api/v1/models/load and NO explicit loadedModelsPath.
        pathResponses.put("/api/v1/models",
                "{\"models\":[{\"key\":\"llama3.1:8b\",\"loaded_instances\":[{\"id\":\"llama3.1:8b\"}]}]}");
        pathResponses.put("/v1/models", "{\"data\":[{\"id\":\"llama3.1:8b\"}]}");

        JsonNode root = mapper.readTree(client.listModelsRaw());

        assertTrue(root.get("data").get(0).get("loaded").asBoolean(),
                "the provider-loaded model shows loaded through the derived path");
        assertTrue(loadState.isLoaded("llama3.1:8b"));
    }

    /** Unsloth-shaped variant-aware provider: local catalog + per-quant variant listing + gguf_variant load. */
    private AiModelProviderConfig unslothProvider() {
        return new AiModelProviderConfig(
                providerBase, "secret-key",
                "/v1/chat/completions", "/api/hub/local",
                "/api/inference/load", "/api/inference/unload",
                null, null,
                null,                                        // defaultModel
                "/api/inference/status",                     // loadedModelsPath
                "model_path",                                // modelPayloadField
                "/api/hub/gguf-variants?repo_id={path}",     // variantsPath
                "gguf_variant");                             // variantPayloadField
    }

    /**
     * The reported bug: loading a locally-installed GGUF model without its version made Unsloth
     * re-download a DEFAULT quant (Q4) even though BF16 was on disk. The list now expands a
     * multi-variant GGUF repo into one row per DOWNLOADED quant — id {@code repo@quant}, label with
     * the version — so the operator picks the exact on-disk variant.
     */
    @Test
    void listModelsExpandsGgufRepoIntoDownloadedVariants() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status",
                "{\"active_model\":\"unsloth/gemma\",\"gguf_variant\":\"BF16\",\"loaded\":[\"unsloth/gemma\"]}");
        pathResponses.put("/api/hub/local",
                "{\"models\":["
                        + "{\"id\":\"unsloth/gemma\",\"display_name\":\"gemma\",\"path\":\"/cache/models--unsloth--gemma\","
                        + "\"model_format\":\"gguf\",\"capabilities\":{\"requires_variant\":true}},"
                        + "{\"id\":\"unsloth/lora\",\"display_name\":\"lora\",\"path\":\"/cache/models--unsloth--lora\","
                        + "\"model_format\":\"safetensors\",\"capabilities\":{\"requires_variant\":false}}"
                        + "]}");
        // Only BF16 is on disk; Q4_K_M is available remotely but NOT downloaded.
        pathResponses.put("/api/hub/gguf-variants",
                "{\"variants\":["
                        + "{\"quant\":\"Q4_K_M\",\"downloaded\":false},"
                        + "{\"quant\":\"BF16\",\"downloaded\":true}"
                        + "]}");

        JsonNode root = mapper.readTree(client.listModelsRaw());

        JsonNode rows = root.get("models");
        assertEquals(2, rows.size(), "one downloaded-variant row + the safetensors row: " + rows);
        JsonNode gguf = rows.get(0);
        assertEquals("unsloth/gemma@BF16", gguf.get("id").asText(), "variant encoded in the id");
        assertEquals("gemma (BF16)", gguf.get("display_name").asText(), "label carries the version");
        assertEquals("BF16", gguf.get("variant").asText());
        assertTrue(gguf.get("loaded").asBoolean(), "the resident variant reads loaded via the composite id");
        assertEquals("unsloth/lora", rows.get(1).get("id").asText(), "safetensors row is not expanded");
        assertTrue(requestedPaths.contains("/api/hub/gguf-variants"), "the variant endpoint was queried");
    }

    /** A GGUF repo with NO downloaded variant is dropped — the list only offers loadable local models. */
    @Test
    void listModelsDropsGgufRepoWithNoDownloadedVariant() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status", "{\"loaded\":[]}");
        pathResponses.put("/api/hub/local",
                "{\"models\":[{\"id\":\"unsloth/gemma\",\"display_name\":\"gemma\","
                        + "\"path\":\"/cache/models--unsloth--gemma\",\"model_format\":\"gguf\","
                        + "\"capabilities\":{\"requires_variant\":true}}]}");
        pathResponses.put("/api/hub/gguf-variants",
                "{\"variants\":[{\"quant\":\"Q4_K_M\",\"downloaded\":false}]}");

        JsonNode root = mapper.readTree(client.listModelsRaw());

        assertEquals(0, root.get("models").size(), "no downloaded quant → the row is dropped");
    }

    /**
     * A repo with SEVERAL downloaded quants lists one row per quant. Regression: a two-quant repo
     * showed a single row, so the operator could not pick which version to load.
     */
    @Test
    void listModelsListsEveryDownloadedQuantOfTheSameRepo() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status", "{\"loaded\":[]}");
        pathResponses.put("/api/hub/local",
                "{\"models\":[{\"id\":\"unsloth/GLM-5.3-Flash-GGUF\",\"display_name\":\"GLM-5.3-Flash-GGUF\","
                        + "\"path\":\"/cache/models--unsloth--GLM-5.3-Flash-GGUF\",\"model_format\":\"gguf\","
                        + "\"capabilities\":{\"requires_variant\":true}}]}");
        pathResponses.put("/api/hub/gguf-variants",
                "{\"variants\":["
                        + "{\"quant\":\"BF16\",\"downloaded\":true},"
                        + "{\"quant\":\"Q8_0\",\"downloaded\":true},"
                        + "{\"quant\":\"Q4_K_M\",\"downloaded\":false}"
                        + "]}");

        JsonNode rows = mapper.readTree(client.listModelsRaw()).get("models");

        assertEquals(2, rows.size(), "both downloaded quants are listed: " + rows);
        assertEquals("unsloth/GLM-5.3-Flash-GGUF@BF16", rows.get(0).get("id").asText());
        assertEquals("unsloth/GLM-5.3-Flash-GGUF@Q8_0", rows.get(1).get("id").asText());
        assertEquals("GLM-5.3-Flash-GGUF (Q8_0)", rows.get(1).get("display_name").asText());
    }

    /**
     * The reported failure: a fully-downloaded two-quant repo still showed ONE variant-less row.
     * Unsloth sets {@code requires_variant} only for PARTIAL repos and reports a partially-scanned
     * one as {@code model_format:"unknown"}, so gating the lookup on either flag collapsed the
     * repo.
     */
    @Test
    void listModelsExpandsDownloadedRepoDespiteRequiresVariantFalse() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status", "{\"loaded\":[]}");
        pathResponses.put("/api/hub/local",
                "{\"models\":["
                        // complete repo: requires_variant false
                        + "{\"id\":\"unsloth/GLM-5.3-Flash-GGUF\",\"display_name\":\"GLM-5.3-Flash-GGUF\","
                        + "\"path\":\"/cache/models--unsloth--GLM\",\"model_format\":\"gguf\","
                        + "\"capabilities\":{\"requires_variant\":false}},"
                        // the same repo again as an unlabelled snapshot row
                        + "{\"id\":\"unsloth/GLM-5.3-Flash-GGUF\",\"display_name\":\"GLM-5.3-Flash-GGUF\","
                        + "\"path\":\"/cache/models--unsloth--GLM/snapshots/abc\",\"model_format\":\"unknown\","
                        + "\"capabilities\":{\"requires_variant\":false}}"
                        + "]}");
        pathResponses.put("/api/hub/gguf-variants",
                "{\"variants\":[{\"quant\":\"BF16\",\"downloaded\":true},{\"quant\":\"Q8_0\",\"downloaded\":true}]}");

        JsonNode rows = mapper.readTree(client.listModelsRaw()).get("models");

        assertEquals(2, rows.size(), "both on-disk quants, deduped across the two source rows: " + rows);
        assertEquals("unsloth/GLM-5.3-Flash-GGUF@BF16", rows.get(0).get("id").asText());
        assertEquals("unsloth/GLM-5.3-Flash-GGUF@Q8_0", rows.get(1).get("id").asText());
    }

    /**
     * A row the provider already pinned to one quant via {@code format_variant} (an LM Studio
     * import) carries that quant in its id WITHOUT any variant lookup. Regression.
     */
    @Test
    void listModelsCarriesAnAlreadyPinnedFormatVariant() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status", "{\"loaded\":[]}");
        pathResponses.put("/api/hub/local",
                "{\"models\":[{\"id\":\"/lmstudio/glm-flash\",\"display_name\":\"glm-flash\","
                        + "\"path\":\"/lmstudio/glm-flash\",\"model_format\":\"gguf\","
                        + "\"format_variant\":\"Q8_0\",\"capabilities\":{\"requires_variant\":false}}]}");

        JsonNode rows = mapper.readTree(client.listModelsRaw()).get("models");

        assertEquals(1, rows.size());
        assertEquals("/lmstudio/glm-flash@Q8_0", rows.get(0).get("id").asText(), "pinned quant rides the id");
        assertEquals("glm-flash (Q8_0)", rows.get(0).get("display_name").asText());
        assertFalse(requestedPaths.contains("/api/hub/gguf-variants"),
                "a pinned row needs no variant lookup");
    }

    /**
     * A provider stored BEFORE variantsPath/variantPayloadField existed still expands and still sends
     * the quant: the Unsloth load route implies the {@code gguf_variant} contract, so an operator does
     * not have to re-save the provider to stop the wrong-quant download.
     */
    @Test
    void legacyUnslothConfigStillExpandsAndSendsTheVariant() throws Exception {
        holder.apply(new AiModelProviderConfig(
                providerBase, "secret-key",
                "/v1/chat/completions", "/api/hub/local",
                "/api/inference/load", "/api/inference/unload",
                null, null, null,
                "/api/inference/status",
                "model_path",
                null,      // variantsPath absent (config predates the field)
                null));    // variantPayloadField absent
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status", "{\"loaded\":[]}");
        pathResponses.put("/api/hub/local",
                "{\"models\":[{\"id\":\"unsloth/gemma\",\"display_name\":\"gemma\","
                        + "\"path\":\"/cache/models--unsloth--gemma\",\"model_format\":\"gguf\","
                        + "\"capabilities\":{\"requires_variant\":true}}]}");
        pathResponses.put("/api/hub/gguf-variants", "{\"variants\":[{\"quant\":\"BF16\",\"downloaded\":true}]}");
        pathResponses.put("/api/inference/load", "{\"status\":\"loaded\"}");

        JsonNode rows = mapper.readTree(client.listModelsRaw()).get("models");
        assertEquals("unsloth/gemma@BF16", rows.get(0).get("id").asText(), "expanded without configured fields");

        client.loadModel("unsloth/gemma@BF16");
        JsonNode load = mapper.readTree(lastBody.get());
        assertEquals("unsloth/gemma", load.get("model_path").asText(), "repo id only");
        assertEquals("BF16", load.get("gguf_variant").asText(), "quant sent under Unsloth's field");
    }

    /** Load of a variant row posts model_path=repo + gguf_variant=quant, never the composite id. */
    @Test
    void loadModelSplitsTheVariantIntoItsOwnPayloadField() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/status", "{\"loaded\":[]}");
        pathResponses.put("/api/inference/load", "{\"status\":\"loaded\"}");

        AiModelProviderClient.ModelActionResult result = client.loadModel("unsloth/gemma@BF16");

        assertTrue(result.ok(), "got: " + result.message());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("unsloth/gemma", sent.get("model_path").asText(), "the bare repo id is the model_path");
        assertEquals("BF16", sent.get("gguf_variant").asText(), "the chosen quant rides its own field");
        assertNull(sent.get("model"), "no generic model key for a variant-aware provider");
    }

    /**
     * The reported failure: pressing Load on a row with NO quant in its id made Unsloth resolve its
     * preferred quant through HF mode and download Q4 even though other quants were on disk. A
     * variant-less load now resolves the on-disk quant and names it, so nothing is downloaded.
     */
    @Test
    void loadModelWithoutAQuantResolvesTheOnDiskQuantInsteadOfDownloading() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/load", "{\"status\":\"loaded\"}");
        pathResponses.put("/api/hub/gguf-variants",
                "{\"variants\":["
                        + "{\"quant\":\"Q4_K_M\",\"downloaded\":false},"
                        + "{\"quant\":\"BF16\",\"downloaded\":true}"
                        + "]}");

        AiModelProviderClient.ModelActionResult result = client.loadModel("unsloth/GLM-5.3-Flash-GGUF");

        assertTrue(result.ok(), "got: " + result.message());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("unsloth/GLM-5.3-Flash-GGUF", sent.get("model_path").asText());
        assertEquals("BF16", sent.get("gguf_variant").asText(),
                "the downloaded quant is named, so the server cannot fall back to Q4: " + sent);
    }

    /**
     * The reported failure: Unload did nothing for a variant-expanded row. loadModel split
     * "repo@quant" but the unload payload sent it whole, so the server matched no model and the
     * weights stayed resident while the toggle reported success.
     */
    @Test
    void unloadModelSplitsTheVariantOutOfTheModelField() throws Exception {
        holder.apply(unslothProvider());
        capabilityState.set(AiModelCapabilityState.Capability.managedCapability("test: managed"));
        pathResponses.put("/api/inference/unload", "{\"status\":\"unloaded\"}");

        AiModelProviderClient.ModelActionResult result =
                client.unloadModel("unsloth/GLM-5.3-Flash-GGUF@UD-Q2_K_XL");

        assertTrue(result.ok(), "got: " + result.message());
        JsonNode sent = mapper.readTree(lastBody.get());
        assertEquals("unsloth/GLM-5.3-Flash-GGUF", sent.get("model_path").asText(),
                "the bare repo id is what the provider matches on: " + sent);
        assertEquals("UD-Q2_K_XL", sent.get("gguf_variant").asText());
        assertFalse(sent.has("instance_id"),
                "a generic managed server gets the model field only — no instance guess: " + sent);
    }

    /** Unsloth status with a GGUF variant yields BOTH the bare repo and the composite repo@quant id. */
    @Test
    void parseLoadedIdsReadsTheUnslothGgufVariantComposite() {
        java.util.Set<String> ids = AiModelProviderClient.parseLoadedIds(
                "{\"active_model\":\"unsloth/gemma\",\"gguf_variant\":\"BF16\",\"loaded\":[\"unsloth/gemma\"]}");
        assertTrue(ids != null && ids.contains("unsloth/gemma@BF16"),
                "the composite variant id is marked loaded: " + ids);
        assertTrue(ids.contains("unsloth/gemma"), "the bare repo id is also present: " + ids);
    }
}
