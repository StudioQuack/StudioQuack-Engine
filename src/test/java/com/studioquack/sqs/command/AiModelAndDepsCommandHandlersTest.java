package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.aimodel.AiModelCapabilityState;
import com.studioquack.sqs.aimodel.AiModelLoadState;
import com.studioquack.sqs.aimodel.AiModelProviderClient;
import com.studioquack.sqs.aimodel.AiModelProviderHolder;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.deps.DependencyInstaller;
import com.studioquack.sqs.deps.DependencyManifestHolder;
import com.studioquack.sqs.deps.DependencyStatusReporter;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the daemon command handlers that RECEIVE the backend pushes: {@code set-ai-model},
 * {@code set-deps}, {@code test-connection}, {@code list-models}. No network is exercised (the
 * provider is unconfigured).
 */
class AiModelAndDepsCommandHandlersTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private DaemonCommand cmd(String type, String payload) {
        return new DaemonCommand("cmd-1", type, "2026-07-14T00:00:00Z", payload);
    }

    /**
     * A {@link DependencyInstaller} with the install pipeline disabled so {@code reconcile} is a clean
     * no-op — these tests assert the manifest APPLY, not the install (that has its own test). The
     * reporter is never touched when installs are disabled.
     */
    private DependencyInstaller disabledInstaller(DependencyManifestHolder holder) {
        MockEnvironment env = new MockEnvironment();
        env.setProperty("sqs.deps.install.enabled", "false");
        // Install disabled → reconcile is a no-op that never touches the container runtime, so a
        // null runtime is safe here (the 4-arg production ctor gained a ContainerRuntime param).
        return new DependencyInstaller(holder, mock(DependencyStatusReporter.class), env, null);
    }

    @Test
    void setAiModelAppliesProviderToHolder() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        AiModelLoadState loadState = new AiModelLoadState();
        AiModelCapabilityState capabilityState = new AiModelCapabilityState();
        SetAiModelCommandHandler handler = new SetAiModelCommandHandler(
                mapper, holder, loadState, capabilityState,
                new AiModelProviderClient(holder, loadState, capabilityState));

        String payload = "{\"server\":\"http://127.0.0.1:11434\",\"apiKey\":\"k\",\"modelsPath\":\"/v1/models\"}";
        DaemonCommandOutcome outcome = handler.handle(cmd("set-ai-model", payload));

        assertTrue(outcome.succeeded());
        assertEquals("http://127.0.0.1:11434", holder.current().server());
        assertTrue(holder.current().configured());
    }

    @Test
    void setAiModelFailsOnMalformedPayload() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        AiModelLoadState loadState = new AiModelLoadState();
        AiModelCapabilityState capabilityState = new AiModelCapabilityState();
        SetAiModelCommandHandler handler = new SetAiModelCommandHandler(
                mapper, holder, loadState, capabilityState,
                new AiModelProviderClient(holder, loadState, capabilityState));
        DaemonCommandOutcome outcome = handler.handle(cmd("set-ai-model", "not-json"));
        assertFalse(outcome.succeeded());
    }

    @Test
    void setAiModelRefusesLinkLocalMetadataHost() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        SetAiModelCommandHandler handler = new SetAiModelCommandHandler(
                mapper, holder, new AiModelLoadState(), new AiModelCapabilityState(),
                new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()));
        DaemonCommandOutcome outcome = handler.handle(cmd("set-ai-model",
                "{\"server\":\"http://169.254.169.254\",\"modelsPath\":\"/latest/meta-data\"}"));
        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().toLowerCase().contains("not allowed"), "got: " + outcome.error());
        assertFalse(holder.current().configured());
    }

    @Test
    void listModelsSkipsBlockedStoredHost() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        holder.apply(new com.studioquack.sqs.aimodel.AiModelProviderConfig(
                "http://169.254.169.254", null, "/v1/chat/completions", "/v1/models",
                null, null, null, null, null));
        ListModelsCommandHandler handler = new ListModelsCommandHandler(
                new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()));
        DaemonCommandOutcome outcome = handler.handle(cmd("list-models", null));
        assertTrue(outcome.succeeded());
        assertEquals("[]", outcome.output());
    }

    /**
     * The loaded-model record tracks the PROVIDER, so a push that swaps the server must clear it —
     * otherwise the picker would offer (and the guard would admit) models the new server never
     * loaded. A key/path-only edit against the SAME server keeps the record.
     */
    @Test
    void setAiModelClearsLoadedStateOnServerChangeOnly() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        AiModelLoadState loadState = new AiModelLoadState();
        AiModelCapabilityState capabilityState = new AiModelCapabilityState();
        SetAiModelCommandHandler handler = new SetAiModelCommandHandler(
                mapper, holder, loadState, capabilityState,
                new AiModelProviderClient(holder, loadState, capabilityState));

        // Deliberately unreachable ports (nothing listening) so the set-ai-model capability probe and
        // loaded-models reconcile both fail fast — this test isolates the clear-on-server-change rule,
        // not provider I/O, and must not collide with a real LM Studio/Ollama on a well-known port.
        handler.handle(cmd("set-ai-model", "{\"server\":\"http://127.0.0.1:59123\",\"loadPath\":\"/api/v1/models/load\"}"));
        loadState.markLoaded("llama3.1:8b");

        // Same server, new key → record survives.
        handler.handle(cmd("set-ai-model",
                "{\"server\":\"http://127.0.0.1:59123\",\"apiKey\":\"k2\",\"loadPath\":\"/api/v1/models/load\"}"));
        assertTrue(loadState.isLoaded("llama3.1:8b"), "same-server edit must keep the loaded record");

        // Different server → record cleared.
        handler.handle(cmd("set-ai-model",
                "{\"server\":\"http://127.0.0.1:59124\",\"loadPath\":\"/api/v1/models/load\"}"));
        assertFalse(loadState.isLoaded("llama3.1:8b"), "a server change must clear the loaded record");
    }

    @Test
    void setDepsAppliesManifest() throws Exception {
        DependencyManifestHolder holder = new DependencyManifestHolder();
        SetDepsCommandHandler handler = new SetDepsCommandHandler(mapper, holder, disabledInstaller(holder));

        String payload = "{\"deps\":[{\"language\":\"python\",\"name\":\"requests\",\"version\":\"2.31.0\",\"state\":\"queued\"}]}";
        DaemonCommandOutcome outcome = handler.handle(cmd("set-deps", payload));

        assertTrue(outcome.succeeded());
        assertEquals(1, holder.current().size());
        assertEquals("requests", holder.current().get(0).name());
        JsonNode ack = mapper.readTree(outcome.output());
        assertEquals(1, ack.get("count").asInt());
    }

    @Test
    void setDepsClearsManifestOnEmptyArray() {
        DependencyManifestHolder holder = new DependencyManifestHolder();
        SetDepsCommandHandler handler = new SetDepsCommandHandler(mapper, holder, disabledInstaller(holder));
        handler.handle(cmd("set-deps", "{\"deps\":[]}"));
        assertTrue(holder.current().isEmpty());
    }

    @Test
    void testConnectionReportsNotConfigured() throws Exception {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        AiModelProviderClient client = new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState());
        TestConnectionCommandHandler handler =
                new TestConnectionCommandHandler(mapper, client, holder, new AiModelCapabilityState());

        DaemonCommandOutcome outcome = handler.handle(cmd("test-connection", null));
        assertTrue(outcome.succeeded(), "the command ran; the connection result is in the output");
        JsonNode result = mapper.readTree(outcome.output());
        assertFalse(result.get("ok").asBoolean());
        assertEquals(0, result.get("status").asInt());
        assertTrue(result.has("models") && result.get("models").isArray());
        assertEquals(0, result.get("models").size());
    }

    /**
     * A payload with no {@code server} is not a usable override, so the handler falls back to the
     * STORED (here unconfigured) config: "No provider configured". No network is exercised.
     */
    @Test
    void testConnectionIgnoresServerlessPayloadAndProbesStored() throws Exception {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        AiModelProviderClient client = new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState());
        TestConnectionCommandHandler handler =
                new TestConnectionCommandHandler(mapper, client, holder, new AiModelCapabilityState());

        DaemonCommandOutcome outcome = handler.handle(cmd("test-connection", "{\"apiKey\":\"k\"}"));
        assertTrue(outcome.succeeded());
        JsonNode result = mapper.readTree(outcome.output());
        assertFalse(result.get("ok").asBoolean());
        assertEquals("No provider configured", result.get("message").asText());
    }

    /**
     * The core fix: when the command carries a DRAFT provider config (server present) the handler
     * probes THAT config, not the daemon's STORED (empty) one. The holder is left unconfigured on
     * purpose: if the payload were ignored the result would be "No provider configured"; instead
     * the probe attempts the forwarded (unreachable) endpoint and fails to CONNECT — proving the
     * draft was used.
     */
    @Test
    void testConnectionProbesForwardedDraftConfig() throws Exception {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        AiModelProviderClient client = new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState());
        TestConnectionCommandHandler handler =
                new TestConnectionCommandHandler(mapper, client, holder, new AiModelCapabilityState());

        // Port 1 on loopback → immediate connection-refused, so the probe fails fast (no hang).
        String payload = "{\"server\":\"http://127.0.0.1:1\",\"modelsPath\":\"/v1/models\"}";
        DaemonCommandOutcome outcome = handler.handle(cmd("test-connection", payload));
        assertTrue(outcome.succeeded());
        JsonNode result = mapper.readTree(outcome.output());
        assertFalse(result.get("ok").asBoolean());
        JsonNode message = result.get("message");
        assertTrue(message == null || !"No provider configured".equals(message.asText()),
                "handler must probe the forwarded draft config, not short-circuit as unconfigured");
        // Probe URL for the draft must be surfaced so operators see what was hit.
        assertNotNull(message);
        assertTrue(message.asText().contains("http://127.0.0.1:1/v1/models"),
                "failure message should include the probed models URL, got: " + message);
        assertTrue(message.asText().toLowerCase().contains("connection refused"),
                "unreachable draft endpoint should report connection refused, got: " + message);
    }

    @Test
    void testConnectionRefusesLinkLocalMetadataUrl() throws Exception {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        TestConnectionCommandHandler handler = new TestConnectionCommandHandler(
                mapper,
                new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()),
                holder, new AiModelCapabilityState());
        DaemonCommandOutcome outcome = handler.handle(cmd("test-connection",
                "{\"server\":\"http://169.254.169.254\",\"modelsPath\":\"/latest/meta-data\"}"));
        assertTrue(outcome.succeeded());
        JsonNode result = mapper.readTree(outcome.output());
        assertFalse(result.get("ok").asBoolean());
        assertTrue(result.get("message").asText().toLowerCase().contains("not allowed"),
                "got: " + result.get("message"));
    }

    @Test
    void lmStudioPresetModelsUrlIsDocumentedOpenAiPath() {
        // Documents the exact URL the daemon GETs for the FE LM Studio preset defaults.
        com.studioquack.sqs.aimodel.AiModelProviderConfig lmStudio =
                new com.studioquack.sqs.aimodel.AiModelProviderConfig(
                        "http://127.0.0.1:1234", null, "/v1/chat/completions", "/v1/models",
                        "/api/v1/models/load", "/api/v1/models/unload",
                        "/api/v1/models/download", "/api/v1/models/download/status", null);
        assertEquals("http://127.0.0.1:1234/v1/models", lmStudio.modelsUrl());
    }

    @Test
    void listModelsReturnsEmptyArrayWhenUnconfigured() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        ListModelsCommandHandler handler =
                new ListModelsCommandHandler(new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()));
        DaemonCommandOutcome outcome = handler.handle(cmd("list-models", null));
        assertTrue(outcome.succeeded());
        assertEquals("[]", outcome.output());
    }

    /** load-model with no usable 'model' in the payload is a clean failure — no provider call. */
    @Test
    void loadModelFailsOnMissingModel() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        LoadModelCommandHandler handler = new LoadModelCommandHandler(
                mapper, new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()),
                new AiModelLoadState(), holder, new AiModelCapabilityState());
        assertFalse(handler.handle(cmd("load-model", null)).succeeded());
        assertFalse(handler.handle(cmd("load-model", "{}")).succeeded());
        assertFalse(handler.handle(cmd("load-model", "{\"model\":\"  \"}")).succeeded());
    }

    /** unload-model with no usable 'model' in the payload is a clean failure — no provider call. */
    @Test
    void unloadModelFailsOnMissingModel() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        UnloadModelCommandHandler handler = new UnloadModelCommandHandler(
                mapper, new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()),
                new AiModelLoadState(), holder, new AiModelCapabilityState());
        assertFalse(handler.handle(cmd("unload-model", null)).succeeded());
        assertFalse(handler.handle(cmd("unload-model", "{\"other\":\"x\"}")).succeeded());
    }

    /**
     * An UNMANAGED provider (no load path configured) cannot load — the handler fails with an
     * actionable message instead of guessing an endpoint. No network is exercised.
     */
    @Test
    void loadModelFailsWhenProviderHasNoLoadPath() {
        AiModelProviderHolder holder = new AiModelProviderHolder();
        holder.apply(new com.studioquack.sqs.aimodel.AiModelProviderConfig(
                "http://127.0.0.1:1", null, "/v1/chat/completions", "/v1/models",
                null, null, null, null, null));
        LoadModelCommandHandler handler = new LoadModelCommandHandler(
                mapper, new AiModelProviderClient(holder, new AiModelLoadState(), new AiModelCapabilityState()),
                new AiModelLoadState(), holder, new AiModelCapabilityState());
        DaemonCommandOutcome outcome = handler.handle(cmd("load-model", "{\"model\":\"gpt-x\"}"));
        assertFalse(outcome.succeeded());
        assertTrue(outcome.error().contains("load path"), "got: " + outcome.error());
    }
}
