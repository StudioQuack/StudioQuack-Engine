package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.AgenticInfo;
import com.studioquack.common.dto.flow.ConnectionDetailDto;
import com.studioquack.common.dto.flow.ConnectionDto;
import com.studioquack.common.dto.flow.ConversationFileRefDto;
import com.studioquack.common.dto.flow.EdgeTransformDto;
import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.PortDto;
import com.studioquack.common.dto.flow.PromptEditSupersede;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.common.enums.flow.PortType;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.files.ClientFileStore;
import com.studioquack.sqs.status.ConversationOutputCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client-side Duckbot boundary bridge — the daemon port of the control plane
 * {@code daemon}/{@code applyDuckbotEndBridge}. Duckbot
 * start/end nodes carry no sandbox script; they adapt the conversation to/from the workflow:
 *
 * <ul>
 * <li><b>start</b> — publishes the triggering user prompt (category {@code conversion}) or the
 * application flow inputs (category {@code application}) onto the start node's OUTPUT
 * wires as <b>source-shaped</b> values {@code {<sourceAttribute>: value, …}}. Consumer
 * attribute mapping (e.g. agent {@code prompt} / {@code role}) belongs exclusively to the
 * consuming node's inbound {@code TransformPipeline}; this bridge never invents those keys.</li>
 * <li><b>end</b> — reads the final answer from the end node's <em>own</em> {@code msg} /
 * {@code files} / {@code voices} inputs (after inbound transform) and routes it back to
 * the control plane via {@link ConversationOutputCallback}, which stores it in the
 * conversation and streams it to the Duckbot screen over SSE.</li>
 * </ul>
 *
 * <p>Requires {@code ExecutionContext.agenticInfo} (conversationId / userPrompt / agentMessageKey),
 * which the control plane ships in the seedContext and the client context now preserves.
 *
 * <p>Do not add per-BPMN-node or per-template branches here. Start/end conversation adaption is
 * the whole type. A new node kind belongs in dispatch / the node library, not another {@code if}
 * in this bridge (SQS-Q-01).
 */
@Slf4j
@Component
public class DuckbotBridge {

    /**
 * Per-file ceiling on the answer callback, in base64 characters. the control plane reads that JSON with
 * Jackson's default 20 000 000-character string limit; staying under it here means one oversized
 * attachment is dropped with a warning instead of failing the answer it was attached to.
 */
    private static final long MAX_ANSWER_FILE_BASE64_CHARS = 2_147_483_648L;

    /**
     * Ceiling on the bytes we will try to read as a JSON file envelope. Above it the value is treated
     * as the file it claims to be: parsing a 100 MiB blob on the chance it is an envelope costs more
     * than the mistake it would catch.
     */
    private static final long MAX_ENVELOPE_BYTES = 2_147_483_648L;

    /** How deep an envelope may nest before we stop unwrapping and deliver what we have. */
    private static final int MAX_ENVELOPE_DEPTH = 3;

    /**
     * How deep {@link #contentBlocksIn} may walk nested maps/lists looking for file descriptors.
     * Separate from {@link #MAX_ENVELOPE_DEPTH}: one is unwrap recursion on spilled blobs, the other is
     * structural search inside a single parsed JSON tree (e.g. {@code result.output_files}).
     */
    private static final int MAX_CONTENT_SEARCH_DEPTH = 4;

    /**
     * Duckbot end node's own text input port ({@code msg}). Only Duckbot's
     * standard ports are named here; never a producer's (agent {@code prompt}/{@code role}, helper
     * {@code content}, …). Mapping onto {@code msg} is the author's inbound edge transform, which has
     * already run by the time the end node reads its input.
     */
    private static final List<String> ANSWER_TEXT_KEYS = List.of("msg");

    /**
     * KEEP — file keys the end node may read. {@code files} is Duckbot's own file input port.
     * {@code output_files} is the architecture-documented file-push exception (LLM helper spill /
     * nested envelope shape under {@code result.output_files}), not an agent-port mapping — never
     * {@code prompt}/{@code role}. Top-level port reading and nested envelope search share this set.
     */
    private static final java.util.Set<String> FILE_KEYS = java.util.Set.of("files", "output_files");

    /**
     * KEEP — Duckbot end node's own audio input port ({@code voices}). Extracted separately from
     * {@link #FILE_KEYS}; VOICE_KEYS wins when any voices are found (including envelopes).
     * Only when none were found does {@link #fromBlocks} MOVE kind=audio file blocks into voices.
     */
    private static final java.util.Set<String> VOICE_KEYS = java.util.Set.of("voices");

    /**
     * KEEP — inline <em>byte-payload</em> aliases inside a ContentBlock / file descriptor map
     * ({@code data}/{@code content}/…). These are file-shape field names, not workflow port names and
     * not an agent node's {@code content} / {@code role} contract.
     */
    private static final java.util.Set<String> CONTENT_KEYS =
            java.util.Set.of("data", "content", "base64", "b64", "bytes");

    /** KEEP — filename field aliases inside a ContentBlock (file descriptor shape, not a port). */
    private static final java.util.Set<String> NAME_KEYS =
            java.util.Set.of("name", "filename", "file_name", "fileName");

    /** KEEP — MIME field aliases inside a ContentBlock (file descriptor shape, not a port). */
    private static final java.util.Set<String> MIME_KEYS =
            java.util.Set.of("mime_type", "mimeType", "mime", "contentType", "content_type");

    /**
     * KEEP — {@code type}/{@code kind} values that mark a ContentBlock as a file (vs text/tool_use).
     * Descriptor taxonomy only; not a workflow port name.
     */
    private static final java.util.Set<String> FILE_KIND_VALUES =
            java.util.Set.of("image", "audio", "video", "document");

    private final ConversationOutputCallback conversationOutputCallback;
    private final com.studioquack.sqs.conversation.ConversationStore conversationStore;
    private final ClientFileStore clientFileStore;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    /**
     * Graph-routed live renderer. Thoughts-port deliveries render as the thinking bubble
     * through it (never as the persisted answer); null in slice tests.
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private DuckbotStreamSink streamSink;

    /**
 * Function-port thoughts land on a child sub-PI whose run id the control plane never registered.
 * Live frames and the final answer POST must use the top-level parent run.
 */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.runtime.SubProcessCompletionRegistry subProcessRegistry;

    /**
     * Cancel marker set by {@code cancel-instance}. A late end-bridge firing after the cancel
     * must not persist or report the answer — the conversation is over and the user already
     * saw the turn stop. Null in slice tests (no guard applied).
     */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.studioquack.sqs.runtime.CancelledRunGuard cancelledRunGuard;

    /**
     * Reasoning a function-port child rendered but had no answer row of its own to persist on,
     * parked so the top-level answer from the same agent recovers it. Keyed by
     * {@code conversationId + sourceNodeId}; TTL-guarded so a turn that never seals can't leak it.
     */
    private final ConcurrentHashMap<String, TimedThinking> pendingThinking = new ConcurrentHashMap<>();
    private static final long PENDING_THINKING_TTL_MS = 15 * 60 * 1000L;
    private static final int PENDING_THINKING_CAP = 512;

    private static final class TimedThinking {
        private final List<ConversationOutputCallback.ThinkingSection> sections = new ArrayList<>();
        private volatile long at = System.currentTimeMillis();
    }

    public DuckbotBridge(ConversationOutputCallback conversationOutputCallback,
                         com.studioquack.sqs.conversation.ConversationStore conversationStore,
                         ClientFileStore clientFileStore,
                         com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        this.conversationOutputCallback = conversationOutputCallback;
        this.conversationStore = conversationStore;
        this.clientFileStore = clientFileStore;
        this.objectMapper = objectMapper;
    }

    public boolean isStart(ExecutionContext ctx) {
        return isBoundary(ctx, "start");
    }

    public boolean isEnd(ExecutionContext ctx) {
        return isBoundary(ctx, "end");
    }

    private boolean isBoundary(ExecutionContext ctx, String nodeType) {
        NodeDto node = ctx == null ? null : ctx.getCurrentNode();
        TemplateDto t = node == null ? null : node.getData();
        if (t == null) {
            return false;
        }
        String category = t.getCategory();
        boolean duck = StringUtils.isNotBlank(category)
                && ("conversion".equalsIgnoreCase(category) || "application".equalsIgnoreCase(category));
        return duck && nodeType.equalsIgnoreCase(t.getNodeType());
    }

    /** Fan the user prompt / flow inputs onto the start node's OUTPUT connections (one-shot). */
    public void applyStart(ExecutionContext ctx) {
        NodeDto currentNode = ctx.getCurrentNode();
        AgenticInfo ai = ctx.getAgenticInfo();
        if (ai == null || StringUtils.isBlank(ai.getConversationId())) {
            log.warn("duckbot.start: no agenticInfo/conversationId on context (seedContext must carry it) — "
                    + "user prompt cannot be injected. node={}", currentNode.getId());
            return;
        }
        String category = currentNode.getData() == null ? "" : StringUtils.defaultString(currentNode.getData().getCategory());
        boolean conversation = "conversion".equalsIgnoreCase(category);

        // Attachments / voice hold `sq-file:` handles after StartRunFileIngest. Publish each file as
        // `{ name, data }` (base64) so agents/Docling can read content without a second fetch; keep
        // the handle under `fileId` for sandbox materialisation. Voice is a one-item list in the
        // SAME ContentBlock shape as files — a voice turn is just an attached audio clip named
        // "user prompt by voice command", published on the start `voices` port with the base64
        // clip under `data`.
        List<Map<String, Object>> attachmentFiles = attachmentFiles(ai);
        List<Map<String, Object>> voiceFiles = voiceFiles(ai);
        boolean hasExtras = !attachmentFiles.isEmpty() || !voiceFiles.isEmpty();

        Object input = conversation ? ai.getUserPrompt() : ai.getFlowInputs();
        if (input == null) {
            // Voice-only / attachment-only turns still need the start bridge; treat missing prompt as "".
            if (!hasExtras) {
                log.info("duckbot.start: nothing to inject (category={} prompt/flowInputs null) node={}",
                        category, currentNode.getId());
                return;
            }
            input = conversation ? "" : Collections.emptyMap();
        }

        if (conversation) {
            // Control plane strips a leading @agent address into userPrompt; strip again here as
            // defense in depth (bare @Name or marker that slipped through).
            String promptText = stripLeadingAgentAddress(String.valueOf(input));
            PromptEditSupersede edit = ai.getPromptEdit();
            if (edit != null) {
                // Before the append, so the edited prompt is never the row being superseded.
                conversationStore.supersedeTurn(ai.getConversationId(), ai.getAgentMessageKey(),
                        edit.getTurnOrdinal(), stripLeadingAgentAddress(edit.getPreviousPrompt()),
                        edit.isDropLaterUserTurns());
            }
            conversationStore.append(ai.getConversationId(), ctx.getUserId(), ai.getAgentMessageKey(),
                    ai.getAppPageId(), "user", promptText, ai.getAttachments());
            if (ai.isIncludeFullHistory()) {
                List<Map<String, String>> history = readHistory(ai.getConversationId());
                if (!history.isEmpty()) {
                    // Conversation mode (the default) — whole thread, mentions scrubbed.
                    // The msg port is declared STRING, so the thread ships as a JSON
                    // array string ([{role, content}, …]): the port value honors its
                    // declared type in every mode, the edge JOLT stays a pure key
                    // rename, and the consuming node (ai-agent) parses its OWN string
                    // port value back into role-tagged messages.
                    input = toThreadJson(history);
                } else {
                    input = promptText;
                }
            } else {
                // Completion mode (explicit Chat-settings choice): only this turn's body
                // via the start msg port.
                input = promptText;
            }
        }
        List<InOutValueDto> out = new ArrayList<>();

        // Preferred path — one output per authored transform edge, published SOURCE-shaped:
        // {<sourceAttribute>: value}. The edge's JOLT is deliberately NOT applied here. Every other
        // node publishes its raw output and the consuming node's inbound leg
        // (TransformPipeline.prepareInbound) performs the mapping; the Duckbot start node is not
        // special in that respect, and pre-shifting here made the boundary the only place in the
        // engine where an output arrived at the wire already renamed. That cost us the mapping
        // itself: a shift only rewrites keys it can find, so a pre-shifted `{prompt: …}` no longer
        // matched its own `msg → prompt` spec on the inbound leg, and anything the spec did not
        // name — the file list travelling beside the message — was dropped when the shift rebuilt
        // the map. Publishing source-shaped restores the control plane behaviour: the author's edge
        // transform decides the target attribute, once, at the consuming node.
        // ONE output per WIRE, not per mapped attribute. A single visual edge that maps two attributes
        // (`msg → prompt` and `files → files`) is two EdgeTransformDto rows sharing the same source and
        // target port, so emitting a DTO per row produced two outputs with the identical binding key
        // (nodeId|portId|portValue|targetNodeId|targetPortId). Both the output store and the inbound
        // merge dedup on that key, so the second silently replaced the first: the agent received the
        // file list and no message. Grouping by wire publishes `{msg: …, files: […]}` as one value,
        // which is also exactly what the wire's own JOLT spec (built from BOTH rows) expects to map.
        List<EdgeTransformDto> edgeTransforms = currentNode.getEdgeTransformListForCurrentNode();
        if (edgeTransforms != null && !edgeTransforms.isEmpty()) {
            // Pass 1 — build every wire's payload WITHOUT siblings. Sibling suppression must be
            // based on what is ACTUALLY published, never on row names alone: a files-named row
            // whose value never ships (overwritten by another row on the same wire, or mapped away)
            // must not withhold the sibling — that silently dropped the only copy of the file on
            // its way to the consumer (a Docling node failed "no document input" with the file
            // attached and the wire carrying just {msg}).
            List<Boolean> messageWires = new ArrayList<>();
            for (Map.Entry<String, List<EdgeTransformDto>> wire : groupByWire(edgeTransforms).entrySet()) {
                List<EdgeTransformDto> rows = wire.getValue();
                EdgeTransformDto first = rows.get(0);
                // KEEP — default short-code is Duckbot start's own message port when the edge omits one.
                String portValue = StringUtils.defaultIfBlank(first.getSourcePortShortCode(), "msg");
                InOutValueDto dto = new InOutValueDto();
                dto.setNodeId(currentNode.getId());
                dto.setPortId(first.getSourcePortId());
                dto.setPortValue(portValue);
                dto.setTargetNodeId(first.getTargetNodeId());
                dto.setTargetPortId(first.getTargetPortId());
                dto.setTargetPortValue(first.getTargetPortShortCode());

                Map<String, Object> sourcePayload = new HashMap<>();
                boolean anyMessageAttribute = false;
                for (EdgeTransformDto edge : rows) {
                    String sourceAttribute = StringUtils.defaultIfBlank(edge.getSourcePath(), portValue);
                    sourcePayload.put(sourceAttribute, sourceValueFor(sourceAttribute, edge.getTargetPath(),
                            portValue, input, attachmentFiles, voiceFiles));
                    anyMessageAttribute |= isMessagePort(sourceAttribute);
                }
                dto.setValue(sourcePayload);
                out.add(dto);
                messageWires.add(anyMessageAttribute && isMessagePort(portValue));
            }
            // Pass 2 — a message wire also offers files/voices under their own source names, for a
            // consumer that reads the wire without mapping them. Never overwrite an attribute the
            // author actually mapped on this wire — and never inject a sibling when ANOTHER wire in
            // this publish already delivers that value to the same target (putIfAbsent cannot see
            // that other DTO). Identity match is exact: sourceValueFor publishes the one shared
            // attachmentFiles/voiceFiles instance.
            Set<String> targetsReceivingFiles = targetsCarryingValue(out, attachmentFiles);
            Set<String> targetsReceivingVoice = targetsCarryingValue(out, voiceFiles);
            for (int i = 0; i < out.size(); i++) {
                if (!messageWires.get(i)) {
                    continue;
                }
                InOutValueDto dto = out.get(i);
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = (Map<String, Object>) dto.getValue();
                putSiblingKeys(payload, attachmentFiles, voiceFiles,
                        !targetsReceivingFiles.contains(dto.getTargetNodeId()),
                        !targetsReceivingVoice.contains(dto.getTargetNodeId()));
            }
        }

        // Fallback — no authored transforms: fan onto the auto-connection list (legacy default key).
        if (out.isEmpty() && currentNode.getConnectionList() != null) {
            List<Boolean> messageWires = new ArrayList<>();
            for (ConnectionDto conn : currentNode.getConnectionList()) {
                if (conn == null) {
                    continue;
                }
                PortType pt = conn.getPortType();
                if (pt != null && pt != PortType.OUTPUT) {
                    continue; // only OUTPUT ports publish a prompt (skip INPUT/FUNCTION/VARIABLE_PROVIDER)
                }
                // KEEP — legacy fan-out default is Duckbot start's own message port.
                String sourcePortValue = StringUtils.defaultIfBlank(conn.getPortValue(), "msg");
                if (conn.getConnectionDetailList() == null) {
                    continue;
                }
                for (ConnectionDetailDto detail : conn.getConnectionDetailList()) {
                    if (detail == null || StringUtils.isBlank(detail.getNodeId())) {
                        continue;
                    }
                    InOutValueDto dto = new InOutValueDto();
                    dto.setNodeId(currentNode.getId());
                    dto.setPortValue(sourcePortValue);
                    dto.setTargetNodeId(detail.getNodeId());
                    PortDto target = detail.getPortDto();
                    if (target != null) {
                        dto.setTargetPortId(target.getId());
                        dto.setTargetPortValue(target.getValue());
                    }
                    // Same value selection as the authored-edge path above. This branch has no edge
                    // transform at all, so nothing downstream will map keys for us — that is why the
                    // message wire also carries files/voices as siblings here, and why the authored-edge
                    // path above must not: there, the author's spec decides what the consumer receives.
                    Object sourceValue = sourceValueFor(sourcePortValue, null, sourcePortValue,
                            input, attachmentFiles, voiceFiles);
                    Map<String, Object> payload = new HashMap<>();
                    payload.put(sourcePortValue, sourceValue);
                    dto.setValue(payload);
                    out.add(dto);
                    messageWires.add(isMessagePort(sourcePortValue));
                }
            }
            // Same payload-based suppression as the authored path above.
            Set<String> targetsReceivingFiles = targetsCarryingValue(out, attachmentFiles);
            Set<String> targetsReceivingVoice = targetsCarryingValue(out, voiceFiles);
            for (int i = 0; i < out.size(); i++) {
                if (!messageWires.get(i)) {
                    continue;
                }
                InOutValueDto dto = out.get(i);
                @SuppressWarnings("unchecked")
                Map<String, Object> payload = (Map<String, Object>) dto.getValue();
                putSiblingKeys(payload, attachmentFiles, voiceFiles,
                        !targetsReceivingFiles.contains(dto.getTargetNodeId()),
                        !targetsReceivingVoice.contains(dto.getTargetNodeId()));
            }
        }
        if (!out.isEmpty()) {
            currentNode.setOutputList(out);
            ai.setUserPrompt(null); // one-shot: never replay the prompt on later nodes
            // Name every published port and its keys. "Where did my message go?" was previously
            // unanswerable from the logs: the count alone cannot distinguish a prompt that was
            // published under a renamed key from one that was never wired at all.
            log.info("duckbot.start: injected {} output(s) node={} promptChars={} files={} voices={} ports={}",
                    out.size(), currentNode.getId(), promptLength(input), attachmentFiles.size(),
                    voiceFiles.size(), describePorts(out));
            if (conversation && promptLength(input) > 0 && out.stream().noneMatch(
                    dto -> isMessagePort(dto.getPortValue()))) {
                // Only the files/voices ports are wired, so the text has nowhere to go. This is a
                // wiring mistake in the workflow, not a transport failure — say so, because the
                // symptom the user sees is an agent answering as if no question was asked.
                log.warn("duckbot.start: the user's message is not wired anywhere — node={} publishes only "
                                + "files/voices ports, so {} char(s) of prompt are dropped. Connect the "
                                + "start node's message port to the node that should read it.",
                        currentNode.getId(), promptLength(input));
            }
        } else {
            log.warn("duckbot.start: no OUTPUT connections to publish the prompt onto node={} (connections={})",
                    currentNode.getId(),
                    currentNode.getConnectionList() == null ? 0 : currentNode.getConnectionList().size());
        }
    }

    /**
     * Groups a start node's edge transforms by the WIRE they belong to, preserving authoring order.
     *
     * <p>The key mirrors {@code bindingKey} in the execution-context store and the inbound merge —
     * source port plus target node/port — because that is the identity those two dedup on. Any two rows
     * that share it are attribute mappings on one edge and must publish one combined value.
     */
    private static java.util.LinkedHashMap<String, List<EdgeTransformDto>> groupByWire(
            List<EdgeTransformDto> edgeTransforms) {
        java.util.LinkedHashMap<String, List<EdgeTransformDto>> byWire = new java.util.LinkedHashMap<>();
        for (EdgeTransformDto edge : edgeTransforms) {
            if (edge == null || StringUtils.isBlank(edge.getTargetNodeId())) {
                continue;
            }
            String key = StringUtils.defaultString(edge.getSourcePortId()) + '|'
                    + StringUtils.defaultString(edge.getSourcePortShortCode()) + '|'
                    + edge.getTargetNodeId() + '|'
                    + StringUtils.defaultString(edge.getTargetPortId());
            byWire.computeIfAbsent(key, k -> new ArrayList<>()).add(edge);
        }
        return byWire;
    }

    /**
     * Conversation thread → the JSON array string the {@code msg} port publishes in
     * conversation mode. The port is declared {@code string}, so the wire value is a
     * string in every mode; a serialization failure degrades to the last message text
     * rather than failing the turn.
     */
    private String toThreadJson(List<Map<String, String>> history) {
        try {
            return objectMapper.writeValueAsString(history);
        } catch (Exception e) {
            return history.get(history.size() - 1).getOrDefault("content", "");
        }
    }

    /** Character count of the published prompt. */
    private static int promptLength(Object input) {
        if (input == null) {
            return 0;
        }
        return String.valueOf(input).length();
    }

    /** {@code port->target[keys]} per published output, for the one log line that explains the fan-out. */
    private static String describePorts(List<InOutValueDto> outputs) {
        List<String> parts = new ArrayList<>();
        for (InOutValueDto dto : outputs) {
            String keys = dto.getValue() instanceof Map<?, ?> map
                    ? String.join("|", map.keySet().stream().map(String::valueOf).toList())
                    : "<scalar>";
            parts.add(StringUtils.defaultIfBlank(dto.getPortValue(), "?")
                    + "->" + StringUtils.defaultIfBlank(dto.getTargetNodeId(), "?") + "[" + keys + "]");
        }
        return String.join(", ", parts);
    }

    /**
     * The value one start OUTPUT wire publishes: the file list for a {@code files} wire, the base64
     * clip for a {@code voices} wire, and the prompt / flow inputs for anything else.
     *
     * <p>Which start <em>output</em> to publish is decided by the authored attribute
     * ({@code sourcePath}, then {@code targetPath} only as a selector among Duckbot start ports
     * {@code files}/{@code voices}), and only then by the port's short code. Selecting on the short
     * code alone was wrong whenever the two disagree — a start node whose output handle is named
     * something generic still carries a {@code files} attribute, and the mismatch published the user's
     * message under the {@code files} key. {@code targetPath} is <b>never</b> used to invent consumer
     * keys (agent {@code prompt}/{@code role}, …); the payload is always keyed by
     * {@code sourceAttribute} in {@link #applyStart}. A consumer {@code prompt} targetPath simply
     * fails the {@code files}/{@code voices} match and the message value is published under the source
     * name for inbound TransformPipeline to map.
     */
    private static Object sourceValueFor(String sourceAttribute, String targetAttribute,
                                        String portShortCode, Object promptInput,
                                        List<Map<String, Object>> attachmentFiles,
                                        List<Map<String, Object>> voiceFiles) {
        // The SOURCE attribute names the start port to publish; the consumer's targetPath is
        // only a fallback selector when the source names no start port. Matching the target
        // first published the wrong list: a voices → files row matched "files" on the TARGET
        // and shipped the (empty) attachment list under the voices key — the voice clip never
        // reached the wire, and Docling read {files: []} after the inbound shift.
        if (namesPort("files", sourceAttribute)) {
            return attachmentFiles;
        }
        if (namesPort("voices", sourceAttribute)) {
            return voiceFiles;
        }
        // Only Duckbot start ports are matched — never consumer attributes like prompt/role.
        if (namesPort("files", targetAttribute, portShortCode)) {
            return attachmentFiles;
        }
        if (namesPort("voices", targetAttribute, portShortCode)) {
            return voiceFiles;
        }
        return promptInput;
    }

    /**
     * @return {@code true} when any of {@code candidates} names {@code port} (leaf of a dotted path).
     * Used only to recognise Duckbot start ports ({@code files}/{@code voices}); callers never pass a
     * consumer port name as {@code port}.
     */
    private static boolean namesPort(String port, String... candidates) {
        for (String candidate : candidates) {
            if (StringUtils.isBlank(candidate)) {
                continue;
            }
            String leaf = candidate.substring(candidate.lastIndexOf('.') + 1).trim();
            if (port.equalsIgnoreCase(leaf)) {
                return true;
            }
        }
        return false;
    }

    /**
     * KEEP — Duckbot start port taxonomy. Every port except the dedicated {@code files} / {@code voices}
     * outputs is the message port: it publishes the prompt and ALSO carries files/voices as siblings
     * under <em>their own source names</em>, so a single message edge (whatever its handle short-code —
     * {@code msg}, {@code output}, …) still delivers them without a dedicated port wired. Does not
     * consult consumer ports ({@code prompt}/{@code role}).
     */
    private static boolean isMessagePort(String portShortCode) {
        return !"files".equalsIgnoreCase(portShortCode) && !"voices".equalsIgnoreCase(portShortCode);
    }

    /**
     * KEEP — sibling keys under Duckbot start's OWN output names ({@code files}, {@code voices}) next
     * to the message attribute. Never writes consumer keys such as {@code prompt}/{@code role}.
     * Omits a key when absent so downstream wiring stays backward compatible. Callers pass
     * {@code injectFiles}/{@code injectVoice} false when another wire in the same publish already
     * maps that attribute to the same target node.
     */
    private static void putSiblingKeys(Map<String, Object> payload,
                                       List<Map<String, Object>> attachmentFiles,
                                       List<Map<String, Object>> voiceFiles,
                                       boolean injectFiles,
                                       boolean injectVoice) {
        if (injectFiles && attachmentFiles != null && !attachmentFiles.isEmpty()) {
            payload.putIfAbsent("files", attachmentFiles);
        }
        if (injectVoice && voiceFiles != null && !voiceFiles.isEmpty()) {
            payload.putIfAbsent("voices", voiceFiles);
        }
    }

    /**
     * Targets whose published payload actually CARRIES {@code value} — identity match, exact and
     * cheap because the bridge publishes the one shared attachment/voice instance. This, not row
     * names, decides sibling suppression: a row that merely NAMES files but never ships the value
     * (overwritten by another row on the same wire) must not withhold the sibling.
     */
    private static Set<String> targetsCarryingValue(List<InOutValueDto> outputs, Object value) {
        Set<String> targets = new HashSet<>();
        if (value == null) {
            return targets;
        }
        for (InOutValueDto dto : outputs) {
            if (dto == null || StringUtils.isBlank(dto.getTargetNodeId())
                    || !(dto.getValue() instanceof Map<?, ?> map)) {
                continue;
            }
            for (Object v : map.values()) {
                if (v == value) {
                    targets.add(dto.getTargetNodeId());
                    break;
                }
            }
        }
        return targets;
    }

    /**
     * Per-attachment ContentBlocks for the start {@code files} port.
     *
     * <p><b>Sealed shape (persisted):</b> {@code { kind, mime_type, source: "sq-file", name, fileId }}
     * — <em>no</em> inline {@code data}. A 5 MiB GIF becomes ~7 MiB base64; two fan-out edges push
     * that past Jackson's 20 MiB string limit and blow up the next node's
     * {@code execution_context} load. Bytes stay in {@link ClientFileStore}; 
     * {@link com.studioquack.sqs.files.ContentBlockHydrator} fills {@code data} only on the
     * private sandbox bake map (agent {@code main_input.files} still sees base64).
     *
     * <p>A ref without resolvable content is skipped (never publish the chat text as a fake document).
     */
    private List<Map<String, Object>> attachmentFiles(AgenticInfo ai) {
        if (ai == null || ai.getAttachments() == null || ai.getAttachments().isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> files = new ArrayList<>();
        for (var ref : ai.getAttachments()) {
            if (ref == null) {
                continue;
            }
            if (!ClientFileStore.isRef(ref.getFileId())) {
                log.warn("duckbot.start: attachment {} has no client-side content; the agent will not "
                        + "receive it", ref.getName());
                continue;
            }
            var stored = clientFileStore.resolve(ref.getFileId());
            if (stored.isEmpty()) {
                log.warn("duckbot.start: attachment handle {} could not be resolved; omitting",
                        ref.getFileId());
                continue;
            }
            String name = StringUtils.defaultIfBlank(stored.get().fileName(),
                    StringUtils.defaultIfBlank(ref.getName(), "document"));
            String mime = StringUtils.defaultIfBlank(stored.get().mimeType(),
                    StringUtils.defaultIfBlank(ref.getMimeType(), "application/octet-stream"));
            String kind = StringUtils.defaultIfBlank(ref.getKind(), inferContentKind(mime));
            Map<String, Object> item = new java.util.LinkedHashMap<>();
            item.put("kind", kind);
            item.put("mime_type", mime);
            item.put("source", "sq-file");
            item.put("name", name);
            item.put("fileId", ref.getFileId());
            files.add(item);
        }
        return files;
    }

    /** Map MIME → ContentBlock kind (image / audio / video / document). */
    static String inferContentKind(String mimeType) {
        if (mimeType == null || mimeType.isBlank()) {
            return "document";
        }
        String mime = mimeType.toLowerCase(java.util.Locale.ROOT);
        if (mime.startsWith("image/")) {
            return "image";
        }
        if (mime.startsWith("audio/")) {
            return "audio";
        }
        if (mime.startsWith("video/")) {
            return "video";
        }
        return "document";
    }

    /** Display name of the voice clip file on the start node's {@code voices} port. */
    private static final String VOICE_FILE_NAME = "user prompt by voice command";

    /**
     * The start node's {@code voices} OUTPUT as a one-item-or-more list in the SAME ContentBlock
     * shape the {@code files} port publishes ({@code kind, mime_type, source, name, fileId}) — a
     * voice turn is indistinguishable from an attached audio clip downstream. The clip is named
     * {@code "user prompt by voice command.<ext>"} with the extension derived from the voice MIME
     * type, and unlike the (possibly multi-MB) file attachments it also carries the base64 clip
     * inline under {@code data}: voice memos are small, and the pre-list voice contract was inline
     * base64, so consumers never need a second fetch. Never logs the payload. Returns an empty list
     * when absent or unresolvable — callers omit the key.
     */
    private List<Map<String, Object>> voiceFiles(AgenticInfo ai) {
        ConversationFileRefDto voice = ai == null ? null : ai.getVoice();
        if (voice == null) {
            return List.of();
        }
        String mime = StringUtils.defaultIfBlank(voice.getMimeType(), null);
        String base64 = null;
        String fileId = null;
        if (ClientFileStore.isRef(voice.getFileId())) {
            var stored = clientFileStore.resolve(voice.getFileId());
            if (stored.isEmpty()) {
                log.warn("duckbot.start: voice handle could not be resolved; omitting voice output");
                return List.of();
            }
            mime = StringUtils.defaultIfBlank(stored.get().mimeType(), mime);
            base64 = Base64.getEncoder().encodeToString(stored.get().content());
            fileId = voice.getFileId();
        } else if (StringUtils.isNotBlank(voice.getContentBase64())) {
            // Ingest may not have run (tests) — prefer any remaining inline content over failing closed.
            base64 = voice.getContentBase64();
        } else {
            log.warn("duckbot.start: voice {} has no client-side content; omitting voice output",
                    voice.getName());
            return List.of();
        }
        String mimeType = StringUtils.defaultIfBlank(mime, "audio/webm");
        Map<String, Object> item = new java.util.LinkedHashMap<>();
        item.put("kind", "audio");
        item.put("mime_type", mimeType);
        item.put("source", fileId != null ? "sq-file" : "base64");
        item.put("name", VOICE_FILE_NAME + voiceExtensionFor(mimeType));
        if (fileId != null) {
            item.put("fileId", fileId);
        }
        item.put("data", base64);
        return List.of(item);
    }

    /** File extension for a voice MIME type ({@code audio/webm;codecs=opus} → {@code .webm}). */
    private static String voiceExtensionFor(String mimeType) {
        String base = mimeType == null ? "" : mimeType.split(";", 2)[0].trim().toLowerCase(java.util.Locale.ROOT);
        return switch (base) {
            case "audio/wav", "audio/x-wav" -> ".wav";
            case "audio/mpeg", "audio/mp3" -> ".mp3";
            case "audio/ogg" -> ".ogg";
            case "audio/mp4", "audio/x-m4a" -> ".m4a";
            case "audio/flac" -> ".flac";
            default -> ".webm"; // the composer records webm/opus
        };
    }

    /**
     * Drop a leading agent address ({@code [@Name](mention:agent:…)} or bare {@code @Name}).
     * Returns the remainder (possibly empty), never null for non-null input.
     */
    static String stripLeadingAgentAddress(String prompt) {
        if (prompt == null) {
            return null;
        }
        String trimmed = StringUtils.stripStart(prompt, null);
        java.util.regex.Matcher marker = java.util.regex.Pattern
                .compile("^\\[@([^\\]]+)\\]\\(mention:agent:([^)]+)\\)\\s*", java.util.regex.Pattern.DOTALL)
                .matcher(trimmed);
        if (marker.find()) {
            return trimmed.substring(marker.end());
        }
        java.util.regex.Matcher bare = java.util.regex.Pattern
                .compile("^@([^\\s:,]+)(?:\\s+([^\\s:,]+))?\\s*[,:]?\\s*", java.util.regex.Pattern.DOTALL)
                .matcher(trimmed);
        if (bare.find()) {
            return trimmed.substring(bare.end());
        }
        return trimmed;
    }

    /**
     * The conversation so far, oldest first, as {@code {role, content}} pairs.
     *
     * <p>KEEP — {@code role}/{@code content} here are Duckbot {@link com.studioquack.sqs.conversation.ConversationStore}
     * message fields (chat transcript shape published on the start message wire when full history is
     * on). They are <b>not</b> an agent node's workflow ports; this method does not read or write
     * consumer attributes such as agent {@code prompt}.
     *
     * <p>Never fails the run: a node that answers without its history is degraded, whereas one that
     * throws produces nothing at all.
     */
    private List<Map<String, String>> readHistory(String conversationId) {
        try {
            List<Map<String, String>> out = new ArrayList<>();
            for (var message : conversationStore.history(conversationId)) {
                String role = StringUtils.defaultString(message.getRole(), "user");
                String content = StringUtils.defaultString(message.getContent(), "");
                if ("user".equalsIgnoreCase(role)) {
                    content = stripLeadingAgentAddress(content);
                }
                out.add(Map.of("role", role, "content", content));
            }
            return out;
        } catch (Exception e) {
            log.warn("duckbot.start: could not read conversation {} ({}); continuing with the prompt only",
                    conversationId, e.getClass().getSimpleName());
            return List.of();
        }
    }

    /** Persist the end node's final answer locally, then push it to the open chat. */
    public void applyEnd(ExecutionContext ctx) {
        NodeDto currentNode = ctx.getCurrentNode();
        AgenticInfo ai = ctx.getAgenticInfo();
        if (ai == null || StringUtils.isBlank(ai.getConversationId())) {
            log.warn("duckbot.end: no agenticInfo/conversationId — final answer cannot reach the screen. node={}",
                    currentNode.getId());
            return;
        }
        // Late end after cancel: the run was cancelled while a node (typically the LLM call) was
        // still in flight. Persisting or reporting now would append an answer to a conversation
        // the user already stopped — drop it (both the raw child id and the reportable parent are
        // checked; cancel marks the business key, which is the top-level run).
        if (cancelledRunGuard != null
                && (cancelledRunGuard.isCancelled(reportableRunId(ctx))
                    || cancelledRunGuard.isCancelled(ctx.getWorkflowExecutionId()))) {
            log.info("duckbot.end: run cancelled — dropping the late answer instead of persisting it "
                    + "conversationId={} node={}", ai.getConversationId(), currentNode.getId());
            return;
        }
        if (currentNode.getInputList() == null || currentNode.getInputList().isEmpty()) {
            log.info("duckbot.end: no inputList on end node={} — nothing to store.", currentNode.getId());
            sealTurnIfTopLevel(ctx, ai);
            return;
        }
        // First pass: collect the turn's thinking sections in production order. They persist WITH
        // the answer row (and ride the conversation-output report) so the bubble survives the next
        // turn, a reload, and every room viewer — before this, thinking lived only in the ephemeral
        // live stream and vanished from the row as soon as the turn sealed.
        List<ConversationOutputCallback.ThinkingSection> thinkingSections = new ArrayList<>();
        for (InOutValueDto in : currentNode.getInputList()) {
            if (in == null || !isThinkingSourced(in)) {
                continue;
            }
            // A FUNCTION-port delivery carries assembled ephemeral stream content:
            // it renders as the thinking bubble, never as the persisted answer. When the
            // producer already streamed this content live, the sink skips the re-render.
            // The delivery is stamped with the SOURCE node's id (nodeId) and canvas label
            // (sourceNodeLabel) at dispatch — the fallback keys and titles by the agent,
            // never by this end node, so chained agents each paint under their own name
            // and the live-stream dedup (keyed by the same source id) actually matches.
            String rendererNode = StringUtils.isNotBlank(in.getNodeId())
                    ? in.getNodeId() : currentNode.getId();
            String sourceLabel = StringUtils.isNotBlank(in.getSourceNodeLabel())
                    ? in.getSourceNodeLabel() : sourceNodeLabelOf(ctx, rendererNode);
            String thinkingText = thinkingTextOf(in.getValue());
            if (StringUtils.isNotBlank(thinkingText)) {
                ConversationOutputCallback.ThinkingSection section =
                        new ConversationOutputCallback.ThinkingSection(rendererNode, sourceLabel, thinkingText);
                thinkingSections.add(section);
                // A function-port child renders the reasoning but has no answer row of its own to
                // persist it on — park it so the top-level answer from the same agent recovers it.
                if (isFunctionPortChild(ctx)) {
                    parkPendingThinking(ai.getConversationId(), rendererNode, section);
                }
            }
            if (streamSink != null) {
                streamSink.renderFallback(reportableRunId(ctx), ai.getConversationId(),
                        ai.getAgentMessageKey(), rendererNode, sourceLabel, "thinking", thinkingText);
            }
        }
        List<ConversationOutputCallback.ThinkingSection> persistedThinking =
                thinkingSections.isEmpty() ? null : List.copyOf(thinkingSections);
        for (InOutValueDto in : currentNode.getInputList()) {
            if (in == null || isThinkingSourced(in)) {
                continue;
            }
            // Normalise every shape the files / voices ports can land in (map.files / map.voices,
            // bare ContentBlock, bare sq-file: handle, list of handles) so a ref never becomes
            // the chat bubble text. VOICE_KEYS wins; otherwise kind=audio file blocks become voices.
            EndAnswer answer = parseEndAnswer(in.getValue());
            String rendererNodeId = StringUtils.isNotBlank(in.getNodeId())
                    ? in.getNodeId() : currentNode.getId();
            // The reasoning may have streamed to a function-port child rather than this answer row's
            // inputList; recover the parked sections so the bubble survives a reload. An inputList
            // that already carries the thinking (persistedThinking) wins.
            List<ConversationOutputCallback.ThinkingSection> effectiveThinking = persistedThinking != null
                    ? persistedThinking : drainPendingThinking(ai.getConversationId(), rendererNodeId);
            // Persist locally first. Prefer file store refs the control plane returns below so a history reload
            // can download through the authorised chat file endpoint; fall back to client handles.
            var saved = conversationStore.append(ai.getConversationId(), ctx.getUserId(),
                    ai.getAgentMessageKey(), ai.getAppPageId(), "assistant", answer.message(),
                    answer.localRefs(), effectiveThinking);
            int msgOrder = conversationStore.messageCount(ai.getConversationId());
            List<ConversationFileRefDto> deliveredRefs = conversationOutputCallback.report(
                    new ConversationOutputCallback.ConversationOutput(
                            reportableRunId(ctx), ai.getConversationId(), ai.getAgentMessageKey(),
                            ai.getAppPageId(), "assistant", answer.message(), msgOrder, answer.files(),
                            answer.voices(), rendererNodeId, effectiveThinking));
            if (deliveredRefs != null && !deliveredRefs.isEmpty() && saved != null) {
                // The browser downloads from the control plane's file store — rewrite the row so reopen works.
                conversationStore.replaceAttachments(saved.getId(), deliveredRefs);
            }
            // The final answer seals the turn: close any assembled-thinking fallback stream.
            if (streamSink != null) {
                streamSink.endFallback(ai.getConversationId());
            }
            log.info("duckbot.end: stored final answer locally + pushed to screen conversationId={} chars={} files={} voices={}",
                    ai.getConversationId(), answer.message().length(), answer.files().size(),
                    answer.voices().size());
        }
        // Top-level end with only thinking (or an empty answer already sealed above): the FE
        // still needs a terminal frame. Function-port children must not seal — endFallback
        // fires only at the very end of the turn.
        sealTurnIfTopLevel(ctx, ai);
    }

    /**
 * Top-level run id the control plane registered. A function-port Duckbot-end child keeps its own
 * {@code workflowExecutionId}; posting under that child 404s ({@code Error_RunNotFound}).
 */
    private String reportableRunId(ExecutionContext ctx) {
        String raw = ctx == null ? null : ctx.getWorkflowExecutionId();
        if (raw == null || raw.isBlank() || subProcessRegistry == null) {
            return raw;
        }
        try {
            String resolved = subProcessRegistry.resolveReportableRunId(raw);
            return resolved == null || resolved.isBlank() ? raw : resolved;
        } catch (RuntimeException e) {
            return raw;
        }
    }

    /**
     * Function-port {@code execute("thoughts")} runs this end node as a child sub-PI.
     * Those bursts must not seal the turn — {@link DuckbotStreamSink#endFallback} fires
     * only on the top-level end dispatch (including thinking-only / empty-answer turns).
     */
    private boolean isFunctionPortChild(ExecutionContext ctx) {
        String raw = ctx == null ? null : ctx.getWorkflowExecutionId();
        if (raw == null || raw.isBlank() || subProcessRegistry == null) {
            return false;
        }
        try {
            String resolved = subProcessRegistry.resolveReportableRunId(raw);
            return resolved != null && !resolved.isBlank() && !resolved.equals(raw);
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Parks a reasoning section a function-port child produced (it has no answer row of its own to
     * persist on) so the top-level answer from the same agent can recover it. Keyed by
     * conversation + source agent — the same identity the answer row uses for its rendererNodeId.
     */
    private void parkPendingThinking(String conversationId, String sourceNodeId,
                                     ConversationOutputCallback.ThinkingSection section) {
        if (conversationId == null || sourceNodeId == null) {
            return;
        }
        if (pendingThinking.size() > PENDING_THINKING_CAP) {
            long cutoff = System.currentTimeMillis() - PENDING_THINKING_TTL_MS;
            pendingThinking.entrySet().removeIf(e -> e.getValue().at < cutoff);
        }
        pendingThinking.compute(conversationId + '\0' + sourceNodeId, (k, existing) -> {
            TimedThinking t = existing == null ? new TimedThinking() : existing;
            t.sections.add(section);
            t.at = System.currentTimeMillis();
            return t;
        });
    }

    /**
     * Removes and returns the reasoning parked for {@code conversationId + sourceNodeId}, or null
     * when none was parked. Draining on the answer row keeps the map bounded to one turn.
     */
    private List<ConversationOutputCallback.ThinkingSection> drainPendingThinking(String conversationId,
                                                                                  String sourceNodeId) {
        if (conversationId == null || sourceNodeId == null) {
            return null;
        }
        TimedThinking t = pendingThinking.remove(conversationId + '\0' + sourceNodeId);
        return t == null || t.sections.isEmpty() ? null : List.copyOf(t.sections);
    }

    private void sealTurnIfTopLevel(ExecutionContext ctx, AgenticInfo ai) {
        if (streamSink == null || ai == null || StringUtils.isBlank(ai.getConversationId())
                || isFunctionPortChild(ctx)) {
            return;
        }
        streamSink.endFallback(ai.getConversationId());
    }

    /**
     * True when this input arrived from a producer's {@code thoughts}/{@code thinking} FUNCTION
     * port (function-port deliveries keep the SOURCE port in {@code portValue}). Duckbot-local
     * rule: thinking content is the only non-answer channel the end node recognises.
     */
    private static boolean isThinkingSourced(InOutValueDto in) {
        // Discrimination is by SOURCE PORT TYPE (stamped from the caller's connection metadata
        // at function-port dispatch), never by port name: FUNCTION-port deliveries render as
        // ephemeral thinking, OUTPUT-port (data-edge) deliveries persist as the answer.
        return "FUNCTION".equalsIgnoreCase(in.getSourcePortType());
    }

    /**
     * Extracts displayable thinking text from a thoughts-port payload: the agent's
     * {@code {event_type, text:"<thinking>…</thinking>", …}} envelope unwraps to the inner text;
     * anything else stringifies as-is.
     */
    private String thinkingTextOf(Object value) {
        // The edge transform already shapes the producer's payload for the end node's input;
        // what remains here is shape-based only — a single-entry map unwraps to its lone value,
        // anything else stringifies as-is. No producer parameter names are known here.
        if (value instanceof Map<?, ?> map && map.size() == 1) {
            Object only = map.values().iterator().next();
            if (only != null) {
                return stripThinkingTags(String.valueOf(only));
            }
        }
        return value == null ? "" : stripThinkingTags(String.valueOf(value));
    }

    /**
     * Unwraps a {@code <thinking>…</thinking>} envelope, preserving the content's whitespace
     * exactly as the producer emitted it: assembled thinking arrives in pieces whose
     * leading/trailing spaces are word separators — stripping them welds words together.
     */
    private static String stripThinkingTags(String text) {
        if (text == null) {
            return "";
        }
        String probe = text.strip();
        if (probe.startsWith("<thinking>") && probe.endsWith("</thinking>")) {
            int start = text.indexOf("<thinking>") + "<thinking>".length();
            int end = text.lastIndexOf("</thinking>");
            if (end >= start) {
                return text.substring(start, end);
            }
        }
        return text;
    }

    /**
     * Canvas label of the node a thinking delivery came from — the bubble title the FE shows
     * so chained agents each paint under their own name. The function-port sub-PI context
     * still carries the graph; when it does not, the node id stands in.
     */
    private static String sourceNodeLabelOf(ExecutionContext ctx, String rendererNodeId) {
        if (ctx.getNodes() != null) {
            for (NodeDto n : ctx.getNodes()) {
                if (n != null && rendererNodeId.equals(n.getId()) && n.getData() != null
                        && StringUtils.isNotBlank(n.getData().getLabel())) {
                    return n.getData().getLabel();
                }
            }
        }
        return rendererNodeId;
    }

    /**
     * Message text + byte-carrying answer files / voices extracted from whatever the end node's
     * input value is.
     *
     * <p>Authors wire the {@code files} and {@code voices} ports in several shapes; stringifying any
     * of them put {@code sq-file:…} on the Duckbot screen where a thumbnail/download should be.
     * Voice clips come from the {@code voices} key when present; otherwise kind=audio file
     * blocks are moved into voices so they do not also render as attachments.
     */
    private EndAnswer parseEndAnswer(Object raw) {
        if (raw == null) {
            return EndAnswer.empty();
        }
        if (ClientFileStore.isRef(raw)) {
            return fromBlocks(List.of(handleBlock(String.valueOf(raw))), List.of(), "");
        }
        if (raw instanceof List<?> list) {
            return fromBlocks(list, List.of(), "");
        }
        if (raw instanceof Map<?, ?> map) {
            if (looksLikeContentBlock(map)) {
                return fromBlocks(List.of(map), List.of(), "");
            }
            String msg = answerText(map);
            List<Map<String, Object>> blocks = fileBlocks(map);
            List<Map<String, Object>> voiceBlocks = voiceBlocks(map);
            // A transform that mapped fileId → msg would put the handle in the bubble; promote it.
            if (ClientFileStore.isRef(msg)) {
                blocks = new ArrayList<>(blocks);
                blocks.add(handleBlock(msg));
                msg = "";
            }
            return fromBlocks(blocks, voiceBlocks, msg == null ? "" : msg);
        }
        String scalar = String.valueOf(raw);
        if (ClientFileStore.isRef(scalar)) {
            return fromBlocks(List.of(handleBlock(scalar)), List.of(), "");
        }
        // A producer that serialised its files to JSON (the LLM helper returns a string) reaches the end
        // node as text. Delivered as-is the user gets a downloadable .txt whose contents are a filename
        // and a base64 blob — the file is in there, just not usable.
        EnvelopeParse fromText = parseEnvelope(scalar.getBytes(StandardCharsets.UTF_8));
        if (!fromText.blocks().isEmpty() || !fromText.voices().isEmpty()) {
            // Envelope assistant text fills an empty end-node msg; never invent over an authored one.
            return fromBlocks(fromText.blocks(), fromText.voices(),
                    StringUtils.defaultString(fromText.message()));
        }
        return new EndAnswer(scalar, List.of(), List.of(), null);
    }

    private EndAnswer fromBlocks(List<?> rawFileBlocks, List<?> rawVoiceBlocks, String message) {
        String msg = message == null ? "" : message;
        EnvelopeExpansion expanded = expandEnvelopes(coerceBlocks(rawFileBlocks), 0);
        // Spilled llm_output.txt often carries the assistant string inside the envelope with empty msg.
        if (msg.isBlank() && StringUtils.isNotBlank(expanded.envelopeMessage())) {
            msg = expanded.envelopeMessage();
        }
        List<Map<String, Object>> fileBlocks = expanded.blocks();
        List<Map<String, Object>> voicesFromFiles = expanded.voices();
        List<Map<String, Object>> voiceBlocks = coerceBlocks(rawVoiceBlocks);
        EnvelopeExpansion voiceExpanded = voiceBlocks.isEmpty()
                ? new EnvelopeExpansion(List.of(), List.of(), null)
                : expandEnvelopes(voiceBlocks, 0);
        if (msg.isBlank() && StringUtils.isNotBlank(voiceExpanded.envelopeMessage())) {
            msg = voiceExpanded.envelopeMessage();
        }
        // Voice-key items stay voices even after envelope unwrap. File-envelope `voices` join them.
        List<Map<String, Object>> allVoices = new ArrayList<>(voicesFromFiles.size()
                + voiceExpanded.blocks().size() + voiceExpanded.voices().size());
        allVoices.addAll(voicesFromFiles);
        allVoices.addAll(voiceExpanded.blocks());
        allVoices.addAll(voiceExpanded.voices());
        // No voices via VOICE_KEYS (or envelope voices): MOVE kind=audio off files so they play,
        // not attach. Image/video/document stay files. When VOICE_KEYS found anything, do not steal.
        if (allVoices.isEmpty()) {
            List<Map<String, Object>> remainingFiles = new ArrayList<>(fileBlocks.size());
            for (Map<String, Object> block : fileBlocks) {
                if (isAudioFileKind(block)) {
                    allVoices.add(block);
                } else {
                    remainingFiles.add(block);
                }
            }
            fileBlocks = remainingFiles;
        }
        List<ConversationOutputCallback.OutputFile> files = outputFilesFromBlocks(fileBlocks);
        List<ConversationOutputCallback.OutputFile> voices = outputFilesFromBlocks(allVoices);
        List<ConversationFileRefDto> localRefs = localRefsFromBlocks(fileBlocks);
        return new EndAnswer(msg, files, voices, localRefs);
    }

    /**
     * Replaces any block whose content is really a JSON <em>envelope of files</em> with the files it
     * describes.
     *
     * <p>Files reach the end node through several producers, and not all of them keep the ContentBlock
     * shape: an LLM helper returns a string, and an oversized string is spilled into the file store as
     * a text blob named after its port. Both then look like ONE text attachment whose bytes happen to
     * be {@code {"name": "...", "data": "<base64>"}}. Handing that to the chat is what put a
     * downloadable {@code llm_output.txt} in front of the user instead of their image, so the envelope
     * is opened here — at the boundary that owns what the user sees — rather than in the browser.
     *
     * @param depth guards against an envelope that nests into itself
     */
    private EnvelopeExpansion expandEnvelopes(List<Map<String, Object>> blocks, int depth) {
        if (blocks.isEmpty() || depth > MAX_ENVELOPE_DEPTH) {
            return new EnvelopeExpansion(blocks, List.of(), null);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        List<Map<String, Object>> voices = new ArrayList<>();
        String envelopeMessage = null;
        for (Map<String, Object> block : blocks) {
            Map<String, Object> normalized = maybeNormalizeFileBlock(block);
            if (normalized == null) {
                continue; // remote URL / unreadable payload — already WARNed
            }
            byte[] content = contentOf(normalized);
            EnvelopeParse nested = parseEnvelope(content);
            if (!nested.blocks().isEmpty() || !nested.voices().isEmpty()) {
                log.info("duckbot.end: opened a file envelope carrying {} file(s) {} voice(s) instead of "
                                + "delivering it as one text attachment",
                        nested.blocks().size(), nested.voices().size());
                if (envelopeMessage == null && StringUtils.isNotBlank(nested.message())) {
                    envelopeMessage = nested.message();
                }
                EnvelopeExpansion deeper = expandEnvelopes(nested.blocks(), depth + 1);
                out.addAll(deeper.blocks());
                voices.addAll(nested.voices());
                voices.addAll(deeper.voices());
                if (envelopeMessage == null && StringUtils.isNotBlank(deeper.envelopeMessage())) {
                    envelopeMessage = deeper.envelopeMessage();
                }
            } else {
                out.add(normalized);
            }
        }
        return new EnvelopeExpansion(out, voices, envelopeMessage);
    }

    /** The block's bytes: inline base64, inline text, or the resolved content of its handle. */
    private byte[] contentOf(Map<String, Object> block) {
        Object handle = block.get("fileId");
        if (ClientFileStore.isRef(handle)) {
            var stored = clientFileStore.resolve(handle);
            return stored.map(ClientFileStore.StoredFile::content).orElse(null);
        }
        String s = firstString(block, CONTENT_KEYS);
        if (s == null || s.isBlank()) {
            return null;
        }
        s = unwrapDataUriOrSkipRemote(s, firstString(block, NAME_KEYS), block.get("source"));
        if (s == null || s.isBlank()) {
            return null;
        }
        if (ClientFileStore.isRef(s)) {
            return clientFileStore.resolve(s).map(ClientFileStore.StoredFile::content).orElse(null);
        }
        try {
            return Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException notBase64) {
            return s.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * The files a JSON envelope describes, or empty when {@code content} is not one.
     *
     * <p>Deliberately narrow: the bytes must parse as a JSON object/array AND describe at least one
     * file (a name or media type plus content). A real document that merely happens to be JSON has no
     * such shape and is left alone as the file it is.
     */
    private List<Map<String, Object>> envelopeBlocks(byte[] content) {
        return parseEnvelope(content).blocks();
    }

    private EnvelopeParse parseEnvelope(byte[] content) {
        if (content == null || content.length == 0 || content.length > MAX_ENVELOPE_BYTES) {
            return EnvelopeParse.empty();
        }
        int first = 0;
        while (first < content.length && Character.isWhitespace(content[first])) {
            first++;
        }
        if (first >= content.length || (content[first] != '{' && content[first] != '[')) {
            return EnvelopeParse.empty();
        }
        try {
            Object parsed = objectMapper.readValue(content, Object.class);
            List<Map<String, Object>> blocks = contentBlocksIn(parsed);
            List<Map<String, Object>> voices = voiceBlocksFromParsed(parsed);
            if (blocks.isEmpty() && voices.isEmpty()) {
                // Diagnosable decline: JSON-looking bytes with no file shape (genuine docs stay files).
                logEnvelopeDecline(parsed);
                return EnvelopeParse.empty();
            }
            return new EnvelopeParse(blocks, voices, envelopeMessageText(parsed));
        } catch (Exception notJson) {
            return EnvelopeParse.empty();
        }
    }

    /**
     * File blocks inside a parsed envelope: bare block, list, {@code files}/{@code output_files}
     * wrapper, or the same shapes nested under objects such as LLM {@code result} (bounded depth).
     */
    private static List<Map<String, Object>> contentBlocksIn(Object parsed) {
        return contentBlocksIn(parsed, 0);
    }

    private static List<Map<String, Object>> contentBlocksIn(Object parsed, int depth) {
        if (parsed == null || depth > MAX_CONTENT_SEARCH_DEPTH) {
            return List.of();
        }
        if (parsed instanceof List<?> list) {
            List<Map<String, Object>> blocks = new ArrayList<>();
            for (Object entry : list) {
                blocks.addAll(contentBlocksIn(entry, depth));
            }
            return blocks;
        }
        if (!(parsed instanceof Map<?, ?> map)) {
            return List.of();
        }
        List<Map<String, Object>> fromKeys = new ArrayList<>();
        for (String key : FILE_KEYS) {
            Object wrapped = map.get(key);
            if (wrapped != null) {
                fromKeys.addAll(contentBlocksIn(wrapped, depth + 1));
            }
        }
        if (!fromKeys.isEmpty()) {
            return fromKeys;
        }
        if (carriesFile(map)) {
            Map<String, Object> normalized = normalizeFileBlock(map);
            return normalized == null ? List.of() : List.of(normalized);
        }
        // Nested producers (LLM {@code result}) keep files off the top level — walk object values only.
        // Skip `voices` so audio on that port is never stolen into the files list.
        List<Map<String, Object>> nested = new ArrayList<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (VOICE_KEYS.contains(String.valueOf(entry.getKey()))) {
                continue;
            }
            Object value = entry.getValue();
            if (value instanceof Map || value instanceof List) {
                nested.addAll(contentBlocksIn(value, depth + 1));
            }
        }
        return nested;
    }

    /**
     * Voice blocks inside a parsed envelope: the {@code voices} key only (bare block, handle, list,
     * or the same shapes nested under objects such as LLM {@code result}). Never reads {@code files}.
     */
    private static List<Map<String, Object>> voiceBlocksFromParsed(Object parsed) {
        return voiceBlocksFromParsed(parsed, 0);
    }

    private static List<Map<String, Object>> voiceBlocksFromParsed(Object parsed, int depth) {
        if (parsed == null || depth > MAX_CONTENT_SEARCH_DEPTH) {
            return List.of();
        }
        if (parsed instanceof List<?> list) {
            List<Map<String, Object>> blocks = new ArrayList<>();
            for (Object entry : list) {
                blocks.addAll(voiceBlocksFromParsed(entry, depth));
            }
            return blocks;
        }
        if (!(parsed instanceof Map<?, ?> map)) {
            return List.of();
        }
        List<Map<String, Object>> fromKeys = blocksUnderPortKeys(map, VOICE_KEYS);
        if (!fromKeys.isEmpty()) {
            return fromKeys;
        }
        List<Map<String, Object>> nested = new ArrayList<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Do not steal audio that arrived on the files port.
            if (FILE_KEYS.contains(String.valueOf(entry.getKey()))) {
                continue;
            }
            Object value = entry.getValue();
            if (value instanceof Map || value instanceof List) {
                nested.addAll(voiceBlocksFromParsed(value, depth + 1));
            }
        }
        return nested;
    }

    /**
     * A file needs payload AND a file-identifying field. Outer LLM envelopes carry a string
     * {@code content} (assistant text) plus {@code output_files} — that string must not count as
     * file bytes without name/mime/kind.
     */
    private static boolean carriesFile(Map<?, ?> map) {
        boolean hasPayload = ClientFileStore.isRef(map.get("fileId"))
                || StringUtils.isNotBlank(firstString(map, CONTENT_KEYS));
        if (!hasPayload) {
            return false;
        }
        return firstString(map, NAME_KEYS) != null
                || firstString(map, MIME_KEYS) != null
                || fileKindValue(map) != null;
    }

    private static Map<String, Object> handleBlock(String handle) {
        Map<String, Object> block = new java.util.LinkedHashMap<>();
        block.put("fileId", handle);
        block.put("source", "sq-file");
        block.put("name", "attachment");
        block.put("mime_type", "application/octet-stream");
        return block;
    }

    /** True when the map itself is one ContentBlock (fileId/data), not an end-input envelope. */
    private static boolean looksLikeContentBlock(Map<?, ?> map) {
        if (map.containsKey("msg") || containsAnyKey(map, FILE_KEYS) || containsAnyKey(map, VOICE_KEYS)) {
            return false;
        }
        return ClientFileStore.isRef(map.get("fileId")) || carriesFile(map);
    }

    private static boolean containsAnyKey(Map<?, ?> map, Set<String> keys) {
        for (String key : keys) {
            if (map.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    private record EndAnswer(String message,
                             List<ConversationOutputCallback.OutputFile> files,
                             List<ConversationOutputCallback.OutputFile> voices,
                             List<ConversationFileRefDto> localRefs) {
        static EndAnswer empty() {
            return new EndAnswer("", List.of(), List.of(), null);
        }
    }

    /** Files and voices found in a parsed envelope, plus optional assistant text from the same tree. */
    private record EnvelopeParse(List<Map<String, Object>> blocks,
                                 List<Map<String, Object>> voices,
                                 String message) {
        static EnvelopeParse empty() {
            return new EnvelopeParse(List.of(), List.of(), null);
        }
    }

    /** Result of unwrapping spilled/nested envelopes while collecting assistant text and voices. */
    private record EnvelopeExpansion(List<Map<String, Object>> blocks,
                                    List<Map<String, Object>> voices,
                                    String envelopeMessage) {
    }

    /**
     * The answer text on the end node's input.
     *
     * <p>By the time the end node runs, its input has already been through the inbound transform, so the
     * canonical key is the end node's own {@code msg} attribute. When the author mapped onto some other
     * name, the single remaining scalar on the input is used rather than stringifying the whole map onto
     * the user's screen — a generic fallback, so no upstream node type's attribute names are baked in.
     * Two or more candidate scalars stay ambiguous and yield nothing, because guessing which one is the
     * answer would put arbitrary internal state in front of the user.
     */
    private static String answerText(Map<?, ?> endInput) {
        for (String key : ANSWER_TEXT_KEYS) {
            Object value = endInput.get(key);
            if (value != null && StringUtils.isNotBlank(String.valueOf(value))) {
                return String.valueOf(value);
            }
        }
        String only = null;
        for (Map.Entry<?, ?> entry : endInput.entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (FILE_KEYS.contains(key) || VOICE_KEYS.contains(key) || entry.getValue() == null
                    || entry.getValue() instanceof Map || entry.getValue() instanceof List) {
                continue;
            }
            if (only != null) {
                return ""; // ambiguous — never guess which of several scalars is the answer
            }
            only = String.valueOf(entry.getValue());
        }
        return only == null ? "" : only;
    }

    /**
     * ContentBlocks under the end node's {@code files} / {@code output_files} keys.
     */
    private static List<Map<String, Object>> fileBlocks(Map<?, ?> endInput) {
        return blocksUnderPortKeys(endInput, FILE_KEYS);
    }

    /**
     * ContentBlocks under the end node's {@code voices} key. Same shapes as {@link #fileBlocks}:
     * bare block, {@code sq-file:} string, list, or a JSON envelope map. Never reads {@code files}.
     */
    private static List<Map<String, Object>> voiceBlocks(Map<?, ?> endInput) {
        return blocksUnderPortKeys(endInput, VOICE_KEYS);
    }

    /**
     * Shared extractor for an end-node port key: first matching key wins, accepting a list, a
     * single ContentBlock, or a bare {@code sq-file:} handle.
     */
    private static List<Map<String, Object>> blocksUnderPortKeys(Map<?, ?> endInput, Set<String> keys) {
        if (endInput == null) {
            return List.of();
        }
        for (String key : keys) {
            Object raw = endInput.get(key);
            if (raw == null) {
                continue;
            }
            // A single block or bare handle on the port is as common as a list.
            if (raw instanceof List<?> list) {
                return coerceBlocks(list);
            }
            return coerceBlocks(List.of(raw));
        }
        return List.of();
    }

    /**
     * Accept ContentBlock maps and bare {@code sq-file:} strings; ignore anything else.
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> coerceBlocks(List<?> rawBlocks) {
        if (rawBlocks == null || rawBlocks.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> blocks = new ArrayList<>();
        for (Object entry : rawBlocks) {
            if (entry instanceof Map<?, ?> map) {
                blocks.add((Map<String, Object>) map);
            } else if (ClientFileStore.isRef(entry)) {
                blocks.add(handleBlock(String.valueOf(entry)));
            }
        }
        return blocks;
    }

    /**
 * Byte-carrying files for the control plane. Resolves {@code sq-file:} handles (in {@code fileId} or
 * mistakenly in {@code data}) so the chat never receives an unopenable client reference.
 */
    private List<ConversationOutputCallback.OutputFile> outputFilesFromBlocks(
            List<Map<String, Object>> blocks) {
        if (blocks.isEmpty()) {
            return List.of();
        }
        List<ConversationOutputCallback.OutputFile> out = new ArrayList<>();
        for (Map<String, Object> block : blocks) {
            Map<String, Object> normalized = maybeNormalizeFileBlock(block);
            if (normalized == null) {
                continue;
            }
            String mime = StringUtils.defaultIfBlank(firstString(normalized, MIME_KEYS),
                    "application/octet-stream");
            String name = StringUtils.defaultIfBlank(firstString(normalized, NAME_KEYS),
                    synthesizeFileName(mime));
            name = reconcileNameExtension(name, mime);
            // Honour an explicit kind from the producer; otherwise infer after mime is final
            // (resolving a handle often upgrades application/octet-stream → image/png).
            String kindHint = StringUtils.defaultIfBlank(fileKindValue(normalized), str(normalized.get("kind")));
            String data = firstString(normalized, CONTENT_KEYS);
            // data holding an sq-file: handle is a common wire mistake — resolve it, never base64-decode the prefix.
            Object handle = ClientFileStore.isRef(data) ? data : normalized.get("fileId");
            if (ClientFileStore.isRef(data) || StringUtils.isBlank(data)) {
                if (!ClientFileStore.isRef(handle)) {
                    log.warn("duckbot.end: output file {} has neither inline data nor a client handle; "
                            + "dropping it from the answer", name);
                    continue;
                }
                var stored = clientFileStore.resolve(handle);
                if (stored.isEmpty()) {
                    log.warn("duckbot.end: output file handle {} could not be resolved; dropping it "
                            + "from the answer", handle);
                    continue;
                }
                data = Base64.getEncoder().encodeToString(stored.get().content());
                name = StringUtils.defaultIfBlank(stored.get().fileName(), name);
                mime = StringUtils.defaultIfBlank(stored.get().mimeType(), mime);
                name = reconcileNameExtension(name, mime);
            }
            if (data.length() > MAX_ANSWER_FILE_BASE64_CHARS) {
                log.warn("duckbot.end: output file {} is {} base64 chars, above the {} char callback "
                                + "ceiling; the answer is delivered without it",
                        name, data.length(), MAX_ANSWER_FILE_BASE64_CHARS);
                continue;
            }
            String kind = StringUtils.defaultIfBlank(kindHint, inferContentKind(mime));
            out.add(new ConversationOutputCallback.OutputFile(name, mime, kind, data));
        }
        return out;
    }

    /**
 * Handle-only refs for the daemon's own conversation row until the control plane returns file store ids.
 */
    private List<ConversationFileRefDto> localRefsFromBlocks(List<Map<String, Object>> blocks) {
        if (blocks.isEmpty()) {
            return null;
        }
        List<ConversationFileRefDto> refs = new ArrayList<>();
        for (Map<String, Object> block : blocks) {
            Map<String, Object> normalized = maybeNormalizeFileBlock(block);
            if (normalized == null) {
                continue;
            }
            Object handle = normalized.get("fileId");
            if (!ClientFileStore.isRef(handle)) {
                String data = firstString(normalized, CONTENT_KEYS);
                if (ClientFileStore.isRef(data)) {
                    handle = data;
                }
            }
            if (!ClientFileStore.isRef(handle)) {
                continue;
            }
            String mime = StringUtils.defaultIfBlank(firstString(normalized, MIME_KEYS),
                    "application/octet-stream");
            String name = StringUtils.defaultIfBlank(firstString(normalized, NAME_KEYS),
                    synthesizeFileName(mime));
            ConversationFileRefDto ref = new ConversationFileRefDto();
            ref.setFileId(String.valueOf(handle));
            ref.setName(reconcileNameExtension(name, mime));
            ref.setMimeType(mime);
            ref.setKind(StringUtils.defaultIfBlank(
                    StringUtils.defaultIfBlank(fileKindValue(normalized), str(normalized.get("kind"))),
                    inferContentKind(mime)));
            refs.add(ref);
        }
        return refs.isEmpty() ? null : refs;
    }

    /**
     * Canonicalise a file-shaped map once ({@code name}/{@code mime_type}/{@code data|fileId}/{@code kind})
     * so readers stay simple. Returns null when the only payload is an unreadable remote URL.
     */
    private static Map<String, Object> maybeNormalizeFileBlock(Map<String, Object> block) {
        if (block == null) {
            return null;
        }
        if (!carriesFile(block) && !ClientFileStore.isRef(block.get("fileId"))) {
            return block;
        }
        return normalizeFileBlock(block);
    }

    private static Map<String, Object> normalizeFileBlock(Map<?, ?> map) {
        String mime = firstString(map, MIME_KEYS);
        String kind = fileKindValue(map);
        if (kind == null) {
            kind = str(map.get("kind")); // preserve producer kind even outside image|audio|video|document
        }
        // Do not infer kind from the placeholder octet-stream — handle resolve often upgrades
        // mime (e.g. → image/png) and outputFilesFromBlocks infers kind after mime is final.
        if (kind == null && mime != null && !isGenericOctetStream(mime)) {
            kind = inferContentKind(mime);
        }
        if (mime == null) {
            mime = "application/octet-stream";
        }
        String name = firstString(map, NAME_KEYS);
        if (name == null) {
            name = synthesizeFileName(mime);
        } else {
            name = reconcileNameExtension(name, mime);
        }
        Object fileId = map.get("fileId");
        String payload = firstString(map, CONTENT_KEYS);
        if (payload != null) {
            payload = unwrapDataUriOrSkipRemote(payload, name, map.get("source"));
        }
        if (!ClientFileStore.isRef(fileId) && (payload == null || payload.isBlank())) {
            return null;
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("name", name);
        out.put("mime_type", mime);
        if (kind != null) {
            out.put("kind", kind);
        }
        if (ClientFileStore.isRef(fileId)) {
            out.put("fileId", fileId);
            out.put("source", "sq-file");
        } else if (ClientFileStore.isRef(payload)) {
            out.put("fileId", payload);
            out.put("source", "sq-file");
        } else {
            out.put("data", payload);
            Object source = map.get("source");
            if (source != null) {
                out.put("source", source);
            }
        }
        return out;
    }

    /**
     * Assistant message text embedded in an envelope (string {@code content} or content-array text
     * blocks). Skips file-shaped maps so base64 under a {@code content} alias is never pasted into chat.
     */
    private static String envelopeMessageText(Object parsed) {
        return envelopeMessageText(parsed, 0);
    }

    private static String envelopeMessageText(Object parsed, int depth) {
        if (parsed == null || depth > MAX_CONTENT_SEARCH_DEPTH) {
            return null;
        }
        if (parsed instanceof List<?> list) {
            for (Object entry : list) {
                String text = envelopeMessageText(entry, depth);
                if (StringUtils.isNotBlank(text)) {
                    return text;
                }
            }
            return null;
        }
        if (!(parsed instanceof Map<?, ?> map)) {
            return null;
        }
        if (!carriesFile(map)) {
            String fromContent = textFromContentField(map.get("content"));
            if (StringUtils.isNotBlank(fromContent)) {
                return fromContent;
            }
        }
        for (Object value : map.values()) {
            if (value instanceof Map || value instanceof List) {
                String text = envelopeMessageText(value, depth + 1);
                if (StringUtils.isNotBlank(text)) {
                    return text;
                }
            }
        }
        return null;
    }

    private static String textFromContentField(Object content) {
        if (content instanceof String s && StringUtils.isNotBlank(s)) {
            return s;
        }
        if (!(content instanceof List<?> list)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (Object entry : list) {
            if (!(entry instanceof Map<?, ?> block)) {
                continue;
            }
            Object text = block.get("text");
            if (text instanceof String ts && StringUtils.isNotBlank(ts)) {
                if (sb.length() > 0) {
                    sb.append('\n');
                }
                sb.append(ts);
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /** INFO-only diagnostics: key names / sizes — never file bytes or document text. */
    private static void logEnvelopeDecline(Object parsed) {
        if (parsed instanceof Map<?, ?> map) {
            log.info("duckbot.end: declined JSON envelope — no file descriptors found; top-level keys={} "
                            + "searchDepth={} reason=no files/output_files entries or nested maps with "
                            + "name|mime|kind plus payload",
                    map.keySet(), MAX_CONTENT_SEARCH_DEPTH);
            return;
        }
        if (parsed instanceof List<?> list) {
            log.info("duckbot.end: declined JSON envelope — no file descriptors found; arrayLength={} "
                            + "searchDepth={} reason=entries lack name|mime|kind plus payload",
                    list.size(), MAX_CONTENT_SEARCH_DEPTH);
        }
    }

    private static String firstString(Map<?, ?> map, java.util.Set<String> keys) {
        if (map == null) {
            return null;
        }
        for (String key : keys) {
            Object value = map.get(key);
            if (value instanceof String s && StringUtils.isNotBlank(s)) {
                return s;
            }
        }
        return null;
    }

    /** True when the block's kind (or LLM type) is audio — the end-node voice fallback. */
    private static boolean isAudioFileKind(Map<?, ?> block) {
        if (block == null) {
            return false;
        }
        String kind = fileKindValue(block);
        if (kind == null) {
            kind = str(block.get("kind"));
        }
        return kind != null && "audio".equalsIgnoreCase(kind);
    }

    /** {@code kind}, or LLM {@code type} when it is image|audio|video|document (not text/tool_use). */
    private static String fileKindValue(Map<?, ?> map) {
        String kind = str(map.get("kind"));
        if (kind != null && FILE_KIND_VALUES.contains(kind.toLowerCase(java.util.Locale.ROOT))) {
            return kind.toLowerCase(java.util.Locale.ROOT);
        }
        String type = str(map.get("type"));
        if (type != null && FILE_KIND_VALUES.contains(type.toLowerCase(java.util.Locale.ROOT))) {
            return type.toLowerCase(java.util.Locale.ROOT);
        }
        return null;
    }

    /**
     * Conventional MIME → preferred file extension. Subtype alone is wrong for several common types
     * ({@code text/plain} → {@code txt}, not {@code plain}).
     */
    private static final Map<String, String> PREFERRED_EXTENSION_BY_MIME = Map.ofEntries(
            Map.entry("text/plain", "txt"),
            Map.entry("text/html", "html"),
            Map.entry("text/css", "css"),
            Map.entry("text/markdown", "md"),
            Map.entry("text/csv", "csv"),
            Map.entry("text/xml", "xml"),
            Map.entry("text/javascript", "js"),
            Map.entry("application/javascript", "js"),
            Map.entry("application/json", "json"),
            Map.entry("application/pdf", "pdf"),
            Map.entry("application/xml", "xml"),
            Map.entry("application/zip", "zip"),
            Map.entry("image/jpeg", "jpg"),
            Map.entry("audio/mpeg", "mp3"),
            Map.entry("video/mp4", "mp4")
    );

    /**
     * Well-known extension → media major type ({@code image}/{@code audio}/{@code video}/{@code text}/
     * specific application types). Used only to detect clearly incompatible pairs
     * (e.g. {@code .txt} on {@code image/png}).
     */
    private static final Map<String, String> EXTENSION_MEDIA_FAMILY = Map.ofEntries(
            Map.entry("txt", "text"),
            Map.entry("text", "text"),
            Map.entry("plain", "text"),
            Map.entry("html", "text"),
            Map.entry("htm", "text"),
            Map.entry("css", "text"),
            Map.entry("csv", "text"),
            Map.entry("md", "text"),
            Map.entry("markdown", "text"),
            Map.entry("png", "image"),
            Map.entry("jpg", "image"),
            Map.entry("jpeg", "image"),
            Map.entry("gif", "image"),
            Map.entry("webp", "image"),
            Map.entry("svg", "image"),
            Map.entry("bmp", "image"),
            Map.entry("tif", "image"),
            Map.entry("tiff", "image"),
            Map.entry("mp3", "audio"),
            Map.entry("wav", "audio"),
            Map.entry("ogg", "audio"),
            Map.entry("m4a", "audio"),
            Map.entry("aac", "audio"),
            Map.entry("flac", "audio"),
            Map.entry("mp4", "video"),
            Map.entry("webm", "video"),
            Map.entry("mov", "video"),
            Map.entry("avi", "video"),
            Map.entry("mkv", "video"),
            Map.entry("pdf", "application"),
            Map.entry("zip", "application"),
            Map.entry("json", "application"),
            Map.entry("xml", "application"),
            Map.entry("js", "application")
    );

    /** Aliases accepted for a preferred extension (plus jpeg↔jpg). */
    private static final Map<String, Set<String>> EXTENSION_ALIASES = Map.of(
            "jpg", Set.of("jpeg"),
            "jpeg", Set.of("jpg"),
            "txt", Set.of("text", "plain"),
            "htm", Set.of("html"),
            "html", Set.of("htm"),
            "md", Set.of("markdown"),
            "mp3", Set.of("mpeg")
    );

    /**
     * {@code image/png} → {@code image.png}. Uses the preferred conventional extension when known
     * so {@code text/plain} synthesises {@code text.txt}, not {@code text.plain}.
     */
    static String synthesizeFileName(String mimeType) {
        String mime = mimeType == null ? "" : mimeType.toLowerCase(Locale.ROOT).trim();
        int slash = mime.indexOf('/');
        if (slash <= 0 || slash >= mime.length() - 1) {
            return "attachment.bin";
        }
        String type = mime.substring(0, slash);
        String ext = preferredExtensionFromMime(mimeType);
        if (ext == null) {
            return type + ".bin";
        }
        String base = switch (type) {
            case "image", "audio", "video", "text", "application", "font" -> type;
            default -> "file";
        };
        return base + "." + ext;
    }

    /**
     * Ensure the name's extension does not contradict the media type. Keeps conventional matches
     * ({@code .txt} for {@code text/plain}); only rewrites when the extension is absent or clearly
     * incompatible (e.g. {@code .txt} on {@code image/png}).
     */
    static String reconcileNameExtension(String name, String mimeType) {
        if (name == null || name.isBlank() || mimeType == null || mimeType.isBlank()) {
            return name;
        }
        if (isGenericOctetStream(mimeType)) {
            return name; // unknown bytes — do not invent ".octet-stream"
        }
        String preferred = preferredExtensionFromMime(mimeType);
        if (preferred == null) {
            return name;
        }
        int dot = name.lastIndexOf('.');
        String base = dot > 0 ? name.substring(0, dot) : name;
        String current = dot > 0 ? name.substring(dot + 1) : "";
        if (current.isBlank()) {
            return base + "." + preferred;
        }
        if (extensionFitsMime(current, mimeType, preferred)) {
            return name;
        }
        return base + "." + preferred;
    }

    /** Preferred extension for a MIME type, or the media subtype when no convention is registered. */
    private static String preferredExtensionFromMime(String mimeType) {
        String mime = normalizeMime(mimeType);
        if (mime == null || isGenericOctetStream(mime)) {
            return null;
        }
        String preferred = PREFERRED_EXTENSION_BY_MIME.get(mime);
        if (preferred != null) {
            return preferred;
        }
        return extensionFromMime(mime);
    }

    private static boolean extensionFitsMime(String current, String mimeType, String preferred) {
        String ext = current.toLowerCase(Locale.ROOT);
        if (ext.equalsIgnoreCase(preferred)) {
            return true;
        }
        Set<String> aliases = EXTENSION_ALIASES.get(preferred.toLowerCase(Locale.ROOT));
        if (aliases != null && aliases.contains(ext)) {
            return true;
        }
        // Also accept the raw subtype (e.g. ".jpeg" when preferred is "jpg")
        String subtype = extensionFromMime(mimeType);
        if (subtype != null && ext.equalsIgnoreCase(subtype)) {
            return true;
        }
        String extFamily = EXTENSION_MEDIA_FAMILY.get(ext);
        if (extFamily == null) {
            // Unknown extension — not clearly incompatible; keep the producer's name.
            return true;
        }
        String mimeFamily = mediaFamily(mimeType);
        // Same family (text/*, image/*, …) → keep; different family → rewrite.
        return mimeFamily != null && mimeFamily.equals(extFamily);
    }

    private static String mediaFamily(String mimeType) {
        String mime = normalizeMime(mimeType);
        if (mime == null) {
            return null;
        }
        int slash = mime.indexOf('/');
        if (slash <= 0) {
            return null;
        }
        return mime.substring(0, slash);
    }

    private static boolean isGenericOctetStream(String mimeType) {
        String mime = normalizeMime(mimeType);
        return "application/octet-stream".equals(mime);
    }

    private static String normalizeMime(String mimeType) {
        if (mimeType == null || mimeType.isBlank()) {
            return null;
        }
        String mime = mimeType.toLowerCase(Locale.ROOT).trim();
        int semi = mime.indexOf(';');
        if (semi >= 0) {
            mime = mime.substring(0, semi).trim();
        }
        return mime.isBlank() ? null : mime;
    }

    private static String extensionFromMime(String mimeType) {
        String mime = normalizeMime(mimeType);
        if (mime == null) {
            return null;
        }
        int slash = mime.indexOf('/');
        if (slash < 0 || slash >= mime.length() - 1) {
            return null;
        }
        String sub = mime.substring(slash + 1);
        int plus = sub.indexOf('+');
        if (plus >= 0) {
            sub = sub.substring(0, plus);
        }
        if (sub.isBlank() || "*".equals(sub)) {
            return null;
        }
        return sub;
    }

    /**
     * {@code data:…;base64,…} → payload after the comma. Remote http(s) URLs cannot be fetched here —
     * skip with WARN (never log the URL's body; the URL itself is metadata).
     */
    private static String unwrapDataUriOrSkipRemote(String raw, String nameHint, Object source) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        if (trimmed.regionMatches(true, 0, "data:", 0, 5)) {
            int comma = trimmed.indexOf(',');
            if (comma < 0 || comma >= trimmed.length() - 1) {
                return null;
            }
            String meta = trimmed.substring(5, comma).toLowerCase(java.util.Locale.ROOT);
            if (!meta.contains(";base64")) {
                // Non-base64 data URIs are not file payloads we can attach.
                return null;
            }
            return trimmed.substring(comma + 1);
        }
        boolean urlSource = source != null && "url".equalsIgnoreCase(String.valueOf(source));
        if (urlSource || looksLikeHttpUrl(trimmed)) {
            log.warn("duckbot.end: skipping file block with remote URL source (name={})",
                    StringUtils.defaultIfBlank(nameHint, "attachment"));
            return null;
        }
        return raw;
    }

    private static boolean looksLikeHttpUrl(String value) {
        String v = value.toLowerCase(java.util.Locale.ROOT);
        return v.startsWith("http://") || v.startsWith("https://");
    }

    private static String str(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
