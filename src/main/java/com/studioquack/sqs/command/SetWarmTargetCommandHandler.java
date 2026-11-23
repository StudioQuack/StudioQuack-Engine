package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.SandboxLanguage;
import com.studioquack.sqs.sandbox.warmpool.WarmPool;
import com.studioquack.sqs.sandbox.warmpool.WarmTargetEntry;
import com.studioquack.sqs.sandbox.warmpool.WarmTargetStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * Server-pushed warm pool setting. Accepts both shapes: Image-keyed
 * {@code {"imageRef":"python:3.11-slim","language":"python","target":4, "caps":{"cpu":1.0,"ramMb":512,"diskMb":256}}}
 * — {@code language} and {@code caps} optional.
 */
@Slf4j
@Component
public class SetWarmTargetCommandHandler implements DaemonCommandHandler {

    public static final String TYPE = "set-warm-target";
    private static final int MAX_TARGET = 64;
    private static final String PERSISTENCE =
            "persisted: targets are restored when the daemon restarts";

    private final ObjectMapper objectMapper;
    private final WarmPool warmPool;
    private final WarmTargetStore warmTargetStore;
    private final Environment environment;
    /**
     * Host GPU probe; a {@code caps.gpu} on a host without passthrough is refused HERE (at command time) so the
     * bad cap is never persisted and never reaches a warm create. Nullable: slice tests that never touch gpu pass
     * {@code null} (refuse-if-requested stays fail-closed).
     */
    private final com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe gpuProbe;

    @org.springframework.beans.factory.annotation.Autowired
    public SetWarmTargetCommandHandler(ObjectMapper objectMapper, WarmPool warmPool,
                                       WarmTargetStore warmTargetStore, Environment environment,
                                       com.studioquack.sqs.sandbox.oci.GpuCapabilityProbe gpuProbe) {
        this.objectMapper = objectMapper;
        this.warmPool = warmPool;
        this.warmTargetStore = warmTargetStore;
        this.environment = environment;
        this.gpuProbe = gpuProbe;
    }

    /**
     * Back-compat ctor (tests without the GPU probe): a gpu cap is refused as unknown-capable.
     */
    public SetWarmTargetCommandHandler(ObjectMapper objectMapper, WarmPool warmPool,
                                       WarmTargetStore warmTargetStore, Environment environment) {
        this(objectMapper, warmPool, warmTargetStore, environment, null);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            String payload = command == null ? null : command.payload();
            if (payload == null || payload.isBlank()) {
                return DaemonCommandOutcome.failed("set-warm-target: empty payload");
            }
            JsonNode root = objectMapper.readTree(payload);
            String imageRef = firstText(root, "imageRef", "image", "ref");
            String language = normalise(firstText(root, "language", "lang"));
            if (language.isBlank()) {
                language = null;
            }

            if (imageRef.isBlank()) {
                // LEGACY path: language required → resolve to that language's effective default image.
                if (language == null) {
                    return DaemonCommandOutcome.failed("set-warm-target: language is required");
                }
                SandboxLanguage lang = SandboxLanguage.fromWire(language);
                if (lang == null) {
                    return DaemonCommandOutcome.failed(
                            "set-warm-target: unknown language '" + language
                                    + "' — pass imageRef explicitly");
                }
                String prop = environment.getProperty("sqs.sandbox.image." + lang.wire());
                imageRef = (prop != null && !prop.isBlank()) ? prop.trim() : lang.defaultImage();
                if (imageRef == null || imageRef.isBlank()) {
                    return DaemonCommandOutcome.failed(
                            "set-warm-target: language '" + language
                                    + "' has no default image — pass imageRef explicitly");
                }
            }

            if (!root.hasNonNull("target")) {
                return DaemonCommandOutcome.failed("set-warm-target: target is required");
            }
            int requested = root.get("target").asInt();
            int target = Math.max(0, Math.min(MAX_TARGET, requested));

            CapsParse capsParse = parseCaps(root.get("caps"));
            if (capsParse.error != null) {
                return DaemonCommandOutcome.failed(capsParse.error);
            }
            // Refuse a gpu cap this host cannot honour BEFORE persisting anything; the
            // same cap would be refused again at warm-create time (OciWarmer) as defence in depth.
            if (capsParse.caps != null && capsParse.caps.gpu() != null && capsParse.caps.gpu() > 0
                    && (gpuProbe == null || !gpuProbe.gpuPassthroughAvailable())) {
                return DaemonCommandOutcome.failed("set-warm-target: caps.gpu requested but this "
                        + "host reports no GPU passthrough — the cap is refused rather than attempted");
            }

            warmPool.setTarget(imageRef, language, target, capsParse.caps);
            WarmPool.ReconcileReport reconcile = warmPool.reconcile();
            warmTargetStore.save(warmPool.entries());
            log.info("set-warm-target applied image={} language={} target={} requested={} caps={} "
                            + "refills={} errors={} (persisted)",
                    imageRef, language, target, requested, capsParse.caps,
                    reconcile.refills(), reconcile.errors());

            ObjectNode ack = objectMapper.createObjectNode();
            ack.put("imageRef", imageRef);
            if (language == null) {
                ack.putNull("language");
            } else {
                ack.put("language", language);
            }
            ack.put("target", target);
            ack.put("requestedTarget", requested);
            if (capsParse.caps == null) {
                ack.putNull("caps");
            } else {
                ack.set("caps", objectMapper.valueToTree(capsParse.caps));
            }
            ack.put("persisted", true);
            ack.put("persistence", PERSISTENCE);
            ack.set("reconcile", objectMapper.valueToTree(reconcile));
            ObjectNode pool = ack.putObject("warmPool");
            pool.put("warmerKind", warmPool.warmerKind());
            pool.set("languages", objectMapper.valueToTree(warmPool.status()));
            pool.put("persistence", PERSISTENCE);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(ack));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("set-warm-target failed: " + message(e));
        }
    }

    /**
     * Optional caps object: each present field must be positive. All-absent / null node → null (uncapped).
     */
    private static CapsParse parseCaps(JsonNode capsNode) {
        if (capsNode == null || capsNode.isNull() || !capsNode.isObject()) {
            return CapsParse.ok(null);
        }
        Double cpu = null;
        Integer ramMb = null;
        Integer diskMb = null;
        Integer gpu = null;
        if (capsNode.has("cpu") && !capsNode.get("cpu").isNull()) {
            if (!capsNode.get("cpu").isNumber() || capsNode.get("cpu").asDouble() <= 0) {
                return CapsParse.err("set-warm-target: caps.cpu must be positive");
            }
            cpu = capsNode.get("cpu").asDouble();
        }
        if (capsNode.has("ramMb") && !capsNode.get("ramMb").isNull()) {
            if (!capsNode.get("ramMb").isNumber() || capsNode.get("ramMb").asInt() <= 0) {
                return CapsParse.err("set-warm-target: caps.ramMb must be positive");
            }
            ramMb = capsNode.get("ramMb").asInt();
        }
        if (capsNode.has("diskMb") && !capsNode.get("diskMb").isNull()) {
            if (!capsNode.get("diskMb").isNumber() || capsNode.get("diskMb").asInt() <= 0) {
                return CapsParse.err("set-warm-target: caps.diskMb must be positive");
            }
            diskMb = capsNode.get("diskMb").asInt();
        }
        if (capsNode.has("gpu") && !capsNode.get("gpu").isNull()) {
            if (!capsNode.get("gpu").isNumber() || capsNode.get("gpu").asInt() <= 0) {
                return CapsParse.err("set-warm-target: caps.gpu must be positive");
            }
            gpu = capsNode.get("gpu").asInt();
        }
        WarmTargetEntry.Caps caps = new WarmTargetEntry.Caps(cpu, ramMb, diskMb, gpu);
        return CapsParse.ok(caps.isEmpty() ? null : caps);
    }

    private record CapsParse(WarmTargetEntry.Caps caps, String error) {
        static CapsParse ok(WarmTargetEntry.Caps caps) {
            return new CapsParse(caps, null);
        }

        static CapsParse err(String error) {
            return new CapsParse(null, error);
        }
    }

    private static String firstText(JsonNode node, String... names) {
        for (String name : names) {
            JsonNode value = node.get(name);
            if (value != null && value.isTextual() && !value.asText().isBlank()) {
                return value.asText().trim();
            }
        }
        return "";
    }

    private static String normalise(String language) {
        return language == null ? "" : language.trim().toLowerCase(Locale.ROOT);
    }

    private static String message(Throwable t) {
        return t.getMessage() == null ? t.toString() : t.getMessage();
    }
}
