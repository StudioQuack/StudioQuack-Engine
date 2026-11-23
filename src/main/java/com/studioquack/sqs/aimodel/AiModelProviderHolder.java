package com.studioquack.sqs.aimodel;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The single, live source of truth for this daemon's AI-model provider connection. Seeded EMPTY at
 * boot; a pushed {@code set-ai-model} command swaps in the real config live (no restart).
 */
@Slf4j
@Component
public class AiModelProviderHolder {

    private final AtomicReference<AiModelProviderConfig> current =
            new AtomicReference<>(AiModelProviderConfig.EMPTY);

    /** The live provider config (never null; EMPTY until the BE pushes one). */
    public AiModelProviderConfig current() {
        return current.get();
    }

    /** Replace the live provider config with a BE-pushed one (null ignored). */
    public void apply(AiModelProviderConfig config) {
        if (config == null) {
            log.warn("ai-model: ignoring null provider config; keeping current");
            return;
        }
        current.set(config);
        // NEVER log the API key — only whether one is present.
        log.info("ai-model: applied provider config server='{}' hasKey={}",
                config.server(), config.apiKey() != null && !config.apiKey().isBlank());
    }
}
