package com.studioquack.sqs.execution.transform;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

/**
 * Default {@link HelperScriptProvider} for — returns no template for any language, so
 * {@link GenerateScriptService} dispatches the raw user script (the control plane's behaviour when
 * no helper was registered). replaces this with a real provider (backed by the copied
 * tier-store helper templates + helper channel) via {@code @ConditionalOnMissingBean}.
 */
@Configuration
public class EmptyHelperScriptProvider {

    @Bean
    @ConditionalOnMissingBean(HelperScriptProvider.class)
    public HelperScriptProvider defaultHelperScriptProvider() {
        return language -> Optional.empty();
    }
}
