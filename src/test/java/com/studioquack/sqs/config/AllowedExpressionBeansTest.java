package com.studioquack.sqs.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The JUEL bean scope must stay an allowlist and must not resolve any bean while the engine is
 * being configured — an eager lookup pulls the delegate chain back into the half-built {@code
 * processEngine} and the daemon never starts.
 */
class AllowedExpressionBeansTest {

    @Test
    void constructionAndContainsKeyNeverInstantiateBeans() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        when(ctx.containsBean(anyString())).thenReturn(true);

        var map = new SqsActivitiEngineConfig.AllowedExpressionBeans(ctx);
        assertThat(map.containsKey("scriptDispatchDelegate")).isTrue();

        verify(ctx, never()).getBean(anyString());
    }

    @Test
    void onlyAllowlistedNamesResolve_evenWhenTheBeanExists() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        when(ctx.containsBean(anyString())).thenReturn(true);
        Object delegate = new Object();
        when(ctx.getBean("scriptDispatchDelegate")).thenReturn(delegate);

        var map = new SqsActivitiEngineConfig.AllowedExpressionBeans(ctx);

        assertThat(map.get("scriptDispatchDelegate")).isSameAs(delegate);
        assertThat(map.containsKey("channelProperties")).isFalse();
        assertThat(map.get("channelProperties")).isNull();
        assertThat(map.containsKey("osSecretManager")).isFalse();
        assertThat(map.containsKey(42)).isFalse();
        verify(ctx, never()).getBean("channelProperties");
    }

    @Test
    void allowlistedNameWithoutABeanIsAbsent() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        when(ctx.containsBean("doclingDelegate")).thenReturn(false);

        var map = new SqsActivitiEngineConfig.AllowedExpressionBeans(ctx);
        assertThat(map.containsKey("doclingDelegate")).isFalse();
        assertThat(map.get("doclingDelegate")).isNull();
    }
}
