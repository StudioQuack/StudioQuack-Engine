package com.studioquack.sqs.engine.expression;

import com.studioquack.common.dto.flow.NodeDto;
import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Config-parameter {@code $(…)} substitution before secrets (dispatch input-prep). */
@ExtendWith(MockitoExtension.class)
class ConfigParameterExpressionResolverTest {

    @Mock
    private ExecutionContextStore contextStore;

    private ExpressionResolver resolver;
    private ExecutionContext ctx;
    private ParameterDto urlParam;
    private ParameterDto secretParam;

    @BeforeEach
    void setUp() {
        resolver = new ExpressionResolver(contextStore);
        ctx = new ExecutionContext();
        ctx.setWorkflowExecutionId("wfe-1");

        Map<String, Object> wf = new HashMap<>();
        wf.put("host", "api.example");
        ctx.setWorkflowVariables(wf);

        urlParam = new ParameterDto();
        urlParam.setShortCode("baseUrl");
        urlParam.setValue("https://$(wf.host)/v1");

        // Compact secret string — must remain untouched beside a $(…) neighbour.
        secretParam = new ParameterDto();
        secretParam.setShortCode("token");
        secretParam.setValue("$secretRef:sec_abcdef0123456789abcdef01:v1");

        TemplateDto data = new TemplateDto();
        data.setConfigurationParameters(new ArrayList<>(List.of(urlParam, secretParam)));

        NodeDto current = new NodeDto();
        current.setId("node-a");
        current.setData(data);
        ctx.setCurrentNode(current);
    }

    @Test
    void resolveInCurrentNode_substitutes_string_config_params() {
        ConfigParameterExpressionResolver.resolveInCurrentNode(ctx, resolver);
        assertEquals("https://api.example/v1", urlParam.getValue());
        assertEquals("$secretRef:sec_abcdef0123456789abcdef01:v1", secretParam.getValue());
    }

    @Test
    void resolveInCurrentNode_leaves_secret_object_wrapper_untouched() {
        Map<String, Object> wrapper = Map.of("$secretRef", "sec_abcdef0123456789abcdef01:v1");
        secretParam.setValue(wrapper);
        ConfigParameterExpressionResolver.resolveInCurrentNode(ctx, resolver);
        assertSame(wrapper, secretParam.getValue());
        assertEquals("https://api.example/v1", urlParam.getValue());
    }

    @Test
    void resolveInCurrentNode_mixed_string_keeps_secretRef_prefix() {
        urlParam.setValue("k=$secretRef:sec_abcdef0123456789abcdef01:v1 h=$(wf.host)");
        ConfigParameterExpressionResolver.resolveInCurrentNode(ctx, resolver);
        assertEquals("k=$secretRef:sec_abcdef0123456789abcdef01:v1 h=api.example", urlParam.getValue());
    }
}
