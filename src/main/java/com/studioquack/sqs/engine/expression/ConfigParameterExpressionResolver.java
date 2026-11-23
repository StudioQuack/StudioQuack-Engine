package com.studioquack.sqs.engine.expression;

import com.studioquack.common.dto.flow.ParameterDto;
import com.studioquack.common.dto.flow.TemplateDto;
import com.studioquack.sqs.execution.context.ExecutionContext;

import java.util.List;

/**
 * dispatch input-prep: resolve {@code $(…)} references inside the current node's
 * <em>configuration-parameter string values</em> before secrets materialisation.
 *
 * <p>Mutates the in-memory {@code currentNode} snapshot only. Leaves {@code $secretRef}
 * placeholders untouched ({@link ExpressionResolver#substituteInString}). Does not touch
 * {@code prepared.inputVariables} — config params are not copied into that port map.
 */
public final class ConfigParameterExpressionResolver {

    private ConfigParameterExpressionResolver() {
    }

    /**
     * Walks {@code ctx.currentNode.data.configurationParameters} and substitutes any
     * String value that contains {@code $(}. Non-string values (e.g. secret object
     * wrappers) are left unchanged.
     */
    public static void resolveInCurrentNode(ExecutionContext ctx, ExpressionResolver expressionResolver) {
        if (ctx == null || expressionResolver == null || ctx.getCurrentNode() == null) {
            return;
        }
        TemplateDto data = ctx.getCurrentNode().getData();
        if (data == null) {
            return;
        }
        List<ParameterDto> params = data.getConfigurationParameters();
        if (params == null || params.isEmpty()) {
            return;
        }
        for (ParameterDto param : params) {
            if (param == null) {
                continue;
            }
            Object value = param.getValue();
            // Only plain strings carry $(…) authored refs; $secretRef object maps stay intact.
            if (value instanceof String s && s.contains("$(")) {
                param.setValue(expressionResolver.substituteInString(s, ctx));
            }
        }
    }
}
