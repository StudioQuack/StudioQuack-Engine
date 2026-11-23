package com.studioquack.sqs.engine.bpmn;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

/**
 * Converts an authored <b>FEEL</b> condition ({@code = expr}) into an Activiti <b>JUEL</b>
 * condition ({@code ${expr}}).
 *
 * <p>Only a route-token comparison (or {@code ${true}} / {@code ${false}}) is a legal
 * gateway condition. An already-{@code ${…}} string that reaches a Spring bean, a method
 * call, or any other identifier is rejected — Activiti evaluates sequence-flow JUEL in
 * the daemon JVM, so a free-text expression is a sandbox escape.
 *
 * <p>Conversions that remain (unit-tested):
 * <ul>
 *   <li>strips the leading FEEL {@code =} marker;</li>
 *   <li>rewrites FEEL single-{@code =} equality to JUEL {@code ==}, preserving the
 *       compound operators {@code ==}, {@code !=}, {@code <=}, {@code >=} — and never
 *       rewrites <em>inside</em> a single-quoted literal ({@code 'a=b'} stays as-is);</li>
 *   <li>wraps the result in {@code ${ ... }};</li>
 *   <li>passes through an already-{@code ${...}} string <em>only</em> when it matches
 *       {@link #isSafeRouteCompare(String)}.</li>
 * </ul>
 */
@Component
public class FeelToJuelTranslator {

    /**
     * Legal gateway JUEL: {@code ${false}}, {@code ${true}}, or
     * {@code ${route__ident == 'literal'}} / {@code !=}. No dots, parens, or other names.
     */
    private static final Pattern SAFE_ROUTE_COMPARE = Pattern.compile(
            "^\\$\\{(?:false|true|route__[A-Za-z][A-Za-z0-9_]*\\s*(?:==|!=)\\s*'(?:[^'\\\\]|\\\\')*')\\}$");

    /**
     * @param feel the authored condition (FEEL {@code = expr}, or already JUEL/plain)
     * @return a JUEL {@code ${...}} condition, or {@code null} when {@code feel} is blank
     *         (a bare {@code =} counts as blank — {@code ${}} is not valid JUEL)
     * @throws IllegalArgumentException for any non-blank condition outside the safe grammar.
     *         Dropping it instead would leave the edge unconditioned — always taken on an
     *         exclusive gateway — so an authoring mistake fails the deploy rather than
     *         silently choosing a branch.
     */
    public String translateCondition(String feel) {
        if (StringUtils.isBlank(feel)) {
            return null;
        }
        String expr = feel.trim();
        if (expr.startsWith("${") && expr.endsWith("}")) {
            return requireSafe(expr);
        }
        if (expr.startsWith("=")) {
            expr = expr.substring(1).trim();
        }
        if (expr.isEmpty()) {
            return null;
        }
        expr = rewriteLoneEqualsOutsideLiterals(expr);
        return requireSafe("${" + expr + "}");
    }

    /**
     * Rewrites FEEL lone-{@code =} equality to JUEL {@code ==} — but never inside a
     * single-quoted literal: a blanket regex would corrupt a route literal such as
     * {@code 'a=b'} into {@code 'a==b'} and the safe-grammar gate would then bless the
     * corrupted text (the literal character class admits {@code =}).
     */
    private static String rewriteLoneEqualsOutsideLiterals(String expr) {
        StringBuilder out = new StringBuilder(expr.length() + 8);
        boolean inLiteral = false;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (inLiteral) {
                out.append(c);
                if (c == '\\' && i + 1 < expr.length()) {
                    out.append(expr.charAt(++i)); // an escaped char (e.g. \') rides along
                } else if (c == '\'') {
                    inLiteral = false;
                }
                continue;
            }
            if (c == '\'') {
                inLiteral = true;
                out.append(c);
                continue;
            }
            if (c == '=' && isLoneEquals(expr, i)) {
                out.append("==");
                continue;
            }
            out.append(c);
        }
        return out.toString();
    }

    /** True when the {@code =} at {@code i} is NOT part of {@code ==}, {@code !=}, {@code <=}, {@code >=}. */
    private static boolean isLoneEquals(String expr, int i) {
        if (i + 1 < expr.length() && expr.charAt(i + 1) == '=') {
            return false; // first half of ==
        }
        if (i > 0) {
            char prev = expr.charAt(i - 1);
            return prev != '=' && prev != '!' && prev != '<' && prev != '>';
        }
        return true;
    }

    /** True when {@code juel} is the only grammar Activiti may evaluate on a sequence flow. */
    public static boolean isSafeRouteCompare(String juel) {
        return juel != null && SAFE_ROUTE_COMPARE.matcher(juel).matches();
    }

    private static String requireSafe(String juel) {
        if (isSafeRouteCompare(juel)) {
            return juel;
        }
        throw new IllegalArgumentException(
                "gateway condition must be a route__ comparison ('${route__<gateway> == '<target>'}'), "
                        + "${true} or ${false} — not an expression: " + abbreviate(juel));
    }

    private static String abbreviate(String value) {
        return value.length() <= 80 ? value : value.substring(0, 80) + "…";
    }
}
