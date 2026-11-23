package com.studioquack.sqs.engine.strategy.helper;

import org.activiti.bpmn.model.TimerEventDefinition;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Shared {@link TimerEventDefinition} factory for the timer start / intermediate-catch
 * strategies AND the single owner of the timer-value grammar every timer path (node-level
 * and boundary) must pass through. Maps the authored {@code type} ∈ {@code date|duration|cycle}
 * + {@code value} onto the matching Activiti expression — and REFUSES anything else.
 *
 * <p><b>Bounds.</b> A node-level timer is the same scheduler load as a boundary timer: a
 * {@code cycle} re-arms itself (and a timer START spawns a new instance per fire), so the
 * cycle is normalised to a bounded, rate-limited repeat ({@link #boundedTimerCycle}) and cron
 * / free-form values fail the deploy. Before this bounding existed here, {@code PT1S} deployed
 * as {@code R/PT1S} — an unbounded instance storm that survives daemon restarts (audit K1).
 */
public final class ActivitiTimerDefinitionFactory {

    /** Repeat count a timer cycle is clamped to; an omitted count ({@code R/…}) becomes this. */
    public static final int MAX_TIMER_CYCLE_REPEATS = 10;

    /** Floor on a timer cycle's interval — a cycle re-arms itself, so a short one compounds. */
    public static final Duration MIN_TIMER_CYCLE_INTERVAL = Duration.ofSeconds(60);

    /**
     * The only accepted cycle grammar: an ISO-8601 repeating interval {@code R[n]/<ISO duration>}.
     * Cron and free-form expressions are refused because they cannot be bounded by rewriting.
     */
    private static final Pattern REPEATING_INTERVAL =
            Pattern.compile("^R(\\d*)/(P.+)$", Pattern.CASE_INSENSITIVE);

    private ActivitiTimerDefinitionFactory() {
    }

    public static TimerEventDefinition build(String nodeId, String type, String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("timer node '" + nodeId
                    + "' has a blank 'value' — a timer cannot be armed without an expression");
        }
        TimerEventDefinition timer = new TimerEventDefinition();
        switch (type == null ? "" : type.trim().toLowerCase(Locale.ROOT)) {
            case "cycle" -> {
                String bounded = boundedTimerCycle(normalizeCycle(value));
                if (bounded == null) {
                    throw new IllegalArgumentException("timer node '" + nodeId
                            + "' has an unboundable cycle '" + value
                            + "' — expected R[n]/<ISO-8601 duration>; cron is not supported");
                }
                timer.setTimeCycle(bounded);
            }
            case "date" -> {
                if (!isIsoDateTime(value)) {
                    throw new IllegalArgumentException("timer node '" + nodeId
                            + "' has an invalid date '" + value + "' — expected an ISO-8601 instant");
                }
                timer.setTimeDate(value);
            }
            case "duration" -> {
                if (!isPositiveIsoDuration(value)) {
                    throw new IllegalArgumentException("timer node '" + nodeId
                            + "' has an invalid duration '" + value
                            + "' — expected a positive ISO-8601 duration (e.g. PT5M)");
                }
                timer.setTimeDuration(value);
            }
            default -> throw new IllegalArgumentException("timer node '" + nodeId
                    + "' has unsupported type '" + type + "' (expected date|duration|cycle)");
        }
        return timer;
    }

    /**
     * Rewrites a user-authored timer cycle into one the engine cannot turn into a job storm: the
     * repeat count is capped at {@link #MAX_TIMER_CYCLE_REPEATS} (an omitted count means "forever",
     * so it becomes the cap) and the interval is raised to {@link #MIN_TIMER_CYCLE_INTERVAL}.
     *
     * @return the bounded cycle, or {@code null} when {@code raw} is not a repeating ISO interval
     */
    public static String boundedTimerCycle(String raw) {
        Matcher matcher = REPEATING_INTERVAL.matcher(StringUtils.trimToEmpty(raw));
        if (!matcher.matches()) {
            return null;
        }
        Duration interval;
        try {
            interval = Duration.parse(matcher.group(2));
        } catch (DateTimeParseException ex) {
            return null;
        }
        if (interval.isZero() || interval.isNegative()) {
            return null;
        }
        if (interval.compareTo(MIN_TIMER_CYCLE_INTERVAL) < 0) {
            interval = MIN_TIMER_CYCLE_INTERVAL;
        }
        long repeats = MAX_TIMER_CYCLE_REPEATS;
        String declared = matcher.group(1);
        if (StringUtils.isNotBlank(declared)) {
            try {
                repeats = Math.clamp(Long.parseLong(declared), 1, MAX_TIMER_CYCLE_REPEATS);
            } catch (NumberFormatException ex) {
                repeats = MAX_TIMER_CYCLE_REPEATS;
            }
        }
        return "R" + repeats + "/" + interval;
    }

    public static boolean isPositiveIsoDuration(String value) {
        try {
            Duration duration = Duration.parse(StringUtils.trimToEmpty(value));
            return !duration.isZero() && !duration.isNegative();
        } catch (DateTimeParseException ex) {
            return false;
        }
    }

    public static boolean isIsoDateTime(String value) {
        try {
            OffsetDateTime.parse(StringUtils.trimToEmpty(value));
            return true;
        } catch (DateTimeParseException ex) {
            try {
                LocalDateTime.parse(StringUtils.trimToEmpty(value));
                return true;
            } catch (DateTimeParseException ignored) {
                return false;
            }
        }
    }

    /** Bare ISO-8601 period ({@code PT5M}) → repeating form ({@code R/PT5M}). */
    private static String normalizeCycle(String value) {
        if (value != null && !value.startsWith("R") && value.startsWith("P")) {
            return "R/" + value;
        }
        return value;
    }
}
