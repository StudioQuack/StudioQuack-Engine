package com.studioquack.common.dto.flow;

import java.util.Locale;
import java.util.Optional;

/**
 * BPMN / ingress role carried by a {@code select}-type configuration parameter option
 * in the {@code value:label:role} CSV grammar.
 *
 * <p>When no role segment is declared on a select option, it defaults to {@link #NORMAL}.
 * Only {@link #LISTEN} blocks the node's canvas INPUT port (acting as an inbound start/event
 * listener); {@link #MESSAGE_CATCH}, {@link #SEND_AND_WAIT}, and {@link #MESSAGE_THROW}
 * keep the INPUT port open.
 */
public enum SelectOptionRole {
    /** Standard execution task (default when role is omitted). */
    NORMAL,
    /** Inbound start / event listener — blocks the canvas INPUT port. */
    LISTEN,
    /** Mid-flow intermediate message catch — INPUT port stays open. */
    MESSAGE_CATCH,
    /** Compound outbound send + inbound reply wait — INPUT port stays open. */
    SEND_AND_WAIT,
    /** Outbound message throw semantics — INPUT port stays open. */
    MESSAGE_THROW;

    /**
     * Parse a wire role token case-insensitively (e.g. {@code "listen"}, {@code "send_and_wait"}).
     * Accepts both underscores and hyphens as word separators. Returns empty for unknown tokens
     * so non-role suffixes (such as URL ports or free-form labels) never misparse as roles.
     */
    public static Optional<SelectOptionRole> fromToken(String token) {
        if (token == null) {
            return Optional.empty();
        }
        String normalized = token.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        if (normalized.isEmpty()) {
            return Optional.empty();
        }
        for (SelectOptionRole candidate : values()) {
            if (candidate.name().equals(normalized)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /** True when selecting this role blocks incoming sequence flows on the node's INPUT port. */
    public boolean blocksInputPort() {
        return this == LISTEN;
    }

    /** True when this role expects inbound webhook/message correlation. */
    public boolean isIngressRole() {
        return this == LISTEN || this == MESSAGE_CATCH || this == SEND_AND_WAIT;
    }
}
