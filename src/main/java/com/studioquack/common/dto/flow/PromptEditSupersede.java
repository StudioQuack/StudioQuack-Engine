package com.studioquack.common.dto.flow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * The turn a prompt edit replaces. The Duckbot start node drops that turn before appending the edited prompt.
 * The turn is the {@code turnOrdinal}-th user turn, confirmed by its text; a mismatch removes nothing.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PromptEditSupersede implements Serializable {
    /** Text of the edited turn before the edit. */
    private String previousPrompt;
    /** 1-based position of the edited turn among this agent's user turns in the conversation. */
    private int turnOrdinal;
    /** True in an agent-only chat: every later turn goes. False keeps later people's turns. */
    private boolean dropLaterUserTurns;
}
