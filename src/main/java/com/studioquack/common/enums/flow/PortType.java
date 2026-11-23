package com.studioquack.common.enums.flow;

/**
 * Defines the different types of ports a node can have,
 * influencing their position and function within the Node Template Architecture.
 */
public enum PortType {
    /**
     * Input port, typically on the left side, receiving data to trigger or be processed by the node.
     */
    INPUT,

    /**
     * Output port, typically on the right side, sending data out from the node after processing.
     */
    OUTPUT,

    /**
     * Variable Provider port, typically on the top side, supplying configuration or dynamic values
     * before the main node script executes.
     */
    VARIABLE_PROVIDER,

    /**
     * Function port, typically on the bottom side, providing reusable functionality that can be called
     * on demand from within the node's script.
     */
    FUNCTION
} 