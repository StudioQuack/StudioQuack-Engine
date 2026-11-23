package com.studioquack.sqs.engine.strategy;

import com.studioquack.common.dto.flow.NodeDto;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Process;

import java.util.Map;

/**
 * Emits the Activiti elements for one workflow node.
 * A sandbox node expands to several elements, so the result names both the entry and the exit.
 */
public interface ActivitiFlowNodeStrategy {

    /** Discriminator: does this strategy handle {@code bpmnType}? */
    boolean supports(String bpmnType);

    /**
     * Emits the Activiti element(s) for {@code node} into {@code process} and returns the
     * entry/exit ids the orchestrator wires edges against.
     */
    GeneratedNode create(BpmnModel model, Process process, NodeDto node, String nodeLabel);

    /**
     * The wiring handles for a generated node.
     *
     * @param entryElementId element inbound edges connect TO
     * @param exitElementId  element outbound edges leave FROM (== entry for simple nodes)
     * @param exitElementIdByPortValue per-source-port exit overrides for event-choice nodes
     *                                 (a message catch with a timeout routes its {@code message}
     *                                 edges from the message catch and its {@code timeout} edges
     *                                 from the timer catch). A port mapped to a BLANK string is
     *                                 deliberately swallowed: the node's own synthesis already
     *                                 terminates that branch, so the authored edge is skipped.
     */
    record GeneratedNode(String entryElementId, String exitElementId,
                         Map<String, String> exitElementIdByPortValue) {
        /** Wiring handles for a node whose single exit serves every outbound edge. */
        public GeneratedNode(String entryElementId, String exitElementId) {
            this(entryElementId, exitElementId, Map.of());
        }

        /** Convenience for the common case where a node is a single element. */
        public static GeneratedNode single(String elementId) {
            return new GeneratedNode(elementId, elementId);
        }

        /**
         * Resolves the exit element for an outbound edge leaving through {@code portValue} (the
         * handle's port half, e.g. {@code "timeout"}). An explicitly mapped port wins — including
         * a blank mapping (swallow); an unmapped port falls back to the default exit.
         */
        public String exitForPort(String portValue) {
            if (portValue != null && exitElementIdByPortValue.containsKey(portValue)) {
                return exitElementIdByPortValue.get(portValue);
            }
            return exitElementId;
        }

        /**
         * True when outbound edges are routed per source port. Such a node is an event CHOICE,
         * not a fan-out: the orchestrator must not wrap it in a parallel fork when more than one
         * outbound edge is authored.
         */
        public boolean hasPortSpecificExits() {
            return !exitElementIdByPortValue.isEmpty();
        }
    }
}
