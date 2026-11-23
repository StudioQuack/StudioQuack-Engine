package com.studioquack.sqs.engine.strategy.helper;

import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.Message;
import org.activiti.bpmn.model.Signal;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.studioquack.sqs.engine.runtime.EngineVariableNames;

import java.util.Objects;
import java.util.function.Function;

/**
 * Idempotently registers BPMN {@link Signal}/{@link Message} definitions on a
 * {@link BpmnModel} and returns their ref id, so multiple events sharing a name reuse the
 * same top-level definition. Ported verbatim from the control plane
 * {@code com.studioquack.flow.strategy.activiti.helper.ActivitiSignalMessageRegistry}.
 *
 * <p>Names come from user config and the id must be an XML NCName, so the id is a sanitised form of
 * the name — which is not injective ({@code "a-b"} and {@code "a_b"} sanitise alike). Two DIFFERENT
 * names must never share one definition, or the second event would subscribe under the first one's
 * name, so a taken id whose definition carries another name is disambiguated with a suffix.
 */
@Component
public class ActivitiSignalMessageRegistry {

    /** @return the {@code signalRef} id for {@code signalName} (creating the Signal if new). */
    public String getOrCreateSignal(BpmnModel model, String signalName) {
        // Engine-internal signalling (the escalation bridge) lives under the reserved prefix;
        // an authored signal there would subscribe to — and be fired by — escalation traffic.
        if (signalName != null && signalName.startsWith(EngineVariableNames.RESERVED_PREFIX)) {
            throw new IllegalArgumentException("signal name '" + signalName + "' uses the reserved '"
                    + EngineVariableNames.RESERVED_PREFIX + "' prefix (engine-internal signalling,"
                    + " e.g. escalation boundaries) — choose another name");
        }
        return doGetOrCreateSignal(model, signalName);
    }

    /**
     * Reserved-name variant for engine-internal signalling (the escalation boundary bridge in
     * {@code ActivitiBoundaryEventHelper}) — same registration, without the authored-name guard.
     */
    String getOrCreateEngineSignal(BpmnModel model, String reservedSignalName) {
        return doGetOrCreateSignal(model, reservedSignalName);
    }

    private String doGetOrCreateSignal(BpmnModel model, String signalName) {
        String id = resolveRefId("Signal_", signalName,
                candidate -> model.containsSignalId(candidate) ? model.getSignal(candidate).getName() : null);
        if (!model.containsSignalId(id)) {
            model.addSignal(new Signal(id, StringUtils.defaultIfBlank(signalName, id)));
        }
        return id;
    }

    /** @return the {@code messageRef} id for {@code messageName} (creating the Message if new). */
    public String getOrCreateMessage(BpmnModel model, String messageName) {
        String id = resolveRefId("Message_", messageName,
                candidate -> model.containsMessageId(candidate) ? model.getMessage(candidate).getName() : null);
        if (!model.containsMessageId(id)) {
            // Message(id, name, itemRef) — itemRef unused (no data-item typing needed).
            model.addMessage(new Message(id, StringUtils.defaultIfBlank(messageName, id), null));
        }
        return id;
    }

    /**
     * @param nameOfRegistered resolves a candidate id to the name already registered under it, or
     *                         {@code null} when the id is free
     * @return the first candidate id that is either free or already bound to this exact name
     */
    private String resolveRefId(String prefix, String name, Function<String, String> nameOfRegistered) {
        String base = prefix + sanitize(name);
        String expectedName = StringUtils.defaultIfBlank(name, base);
        String candidate = base;
        for (int suffix = 2; ; suffix++) {
            String registered = nameOfRegistered.apply(candidate);
            if (registered == null || Objects.equals(registered, expectedName)) {
                return candidate;
            }
            candidate = base + "_" + suffix;
        }
    }

    /**
     * Sanitises a user-configured name into an XML-NCName-safe id fragment. A blank name fails
     * loudly: the required-parameter gate rejects blank names at deploy, so reaching here with
     * one means the gate was bypassed — minting a random id would create an unaddressable
     * subscription that changes on every deploy.
     */
    private String sanitize(String name) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalArgumentException(
                    "signal/message name is blank — a random id would be unaddressable; "
                            + "the required-parameter gate should have rejected this at deploy");
        }
        return name.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
