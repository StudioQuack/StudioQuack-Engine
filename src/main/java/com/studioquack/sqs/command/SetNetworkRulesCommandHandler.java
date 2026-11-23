package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.sandbox.reverse.NetworkRuleStore;
import com.studioquack.sqs.sandbox.stateful.StatefulInstanceManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Handles the BE-pushed {@code set-network-rules} command: replaces the daemon's projected Network Access
 * Control set wholesale (a projection is never a delta, so a rule deleted while the daemon was down cannot
 * linger) and persists it. Wire contract: payload: {rules:[{sourceEnvId, port}], — the FULL inbound set for
 * this engine outbound:[{targetEnvId, port}]} — the FULL outbound mirror (what this engine's containers may
 * dial) result: {rules:&lt;count&gt;, outbound:&lt;count&gt;, persisted:true} Rules stay keyed by engine id on
 * the wire; the reverse-port relay resolves inbound ids at accept time and the sandbox egress manager resolves
 * outbound ids at rule-programming time, both vithe same-owner peer directory.
 */
@Slf4j
@Component
public class SetNetworkRulesCommandHandler implements DaemonCommandHandler {

    /**
     * BE wire command type (matches the control plane).
     */
    public static final String TYPE = "set-network-rules";

    private final ObjectMapper objectMapper;
    private final NetworkRuleStore networkRuleStore;
    /**
     * Lazy — the stateful manager sits behind the sandbox stack, and this handler is wired on the command lane
     * long before that stack is ready. Only ever touched after a rules push.
     */
    private final ObjectProvider<StatefulInstanceManager> statefulInstanceManager;

    public SetNetworkRulesCommandHandler(ObjectMapper objectMapper, NetworkRuleStore networkRuleStore,
                                         ObjectProvider<StatefulInstanceManager> statefulInstanceManager) {
        this.objectMapper = objectMapper;
        this.networkRuleStore = networkRuleStore;
        this.statefulInstanceManager = statefulInstanceManager;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            String payload = command == null ? null : command.payload();
            JsonNode root = objectMapper.readTree(payload == null || payload.isBlank() ? "{}" : payload);
            List<NetworkRuleStore.Rule> parsed = NetworkRuleStore.parse(root);
            List<NetworkRuleStore.OutboundRule> parsedOutbound = NetworkRuleStore.parseOutbound(root);
            networkRuleStore.replaceAll(parsed, parsedOutbound);


            // The operator opens port 5001 in Access & Security and the target stays unreachable.
            // Re-assert surviving reverse ports against the fresh projection now. Never fail the
            // ack over this: the rules ARE applied; the relay's accept-time ACL enforces them
            // regardless of when a listener (re)binds.
            try {
                statefulInstanceManager.ifAvailable(StatefulInstanceManager::republishSurvivingReversePorts);
            } catch (Exception republish) {
                log.warn("set-network-rules: reverse-port republish after rules push failed: {}",
                        republish.toString());
            }
            ObjectNode ack = objectMapper.createObjectNode();
            ack.put("rules", parsed.size());
            ack.put("outbound", parsedOutbound.size());
            ack.put("persisted", true);
            return DaemonCommandOutcome.ok(objectMapper.writeValueAsString(ack));
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("set-network-rules failed: " + e.getMessage());
        }
    }
}
