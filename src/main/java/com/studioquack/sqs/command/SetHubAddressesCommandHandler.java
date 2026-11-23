package com.studioquack.sqs.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import com.studioquack.sqs.channel.net.HubAddress;
import com.studioquack.sqs.channel.net.HubList;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces the ordered hub-proxy list and marks the current hub gone when it is no longer in the new list.
 * Wire name is {@link #TYPE}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SetHubAddressesCommandHandler implements DaemonCommandHandler {

    /**
     * Wire command type: {@code set} + {@code hub} + {@code addresses}.
     */
    public static final String TYPE = String.join("-", "set", "hub", "addresses");

    private final ObjectMapper objectMapper;
    private final HubList hubList;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        String payload = command == null ? null : command.payload();
        if (payload == null || payload.isBlank()) {
            return DaemonCommandOutcome.failed(TYPE + ": missing payload");
        }
        List<HubAddress> hubs = new ArrayList<>();
        try {
            JsonNode root = objectMapper.readTree(payload);
            JsonNode array = root == null ? null : root.get("hubAddresses");
            if (array == null || !array.isArray()) {
                return DaemonCommandOutcome.failed(TYPE + ": payload carries no hubAddresses array");
            }
            for (JsonNode node : array) {
                HubAddress hub = objectMapper.treeToValue(node, HubAddress.class);
                if (hub != null && hub.usable()) {
                    hubs.add(hub);
                }
            }
        } catch (Exception parse) {
            return DaemonCommandOutcome.failed(TYPE + ": malformed payload: "
                    + (parse.getMessage() == null ? parse.toString() : parse.getMessage()));
        }
        hubList.replace(hubs);
        log.info("{}: {} hub(s); currentGone={}", TYPE, hubs.size(), hubList.currentGone());
        return DaemonCommandOutcome.ok("{\"hubs\":" + hubs.size() + "}");
    }
}
