package com.studioquack.sqs.channel.net;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One hub-proxy entry from handshake v2 {@code hubAddresses[]}. The daemon probes in list order and speaks
 * HTTPS to {@code address:port} under the pinned platform CA.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HubAddress(String gatewayId, String address, int port) {

    public HubAddress {
        if (port <= 0) {
            port = 5005;
        }
    }

    public boolean usable() {
        return address != null && !address.isBlank() && port > 0 && port <= 65_535;
    }
}
