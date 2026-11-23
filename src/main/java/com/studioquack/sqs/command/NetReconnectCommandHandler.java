package com.studioquack.sqs.command;

import com.studioquack.sqs.channel.SqsChannelBootstrap;
import com.studioquack.sqs.channel.command.DaemonCommand;
import com.studioquack.sqs.channel.command.DaemonCommandHandler;
import com.studioquack.sqs.channel.command.DaemonCommandOutcome;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Server-pushed {@code net-reconnect} (replaces {@code wg-reconnect}): re-handshake / {@code up}.
 */
@Slf4j
@Component
public class NetReconnectCommandHandler implements DaemonCommandHandler {

    public static final String TYPE = "net-reconnect";

    private final ObjectProvider<SqsChannelBootstrap> channelBootstrap;

    public NetReconnectCommandHandler(ObjectProvider<SqsChannelBootstrap> channelBootstrap) {
        this.channelBootstrap = channelBootstrap;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public DaemonCommandOutcome handle(DaemonCommand command) {
        try {
            SqsChannelBootstrap bootstrap = channelBootstrap.getIfAvailable();
            if (bootstrap == null) {
                return DaemonCommandOutcome.failed("net-reconnect: secure channel is not enabled on this daemon");
            }
            bootstrap.retryNet();
            String lastError = bootstrap.netLastError();
            log.info("net-reconnect: accepted; watchdog kicked (lastError={})", lastError);
            return DaemonCommandOutcome.ok(lastError == null
                    ? "{\"reconnect\":\"requested\"}"
                    : "{\"reconnect\":\"requested\",\"previousError\":\"" + lastError.replace("\"", "'") + "\"}");
        } catch (Exception e) {
            return DaemonCommandOutcome.failed("net-reconnect failed: "
                    + (e.getMessage() == null ? e.toString() : e.getMessage()));
        }
    }
}
