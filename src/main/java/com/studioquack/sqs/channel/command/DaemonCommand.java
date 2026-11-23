package com.studioquack.sqs.channel.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One command pushed from the BE to the daemon inside a heartbeat response ({@code commands[]}). Wire shape
 * (camelCase JSON):
 * {@code { "commandId": "...", "type": "deploy", "issuedAt": "...", "payload": "<opaque JSON string>" }}
 * {@link #payload} is an opaque JSON string the BE attached for payload-bearing command types (e.g.
 * {@code deploy}, {@code start-run}); the handler forwards / parses it.
 *
 * @param commandId stable id the daemon echoes back in its result / ack so the BE can drop the command from
 * its pending queue
 * @param type command type used to route to a {@link DaemonCommandHandler}
 * @param issuedAt BE-stamped issue time (opaque; may be {@code null})
 * @param payload opaque JSON payload string (may be {@code null})
 * @param mac {@code sqcmd1.} HMAC; {@code null} on pre-hybrid envs
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DaemonCommand(String commandId, String type, String issuedAt, String payload, String mac) {
    public DaemonCommand(String commandId, String type, String issuedAt, String payload) {
        this(commandId, type, issuedAt, payload, null);
    }

    public DaemonCommand(String commandId, String type, String issuedAt) {
        this(commandId, type, issuedAt, null, null);
    }
}
