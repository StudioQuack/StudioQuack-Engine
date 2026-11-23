package com.studioquack.sqs.channel.command;

/**
 * Terminal outcome of executing a {@link DaemonCommand}. The
 * {@link com.studioquack.sqs.channel.HeartbeatClient} maps this onto the {@code commandResults[]} entry it
 * ships on the next beat — byte-compatible with the control plane (
 * {@code {commandId,type,status,output,error}}).
 *
 * @param status {@code "ok"}, {@code "failed"}, or overlay {@code "accepted"} (queued, handler still running).
 * @param output free-form success payload (e.g. a RUNNING-status ack blob); {@code null}/empty is omitted on
 * the wire
 * @param error failure detail surfaced verbatim to the BE log + FE toast; {@code null}/empty is omitted on the
 * wire
 */
public record DaemonCommandOutcome(String status, String output, String error) {

    /**
     * Wire status literal for a successful command.
     */
    public static final String STATUS_OK = "ok";

    /**
     * Wire status literal for a failed command.
     */
    public static final String STATUS_FAILED = "failed";

    /**
     * Wire status literal for overlay accept-and-run: the HTTPS thread queued {@code deploy}/{@code start-run} and
     * returned before the handler finished.
     */
    public static final String STATUS_ACCEPTED = "accepted";

    /**
     * Convenience: a successful outcome carrying an optional output blob.
     */
    public static DaemonCommandOutcome ok(String output) {
        return new DaemonCommandOutcome(STATUS_OK, output, null);
    }

    /**
     * Convenience: overlay ACCEPT ack (handler still running on the serial lane).
     */
    public static DaemonCommandOutcome accepted() {
        return new DaemonCommandOutcome(STATUS_ACCEPTED, null, null);
    }

    /**
     * Convenience: a failed outcome carrying an error message.
     */
    public static DaemonCommandOutcome failed(String error) {
        return new DaemonCommandOutcome(STATUS_FAILED, null, error);
    }

    /**
     * True when {@link #status} is the {@code ok} literal.
     */
    public boolean succeeded() {
        return STATUS_OK.equals(status);
    }
}
