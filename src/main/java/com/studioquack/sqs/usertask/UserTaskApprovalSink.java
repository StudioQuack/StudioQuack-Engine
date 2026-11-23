package com.studioquack.sqs.usertask;

import java.util.Map;

/**
 * Where a pending approval is announced. {@link ChannelUserTaskApprovalSink} posts it to the
 * control plane over the enrolled channel; {@link LoggingUserTaskApprovalSink} stands in when the
 * channel is off (tests, an unenrolled daemon) so nothing dials a backend.
 */
public interface UserTaskApprovalSink {

    /** What the reporter should do after one delivery attempt. */
    enum Outcome {
        /** The backend recorded the approval (or already had it) — stop. */
        ACCEPTED,
        /** The backend refused it for good (unknown workflow, not admitted on this env) — stop. */
        REFUSED,
        /** Transient (network, 5xx, 429) — try again later. */
        RETRY
    }

    /**
     * Announces one pending approval.
     *
     * @param report the wire body: ids, subject/message/recipient fields, and the optional values
     */
    Outcome open(Map<String, Object> report);
}
