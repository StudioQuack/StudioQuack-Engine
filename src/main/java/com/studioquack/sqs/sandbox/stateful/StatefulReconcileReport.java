package com.studioquack.sqs.sandbox.stateful;

import java.util.List;

/**
 * The outcome of one {@link StatefulInstanceManager#reconcileWithHost()} pass over the host's live
 * {@code studioquack.sqs.stateful=1} containers, run once at daemon boot (re-registration). This
 * is the record that makes the "one stateful instance per image per engine" invariant honest
 * against the host instead of the control-plane row alone.
 */
public record StatefulReconcileReport(
        String ranAt,
        boolean runtimeAvailable,
        List<Entry> entries,
        int adopted,
        int reaped,
        int reapedDuplicate,
        int ignored) {

    /** One container's reconcile decision. */
    public record Entry(
            String instanceId,
            String containerId,
            String containerName,
            String imageRef,
            String state,
            String action,
            String reason) {
    }

    public static final String ACTION_ADOPTED = "adopted";
    public static final String ACTION_REAPED = "reaped";
    public static final String ACTION_REAPED_DUPLICATE = "reaped-duplicate";
    public static final String ACTION_IGNORED = "ignored";

    /** Empty report for "no runtime at boot" — nothing to enumerate, nothing reaped. */
    public static StatefulReconcileReport noRuntime(String ranAt) {
        return new StatefulReconcileReport(ranAt, false, List.of(), 0, 0, 0, 0);
    }

    /** Builds the counted report from decided entries. */
    public static StatefulReconcileReport of(String ranAt, List<Entry> entries) {
        int adopted = 0;
        int reaped = 0;
        int reapedDuplicate = 0;
        int ignored = 0;
        for (Entry e : entries) {
            switch (e.action()) {
                case ACTION_ADOPTED -> adopted++;
                case ACTION_REAPED -> reaped++;
                case ACTION_REAPED_DUPLICATE -> reapedDuplicate++;
                default -> ignored++;
            }
        }
        return new StatefulReconcileReport(ranAt, true, List.copyOf(entries),
                adopted, reaped, reapedDuplicate, ignored);
    }
}
