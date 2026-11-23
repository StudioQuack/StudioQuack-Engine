package com.studioquack.sqs.engine.quota;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local half of the monthly node-activation allowance. Activations increment an
 * in-memory delta that the heartbeat ships as {@code nodeExecutionsDelta}. The
 * control plane's reply is the last committed snapshot; the allowance is spent when
 * {@code consumed + unflushed >= granted}, which puts the engine into limits-exceeded
 * mode ({@link com.studioquack.sqs.engine.halt.ExecutionHaltGuard}). Until the first
 * snapshot arrives this meter admits; the guard keeps the engine offline until that
 * first beat anyway. A reply with {@code enforced: false} stops this meter from counting.
 */
@Component
public class NodeExecutionMeter {

    public static final String ERROR_CODE = "Error_NodeQuotaExhausted";

    public static final String EXHAUSTED_MESSAGE =
            "Monthly node execution limit reached. The engine is in limits-exceeded mode: active runs are paused "
                    + "and new runs are refused until the allowance renews.";

    public static final String RESOURCE_DELTA_KEY = "nodeExecutionsDelta";

    private final AtomicLong pending = new AtomicLong();
    private final AtomicBoolean snapshotSeen = new AtomicBoolean();
    private volatile boolean enforced;
    private volatile long granted = Long.MAX_VALUE;
    private volatile long consumed;
    /** {@code periodStart} of the last snapshot; a different one means the allowance renewed. */
    private volatile String periodStart;

    /** One designer-node activation. No-op once the control plane says this tier is not metered. */
    public void recordActivation() {
        if (snapshotSeen.get() && !enforced) {
            return;
        }
        pending.incrementAndGet();
    }

    /**
     * False only after a snapshot that enforces the allowance and the committed
     * count plus the unflushed delta has reached it.
     */
    public boolean admitNewRun() {
        if (!snapshotSeen.get() || !enforced) {
            return true;
        }
        long allowance = granted;
        if (allowance <= 0L || allowance == Long.MAX_VALUE) {
            return true;
        }
        return consumed + pending.get() < allowance;
    }

    /** True when local activations (e.g. timer-triggered runs) are waiting to be flushed to the control plane. */
    public boolean hasPendingDelta() {
        return pending.get() > 0L;
    }

    /** Current unflushed local node activations (read-only snapshot for reverse-channel inspection). */
    public long pendingCount() {
        return pending.get();
    }

    /** Detach the unflushed count for this beat. Restore it if the beat does not land. */
    public long drainDelta() {
        return pending.getAndSet(0L);
    }

    public void restoreDelta(long delta) {
        if (delta > 0L) {
            pending.addAndGet(delta);
        }
    }

    /** Apply the heartbeat {@code nodeQuota} object. A missing object leaves the previous snapshot. */
    public void applySnapshot(Map<String, Object> nodeQuota) {
        if (nodeQuota == null || nodeQuota.isEmpty()) {
            return;
        }
        Object enforcedValue = nodeQuota.get("enforced");
        boolean nowEnforced = Boolean.TRUE.equals(enforcedValue)
                || "true".equalsIgnoreCase(String.valueOf(enforcedValue));
        snapshotSeen.set(true);
        if (!nowEnforced) {
            enforced = false;
            pending.set(0L);
            return;
        }
        enforced = true;
        Object start = nodeQuota.get("periodStart");
        String newStart = start == null ? null : String.valueOf(start);
        if (newStart != null && periodStart != null && !newStart.equals(periodStart)) {
            // Renewed: the unflushed count belongs to the period the control plane already closed, so it
            // must not be charged against the new allowance.
            pending.set(0L);
        }
        if (newStart != null) {
            periodStart = newStart;
        }
        granted = asLong(nodeQuota.get("granted"), granted);
        consumed = asLong(nodeQuota.get("consumed"), consumed);
    }

    private static long asLong(Object raw, long fallback) {
        if (raw instanceof Number number) {
            return number.longValue();
        }
        if (raw instanceof String text && !text.isBlank()) {
            try {
                return Long.parseLong(text.trim());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }
}
