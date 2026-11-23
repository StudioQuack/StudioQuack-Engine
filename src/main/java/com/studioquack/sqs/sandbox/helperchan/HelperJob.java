package com.studioquack.sqs.sandbox.helperchan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One registered job's state on the helper channel: the metadata + execution-context the
 * in-sandbox SDK reads, and the result / metrics / logs it POSTs back. Thread-safe: the sandbox
 * thread POSTs {@code result}/{@code metric}/{@code log} concurrently while the dispatcher reads
 * them after the container exits.
 */
public final class HelperJob {

    private final String id;
    private final String language;
    private final Instant enqueuedAt;
    private final String jobMode;
    private final Map<String, Object> executionContext;
    private final Map<String, byte[]> inputs;
    private final Map<String, Object> limits;

    /**
     * {@code true} for a PERSISTENT, per-request stateful registration: many of these coexist against
     * the SAME live instance/container (one token per request), and NONE is revoked until its own
     * request completes — the open channel keeps {@code studioquack.execute} + the LLM helpers
     * reachable for the whole request. {@code false} (default) is the classic one-shot ephemeral
     * dispatch (registered + revoked around a single container run).
     */
    private final boolean persistent;

    /** Stateful instance image the caller runs on, or {@code null} when not a stateful dispatch. */
    private final String callerImageRef;

    /**
     * When non-null (including empty) the job is in TEST-MOCK mode: helper POSTs are answered from
     * this map instead of a live sidecar. {@code null} is the production path.
     */
    private final Map<String, Object> mockResponses;

    /** Resolved secret literals for this dispatch — in-process only; used to redact helper logs. */
    private final Set<String> secretLiterals;

    private volatile HelperResult result;
    private volatile HelperFailure failure;
    private volatile Map<String, Object> transformedContext;
    private final List<Map<String, Object>> metrics = Collections.synchronizedList(new ArrayList<>());
    private final List<Map<String, Object>> logs = Collections.synchronizedList(new ArrayList<>());
    private final List<Map<String, Object>> dispatch = Collections.synchronizedList(new ArrayList<>());
    /** Recorded mock-mode helper invocations (thread-safe, like {@link #metrics()}). */
    private final List<Map<String, Object>> mockInvocations = Collections.synchronizedList(new ArrayList<>());

    /** Slides the stateful launcher idle reaper on every helper-channel frame. */
    private final AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());

    /**
     * Helper requests of this job still being served. While one is open — an LLM generation, a
     * parked {@code studioquack.execute()} — the job is waiting on the platform, not idle, however
     * long that takes, so the launcher's idle reaper must not fire.
     */
    private final AtomicInteger openRequests = new AtomicInteger();

    /**
     * Set once the job's bearer is revoked — its dispatch ended, normally or by cancel. A
     * {@code studioquack.execute()} still parked for this job has nobody left to answer.
     */
    private volatile boolean revoked;

    /** Callbacks run once on revoke (see {@link #onRevoke}); guarded by itself. */
    private final Set<Runnable> revokeListeners = new java.util.LinkedHashSet<>();

    private HelperJob(Builder b) {
        this.id = b.id;
        this.language = b.language;
        this.enqueuedAt = b.enqueuedAt == null ? Instant.now() : b.enqueuedAt;
        this.jobMode = b.jobMode == null ? "user" : b.jobMode;
        this.executionContext = b.executionContext == null ? new LinkedHashMap<>() : b.executionContext;
        this.inputs = b.inputs == null ? new LinkedHashMap<>() : b.inputs;
        this.limits = b.limits == null ? new LinkedHashMap<>() : b.limits;
        this.persistent = b.persistent;
        this.callerImageRef = b.callerImageRef;
        this.mockResponses = b.mockResponses == null ? null : new LinkedHashMap<>(b.mockResponses);
        this.secretLiterals = b.secretLiterals == null || b.secretLiterals.isEmpty()
                ? Set.of() : Set.copyOf(b.secretLiterals);
    }

    public static Builder builder(String id, String language) {
        return new Builder(id, language);
    }

    public String id() { return id; }
    public String language() { return language; }
    public Instant enqueuedAt() { return enqueuedAt; }
    public String jobMode() { return jobMode; }
    public Map<String, Object> executionContext() { return executionContext; }
    public Map<String, byte[]> inputs() { return inputs; }
    public Map<String, Object> limits() { return limits; }

    /** True for a persistent, per-request stateful registration. */
    public boolean persistent() { return persistent; }

    /** Image ref of the stateful instance the caller runs on, or {@code null}. */
    public String callerImageRef() { return callerImageRef; }

    /**
     * TEST-MOCK responses, or {@code null} on the production path. Empty map is still mock mode.
     */
    public Map<String, Object> mockResponses() { return mockResponses; }

    /** Distinct resolved secret literals for this job (empty when none). */
    public Set<String> secretLiterals() { return secretLiterals; }

    public HelperResult result() { return result; }
    public void setResult(HelperResult r) { this.result = r; }

    /** The typed error the SDK posted via {@code /fail}, or {@code null} on success. */
    public HelperFailure failure() { return failure; }
    public void setFailure(HelperFailure f) { this.failure = f; }

    public Map<String, Object> transformedContext() { return transformedContext; }
    public void setTransformedContext(Map<String, Object> c) { this.transformedContext = c; }

    public List<Map<String, Object>> metrics() { return metrics; }
    public List<Map<String, Object>> logs() { return logs; }
    public List<Map<String, Object>> dispatch() { return dispatch; }

    /** Per-job helper-log list cap (PRO-Q-08). */
    public static final int MAX_LOGS = 256;
    static final int MAX_LOG_KEYS = 16;
    static final int MAX_LOG_DEPTH = 3;
    static final int MAX_LOG_STRING_CHARS = 4096;
    static final int MAX_LOG_LIST_ITEMS = 16;

    /**
     * Clamps and appends one helper log. Returns {@code false} when the job is at
     * {@link #MAX_LOGS} (a single {@code kind:truncated} note occupies the last slot).
     */
    public boolean tryAppendLog(Map<String, Object> entry) {
        if (entry == null) {
            return false;
        }
        Map<String, Object> clamped = clampLogMap(entry, 0);
        synchronized (logs) {
            if (logs.size() >= MAX_LOGS) {
                return false;
            }
            if (logs.size() == MAX_LOGS - 1) {
                Map<String, Object> omit = new LinkedHashMap<>();
                omit.put("kind", "truncated");
                omit.put("level", "info");
                omit.put("at", Instant.now().toString());
                omit.put("message", "further helper logs omitted");
                logs.add(omit);
                return false;
            }
            logs.add(clamped);
            return true;
        }
    }

    static Map<String, Object> clampLogMap(Map<?, ?> in, int depth) {
        Map<String, Object> out = new LinkedHashMap<>();
        int n = 0;
        for (Map.Entry<?, ?> e : in.entrySet()) {
            if (n++ >= MAX_LOG_KEYS) {
                break;
            }
            out.put(String.valueOf(e.getKey()), clampLogValue(e.getValue(), depth + 1));
        }
        return out;
    }

    static Object clampLogValue(Object v, int depth) {
        if (v == null) {
            return null;
        }
        if (v instanceof Map<?, ?> m) {
            return depth >= MAX_LOG_DEPTH ? "[truncated]" : clampLogMap(m, depth);
        }
        if (v instanceof List<?> list) {
            if (depth >= MAX_LOG_DEPTH) {
                return "[truncated]";
            }
            List<Object> out = new ArrayList<>();
            for (int i = 0; i < list.size() && i < MAX_LOG_LIST_ITEMS; i++) {
                out.add(clampLogValue(list.get(i), depth + 1));
            }
            return out;
        }
        if (v instanceof Number || v instanceof Boolean) {
            return v;
        }
        String s = v instanceof String str ? str : String.valueOf(v);
        return s.length() > MAX_LOG_STRING_CHARS ? s.substring(0, MAX_LOG_STRING_CHARS) : s;
    }

    /** Recorded mock-mode helper invocations (thread-safe). */
    public List<Map<String, Object>> mockInvocations() { return mockInvocations; }

    /** Marks helper-channel activity so a streaming stateful launcher is not idle-reaped mid-turn. */
    public void touchActivity() {
        lastActivityNanos.set(System.nanoTime());
    }

    /** Marks the start of one helper request; pair with {@link #endRequest()} in a finally. */
    public void beginRequest() {
        openRequests.incrementAndGet();
        touchActivity();
    }

    /** Marks the end of a helper request opened by {@link #beginRequest()}. */
    public void endRequest() {
        openRequests.decrementAndGet();
        touchActivity();
    }

    /**
     * Idle reaper input: "now" while a helper request is open, otherwise the nanoseconds of the
     * last helper-channel frame.
     */
    public long lastActivityNanos() {
        return openRequests.get() > 0 ? System.nanoTime() : lastActivityNanos.get();
    }

    /**
     * Called by {@code HelperRequestRouter.revoke} when the job's dispatch is over. Runs every
     * {@link #onRevoke} callback once, on this thread — callbacks only signal (complete a future,
     * cancel a call), so revoke stays O(listeners) and never blocks on the work they release.
     */
    public void markRevoked() {
        List<Runnable> toRun;
        synchronized (revokeListeners) {
            if (revoked) {
                return;
            }
            revoked = true;
            toRun = new ArrayList<>(revokeListeners);
            revokeListeners.clear();
        }
        toRun.forEach(HelperJob::runQuietly);
    }

    /** {@code true} once the job's dispatch is over (see {@link #markRevoked()}). */
    public boolean revoked() {
        return revoked;
    }

    /**
     * Runs {@code listener} once when this job is revoked — at once if it already is. A call parked
     * for this job (a sub-process wait, a cross-env call) ends the moment its caller is gone, with
     * nothing polling.
     */
    public void onRevoke(Runnable listener) {
        synchronized (revokeListeners) {
            if (!revoked) {
                revokeListeners.add(listener);
                return;
            }
        }
        runQuietly(listener);
    }

    /** Drops a listener whose call finished before the job was revoked. */
    public void removeRevokeListener(Runnable listener) {
        synchronized (revokeListeners) {
            revokeListeners.remove(listener);
        }
    }

    private static void runQuietly(Runnable listener) {
        try {
            listener.run();
        } catch (RuntimeException ignored) {
            // One failing listener must not keep the others from releasing their calls.
        }
    }

    /** Null-safe append of one mock-mode invocation for later test assertion. */
    public void recordMockInvocation(Map<String, Object> invocation) {
        if (invocation != null) {
            mockInvocations.add(invocation);
        }
    }

    /** Fluent builder — the SDK-facing fields are all optional besides id + language. */
    public static final class Builder {
        private final String id;
        private final String language;
        private Instant enqueuedAt;
        private String jobMode;
        private Map<String, Object> executionContext;
        private Map<String, byte[]> inputs;
        private Map<String, Object> limits;
        private boolean persistent;
        private String callerImageRef;
        private Map<String, Object> mockResponses;
        private Set<String> secretLiterals;

        private Builder(String id, String language) {
            this.id = id;
            this.language = language;
        }

        public Builder enqueuedAt(Instant v) { this.enqueuedAt = v; return this; }
        public Builder jobMode(String v) { this.jobMode = v; return this; }
        public Builder executionContext(Map<String, Object> v) { this.executionContext = v; return this; }
        public Builder inputs(Map<String, byte[]> v) { this.inputs = v; return this; }
        public Builder limits(Map<String, Object> v) { this.limits = v; return this; }

        /** Mark this a persistent, per-request stateful registration (not one-shot). */
        public Builder persistent(boolean v) { this.persistent = v; return this; }

        /** Which stateful instance image the caller runs on (nullable). */
        public Builder callerImageRef(String v) { this.callerImageRef = v; return this; }

        /**
         * TEST-MOCK responses. Non-null (including empty) selects mock mode; {@code null} is production.
         */
        public Builder mockResponses(Map<String, Object> v) { this.mockResponses = v; return this; }

        public Builder secretLiterals(Set<String> v) { this.secretLiterals = v; return this; }

        public HelperJob build() {
            return new HelperJob(this);
        }
    }
}
