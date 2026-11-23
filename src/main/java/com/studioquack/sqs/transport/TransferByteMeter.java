package com.studioquack.sqs.transport;

import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cumulative inbound+outbound byte counter since process start (cheap {@link AtomicLong} adds;
 * never blocks execution). The backend reads {@code transferBytesTotal} on heartbeat/status
 * and computes deltas for billing telemetry.
 */
@Component
public class TransferByteMeter {

    private static volatile TransferByteMeter instance;

    private final AtomicLong transferBytesTotal = new AtomicLong();

    @PostConstruct
    void register() {
        instance = this;
    }

    /** Best-effort add from any transport chokepoint; no-op when bytes &lt;= 0 or bean absent. */
    public static void add(long bytes) {
        if (bytes <= 0L) {
            return;
        }
        TransferByteMeter m = instance;
        if (m != null) {
            m.transferBytesTotal.addAndGet(bytes);
        }
    }

    public long total() {
        return transferBytesTotal.get();
    }

    /** Snapshot for heartbeat/status payloads; {@code 0} when the bean is not wired yet. */
    public static long snapshotTotal() {
        TransferByteMeter m = instance;
        return m == null ? 0L : m.total();
    }
}
