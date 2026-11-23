package com.studioquack.sqs.channel.command;

import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Runs heartbeat-delivered commands OFF the heartbeat loop thread. The beat loop is the daemon's last-resort
 * control channel — liveness, log carriage and emergency engine management — so it must never be held by a
 * command handler.
 */
@Slf4j
public final class CommandLaneDispatcher {

    private final DaemonCommandRegistry registry;
    private final Consumer<DaemonCommandResult> resultSink;
    private final Runnable wakeHook;
    private final ExecutorService serialLane;
    private final ExecutorService immediateLane;
    /**
     * CommandIds currently queued or executing in either lane (re-ship suppression).
     */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();

    public CommandLaneDispatcher(DaemonCommandRegistry registry,
                                 Consumer<DaemonCommandResult> resultSink,
                                 Runnable wakeHook) {
        this.registry = registry;
        this.resultSink = resultSink;
        this.wakeHook = wakeHook;
        this.serialLane = Executors.newSingleThreadExecutor(r -> newLaneThread(r, "sqs-command-lane"));
        this.immediateLane = Executors.newCachedThreadPool(r -> newLaneThread(r, "sqs-command-immediate"));
    }

    /**
     * Queues the command for execution on its lane and returns immediately. Malformed commands (no commandId) are
     * dropped here exactly as the registry would drop them; duplicate submissions of an in-flight commandId are
     * no-ops.
     */
    public void submit(DaemonCommand command) {
        submit(command, DaemonCommandRegistry.INBOUND_HEARTBEAT);
    }

    /**
     * As {@link #submit(DaemonCommand)} but records {@code inboundTransport} on the registry dispatch (overlay
     * accept-and-run uses {@link DaemonCommandRegistry#INBOUND_OVERLAY}).
     */
    public void submit(DaemonCommand command, String inboundTransport) {
        if (command == null || command.commandId() == null || command.commandId().isBlank()) {
            return;
        }
        if (registry == null) {
            return;
        }
        if (!inFlight.add(command.commandId())) {
            log.debug("channel/commands: id={} type={} already queued/running on a lane — re-ship suppressed",
                    command.commandId(), command.type());
            return;
        }
        String transport = inboundTransport == null || inboundTransport.isBlank()
                ? DaemonCommandRegistry.INBOUND_HEARTBEAT : inboundTransport;
        ExecutorService lane = DaemonCommandRegistry.isImmediateLaneType(command.type())
                ? immediateLane : serialLane;
        try {
            lane.execute(() -> executeOffLoop(command, transport));
        } catch (RuntimeException rejected) {
            // Lane is shutting down: release the id so a later delivery is not suppressed forever.
            inFlight.remove(command.commandId());
            throw rejected;
        }
    }

    /**
     * Stops both lanes (best-effort); in-flight handlers are interrupted, queued ones dropped.
     */
    public void shutdown() {
        serialLane.shutdownNow();
        immediateLane.shutdownNow();
    }

    /**
     * Visible for tests: true while a commandId is queued or executing on a lane.
     */
    boolean isInFlight(String commandId) {
        return commandId != null && inFlight.contains(commandId);
    }

    private void executeOffLoop(DaemonCommand command, String inboundTransport) {
        try {
            registry.dispatch(command, inboundTransport).ifPresent(result -> {
                resultSink.accept(result);
                // The result must not wait out a keepalive: wake the beat loop so it ships now.
                // The hook is a notification, never part of the command — it must not break the lane.
                try {
                    wakeHook.run();
                } catch (RuntimeException hookErr) {
                    log.debug("channel/commands: wake hook failed: {}", hookErr.toString());
                }
            });
        } catch (RuntimeException ex) {
            // dispatch() converts handler failures to results, so reaching here is a registry bug;
            // the lane must survive it or every later command would stall behind a dead thread.
            log.warn("channel/commands: dispatch escaped with id={} type={}: {}",
                    command.commandId(), command.type(), ex.toString());
        } finally {
            inFlight.remove(command.commandId());
        }
    }

    private static Thread newLaneThread(Runnable r, String prefix) {
        Thread t = new Thread(r, prefix + "-" + SEQ.incrementAndGet());
        t.setDaemon(true);
        // Pin the fat-jar classloader: handlers spawned here (e.g. start-run → Activiti commit)
        // inherit this thread's TCCL, and the system CL cannot see nested BOOT-INF/lib.
        t.setContextClassLoader(CommandLaneDispatcher.class.getClassLoader());
        return t;
    }

    private static final AtomicLong SEQ = new AtomicLong();
}
