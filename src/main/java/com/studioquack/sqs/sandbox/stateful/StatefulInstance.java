package com.studioquack.sqs.sandbox.stateful;

import java.time.Instant;
import java.util.Locale;

/**
 * The daemon's record of one live (or terminal) stateful instance: the user's OCI image running
 * the user's own long-running app, brought up per env, one instance per (env, imageRef). Holds the
 * identity the control plane registry mirrors ({@code instanceId} / {@code containerId} / {@code
 * status} / {@code health}) plus the named container + named volume the daemon manages, and the
 * caps applied at start.
 */
public final class StatefulInstance {

    /** Instance id reserved for the platform Docling sidecar (case-insensitive match). */
    private static final String PLATFORM_DOCLING_INSTANCE_ID = "docling";

    /** Instance id of the built-in node library system executor (container {@code sq-stateful-<id>}). */
    public static final String BUILTIN_NODE_LIBRARY_INSTANCE_ID = "sq-builtin-node-library";

    /**
     * True for the built-in node library. It is the engine's own runtime and starts with no
     * resource limits at all (operator decision, Nov 2026): a cgroup controller a host lacks
     * can then never keep built-in nodes from running.
     */
    public static boolean isBuiltinNodeLibrary(String instanceId) {
        return instanceId != null && BUILTIN_NODE_LIBRARY_INSTANCE_ID.equals(instanceId.trim());
    }

    /**
     * Matched as a substring of the normalised image ref (lowercase, {@code /} → {@code -}),
     * mirroring {@code StatefulInstanceServiceImpl.isPlatformManagedRef}.
     */
    private static final String PLATFORM_DOCLING_IMAGE_MARKER = "docling-serve";

    private final String instanceId;
    /** Mutable only through {@link #rebindEnv}: a survivor of a previous enrolment joins the current env. */
    private volatile String envId;
    private final String imageRef;
    private final String containerName;
    private final String volumeName;
    private final StatefulCaps caps;
    private final boolean stateful;
    private final Instant startedAt;

    private volatile String containerId;
    private volatile String status;
    private volatile String healthJson;
    /** True when the in-container helper broker was planted and its UDS is bound. */
    private volatile boolean brokerReady;
    /**
     * Why the helper broker is unavailable when {@link #brokerReady} is false.
     * Null/blank = ready or never attempted; non-blank = cause for operators and in-container apps.
     */
    private volatile String brokerNote;
    /**
     * Last output lines of a container that died during boot, captured before the failed start
     * removed it — the only log an operator can still read for this instance afterwards.
     */
    private volatile String lastBootOutput;

    /**
     * True when the image carried {@code studioquack.sqs.dispatch=1} (truthy {@code "1"}/{@code
     * "true"}) at start, or when reconcile rebuilt the flag from the container label. Default false =
     * unlabeled images stay per-instance FIFO.
     */
    private volatile boolean dispatchCapable;

    /**
     * The reverse port this instance published on the engine's overlay address, or {@code null} when
     * it published none. Tracked so {@code stop} can unpublish exactly what {@code start} claimed.
     */
    private volatile Integer publishedReversePort;

    /**
     * The overlay network (interface name) this instance was started on. A stateful instance keeps its
     * network for life: during a gateway drain the OLD instance stays on the old network until its
     * successor is ready on the new one.
     */
    private volatile String networkKey;

    /** The bridge address the network pin was programmed for (released at stop). */
    private volatile String assignedBridgeIp;

    /**
     * True for a temporary container: a Create Node trial run starts one for a single job and
     * removes it when that job ends. No (env, imageRef) lookup resolves it, no inventory lists it,
     * and a dispatch that finds it gone never falls back to an instance of the same image.
     */
    private final boolean temporary;

    public StatefulInstance(String instanceId, String envId, String imageRef, String containerName,
                            String volumeName, StatefulCaps caps, boolean stateful) {
        this(instanceId, envId, imageRef, containerName, volumeName, caps, stateful, false);
    }

    public StatefulInstance(String instanceId, String envId, String imageRef, String containerName,
                            String volumeName, StatefulCaps caps, boolean stateful, boolean temporary) {
        this.instanceId = instanceId;
        this.envId = envId;
        this.imageRef = imageRef;
        this.containerName = containerName;
        this.volumeName = volumeName;
        this.caps = caps == null ? StatefulCaps.uncapped() : caps;
        this.stateful = stateful;
        this.temporary = temporary;
        this.startedAt = Instant.now();
        this.status = StatefulStatus.STARTING;
    }

    /** See {@link #temporary}. */
    public boolean isTemporary() {
        return temporary;
    }

    /** The stable routing key: a live instance is looked up by (env + imageRef). */
    public static String key(String envId, String imageRef) {
        return (envId == null ? "" : envId) + "|" + (imageRef == null ? "" : imageRef);
    }

    public String key() {
        return key(envId, imageRef);
    }

    /** Package-private: only the manager moves the routing key, under its start lock. */
    void rebindEnv(String envId) {
        this.envId = envId;
    }

    public String instanceId() { return instanceId; }
    public String envId() { return envId; }
    public String imageRef() { return imageRef; }
    public String containerName() { return containerName; }
    public String volumeName() { return volumeName; }
    public StatefulCaps caps() { return caps; }
    public boolean stateful() { return stateful; }
    public Instant startedAt() { return startedAt; }

    public String containerId() { return containerId; }
    public void setContainerId(String v) { this.containerId = v; }

    public String status() { return status; }
    public void setStatus(String v) { this.status = v; }

    public void setHealthJson(String v) { this.healthJson = v; }

    public boolean brokerReady() { return brokerReady; }
    public void setBrokerReady(boolean v) { this.brokerReady = v; }

    public boolean dispatchCapable() { return dispatchCapable; }
    public void setDispatchCapable(boolean v) { this.dispatchCapable = v; }

    public Integer publishedReversePort() { return publishedReversePort; }
    public void setPublishedReversePort(Integer v) { this.publishedReversePort = v; }
    public String brokerNote() { return brokerNote; }
    public void setBrokerNote(String v) { this.brokerNote = v; }
    public String lastBootOutput() { return lastBootOutput; }
    public void setLastBootOutput(String v) { this.lastBootOutput = v; }

    public String networkKey() { return networkKey; }
    public void setNetworkKey(String v) { this.networkKey = v; }

    public String assignedBridgeIp() { return assignedBridgeIp; }
    public void setAssignedBridgeIp(String v) { this.assignedBridgeIp = v; }

    /** True when the instance is currently RUNNING (dispatch may route to it). */
    public boolean isRunning() {
        return StatefulStatus.RUNNING.equals(status);
    }

    /**
     * True when this is the platform-managed Docling sidecar: instance id {@code docling}
     * (ignore case, trimmed) or an image ref whose normalised lowercase form (with
     * {@code /} → {@code -}) contains {@code docling-serve}.
     */
    public boolean isPlatformManaged() {
        if (instanceId != null
                && PLATFORM_DOCLING_INSTANCE_ID.equalsIgnoreCase(instanceId.trim())) {
            return true;
        }
        if (imageRef == null || imageRef.isBlank()) {
            return false;
        }
        String normalised = imageRef.toLowerCase(Locale.ROOT).replace('/', '-');
        return normalised.contains(PLATFORM_DOCLING_IMAGE_MARKER);
    }
}
