package com.studioquack.sqs.channel.net;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.channel.config.HttpsEndpointPolicy;
import com.studioquack.sqs.sandbox.runtime.CliRunner;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * ProcessBuilder adapter for the bundled {@code sq-net} CLI. The daemon user is an operator of the agent
 * socket — no sudo.
 */
@Slf4j
public final class CliNetAgentClient implements NetAgentClient {

    private static final Duration UP_TIMEOUT = Duration.ofSeconds(75);
    private static final Duration STATUS_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration SHORT_TIMEOUT = Duration.ofSeconds(10);
    /**
     * {@code status --json} grows ~1.5 KB per mesh peer; the cap bounds a runaway CLI, not a real mesh.
     */
    private static final int STDOUT_CAP = 4 * 1024 * 1024;
    private static final int STDERR_CAP = 64 * 1024;

    private final String cliPath;
    private final String socketPath;
    private final String fallbackInterfaceName;
    private final ObjectMapper mapper;

    public CliNetAgentClient(String cliPath, String fallbackInterfaceName, ObjectMapper mapper) {
        this(cliPath, fallbackInterfaceName, mapper, "");
    }

    public CliNetAgentClient(String cliPath, String fallbackInterfaceName, ObjectMapper mapper,
            String socketPath) {
        this.cliPath = (cliPath == null || cliPath.isBlank()) ? "sq-net" : cliPath.trim();
        this.socketPath = socketPath == null ? "" : socketPath.trim();
        this.fallbackInterfaceName = (fallbackInterfaceName == null || fallbackInterfaceName.isBlank())
                ? "sqnet0" : fallbackInterfaceName.trim();
        this.mapper = mapper == null ? new ObjectMapper() : mapper;
    }

    /**
     * {@code true} when {@code sq-net version} exits 0 within the short timeout.
     */
    public static boolean available(String cliPath) {
        String cli = (cliPath == null || cliPath.isBlank()) ? "sq-net" : cliPath.trim();
        try {
            return exec(List.of(cli, "version"), SHORT_TIMEOUT).exitCode() == 0;
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public void up(String controlUrl, String authKey, String hostname) throws Exception {
        if (controlUrl == null || controlUrl.isBlank()) {
            throw new IllegalArgumentException("sq-net up: controlUrl is required");
        }
        // Same rule as the handshake validator: https, or http only on loopback.
        HttpsEndpointPolicy.require(controlUrl.trim(), "sq-net up --login-server");
        // The sink re-checks what becomes argv, independent of the handshake validator.
        if (hasControlChars(controlUrl) || (authKey != null && hasControlChars(authKey.trim()))) {
            throw new IllegalArgumentException("sq-net up: control characters in controlUrl/authKey");
        }
        if (hostname != null && !hostname.isBlank() && !NetHandshakeValidator.validHostname(hostname)) {
            throw new IllegalArgumentException("sq-net up: hostname is not a valid host name");
        }
        Path keyFile = null;
        try {
            List<String> argv = new ArrayList<>(cliPrefix());
            argv.add("up");
            // Daemon owns this agent. reset drops leftover prefs from a previous env /
            // install (hostname, tags, routes, login-server).
            // "changing settings via 'sq-net up' requires mentioning all non-default flags".
            argv.add("--reset");
            argv.add("--login-server=" + controlUrl.trim());
            if (authKey != null && !authKey.isBlank()) {
                keyFile = writeAuthKeyFile(authKey.trim());
                argv.add("--authkey=file:" + keyFile.toAbsolutePath());
            }
            if (hostname != null && !hostname.isBlank()) {
                argv.add("--hostname=" + hostname.trim());
            }
            argv.add("--accept-dns=false");
            argv.add("--accept-routes=false");
            argv.add("--advertise-tags=");
            if (needsUnattended(System.getProperty("os.name"))) {
                // Windows agents serve the profile of whichever client is CONNECTED;
                // without Unattended Mode the session idles the moment this CLI exits,
                // so `up` succeeds and the agent is never Running again (wg-not-up).
                argv.add("--unattended");
            }
            argv.add("--timeout=60s");
            CliRunner.Result r = exec(argv, UP_TIMEOUT);
            if (r.exitCode() != 0) {
                throw new IllegalStateException("sq-net up failed (exit " + r.exitCode() + "): "
                        + trim(r.stdout() + r.stderr()));
            }
        } finally {
            if (keyFile != null) {
                try {
                    Files.deleteIfExists(keyFile);
                } catch (IOException ignored) {
                    // Best-effort wipe of the one-time key file
                }
            }
        }
    }

    /**
     * {@code --unattended} is a Windows-only flag of {@code sq-net up}.
     */
    static boolean needsUnattended(String osName) {
        return osName != null && osName.toLowerCase(java.util.Locale.ROOT).startsWith("windows");
    }

    @Override
    public void down() {
        try {
            exec(cmd("down"), SHORT_TIMEOUT);
        } catch (Exception ex) {
            log.warn("channel/net: sq-net down failed ({})", ex.toString());
        }
    }

    @Override
    public void logout() {
        try {
            exec(cmd("logout"), SHORT_TIMEOUT);
        } catch (Exception ex) {
            log.warn("channel/net: sq-net logout failed ({})", ex.toString());
        }
    }

    @Override
    public String version() {
        try {
            CliRunner.Result r = exec(List.of(cliPath, "version"), SHORT_TIMEOUT);
            if (r.exitCode() == 0) {
                return r.stdout().strip();
            }
        } catch (Exception ignored) {
            // Version is diagnostic only
        }
        return "";
    }

    @Override
    public NetAgentStatus status() throws Exception {
        CliRunner.Result r = exec(cmd("status", "--json"), STATUS_TIMEOUT);
        if (r.exitCode() != 0) {
            throw new IllegalStateException("sq-net status failed (exit " + r.exitCode() + "): "
                    + trim(r.stdout() + r.stderr()));
        }
        return parseStatus(r.stdout());
    }

    NetAgentStatus parseStatus(String json) throws Exception {
        JsonNode root = mapper.readTree(json == null ? "{}" : json);
        String backendState = text(root, "BackendState");
        JsonNode self = root.path("Self");
        String hostname = text(self, "HostName");
        if (hostname.isEmpty()) {
            hostname = text(root, "Self.HostName");
        }
        String publicKey = text(self, "PublicKey");
        List<String> selfIps = stringList(self.get("TailscaleIPs"));
        if (selfIps.isEmpty()) {
            selfIps = stringList(root.get("TailscaleIPs"));
        }
        String iface = text(root, "InterfaceName");
        if (iface.isEmpty()) {
            iface = text(self, "IfName");
        }
        if (iface.isEmpty()) {
            iface = fallbackInterfaceName;
        }
        List<NetAgentStatus.Peer> peers = new ArrayList<>();
        JsonNode peerMap = root.get("Peer");
        if (peerMap != null && peerMap.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = peerMap.fields();
            while (it.hasNext()) {
                JsonNode n = it.next().getValue();
                if (n == null || n.isNull()) {
                    continue;
                }
                peers.add(new NetAgentStatus.Peer(
                        text(n, "HostName"),
                        stringList(n.get("TailscaleIPs")),
                        n.path("Online").asBoolean(false)));
            }
        }
        return new NetAgentStatus(backendState, hostname, publicKey, selfIps, iface, peers);
    }

    /**
     * Root flags then the subcommand. {@code --socket} is a CLI-root flag (before {@code up}).
     */
    private List<String> cliPrefix() {
        List<String> argv = new ArrayList<>();
        argv.add(cliPath);
        if (!socketPath.isBlank()) {
            argv.add("--socket=" + socketPath);
        }
        return argv;
    }

    private List<String> cmd(String... rest) {
        List<String> argv = cliPrefix();
        argv.addAll(List.of(rest));
        return argv;
    }

    private static boolean hasControlChars(String v) {
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                return true;
            }
        }
        return false;
    }

    static Path writeAuthKeyFile(String authKey) throws IOException {
        Path tmp = Files.createTempFile("sq-net-auth", ".key");
        restrictOwnerOnly(tmp);
        Files.writeString(tmp, authKey, StandardCharsets.US_ASCII);
        return tmp;
    }

    /**
     * POSIX {@code 0600}, or on Windows an explicit owner-only DACL, before the key is written.
     */
    static void restrictOwnerOnly(Path tmp) throws IOException {
        try {
            Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------"));
            return;
        } catch (UnsupportedOperationException ignored) {
            // Windows — POSIX view is absent; fall through to ACL.
        }
        AclFileAttributeView view = Files.getFileAttributeView(tmp, AclFileAttributeView.class);
        if (view == null) {
            throw new IOException("sq-net auth key: cannot restrict ACL on " + tmp);
        }
        UserPrincipal owner = view.getOwner();
        AclEntry allowOwner = AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(owner)
                .setPermissions(EnumSet.of(
                        AclEntryPermission.READ_DATA,
                        AclEntryPermission.WRITE_DATA,
                        AclEntryPermission.APPEND_DATA,
                        AclEntryPermission.READ_ATTRIBUTES,
                        AclEntryPermission.WRITE_ATTRIBUTES,
                        AclEntryPermission.READ_NAMED_ATTRS,
                        AclEntryPermission.WRITE_NAMED_ATTRS,
                        AclEntryPermission.READ_ACL,
                        AclEntryPermission.DELETE,
                        AclEntryPermission.SYNCHRONIZE))
                .build();
        view.setAcl(List.of(allowOwner));
    }

    /**
     * Output has to be drained while the CLI runs, never read after it exits: Java on Windows hands the child a ~4
     * KB pipe, smaller than a Running agent's {@code status --json}, and a child blocked on a full pipe never
     * exits — every read would end at the timeout.
     */
    private static CliRunner.Result exec(List<String> argv, Duration timeout) throws IOException {
        CliRunner.Result r = CliRunner.run(argv, null, timeout, STDOUT_CAP, STDERR_CAP);
        if (r.timedOut()) {
            throw new IOException("timed out running " + argv.getFirst());
        }
        // 1 without a timeout is CliRunner's "never started / interrupted" result.
        if (r.exitCode() == -1) {
            throw new IOException("could not run " + argv.getFirst() + ": " + trim(r.stderr()));
        }
        return r;
    }

    private static String text(JsonNode node, String field) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return "";
        }
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? "" : v.asText("");
    }

    private static List<String> stringList(JsonNode arr) {
        if (arr == null || !arr.isArray()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (JsonNode n : arr) {
            if (n != null && n.isTextual() && !n.asText().isBlank()) {
                out.add(n.asText());
            }
        }
        return List.copyOf(out);
    }

    private static String trim(String s) {
        if (s == null) {
            return "";
        }
        String t = s.replaceAll("\\s+", " ").strip();
        return t.length() > 240 ? t.substring(0, 240) + "…" : t;
    }

    /**
     * Lower-case OS token for logs.
     */
    static String osToken() {
        return System.getProperty("os.name", "unknown").toLowerCase(Locale.ROOT);
    }
}
