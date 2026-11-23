package com.studioquack.sqs.channel.net;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CliNetAgentClient} against a fake {@code sq-net} script: status JSON parse
 * ({@code Self.TailscaleIPs}, {@code Peer} map, {@code BackendState}) and {@code up}
 * passing the auth key as {@code --authkey=file:} — never the raw key on argv.
 */
class CliNetAgentClientTest {

    private static final String AUTH_KEY = "mesh-auth-test-secret-do-not-log";
    private static final String STATUS_JSON = """
            {
              "BackendState": "Running",
              "InterfaceName": "sqnet0",
              "Self": {
                "HostName": "env-abc",
                "PublicKey": "nodekey:self",
                "TailscaleIPs": ["100.64.0.10", "fd7a:115c:a1e0::a"]
              },
              "Peer": {
                "nodekey:peer-x": {
                  "HostName": "env-x",
                  "TailscaleIPs": ["100.64.0.20"],
                  "Online": true
                },
                "nodekey:peer-off": {
                  "HostName": "env-offline",
                  "TailscaleIPs": ["100.64.0.21"],
                  "Online": false
                }
              }
            }
            """;

    @Test
    void parseStatus_readsSelfIpsPeerMapAndBackendState() throws Exception {
        CliNetAgentClient client = new CliNetAgentClient("sq-net", "sqnet0", new ObjectMapper());
        NetAgentStatus st = client.parseStatus(STATUS_JSON);

        assertEquals("Running", st.backendState());
        assertTrue(st.running());
        assertEquals("env-abc", st.hostname());
        assertEquals("nodekey:self", st.publicKey());
        assertEquals("sqnet0", st.interfaceName());
        assertEquals(List.of("100.64.0.10", "fd7a:115c:a1e0::a"), st.selfIps());
        assertEquals("100.64.0.10", st.primaryIp().orElseThrow());

        assertEquals(2, st.peers().size());
        NetAgentStatus.Peer online = st.peerByEnvId("x").orElseThrow();
        assertEquals("env-x", online.hostname());
        assertEquals("100.64.0.20", online.primaryIp().orElseThrow());
        assertTrue(online.online());
        assertTrue(st.peerByEnvId("offline").isPresent());
        assertFalse(st.peerByEnvId("offline").orElseThrow().online());
    }

    @Test
    void fakeCli_statusAndUp_recordsFileAuthKeyNotRawSecret(@TempDir Path dir) throws Exception {
        Path argvLog = dir.resolve("argv.log");
        Path cli = writeFakeSqNet(dir, argvLog);
        CliNetAgentClient client = new CliNetAgentClient(cli.toString(), "sqnet0", new ObjectMapper());

        assertTrue(CliNetAgentClient.available(cli.toString()));
        assertEquals("1.102.0-sq-net", client.version());

        NetAgentStatus st = client.status();
        assertEquals("Running", st.backendState());
        assertEquals("100.64.0.10", st.primaryIp().orElseThrow());
        assertEquals("env-x", st.peerByEnvId("x").orElseThrow().hostname());

        client.up("https://hs.example/control", AUTH_KEY, "env-abc");

        String log = Files.readString(argvLog);
        assertTrue(log.contains("up"), "up invocation must be recorded: " + log);
        assertTrue(log.contains("--authkey=file:"),
                "auth key must ride file: form, not argv plaintext: " + log);
        assertTrue(log.contains("--reset"),
                "up must --reset leftover prefs or the CLI refuses a partial flag set: " + log);
        assertTrue(log.contains("--login-server=https://hs.example/control"), log);
        assertTrue(log.contains("--hostname=env-abc"), log);
        assertFalse(log.contains(AUTH_KEY),
                "raw auth key must never appear on the long-lived argv log: " + log);
        assertEquals(CliNetAgentClient.needsUnattended(System.getProperty("os.name")),
                log.contains("--unattended"),
                "--unattended rides only on Windows (a Windows-only flag): " + log);
    }

    /**
     * Windows agents idle the moment the last LocalAPI client disconnects unless Unattended
     * Mode is set — `up` "succeeded" and the agent was never Running (wg-not-up forever).
     */
    @Test
    void needsUnattended_onlyOnWindows() {
        assertTrue(CliNetAgentClient.needsUnattended("Windows 10"));
        assertTrue(CliNetAgentClient.needsUnattended("Windows Server 2022"));
        assertFalse(CliNetAgentClient.needsUnattended("Mac OS X"));
        assertFalse(CliNetAgentClient.needsUnattended("Linux"));
        assertFalse(CliNetAgentClient.needsUnattended(null));
    }

    @Test
    void fakeCli_up_pinsRootSocketFlagBeforeSubcommand(@TempDir Path dir) throws Exception {
        Path argvLog = dir.resolve("argv.log");
        Path cli = writeFakeSqNet(dir, argvLog);
        CliNetAgentClient client = new CliNetAgentClient(cli.toString(), "sqnet0",
                new ObjectMapper(), "/var/run/sq-net-agent/agent.sock");
        client.up("https://hs.example/control", AUTH_KEY, "env-abc");
        String log = Files.readString(argvLog);
        assertTrue(log.contains("--socket=/var/run/sq-net-agent/agent.sock"), log);
        int socketAt = log.indexOf("--socket=/var/run/sq-net-agent/agent.sock");
        int upAt = log.indexOf("\nup\n");
        assertTrue(socketAt >= 0 && upAt > socketAt, "root --socket must precede up: " + log);
    }

    /**
     * A Running agent's status outgrows the pipe the JVM gives the child (~4 KB on Windows,
     * 64 KB here). Read only after exit, the CLI blocks on the full pipe until the timeout and
     * a Running agent is reported down — overlay never repointed, commands on the heartbeat queue.
     */
    @Test
    void status_largerThanThePipeBuffer_isReadInsteadOfTimingOut(@TempDir Path dir) throws Exception {
        Path cli = dir.resolve("sq-net");
        Files.writeString(cli, """
                #!/bin/sh
                printf '{"BackendState":"Running","Self":{"TailscaleIPs":["100.64.0.10"]},"Pad":"'
                head -c 262144 /dev/zero | tr '\\0' 'a'
                printf '"}\\n'
                """);
        makeExecutable(cli);
        CliNetAgentClient client = new CliNetAgentClient(cli.toString(), "sqnet0", new ObjectMapper());

        NetAgentStatus st = client.status();

        assertTrue(st.running());
        assertEquals("100.64.0.10", st.primaryIp().orElseThrow());
    }

    @Test
    void authKeyTempFileIsOwnerOnly() throws Exception {
        Path key = CliNetAgentClient.writeAuthKeyFile(AUTH_KEY);
        try {
            AclFileAttributeView acl = Files.getFileAttributeView(key, AclFileAttributeView.class);
            if (acl != null) {
                UserPrincipal owner = acl.getOwner();
                for (AclEntry entry : acl.getAcl()) {
                    if (entry.type() != AclEntryType.ALLOW) {
                        continue;
                    }
                    String name = entry.principal().getName();
                    assertFalse(name.toLowerCase(Locale.ROOT).contains("everyone"),
                            "Everyone must not have ALLOW on auth key: " + name);
                    assertFalse(name.toLowerCase(Locale.ROOT).contains("authenticated users"),
                            "Authenticated Users must not have ALLOW on auth key: " + name);
                    if (!entry.principal().equals(owner)) {
                        assertFalse(name.toLowerCase(Locale.ROOT).endsWith("\\users"),
                                "BUILTIN\\Users must not have ALLOW on auth key: " + name);
                    }
                }
            } else {
                assertEquals("rw-------",
                        PosixFilePermissions.toString(Files.getPosixFilePermissions(key)));
            }
        } finally {
            Files.deleteIfExists(key);
        }
    }

    /** Writes an executable stand-in for {@code sq-net} that logs argv and prints status JSON. */
    private static Path writeFakeSqNet(Path dir, Path argvLog) throws Exception {
        Path cli = dir.resolve("sq-net");
        // Built with replace, not formatted(): the script itself contains printf '%s'.
        String log = argvLog.toAbsolutePath().toString();
        String script = """
                #!/bin/sh
                # Record every invocation (the daemon deletes the auth-key file after up).
                printf '%s\\n' "$0" "$@" >> '__ARGV_LOG__'
                printf '---\\n' >> '__ARGV_LOG__'
                cmd="$1"
                case "$1" in --socket=*) shift; cmd="$1" ;; esac
                case "$cmd" in
                  version)
                    printf '1.102.0-sq-net\\n'
                    exit 0
                    ;;
                  status)
                    cat <<'JSON'
                __STATUS_JSON__
                JSON
                    exit 0
                    ;;
                  up|down|logout)
                    exit 0
                    ;;
                  *)
                    exit 1
                    ;;
                esac
                """.replace("__ARGV_LOG__", log).replace("__STATUS_JSON__", STATUS_JSON);
        Files.writeString(cli, script);
        makeExecutable(cli);
        return cli;
    }

    private static void makeExecutable(Path cli) throws Exception {
        try {
            Files.setPosixFilePermissions(cli, PosixFilePermissions.fromString("rwxr-xr-x"));
        } catch (UnsupportedOperationException ignored) {
            cli.toFile().setExecutable(true);
        }
    }
}
