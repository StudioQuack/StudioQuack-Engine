package com.studioquack.sqs.channel.wg;

import lombok.extern.slf4j.Slf4j;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Ensures Windows Firewall allows BE→daemon overlay reverse traffic on the mesh CGNAT range (
 * {@code 100.64.0.0/10} → TCP {@code:9876}/{@code:8443}). Handshake can succeed (UDP) while Defender still
 * drops inbound TCP on the tunnel adapter.
 */
@Slf4j
public final class WindowsOverlayFirewall {

    /**
     * CGNAT overlay range the mesh agent assigns.
     */
    public static final String DEFAULT_OVERLAY_REMOTE = "100.64.0.0/10";

    private static final String RULE_NAME = "StudioQuack mesh overlay listeners";

    private WindowsOverlayFirewall() {
    }

    public static void ensureInboundAllow(int localPort) {
        ensureInboundAllow(localPort, DEFAULT_OVERLAY_REMOTE);
    }

    public static void ensureInboundAllow(int localPort, String remoteCidr) {
        if (localPort <= 0 || localPort > 65535) {
            return;
        }
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (!os.contains("win")) {
            return;
        }
        String remote = (remoteCidr == null || remoteCidr.isBlank())
                ? DEFAULT_OVERLAY_REMOTE : remoteCidr.trim();
        try {
            // Delete-then-add so a stale port/scope from a prior install is replaced.
            runNetsh("advfirewall", "firewall", "delete", "rule", "name=" + RULE_NAME);
            int code = runNetsh(
                    "advfirewall", "firewall", "add", "rule",
                    "name=" + RULE_NAME,
                    "dir=in",
                    "action=allow",
                    "protocol=TCP",
                    "localport=" + localPort,
                    "remoteip=" + remote,
                    "profile=any",
                    "enable=yes",
                    "description=StudioQuack BE/peer to daemon over the private mesh");
            if (code == 0) {
                log.info("channel/overlay: Windows firewall allow ok (TCP {} from {})", localPort, remote);
            } else {
                log.warn("channel/overlay: Windows firewall allow failed (exit {}); "
                                + "overlay commands may fall back to heartbeat until an elevated rule exists "
                                + "for TCP {} from {}",
                        code, localPort, remote);
            }
        } catch (Exception ex) {
            log.warn("channel/overlay: Windows firewall ensure failed ({}); "
                            + "overlay commands may fall back to heartbeat",
                    ex.toString());
        }
    }

    /**
     * Best-effort: drop the inbound overlay allow rule created at listener start.
     *
     * @return {@code true} when this host is Windows and {@code netsh} was invoked
     */
    public static boolean removeInboundAllow() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (!os.contains("win")) {
            return false;
        }
        try {
            int code = runNetsh("advfirewall", "firewall", "delete", "rule", "name=" + RULE_NAME);
            if (code == 0) {
                log.info("channel/overlay: Windows firewall rule removed ({})", RULE_NAME);
            } else {
                log.warn("channel/overlay: Windows firewall delete failed (exit {}); rule may remain: {}",
                        code, RULE_NAME);
            }
        } catch (Exception ex) {
            log.warn("channel/overlay: Windows firewall delete failed ({})", ex.toString());
        }
        return true;
    }

    private static int runNetsh(String... args) throws Exception {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command().add("netsh.exe");
        for (String a : args) {
            pb.command().add(a);
        }
        pb.redirectErrorStream(true);
        Process p = pb.start();
        boolean finished = p.waitFor(15, TimeUnit.SECONDS);
        if (!finished) {
            p.destroyForcibly();
            return -1;
        }
        return p.exitValue();
    }
}
