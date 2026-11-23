package com.studioquack.sqs.sandbox.egress;

import com.studioquack.sqs.sandbox.runtime.CliRunner;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Runs the {@link OverlayEgressRules} argv in the network namespace that actually holds the
 * sandbox bridges, when that is not the daemon host's own. Rootless Podman — local, or inside a
 * rootless machine — keeps the bridges in a namespace entered with {@code podman unshare
 * --rootless-netns}, and deletes it, with every rule in it, when its last container exits; a
 * daemon-owned pause pod ({@value #ANCHOR_POD}) keeps it alive.
 */
public final class PodmanNetnsTarget {

    /** Pause pod keeping a rootless network namespace — and the rules in it — alive. */
    public static final String ANCHOR_POD = "sqs-egress-anchor";

    /** Carries the provenance label every uninstall path reaps by. */
    static final String ANCHOR_LABEL = "studioquack.sqs=egress-anchor";

    /** Machine names become argv; anything else is not a machine Podman created. */
    private static final Pattern MACHINE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");

    private static final Duration PODMAN_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration SCRIPT_TIMEOUT = Duration.ofSeconds(60);

    static final String RC_MARK = "__SQ_RC__ ";
    static final String ERR_MARK = "__SQ_ERR__ ";
    static final String OK_MARK = "__SQ_OK__";
    static final String FAIL_MARK = "__SQ_FAIL__ ";

    /** Runs one host process; {@code stdin} may be null. */
    @FunctionalInterface
    public interface Runner {
        CliRunner.Result run(List<String> argv, byte[] stdin, Duration timeout);
    }

    private final String cli;
    private final String description;
    private final List<String> shell;
    private final boolean needsAnchor;
    private final String anchorNetwork;
    private final Runner runner;

    PodmanNetnsTarget(String cli, String description, List<String> shell, boolean needsAnchor,
                      String anchorNetwork, Runner runner) {
        this.cli = cli;
        this.description = description;
        this.shell = List.copyOf(shell);
        this.needsAnchor = needsAnchor;
        this.anchorNetwork = anchorNetwork;
        this.runner = runner;
    }

    /** A machine target, as {@link #detectMachine} builds it. */
    static PodmanNetnsTarget machine(String cli, String machine, boolean rootful, String anchorNetwork,
                                     Runner runner) {
        return new PodmanNetnsTarget(cli, "Podman machine " + machine,
                List.of(cli, "machine", "ssh", machine,
                        rootful ? "sudo -n sh -s" : "podman unshare --rootless-netns sh -s"),
                !rootful, anchorNetwork, runner);
    }

    /** The local rootless target, as {@link #detectLocalRootless} builds it. */
    static PodmanNetnsTarget localRootless(String cli, String anchorNetwork, Runner runner) {
        return new PodmanNetnsTarget(cli, "rootless Podman",
                List.of(cli, "unshare", "--rootless-netns", "sh", "-s"), true, anchorNetwork, runner);
    }

    public static Runner defaultRunner() {
        return (argv, stdin, timeout) -> CliRunner.run(argv, stdin, timeout, 256 * 1024, 64 * 1024);
    }

    /**
     * The running default Podman machine, or null when there is none to enforce in: no machine,
     * not running, unreadable output, or an image without iptables (the rules are iptables argv).
     */
    public static PodmanNetnsTarget detectMachine(String cli, String anchorNetwork, Runner runner) {
        CliRunner.Result r = runner.run(List.of(cli, "machine", "inspect", "--format",
                "{{.Name}}|{{.State}}|{{.Rootful}}"), null, PODMAN_TIMEOUT);
        if (r == null || !r.ok()) {
            return null;
        }
        String[] p = firstLine(r.stdout()).split("\\|", -1);
        if (p.length < 3 || !MACHINE_NAME.matcher(p[0].trim()).matches()
                || !"running".equalsIgnoreCase(p[1].trim())) {
            return null;
        }
        return withIptables(machine(cli, p[0].trim(), Boolean.parseBoolean(p[2].trim()), anchorNetwork, runner));
    }

    /** Rootless local Podman with iptables in its namespace, or null (rootful / unreadable / none). */
    public static PodmanNetnsTarget detectLocalRootless(String cli, String anchorNetwork, Runner runner) {
        CliRunner.Result r = runner.run(List.of(cli, "info", "--format", "{{.Host.Security.Rootless}}"),
                null, PODMAN_TIMEOUT);
        if (r == null || !r.ok() || !"true".equalsIgnoreCase(firstLine(r.stdout()))) {
            return null;
        }
        return withIptables(localRootless(cli, anchorNetwork, runner));
    }

    private static PodmanNetnsTarget withIptables(PodmanNetnsTarget t) {
        return t.run(List.of("iptables", "--version")).ok() ? t : null;
    }

    /** Where the rules run, for logs and the readiness reason. */
    public String describe() {
        return description;
    }

    /** Runs one argv in the sandbox network namespace. */
    public SandboxEgressManager.ExecResult run(List<String> argv) {
        String script = "err=$(" + quoteAll(argv) + " 2>&1 >/dev/null)\n"
                + "rc=$?\n"
                + "printf '" + RC_MARK + "%s\\n' $rc\n"
                + "printf '%s\\n' \"$err\" | head -n 1 | sed 's/^/" + ERR_MARK + "/'\n";
        CliRunner.Result r = runScript(script);
        Integer rc = null;
        String err = "";
        for (String line : lines(r)) {
            if (line.startsWith(RC_MARK)) {
                rc = parseIntOrNull(line.substring(RC_MARK.length()).trim());
            } else if (line.startsWith(ERR_MARK)) {
                err = line.substring(ERR_MARK.length());
            }
        }
        if (rc == null) {
            return new SandboxEgressManager.ExecResult(false, -1, unreachable(r));
        }
        return new SandboxEgressManager.ExecResult(rc == 0, rc, err);
    }

    /**
     * {@code SandboxEgressManager.applyAll} in ONE round trip, with its exact semantics: a check
     * (any {@code -C}) that succeeds skips the command after it; a failing {@code ip rule del}
     * is ignored; any other failure is tolerated only when its error says the object already
     * exists, and otherwise stops the run. Returns that failure's first error line, or null.
     */
    public String applyAll(List<List<String>> commands) {
        CliRunner.Result r = runScript(batchScript(commands));
        for (String line : lines(r)) {
            if (line.startsWith(OK_MARK)) {
                return null;
            }
            if (line.startsWith(FAIL_MARK)) {
                String rest = line.substring(FAIL_MARK.length());
                int sp = rest.indexOf(' ');
                String msg = sp < 0 ? "" : rest.substring(sp + 1).trim();
                return msg.isEmpty() ? "rule command failed" : msg;
            }
        }
        return unreachable(r);
    }

    /**
     * Rootless only: makes sure the anchor pod exists and runs. Null on success, else the reason the
     * namespace cannot be held — the rules would vanish with it, so the caller fails closed.
     */
    public String ensureAnchor() {
        if (!needsAnchor) {
            return null;
        }
        CliRunner.Result exists = runner.run(List.of(cli, "pod", "exists", ANCHOR_POD), null, PODMAN_TIMEOUT);
        if (exists == null || exists.exitCode() != 0) {
            CliRunner.Result created = runner.run(List.of(cli, "pod", "create", "--name", ANCHOR_POD,
                    "--network", anchorNetwork, "--label", ANCHOR_LABEL), null, PODMAN_TIMEOUT);
            if (created == null || !created.ok()) {
                return "anchor pod create failed: " + firstLine(created == null ? "" : created.stderr());
            }
        }
        CliRunner.Result started = runner.run(List.of(cli, "pod", "start", ANCHOR_POD), null, PODMAN_TIMEOUT);
        return started != null && started.ok()
                ? null : "anchor pod start failed: " + firstLine(started == null ? "" : started.stderr());
    }

    /**
     * The interpreter behind {@link #applyAll}: {@code c<i>} runs command i, {@code k<i>} says how its
     * failure is judged. Mirrors the manager's Java loop step for step.
     */
    static String batchScript(List<List<String>> commands) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < commands.size(); i++) {
            List<String> cmd = commands.get(i);
            s.append('c').append(i).append("() { ").append(quoteAll(cmd)).append("; }\n");
            s.append('k').append(i).append('=').append(kind(cmd)).append('\n');
        }
        s.append("n=").append(commands.size()).append('\n')
                .append("i=0\n")
                .append("while [ $i -lt $n ]; do\n")
                .append("  err=$(c$i 2>&1 >/dev/null)\n")
                .append("  rc=$?\n")
                .append("  eval \"k=\\$k$i\"\n")
                .append("  case $k in\n")
                .append("    check) if [ $rc -eq 0 ]; then i=$((i+1)); fi ;;\n")
                .append("    plain) if [ $rc -ne 0 ]; then\n")
                .append("        case $(printf '%s' \"$err\" | tr 'A-Z' 'a-z') in\n")
                .append("          *exist*|*already*) ;;\n")
                .append("          *) printf '").append(FAIL_MARK).append("%s ' $i; printf '%s\\n' \"$err\" | head -n 1; exit 0 ;;\n")
                .append("        esac\n")
                .append("      fi ;;\n")
                .append("  esac\n")
                .append("  i=$((i+1))\n")
                .append("done\n")
                .append("echo ").append(OK_MARK).append('\n');
        return s.toString();
    }

    /** check | tol (ip rule del) | plain — the same split as the manager's isCheck / toleratedFailure. */
    private static String kind(List<String> cmd) {
        List<String> bare = cmd.size() >= 2 && "sudo".equals(cmd.get(0)) ? cmd.subList(2, cmd.size()) : cmd;
        if (bare.contains("-C")) {
            return "check";
        }
        if (bare.contains("rule") && bare.contains("del")) {
            return "tol";
        }
        return "plain";
    }

    private CliRunner.Result runScript(String script) {
        return runner.run(shell, script.getBytes(StandardCharsets.UTF_8), SCRIPT_TIMEOUT);
    }

    private String unreachable(CliRunner.Result r) {
        String why = r == null ? "" : firstLine(r.stderr().isBlank() ? r.stdout() : r.stderr());
        return description + " unreachable" + (why.isEmpty() ? "" : ": " + why);
    }

    static String quoteAll(List<String> argv) {
        StringBuilder b = new StringBuilder();
        for (String a : argv) {
            if (b.length() > 0) {
                b.append(' ');
            }
            b.append('\'').append(a.replace("'", "'\\''")).append('\'');
        }
        return b.toString();
    }

    private static List<String> lines(CliRunner.Result r) {
        return r == null || r.stdout() == null ? List.of() : r.stdout().lines().toList();
    }

    private static String firstLine(String s) {
        if (s == null) {
            return "";
        }
        return s.lines().map(String::trim).filter(l -> !l.isEmpty()).findFirst().orElse("");
    }

    private static Integer parseIntOrNull(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
