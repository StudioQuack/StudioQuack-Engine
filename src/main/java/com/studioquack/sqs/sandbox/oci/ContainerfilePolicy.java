package com.studioquack.sqs.sandbox.oci;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Fail-closed check of the Containerfile {@code podman build} will actually compile. Buildah
 * honors per-instruction {@code RUN --network} and {@code RUN --mount} (CVE-2024-1753 /
 * CVE-2024-9675 class): a staged file can escape the build network and bind-mount the host even
 * when the CLI {@code --network} flag is pinned to {@code sqs-build}.
 */
public final class ContainerfilePolicy {

    /** A rejected instruction. The message names the line and the reason, not the file body. */
    public static final class Violation extends IllegalArgumentException {
        public final int line;

        public Violation(int line, String reason) {
            super("line " + line + ": " + reason);
            this.line = line;
        }
    }

    private ContainerfilePolicy() {
    }

    /**
     * The file Buildah picks when {@code -f} is omitted: {@code Containerfile},
     * then {@code Dockerfile}. {@code null} when neither is present.
     */
    public static Path resolveBuildFile(Path contextDir) {
        if (contextDir == null) {
            return null;
        }
        Path containerfile = contextDir.resolve("Containerfile");
        if (Files.isRegularFile(containerfile)) {
            return containerfile;
        }
        Path dockerfile = contextDir.resolve("Dockerfile");
        if (Files.isRegularFile(dockerfile)) {
            return dockerfile;
        }
        return null;
    }

    /** @throws Violation when a RUN flag is outside the allow-list, or a heredoc is unterminated */
    public static void validate(String content) {
        if (content == null) {
            throw new Violation(1, "Containerfile is missing");
        }
        String text = content.startsWith("\uFEFF") ? content.substring(1) : content;
        String[] raw = text.split("\n", -1);
        List<Phys> lines = new ArrayList<>(raw.length);
        for (int i = 0; i < raw.length; i++) {
            String row = raw[i];
            if (row.endsWith("\r")) {
                row = row.substring(0, row.length() - 1);
            }
            lines.add(new Phys(i + 1, row));
        }
        char escape = '\\';
        boolean directives = true;
        boolean sawEscape = false;
        List<Here> heredocs = new ArrayList<>();
        int i = 0;
        while (i < lines.size()) {
            if (!heredocs.isEmpty()) {
                Here here = heredocs.get(0);
                String cmp = here.stripTabs ? stripTabs(lines.get(i).text) : lines.get(i).text;
                if (cmp.equals(here.word)) {
                    heredocs.remove(0);
                }
                i++;
                continue;
            }
            Phys phys = lines.get(i);
            if (phys.text.isBlank()) {
                directives = false;
                i++;
                continue;
            }
            if (isComment(phys.text)) {
                if (directives && isDirective(phys.text)) {
                    escape = applyDirective(phys, escape, sawEscape);
                    // Only an escape directive consumes the one-shot slot. A
                    // preceding # syntax= must not hide a later # escape=.
                    if ("escape".equals(directiveKey(phys.text))) {
                        sawEscape = true;
                    }
                } else {
                    directives = false;
                }
                i++;
                continue;
            }
            directives = false;
            StringBuilder logical = new StringBuilder();
            int startLine = phys.number;
            while (true) {
                if (i >= lines.size()) {
                    throw new Violation(startLine, "unterminated line continuation");
                }
                String piece = lines.get(i).text;
                if (continues(piece, escape)) {
                    logical.append(piece, 0, piece.length() - 1);
                    i++;
                    if (i >= lines.size()) {
                        throw new Violation(startLine, "unterminated line continuation");
                    }
                    continue;
                }
                logical.append(piece);
                i++;
                break;
            }
            String instruction = logical.toString();
            parseRun(instruction, startLine);
            heredocs.addAll(findHeredocs(instruction, startLine));
        }
        if (!heredocs.isEmpty()) {
            throw new Violation(heredocs.get(0).openLine, "unterminated heredoc");
        }
    }

    private static String directiveKey(String text) {
        String body = text.trim().substring(1).trim();
        int eq = body.indexOf('=');
        return eq < 0 ? "" : body.substring(0, eq).trim().toLowerCase(Locale.ROOT);
    }

    private static char applyDirective(Phys phys, char current, boolean sawEscape) {
        String body = phys.text.trim().substring(1).trim();
        int eq = body.indexOf('=');
        String key = body.substring(0, eq).trim().toLowerCase(Locale.ROOT);
        String value = body.substring(eq + 1).trim();
        if (!"escape".equals(key)) {
            return current;
        }
        if (sawEscape) {
            throw new Violation(phys.number, "duplicate escape directive");
        }
        if (value.length() != 1 || (value.charAt(0) != '\\' && value.charAt(0) != '`')) {
            throw new Violation(phys.number, "invalid escape directive");
        }
        return value.charAt(0);
    }

    private static void parseRun(String instruction, int line) {
        List<String> tokens = tokens(instruction, line);
        if (tokens.isEmpty()) {
            return;
        }
        int idx = 0;
        if (!tokens.get(0).equalsIgnoreCase("RUN")) {
            if (!(tokens.get(0).equalsIgnoreCase("ONBUILD") && tokens.size() > 1
                    && tokens.get(1).equalsIgnoreCase("RUN"))) {
                return;
            }
            idx = 1;
        }
        // Exec form has no RUN flags. JSON contents are the command, not flags.
        if (idx + 1 < tokens.size() && tokens.get(idx + 1).startsWith("[")) {
            return;
        }
        idx++;
        while (idx < tokens.size()) {
            String token = tokens.get(idx);
            if (!token.startsWith("--")) {
                return;
            }
            if ("--".equals(token)) {
                return;
            }
            String name = token;
            String inline = null;
            int eq = token.indexOf('=');
            if (eq >= 0) {
                name = token.substring(0, eq);
                inline = token.substring(eq + 1);
            }
            if (name.startsWith("--network") || name.startsWith("--security") || name.startsWith("--device")
                    || !"--mount".equals(name)) {
                throw new Violation(line, "RUN flag " + bound(name) + " is not allowed");
            }
            String spec = inline;
            if (spec == null) {
                if (idx + 1 < tokens.size() && !tokens.get(idx + 1).startsWith("--")) {
                    spec = tokens.get(++idx);
                } else {
                    throw new Violation(line, "RUN --mount is missing its spec");
                }
            }
            checkMount(line, spec);
            idx++;
        }
    }

    private static void checkMount(int line, String spec) {
        if (spec.length() >= 2) {
            char q = spec.charAt(0);
            if ((q == '\'' || q == '"') && spec.charAt(spec.length() - 1) == q) {
                spec = spec.substring(1, spec.length() - 1);
            }
        }
        String type = null;
        boolean forbiddenOpt = false;
        for (String opt : splitOptions(spec)) {
            int eq = opt.indexOf('=');
            String key = (eq < 0 ? opt : opt.substring(0, eq)).trim().toLowerCase(Locale.ROOT);
            String value = eq < 0 ? "" : opt.substring(eq + 1).trim();
            if ("type".equals(key)) {
                type = value.toLowerCase(Locale.ROOT);
            } else if ("source".equals(key) || "src".equals(key) || "from".equals(key)) {
                forbiddenOpt = true;
            }
        }
        if (!"cache".equals(type) && !"tmpfs".equals(type)) {
            throw new Violation(line, "RUN --mount type " + bound(type == null ? "" : type) + " is not allowed");
        }
        if (forbiddenOpt) {
            throw new Violation(line, "RUN --mount type=" + type + " must not set source, src, or from");
        }
    }

    private static List<String> splitOptions(String spec) {
        List<String> opts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        char quote = 0;
        for (int i = 0; i < spec.length(); i++) {
            char c = spec.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
                cur.append(c);
                continue;
            }
            if (c == '\'' || c == '"') {
                quote = c;
                cur.append(c);
                continue;
            }
            if (c == ',') {
                opts.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        if (!cur.isEmpty()) {
            opts.add(cur.toString());
        }
        return opts;
    }

    private static List<String> tokens(String instruction, int line) {
        List<String> out = new ArrayList<>();
        int i = 0;
        int n = instruction.length();
        while (i < n) {
            while (i < n && Character.isWhitespace(instruction.charAt(i))) {
                i++;
            }
            if (i >= n) {
                break;
            }
            StringBuilder sb = new StringBuilder();
            char quote = 0;
            while (i < n) {
                char c = instruction.charAt(i);
                if (quote != 0) {
                    if (c == quote) {
                        quote = 0;
                    }
                    sb.append(c);
                    i++;
                    continue;
                }
                if (c == '\'' || c == '"') {
                    quote = c;
                    sb.append(c);
                    i++;
                    continue;
                }
                if (Character.isWhitespace(c)) {
                    break;
                }
                sb.append(c);
                i++;
            }
            if (quote != 0) {
                throw new Violation(line, "unterminated quote");
            }
            if (!sb.isEmpty()) {
                out.add(sb.toString());
            }
        }
        return out;
    }

    /** Heredoc redirects outside quotes. Body lines must not be parsed as instructions. */
    private static List<Here> findHeredocs(String instruction, int line) {
        List<Here> found = new ArrayList<>();
        int i = 0;
        int n = instruction.length();
        while (i < n) {
            char c = instruction.charAt(i);
            if (c == '\'' || c == '"') {
                int end = instruction.indexOf(c, i + 1);
                if (end < 0) {
                    break;
                }
                i = end + 1;
                continue;
            }
            if (c == '<' && i + 1 < n && instruction.charAt(i + 1) == '<') {
                i += 2;
                boolean strip = false;
                if (i < n && instruction.charAt(i) == '-') {
                    strip = true;
                    i++;
                }
                while (i < n && Character.isWhitespace(instruction.charAt(i))) {
                    i++;
                }
                if (i >= n) {
                    throw new Violation(line, "invalid heredoc");
                }
                char q = instruction.charAt(i);
                String word;
                if (q == '\'' || q == '"') {
                    int end = instruction.indexOf(q, i + 1);
                    if (end < 0) {
                        throw new Violation(line, "invalid heredoc");
                    }
                    word = instruction.substring(i + 1, end);
                    i = end + 1;
                } else {
                    int start = i;
                    while (i < n && !Character.isWhitespace(instruction.charAt(i)) && instruction.charAt(i) != '<') {
                        i++;
                    }
                    word = instruction.substring(start, i);
                }
                if (word.isEmpty()) {
                    throw new Violation(line, "invalid heredoc");
                }
                found.add(new Here(word, strip, line));
                continue;
            }
            i++;
        }
        return found;
    }

    private static boolean continues(String piece, char escape) {
        int count = 0;
        for (int k = piece.length() - 1; k >= 0 && piece.charAt(k) == escape; k--) {
            count++;
        }
        return (count % 2) == 1;
    }

    private static boolean isComment(String text) {
        int i = 0;
        while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
            i++;
        }
        return i < text.length() && text.charAt(i) == '#';
    }

    private static boolean isDirective(String text) {
        String body = text.trim().substring(1).trim();
        int eq = body.indexOf('=');
        if (eq <= 0) {
            return false;
        }
        String key = body.substring(0, eq).trim();
        if (key.isEmpty()) {
            return false;
        }
        for (int i = 0; i < key.length(); i++) {
            char c = key.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '-') {
                return false;
            }
        }
        return true;
    }

    private static String stripTabs(String text) {
        int i = 0;
        while (i < text.length() && text.charAt(i) == '\t') {
            i++;
        }
        return text.substring(i);
    }

    private static String bound(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.length() > 64 ? value.substring(0, 64) : value;
        return trimmed.replace('\n', ' ').replace('\r', ' ');
    }

    private record Phys(int number, String text) {
    }

    private record Here(String word, boolean stripTabs, int openLine) {
    }
}
