package com.studioquack.sqs.command;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Identity of the running daemon jar for the {@code status} payload: sha256 of the boot jar, its size and
 * modification time, plus the JVM. Computed once.
 */
final class DaemonBuildIdentity {

    private static volatile Map<String, Object> cached;

    private DaemonBuildIdentity() {
    }

    static Map<String, Object> snapshot() {
        Map<String, Object> c = cached;
        if (c != null) {
            return c;
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("javaVersion", System.getProperty("java.version"));
        try {
            CodeSource src = DaemonBuildIdentity.class.getProtectionDomain().getCodeSource();
            if (src != null && src.getLocation() != null) {
                Path jar = jarPathOf(src.getLocation().toURI().getSchemeSpecificPart());
                if (jar != null && Files.isRegularFile(jar)) {
                    m.put("jar", jar.getFileName().toString());
                    m.put("jarBytes", Files.size(jar));
                    FileTime mt = Files.getLastModifiedTime(jar);
                    m.put("jarModified", mt.toInstant().toString());
                    m.put("jarSha256", sha256(jar));
                }
            }
        } catch (Exception ex) {
            m.put("error", ex.toString());
        }
        cached = Map.copyOf(m);
        return cached;
    }

    /**
     * The boot jar's path from a code-source location. Spring Boot nested URLs look like
     * {@code nested:/path/app.jar/!BOOT-INF/classes/!/}; Windows paths arrive as {@code /C:/Users/…}, which only a
     * {@code file:} URI turns into {@code C:\Users\…} ({@code Path.of} rejects the colon).
     */
    static Path jarPathOf(String location) throws URISyntaxException {
        if (location == null) {
            return null;
        }
        String s = location;
        for (boolean stripped = true; stripped; ) {
            stripped = false;
            for (String scheme : new String[] {"jar:", "nested:", "file:"}) {
                if (s.startsWith(scheme)) {
                    s = s.substring(scheme.length());
                    stripped = true;
                }
            }
        }
        int bang = s.indexOf("!/");
        if (bang >= 0) {
            s = s.substring(0, bang);
        }
        int nestedBang = s.indexOf("/!");
        if (nestedBang >= 0) {
            s = s.substring(0, nestedBang);
        }
        while (s.startsWith("//")) {
            s = s.substring(1);
        }
        if (!s.startsWith("/")) {
            s = "/" + s;
        }
        return Path.of(new URI("file", null, s, null));
    }

    private static String sha256(Path jar) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(jar)) {
            byte[] buf = new byte[1 << 16];
            int n;
            while ((n = in.read(buf)) > 0) {
                md.update(buf, 0, n);
            }
        }
        return HexFormat.of().formatHex(md.digest());
    }
}
