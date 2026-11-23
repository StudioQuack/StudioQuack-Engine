package com.studioquack.sqs.sandbox.secrets;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A versioned secret reference, e.g. {@code sec_a1b2c3d4e5f60718293a4b5c:v3}.
 *
 * @param id The secret id ({@code sec_<24hex>}).
 * @param version The 1-based version.
 */
public record SecretRef(String id, int version) {

    private static final Pattern REF = Pattern.compile("^(sec_[0-9a-f]{24}):v([1-9][0-9]{0,5})$");

    /** Parses the wire string form, returning empty when malformed. */
    public static Optional<SecretRef> parse(String s) {
        if (s == null) {
            return Optional.empty();
        }
        Matcher m = REF.matcher(s.trim());
        if (!m.matches()) {
            return Optional.empty();
        }
        return Optional.of(new SecretRef(m.group(1), Integer.parseInt(m.group(2))));
    }

    /** The wire string form ({@code sec_…:vN}). */
    public String wire() {
        return id + ":v" + version;
    }
}
