package com.studioquack.common.security.tls;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.Provider;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TLS 1.3 hybrid key-exchange helper for StudioQuack private-network application
 * legs ({@code :9876}, {@code :8443}, hub {@code :5005}, platform dispatch TLS).
 *
 * <p>JDK 25 SunJSSE accepts {@code X25519MLKEM768} on {@link SSLParameters#setNamedGroups}
 * and silently drops it. BouncyCastle JSSE implements the group; this helper
 * constructs BCJSSE {@link SSLContext}s bound to a {@link BouncyCastleProvider}
 * instance that is <em>not</em> inserted into the JVM-global provider list
 * (bc-java #2252: a JDK ML-KEM crypto provider disables the hybrid group).
 * Other TLS in the process (LLM providers, Stripe, Azure SDK, Docling, public
 * downloads) keeps SunJSSE.
 */
public final class HybridTls {

    /** IANA / BCJSSE name for the TLS 1.3 X25519 + ML-KEM-768 hybrid group. */
    public static final String HYBRID_GROUP = "X25519MLKEM768";

    /**
     * Groups advertised on our overlay contexts: hybrid first, then classic
     * fallback so a peer without BCJSSE still connects.
     */
    public static final String[] NAMED_GROUPS = {
            HYBRID_GROUP, "x25519", "secp256r1"
    };

    /**
     * Protocol offer order. TLS 1.2 is offered first so a peer still on SunJSSE can complete
     * the handshake. The hybrid named group applies on TLS 1.3 only.
     */
    private static final String[] TLS12_FIRST = {"TLSv1.2", "TLSv1.3"};
    private static final Logger LOG = LoggerFactory.getLogger(HybridTls.class);

    /** Crypto provider for the BCJSSE context only — never registered globally. */
    private static final BouncyCastleProvider CRYPTO;
    /** JSSE provider bound to {@link #CRYPTO}; not inserted into {@code Security}. */
    private static final Provider JSSE;
    private static final boolean AVAILABLE;

    /** Env ids already logged at INFO for {@code net.tls group=… env=…}. */
    private static final Set<String> LOGGED_ENVS = ConcurrentHashMap.newKeySet();

    static {
        BouncyCastleProvider crypto = null;
        Provider jsse = null;
        boolean ok = false;
        try {
            crypto = new BouncyCastleProvider();
            // Bind BC crypto to this JSSE instance (do not Security.insertProviderAt).
            jsse = new BouncyCastleJsseProvider(crypto);
            SSLContext probe = SSLContext.getInstance("TLSv1.3", jsse);
            probe.init(null, null, new SecureRandom());
            ok = supportsHybrid(probe);
        } catch (Throwable ignored) {
            // available() is false; newContext fails closed.
        }
        CRYPTO = crypto;
        JSSE = jsse;
        AVAILABLE = ok;
    }

    private HybridTls() {
    }

    /**
     * {@code true} when BCJSSE is constructible and accepts {@link #HYBRID_GROUP}.
     */
    public static boolean available() {
        // CRYPTO is the BC instance JSSE is bound to; both must stay alive.
        return AVAILABLE && CRYPTO != null && JSSE != null;
    }

    /**
     * Key managers for a BCJSSE context, from BCJSSE's OWN {@code KeyManagerFactory}.
     *
     * <p>Never hand BCJSSE a SunJSSE ({@code KeyManagerFactory.getDefaultAlgorithm()})
     * key manager: BCJSSE imports it but cannot select an <b>EC</b> credential through
     * it and refuses every cipher suite with {@code handshake_failure(40)} — "found no
     * selectable cipher suite". RSA keys happen to work, which is why this went
     * unnoticed until the EC mesh leaf (Sep 2026: cross-env LLM calls → 502 →
     * {@code Error_PeerUnreachable}). Every {@link #newContext} caller that presents a
     * certificate must build its key managers here.
     */
    public static KeyManager[] keyManagers(java.security.KeyStore keyStore, char[] password) {
        if (JSSE == null) {
            throw new IllegalStateException(
                    "hybrid TLS: BouncyCastle JSSE is not available on this classpath");
        }
        try {
            javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance("PKIX", JSSE);
            kmf.init(keyStore, password);
            return kmf.getKeyManagers();
        } catch (Exception ex) {
            throw new IllegalStateException("hybrid TLS: failed to build BCJSSE key managers", ex);
        }
    }

    /**
     * New TLSv1.3 context on BCJSSE. {@code kms}/{@code tms} may be {@code null}
     * (provider defaults). Does not change the JVM-global provider order.
     * Non-null {@code kms} must come from {@link #keyManagers} (see there).
     */
    public static SSLContext newContext(KeyManager[] kms, javax.net.ssl.TrustManager[] tms) {
        if (JSSE == null) {
            throw new IllegalStateException(
                    "hybrid TLS: BouncyCastle JSSE is not available on this classpath");
        }
        try {
            SSLContext ctx = SSLContext.getInstance("TLSv1.3", JSSE);
            ctx.init(kms, tms, new SecureRandom());
            return ctx;
        } catch (Exception ex) {
            throw new IllegalStateException("hybrid TLS: failed to create BCJSSE context", ex);
        }
    }

    /**
     * TLS 1.3-only parameters with {@link #NAMED_GROUPS} (hybrid first).
     *
     * @param clientMode unused by JSSE named-group selection; kept so callers
     *                   document which side they configure
     */
    public static SSLParameters hybridParameters(SSLContext ctx, boolean clientMode) {
        return parameters(ctx, clientMode, NAMED_GROUPS);
    }

    /**
     * Same as {@link #hybridParameters} with an explicit named-group list
     * (tests force {@link #HYBRID_GROUP} alone).
     */
    static SSLParameters parameters(SSLContext ctx, boolean clientMode, String[] namedGroups) {
        SSLParameters params = ctx != null ? ctx.getDefaultSSLParameters() : new SSLParameters();
        // Rollout-safe order: TLS 1.2 first (see TLS12_FIRST). The hybrid group still leads
        // the named-group list, so a BCJSSE↔BCJSSE pair negotiates TLS 1.3 + X25519MLKEM768.
        params.setProtocols(TLS12_FIRST);
        String[] groups = namedGroups == null || namedGroups.length == 0
                ? NAMED_GROUPS : namedGroups.clone();
        try {
            params.setNamedGroups(groups);
        } catch (IllegalArgumentException ex) {
            // Hybrid-only lists must fail loudly (the handshake test proves
            // the group is real). Mixed lists may drop an unknown hybrid name.
            if (groups.length == 1 && HYBRID_GROUP.equalsIgnoreCase(groups[0])) {
                throw ex;
            }
            params.setNamedGroups(new String[]{"x25519", "secp256r1"});
        }
        return params;
    }

    /**
     * Best-effort negotiated group. BCJSSE exposes it via
     * {@code BCExtendedSSLSession} / {@code SecurityParameters#getNegotiatedGroup};
     * returns {@code unknown} when the provider does not surface it.
     */
    public static String negotiatedGroup(SSLSession session) {
        if (session == null) {
            return "unknown";
        }
        String fromValues = fromSessionValues(session);
        if (fromValues != null) {
            return fromValues;
        }
        Object current = session;
        for (int depth = 0; depth < 8 && current != null; depth++) {
            String found = namedGroupFrom(current);
            if (found != null) {
                return found;
            }
            current = unwrapDelegate(current);
        }
        return "unknown";
    }

    /**
     * Logs {@code net.tls group=<name> env=<id>} once per environment at INFO.
     * Ids and group names only — no host, cert, or payload.
     */
    public static void logNegotiatedOnce(SSLSession session, String envId) {
        if (envId == null || envId.isBlank()) {
            return;
        }
        if (!LOGGED_ENVS.add(envId)) {
            return;
        }
        LOG.info("net.tls group={} env={}", negotiatedGroup(session), envId);
    }

    private static boolean supportsHybrid(SSLContext ctx) {
        try {
            SSLParameters supported = ctx.getSupportedSSLParameters();
            if (containsIgnoreCase(supported == null ? null : supported.getNamedGroups(), HYBRID_GROUP)) {
                return true;
            }
            SSLParameters p = ctx.getDefaultSSLParameters();
            p.setNamedGroups(new String[]{HYBRID_GROUP});
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private static String fromSessionValues(SSLSession session) {
        try {
            String[] names = session.getValueNames();
            if (names == null) {
                return null;
            }
            for (String name : names) {
                if (name == null || !name.toLowerCase(Locale.ROOT).contains("group")) {
                    continue;
                }
                Object v = session.getValue(name);
                String mapped = stringifyGroup(v);
                if (mapped != null) {
                    return mapped;
                }
            }
        } catch (RuntimeException ignored) {
            // Session value enumeration is best-effort.
        }
        return null;
    }

    private static String namedGroupFrom(Object o) {
        for (String methodName : new String[]{"getNegotiatedNamedGroup", "getNegotiatedGroup"}) {
            try {
                Method m = o.getClass().getMethod(methodName);
                String mapped = stringifyGroup(m.invoke(o));
                if (mapped != null) {
                    return mapped;
                }
            } catch (ReflectiveOperationException ignored) {
                // Try the next name / fields.
            }
        }
        Class<?> c = o.getClass();
        while (c != null && c != Object.class) {
            for (Field f : c.getDeclaredFields()) {
                String fn = f.getName().toLowerCase(Locale.ROOT);
                String tn = f.getType().getName();
                boolean interesting = fn.contains("securityparameters")
                        || fn.contains("sessionparameters")
                        || tn.contains("SecurityParameters")
                        || tn.contains("SessionParameters");
                if (!interesting) {
                    continue;
                }
                try {
                    f.setAccessible(true);
                    Object inner = f.get(o);
                    if (inner != null && inner != o) {
                        String mapped = namedGroupFrom(inner);
                        if (mapped != null) {
                            return mapped;
                        }
                    }
                } catch (ReflectiveOperationException ignored) {
                    // Keep walking.
                }
            }
            c = c.getSuperclass();
        }
        return null;
    }

    private static Object unwrapDelegate(Object o) {
        for (String methodName : new String[]{"getSession", "unwrap", "getDelegate"}) {
            try {
                Method m = o.getClass().getMethod(methodName);
                Object inner = m.invoke(o);
                if (inner != null && inner != o) {
                    return inner;
                }
            } catch (ReflectiveOperationException ignored) {
                // Next unwrap name.
            }
        }
        return null;
    }

    private static String stringifyGroup(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof String s) {
            return s.isBlank() ? null : s.trim();
        }
        if (v instanceof Integer n) {
            return n < 0 ? null : namedGroupName(n);
        }
        if (v instanceof Number n) {
            return namedGroupName(n.intValue());
        }
        String s = String.valueOf(v).trim();
        return s.isEmpty() || "null".equals(s) ? null : s;
    }

    private static String namedGroupName(int id) {
        try {
            Class<?> ng = Class.forName("org.bouncycastle.tls.NamedGroup");
            for (String methodName : new String[]{"getStandardName", "getName"}) {
                try {
                    Method m = ng.getMethod(methodName, int.class);
                    Object n = m.invoke(null, id);
                    if (n instanceof String s && !s.isBlank()) {
                        return s;
                    }
                } catch (NoSuchMethodException ignored) {
                    // Next spelling.
                }
            }
        } catch (ReflectiveOperationException ignored) {
            // Name table unavailable.
        }
        return null;
    }

    private static boolean containsIgnoreCase(String[] groups, String wanted) {
        if (groups == null) {
            return false;
        }
        for (String g : groups) {
            if (g != null && g.equalsIgnoreCase(wanted)) {
                return true;
            }
        }
        return false;
    }
}
