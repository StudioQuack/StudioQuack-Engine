package com.studioquack.common.security.tls;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;
import java.net.Socket;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.function.Supplier;

/**
 * {@link X509ExtendedKeyManager} that follows a rotating certificate <em>without rebuilding
 * the SSLContext</em>: the delegate is rebuilt whenever the leaf reference from
 * {@code leafSource} changes, so a renewed cert is presented on the very next TLS handshake —
 * no listener restart, no dropped in-flight connection.
 *
 * <p><b>Why this exists.</b> A certificate baked into an SSLContext at listener construction
 * is presented until the process restarts. For time-boxed credentials (the 24-h mesh listener
 * leaf, the 1-year mTLS enrolment leaf) that turns a routine expiry into a full outage of the
 * leg, recoverable only by restart. Both daemon listeners now source their leaf through this
 * manager, so the regular renewal flows (per-handshake mesh re-sign, daily mTLS re-enrol)
 * take effect immediately.
 *
 * <p>The delegate is always built by {@link HybridTls#keyManagers} (BCJSSE's own factory), so
 * EC-credential selection keeps working — this class only routes, it never selects credentials
 * itself. The leaf reference is the version marker: callers install a freshly parsed
 * certificate object on every renewal, so a reference change means "renewed". The private key
 * is stable for the credential's lifetime, so a torn read between chain and key is impossible.
 *
 * <p>Fail-closed: when {@code leafSource} yields {@code null} (pre-enrolment) or the delegate
 * build fails, every selection answers {@code null} and the handshake is refused — a wrong or
 * missing certificate is never presented.
 */
public final class RotatingKeyManager extends X509ExtendedKeyManager {

    private final Supplier<X509Certificate> leafSource;
    private final Supplier<PrivateKey> keySource;
    /** The leaf {@link #delegate} was built from; reference-compared. */
    private volatile X509Certificate builtFor;
    /** BCJSSE-built delegate presenting {@link #builtFor}; {@code null} when no leaf is held. */
    private volatile X509ExtendedKeyManager delegate;

    public RotatingKeyManager(Supplier<X509Certificate> leafSource, Supplier<PrivateKey> keySource) {
        this.leafSource = leafSource;
        this.keySource = keySource;
    }

    /** The delegate for the CURRENT leaf, rebuilding once per renewal. {@code null} pre-cert. */
    private X509ExtendedKeyManager current() {
        X509Certificate leaf = leafSource.get();
        X509ExtendedKeyManager d = delegate;
        if (d != null && builtFor == leaf) {
            return d;
        }
        synchronized (this) {
            if (delegate == null || builtFor != leaf) {
                delegate = build(leaf);
                builtFor = leaf;
            }
            return delegate;
        }
    }

    private X509ExtendedKeyManager build(X509Certificate leaf) {
        if (leaf == null) {
            return null;
        }
        try {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(null, null);
            char[] pw = "rotate".toCharArray();
            ks.setKeyEntry("leaf", keySource.get(), pw, new Certificate[]{leaf});
            for (KeyManager km : HybridTls.keyManagers(ks, pw)) {
                if (km instanceof X509ExtendedKeyManager x509) {
                    return x509;
                }
            }
            return null;
        } catch (Exception ex) {
            // Fail closed: no delegate → no alias → the handshake is refused, never a wrong cert.
            return null;
        }
    }

    @Override
    public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.chooseServerAlias(keyType, issuers, socket);
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        // HttpsServer handshakes over SSLEngine — this is the method that actually runs.
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.chooseEngineServerAlias(keyType, issuers, engine);
    }

    @Override
    public X509Certificate[] getCertificateChain(String alias) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.getCertificateChain(alias);
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.getPrivateKey(alias);
    }

    @Override
    public String[] getServerAliases(String keyType, Principal[] issuers) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.getServerAliases(keyType, issuers);
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] issuers) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.getClientAliases(keyType, issuers);
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.chooseClientAlias(keyType, issuers, socket);
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        X509ExtendedKeyManager d = current();
        return d == null ? null : d.chooseEngineClientAlias(keyType, issuers, engine);
    }
}
