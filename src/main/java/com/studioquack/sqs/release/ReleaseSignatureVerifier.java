package com.studioquack.sqs.release;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Ed25519 check on a release manifest downloaded from the control plane {@code /releases/**} tree.
 * The trust root is the public key compiled into this binary, never one fetched from the origin
 * that serves the artefact: a compromised origin can serve a matching tarball + sha256, but it
 * cannot sign the manifest that carries the sha256.
 */
@Slf4j
@Component
public class ReleaseSignatureVerifier {

    /**
     * Base64 X.509 SubjectPublicKeyInfo of the current release key only.
     * The previous key is disclosed and must not be listed here.
     */
    static final List<String> PINNED_KEYS = List.of(
            "MCowBQYDK2VwAyEAFlcGLgDzkqL9aaiBi6FZw1T3UdswnliXSZqZK9ytp4s=");

    private final List<PublicKey> keys;
    private final boolean required;

    @Autowired
    public ReleaseSignatureVerifier(@Value("${sqs.release.signing.required:true}") boolean required) {
        this(PINNED_KEYS, required);
    }

    public ReleaseSignatureVerifier(List<String> base64Keys, boolean required) {
        this.keys = decode(base64Keys);
        this.required = required;
    }

    /** No pinned key, not required: what a caller without the bean (tests) gets. */
    public static ReleaseSignatureVerifier transitional() {
        return new ReleaseSignatureVerifier(List.of(), false);
    }

    /**
     * @param what           short label for logs/errors, e.g. {@code "node-library manifest"}
     * @param manifest       the exact bytes that were downloaded
     * @param signatureFile  the {@code .sig.v2} body, or {@code null} when the origin serves none
     * @throws ReleaseSignatureException when the manifest must not be used
     */
    public void verify(String what, byte[] manifest, byte[] signatureFile) {
        if (signatureFile == null) {
            if (required) {
                throw new ReleaseSignatureException(what + " is unsigned and sqs.release.signing.required=true");
            }
            log.warn("release-signing: {} is unsigned — accepted during the signing transition (T-13)", what);
            return;
        }
        if (keys.isEmpty()) {
            if (required) {
                throw new ReleaseSignatureException(what + ": this build pins no release key");
            }
            log.warn("release-signing: {} carries a signature but this build pins no release key — not verified", what);
            return;
        }
        byte[] sig;
        try {
            sig = Base64.getMimeDecoder().decode(new String(signatureFile, StandardCharsets.US_ASCII).trim());
        } catch (IllegalArgumentException e) {
            throw new ReleaseSignatureException(what + ": signature file is not base64");
        }
        for (PublicKey key : keys) {
            if (verifies(key, manifest, sig)) {
                return;
            }
        }
        throw new ReleaseSignatureException(what + ": signature does not verify against the pinned release key");
    }

    private static boolean verifies(PublicKey key, byte[] data, byte[] sig) {
        try {
            Signature s = Signature.getInstance("Ed25519");
            s.initVerify(key);
            s.update(data);
            return s.verify(sig);
        } catch (SignatureException e) {
            return false;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Ed25519 unavailable in this JRE", e);
        }
    }

    private static List<PublicKey> decode(List<String> base64Keys) {
        List<PublicKey> out = new ArrayList<>();
        try {
            KeyFactory kf = KeyFactory.getInstance("Ed25519");
            for (String b64 : base64Keys) {
                out.add(kf.generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(b64.trim()))));
            }
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalStateException("Pinned release key is not a valid Ed25519 SubjectPublicKeyInfo", e);
        }
        return List.copyOf(out);
    }
}
