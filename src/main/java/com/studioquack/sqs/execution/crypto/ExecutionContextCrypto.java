package com.studioquack.sqs.execution.crypto;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * At-rest encryption for the {@code execution_context} store — ported byte-for-byte from
 * the control plane {@code com.studioquack.flow.security.ExecutionContextCrypto}, with the ONLY
 * change being where the key comes from: a {@link ContextKeyProvider} SPI (OS secret
 * manager in prod, dev property behind a guard) instead of a raw Spring property.
 *
 * <p>The cryptographic parameters are identical to the control plane so a row sealed by the
 * daemon and a row sealed by the control plane are interchangeable on the wire:
 * <ul>
 * <li><b>Cipher:</b> AES-GCM, 128-bit auth tag, random 96-bit (12-byte) IV per seal.</li>
 * <li><b>AAD binding:</b> the row's {@code nodeRefId} is fed as GCM AAD, so a ciphertext
 * copied onto another row refuses to open (swap/splice resistance).</li>
 * <li><b>Wire shape:</b> ciphertext hides inside the loosely-typed fields — a sealed map
 * is {@code {"$sqenc":"<b64>","v":1}} and a sealed list is
 * {@code [{"$sqenc":"<b64>","v":1}]} — so there is no schema migration and plaintext
 * rows keep reading transparently (marker absent → pass-through).</li>
 * <li><b>Failure policy:</b> fail-loud both ways — a seal failure aborts the write, and a
 * marked row with no configured key throws on read instead of leaking ciphertext.</li>
 * </ul>
 *
 * <p>Sealed structures are stored as JSON text. The cipher operates on {@code Map} and
 * {@code List} values and does not depend on the database.
 */
@Slf4j
@Component
public class ExecutionContextCrypto {

    /** Marker field carrying the Base64 {@code IV||ciphertext||tag} payload. */
    public static final String MARKER = "$sqenc";
    private static final String VERSION_FIELD = "v";
    private static final int CURRENT_VERSION = 1;
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * The sealed envelope embeds the Base64 ciphertext as one JSON string, so a legitimate
     * multi-tens-of-MB payload (agentic prompts with node-library results, Docling markdown)
     * blows past Jackson's stock 20 MB string cap on OPEN even though the write went through.
     * Bounded headroom instead of unlimited so a true runaway still fails fast.
     */
    private static final int MAX_ENVELOPE_STRING_LENGTH = 250_000_000;

    private final ObjectMapper objectMapper;
    private final ContextKeyProvider keyProvider;

    private SecretKey key;

    public ExecutionContextCrypto(ObjectMapper objectMapper, ContextKeyProvider keyProvider) {
        // copy() gets its own JsonFactory, so the raised cap stays local to the crypto instead
        // of loosening the app-wide mapper.
        this.objectMapper = objectMapper.copy();
        this.objectMapper.getFactory().setStreamReadConstraints(StreamReadConstraints.builder()
                .maxStringLength(MAX_ENVELOPE_STRING_LENGTH)
                .build());
        this.keyProvider = keyProvider;
    }

    /**
     * Resolves the key from the {@link ContextKeyProvider} and validates it. Runs automatically
     * at bean init; also callable directly when the crypto is constructed outside Spring (tests).
     */
    @PostConstruct
    public void init() {
        String keyBase64 = keyProvider == null ? null : keyProvider.contextEncryptionKeyBase64();
        String source = keyProvider == null ? "none" : keyProvider.source();
        String trimmed = keyBase64 == null ? "" : keyBase64.trim();
        if (trimmed.isEmpty()) {
            log.warn("execution_context at-rest encryption DISABLED (key source={}) — node "
                    + "payloads persist as PLAINTEXT. Wire up the OS secret-manager "
                    + "ContextKeyProvider. Plaintext rows remain "
                    + "readable after enabling, so the switch is safe to flip at any time.", source);
            return;
        }
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(trimmed);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("context encryption key (source=" + source
                    + ") is not valid Base64", e);
        }
        if (raw.length != 16 && raw.length != 24 && raw.length != 32) {
            throw new IllegalStateException("context encryption key (source=" + source
                    + ") must decode to 16/24/32 bytes, got " + raw.length);
        }
        this.key = new SecretKeySpec(raw, "AES");
        log.info("execution_context at-rest encryption ENABLED (AES-{}-GCM, key source={})",
                raw.length * 8, source);
    }

    /** True when a key is configured and payloads are sealed on write. */
    public boolean enabled() {
        return key != null;
    }

    /**
     * The algorithm name stamped into envelope metadata ({@code envelopeMeta.sealAlg}), derived
     * from the ACTUAL configured key — {@code AES-128/192/256-GCM} — or {@code null} when at-rest
     * encryption is disabled and rows persist as plaintext (the metadata must not overclaim).
     */
    public String sealAlgorithm() {
        return enabled() ? "AES-" + (key.getEncoded().length * 8) + "-GCM" : null;
    }

    /* ------------------------------------------------------------------ */
    /* Map payloads (contextBlob)                                          */
    /* ------------------------------------------------------------------ */

    /** Seals a map payload; null/empty and disabled-mode pass through. */
    public Map<String, Object> sealMap(Map<String, Object> plain, String aad) {
        if (plain == null || plain.isEmpty() || !enabled()) {
            return plain;
        }
        return sealEnvelope(plain, aad);
    }

    /** Opens a map payload; plaintext legacy rows pass through. */
    public Map<String, Object> openMap(Map<String, Object> stored, String aad) {
        if (stored == null || !stored.containsKey(MARKER)) {
            return stored;
        }
        Object opened = open(stored, aad);
        return objectMapper.convertValue(opened, new TypeReference<LinkedHashMap<String, Object>>() {});
    }

    /* ------------------------------------------------------------------ */
    /* List payloads (inputs / outputs)                                    */
    /* ------------------------------------------------------------------ */

    /** Seals a list payload; null/empty and disabled-mode pass through. */
    public List<Map<String, Object>> sealList(List<Map<String, Object>> plain, String aad) {
        if (plain == null || plain.isEmpty() || !enabled()) {
            return plain;
        }
        return List.of(sealEnvelope(plain, aad));
    }

    /** Opens a list payload; plaintext legacy rows pass through. */
    public List<Map<String, Object>> openList(List<Map<String, Object>> stored, String aad) {
        if (stored == null || stored.size() != 1) {
            return stored;
        }
        Map<String, Object> first = stored.get(0);
        if (first == null || !first.containsKey(MARKER)) {
            return stored;
        }
        Object opened = open(first, aad);
        return objectMapper.convertValue(opened, new TypeReference<List<Map<String, Object>>>() {});
    }

    /* ------------------------------------------------------------------ */
    /* Internals                                                           */
    /* ------------------------------------------------------------------ */

    private Map<String, Object> sealEnvelope(Object plain, String aad) {
        try {
            byte[] plaintext = objectMapper.writeValueAsBytes(plain);
            byte[] iv = new byte[IV_BYTES];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            if (aad != null && !aad.isEmpty()) {
                cipher.updateAAD(aad.getBytes(StandardCharsets.UTF_8));
            }
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] wire = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, wire, 0, iv.length);
            System.arraycopy(ciphertext, 0, wire, iv.length, ciphertext.length);
            Map<String, Object> envelope = new LinkedHashMap<>(2);
            envelope.put(MARKER, Base64.getEncoder().encodeToString(wire));
            envelope.put(VERSION_FIELD, CURRENT_VERSION);
            return envelope;
        } catch (Exception e) {
            // Fail-loud: encryption was REQUESTED (key configured); silently
            // persisting plaintext would be a security downgrade.
            throw new IllegalStateException(
                    "execution_context seal failed (aad=" + aad + "): " + e.getMessage(), e);
        }
    }

    private Object open(Map<String, Object> envelope, String aad) {
        if (!enabled()) {
            throw new IllegalStateException(
                    "execution_context row is encrypted but no decryption key is configured — "
                            + "supply the ContextKeyProvider key that sealed it");
        }
        try {
            byte[] wire = Base64.getDecoder().decode(String.valueOf(envelope.get(MARKER)));
            if (wire.length <= IV_BYTES) {
                throw new IllegalStateException("ciphertext too short");
            }
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BITS, wire, 0, IV_BYTES));
            if (aad != null && !aad.isEmpty()) {
                cipher.updateAAD(aad.getBytes(StandardCharsets.UTF_8));
            }
            byte[] plaintext = cipher.doFinal(wire, IV_BYTES, wire.length - IV_BYTES);
            return objectMapper.readValue(plaintext, Object.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "execution_context open failed (aad=" + aad + "): " + e.getMessage(), e);
        }
    }
}
