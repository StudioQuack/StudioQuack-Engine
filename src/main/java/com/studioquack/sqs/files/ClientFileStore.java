package com.studioquack.sqs.files;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.studioquack.sqs.execution.crypto.ExecutionContextCrypto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The single secure path every file on the client travels. Duckbot attachments, webhook uploads
 * relayed as base64, a node's deploy-time {@code file} parameter, an oversized parameter spilled
 * out of the execution context, and anything handed between sandboxes all go through here.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ClientFileStore {

    /** Prefix marking a parameter value as a handle into this store. */
    public static final String REF_PREFIX = "sq-file:";

    /** Reclaimed when the run reaches a terminal state. */
    public static final String SCOPE_RUN = "RUN";
    /** Reclaimed when the workflow's deployment is replaced or removed. */
    public static final String SCOPE_DEPLOY = "DEPLOY";

    /** Key inside the sealed envelope holding the base64 content. */
    private static final String CONTENT_KEY = "b64";

    private final ClientFileRepository repository;
    private final ExecutionContextCrypto crypto;
    private final ObjectMapper objectMapper;

    /**
     * Per-file ceiling. The sandbox scratch is a 64 MiB tmpfs shared with the script, the {@code .env}
     * and every other file the node needs, so the limit is well under that: a file that cannot be
     * materialised is better refused at ingest, where the error is actionable, than at exec time.
     */
    @Value("${sqs.files.max-bytes:16777216}")
    private long maxBytes;

    /** What a caller gets back when resolving a ref. */
    public record StoredFile(String id, String fileName, String mimeType, long size, byte[] content) { }

    /** True when {@code value} is a handle into this store. */
    public static boolean isRef(Object value) {
        return value instanceof String s && s.startsWith(REF_PREFIX);
    }

    /**
     * Seals {@code content} and returns its {@code sq-file:<id>} handle.
     *
     * @throws IllegalArgumentException when the file exceeds {@link #maxBytes}, so an oversized input
     *                                  fails where the caller can report it rather than mid-execution
     */
    @Transactional
    public String store(String scope, String scopeKey, String fileName, String mimeType, byte[] content) {
        if (content == null) {
            throw new IllegalArgumentException("file content is required");
        }
        if (content.length > maxBytes) {
            throw new IllegalArgumentException(
                    "file '" + safeName(fileName) + "' is " + content.length
                            + " bytes, above the " + maxBytes + " byte client limit");
        }
        String id = UUID.randomUUID().toString();
        String name = safeName(fileName);

        Map<String, Object> plain = new LinkedHashMap<>();
        plain.put(CONTENT_KEY, Base64.getEncoder().encodeToString(content));
        String envelope = writeEnvelope(crypto.sealMap(plain, id), id);

        repository.save(ClientFileEntity.builder()
                .id(id)
                .scope(scope == null ? SCOPE_RUN : scope)
                .scopeKey(scopeKey == null ? "" : scopeKey)
                .fileName(name)
                .mimeType(mimeType)
                .sizeBytes(content.length)
                .contentEnvelope(envelope)
                .createdAt(OffsetDateTime.now())
                .build());

        log.debug("client-file.stored id={} name={} bytes={} scope={}/{}",
                id, name, content.length, scope, scopeKey);
        return REF_PREFIX + id;
    }

    /** Resolves a {@code sq-file:<id>} handle, or empty when it is unknown or cannot be opened. */
    @Transactional(readOnly = true)
    public Optional<StoredFile> resolve(Object ref) {
        if (!isRef(ref)) {
            return Optional.empty();
        }
        String id = ((String) ref).substring(REF_PREFIX.length());
        ClientFileEntity entity = repository.findById(id).orElse(null);
        if (entity == null) {
            log.warn("client-file.missing id={} — the referencing parameter cannot be materialised", id);
            return Optional.empty();
        }
        byte[] content = openContent(entity);
        if (content == null) {
            return Optional.empty();
        }
        return Optional.of(new StoredFile(
                entity.getId(), entity.getFileName(), entity.getMimeType(), entity.getSizeBytes(), content));
    }

    /** Everything held for one run or one workflow's deployment. */
    @Transactional(readOnly = true)
    public List<ClientFileEntity> listForScope(String scope, String scopeKey) {
        return repository.findByScopeAndScopeKey(scope, scopeKey);
    }

    /** Reclaims a run's files on terminal status, or a workflow's on redeploy. @return rows removed. */
    @Transactional
    public long deleteScope(String scope, String scopeKey) {
        if (scope == null || scopeKey == null || scopeKey.isBlank()) {
            return 0;
        }
        long removed = repository.deleteByScopeAndScopeKey(scope, scopeKey);
        if (removed > 0) {
            log.debug("client-file.scope_cleared scope={}/{} files={}", scope, scopeKey, removed);
        }
        return removed;
    }

    /** Backstop for rows whose owning run never reported terminal. @return rows removed. */
    @Transactional
    public long deleteOlderThan(OffsetDateTime cutoff) {
        return repository.deleteByCreatedAtBefore(cutoff);
    }

    private String writeEnvelope(Map<String, Object> sealed, String id) {
        try {
            return objectMapper.writeValueAsString(sealed);
        } catch (Exception e) {
            // Fail loud, matching the execution-context policy: silently storing unsealed content
            // would put file plaintext in the database without anyone noticing.
            throw new IllegalStateException("client-file: could not serialise sealed envelope for " + id, e);
        }
    }

    @SuppressWarnings("unchecked")
    private byte[] openContent(ClientFileEntity entity) {
        try {
            Map<String, Object> stored = objectMapper.readValue(entity.getContentEnvelope(), Map.class);
            Map<String, Object> plain = crypto.openMap(stored, entity.getId());
            Object b64 = plain == null ? null : plain.get(CONTENT_KEY);
            if (b64 == null) {
                log.warn("client-file.unreadable id={} — envelope carried no content", entity.getId());
                return null;
            }
            return Base64.getDecoder().decode(String.valueOf(b64));
        } catch (Exception e) {

            log.warn("client-file.open_failed id={} type={}", entity.getId(), e.getClass().getSimpleName());
            return null;
        }
    }

    /**
     * Any separator is dropped and the remainder is restricted to an allowlist, which also keeps
     * shell-significant characters out of the bootstrap that decodes it.
     */
    static String safeName(String raw) {
        if (raw == null || raw.isBlank()) {
            return "file";
        }
        String base = raw.replace('\\', '/');
        int slash = base.lastIndexOf('/');
        if (slash >= 0) {
            base = base.substring(slash + 1);
        }
        base = base.replaceAll("[^A-Za-z0-9._-]", "_");
        // A name of dots only would still resolve to a directory entry.
        while (base.startsWith(".")) {
            base = base.substring(1);
        }
        if (base.isBlank()) {
            return "file";
        }
        return base.length() > 128 ? base.substring(base.length() - 128) : base;
    }
}
