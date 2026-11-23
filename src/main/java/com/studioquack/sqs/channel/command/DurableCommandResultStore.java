package com.studioquack.sqs.channel.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.CRC32C;

/**
 * Durable record of executed command results — the crash-safe backing of {@link DaemonCommandRegistry}'s
 * in-memory dedup cache. Why it exists.
 */
@Slf4j
@Component
public class DurableCommandResultStore implements AutoCloseable {

    private static final byte[] MAGIC = "SQCR".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_LEN = MAGIC.length + 4;
    private static final int MAX_RECORD = 1 << 20; // 1 MiB — a `logs` result carries up to 200 lines

    /**
     * Default TTL mirrors the registry's dedup window (7 days).
     */
    static final long DEFAULT_TTL_MS = 604_800_000L;

    /**
     * JSON record persisted per executed command.
     */
    public record StoredCommandResult(String commandId, String type, String status,
                                      String output, String error, long completedAtMs) {
    }

    private final Path path;
    private final ObjectMapper mapper;
    private final long ttlMs;
    private final Object lock = new Object();
    /**
     * CommandId → terminal result (insertion-ordered); rebuilt from the file at open.
     */
    private final Map<String, DaemonCommandResult> results = new LinkedHashMap<>();
    private final Map<String, Long> completedAt = new HashMap<>();
    /**
     * {@code null} when the file could not be opened — store disabled for this boot.
     */
    private volatile RandomAccessFile raf;

    @Autowired
    public DurableCommandResultStore(ObjectMapper mapper,
                                     @Value("${sqs.channel.command-results-path:}") String pathProp,
                                     @Value("${sqs.channel.command-results-ttl-ms:604800000}") long ttlMs) {
        this.mapper = mapper;
        this.path = resolvePath(pathProp);
        this.ttlMs = ttlMs > 0 ? ttlMs : DEFAULT_TTL_MS;
    }

    private DurableCommandResultStore(Path path, ObjectMapper mapper, long ttlMs) {
        this.path = path;
        this.mapper = mapper;
        this.ttlMs = ttlMs > 0 ? ttlMs : DEFAULT_TTL_MS;
    }

    /**
     * Test/embedded factory: open (or create) a store at an explicit path, replaying it immediately.
     */
    public static DurableCommandResultStore openAt(Path path, ObjectMapper mapper, long ttlMs) throws IOException {
        DurableCommandResultStore store = new DurableCommandResultStore(path, mapper, ttlMs);
        store.open();
        return store;
    }

    @PostConstruct
    void init() {
        try {
            open();
        } catch (IOException e) {
            log.warn("command-result-store: could not open {} ({}); result durability disabled this boot.",
                    path, e.getMessage());
        }
    }

    private void open() throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw");
        try {
            if (file.length() == 0) {
                file.write(MAGIC);
                file.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(VERSION).array());
                file.getFD().sync();
            } else {
                verifyHeader(file);
            }
            this.raf = file;
            int kept = replay(file);
            log.info("command-result-store: replayed path={} results={} ttlMs={}", path, kept, ttlMs);
        } catch (IOException e) {
            try {
                file.close();
            } catch (IOException ignored) {
                // Preserve the open failure as the reported one.
            }
            throw e;
        }
    }

    private void verifyHeader(RandomAccessFile file) throws IOException {
        byte[] hdr = new byte[HEADER_LEN];
        file.seek(0);
        file.readFully(hdr);
        for (int i = 0; i < MAGIC.length; i++) {
            if (hdr[i] != MAGIC[i]) {
                throw new IOException("command-result-store: bad magic; refusing to overwrite " + path);
            }
        }
        int v = ByteBuffer.wrap(hdr, MAGIC.length, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (v != VERSION) {
            throw new IOException("command-result-store: unsupported format_version " + v
                    + " (we speak " + VERSION + ")");
        }
    }

    /**
     * Folds the log into the in-memory index, latest record per commandId, dropping entries older than the TTL. A
     * torn / bad-CRC tail stops the scan — everything before it is intact.
     *
     * @return how many live results were loaded
     */
    private int replay(RandomAccessFile file) throws IOException {
        synchronized (lock) {
            long cutoff = System.currentTimeMillis() - ttlMs;
            long len = file.length();
            long pos = HEADER_LEN;
            file.seek(HEADER_LEN);
            while (pos + 8 <= len) {
                byte[] hdr = new byte[8];
                file.readFully(hdr);
                ByteBuffer hb = ByteBuffer.wrap(hdr).order(ByteOrder.LITTLE_ENDIAN);
                int size = hb.getInt();
                int crcExpected = hb.getInt();
                if (size < 0 || size > MAX_RECORD || pos + 8 + size + 1 > len) {
                    log.warn("command-result-store: torn/bogus record header at offset {}", pos);
                    break;
                }
                byte[] body = new byte[size];
                file.readFully(body);
                int nl = file.read();
                CRC32C crc = new CRC32C();
                crc.update(body, 0, body.length);
                if ((int) crc.getValue() != crcExpected) {
                    log.warn("command-result-store: bad CRC at offset {}; stopping replay", pos);
                    break;
                }
                if (nl != '\n') {
                    log.warn("command-result-store: missing record terminator at offset {}", pos);
                    break;
                }
                try {
                    StoredCommandResult rec = mapper.readValue(body, StoredCommandResult.class);
                    if (rec.commandId() != null && !rec.commandId().isBlank()
                            && rec.completedAtMs() >= cutoff) {
                        results.put(rec.commandId(), new DaemonCommandResult(
                                rec.commandId(), rec.type(), rec.status(), rec.output(), rec.error()));
                        completedAt.put(rec.commandId(), rec.completedAtMs());
                    }
                } catch (Exception e) {
                    log.warn("command-result-store: bad json at offset {}: {}", pos, e.getMessage());
                }
                pos += 8 + size + 1;
            }
            file.seek(file.length());
            return results.size();
        }
    }

    /**
     * The stored terminal result for {@code commandId}, empty when unknown or expired.
     */
    public Optional<DaemonCommandResult> load(String commandId) {
        if (commandId == null || commandId.isBlank()) {
            return Optional.empty();
        }
        synchronized (lock) {
            DaemonCommandResult result = results.get(commandId);
            if (result == null) {
                return Optional.empty();
            }
            Long at = completedAt.get(commandId);
            if (at == null || at < System.currentTimeMillis() - ttlMs) {
                results.remove(commandId);
                completedAt.remove(commandId);
                return Optional.empty();
            }
            return Optional.of(result);
        }
    }

    /**
     * Persists a terminal result (append + fsync) and indexes it for {@link #load}. No-op for a blank id, and
     * silently skipped when the store is disabled this boot — the caller's command flow must never fail because
     * durability did.
     */
    public void record(DaemonCommandResult result) {
        if (result == null || result.commandId() == null || result.commandId().isBlank()) {
            return;
        }
        RandomAccessFile file = raf;
        if (file == null) {
            return;
        }
        long now = System.currentTimeMillis();
        byte[] body;
        try {
            body = mapper.writeValueAsBytes(new StoredCommandResult(result.commandId(), result.type(),
                    result.status(), result.output(), result.error(), now));
        } catch (Exception e) {
            log.warn("command-result-store: serialise failed id={}: {}", result.commandId(), e.getMessage());
            return;
        }
        if (body.length > MAX_RECORD) {
            log.warn("command-result-store: result id={} too large ({} B) — not persisted",
                    result.commandId(), body.length);
            return;
        }
        synchronized (lock) {
            try {
                CRC32C crc = new CRC32C();
                crc.update(body, 0, body.length);
                ByteBuffer hdr = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                hdr.putInt(body.length);
                hdr.putInt((int) crc.getValue());
                file.seek(file.length());
                file.write(hdr.array());
                file.write(body);
                file.write('\n');
                file.getFD().sync();
            } catch (IOException e) {
                log.warn("command-result-store: append failed id={}: {}", result.commandId(), e.getMessage());
                return;
            }
            results.put(result.commandId(), result);
            completedAt.put(result.commandId(), now);
        }
    }

    private static Path resolvePath(String prop) {
        if (prop != null && !prop.isBlank()) {
            return Path.of(prop);
        }
        String dataDir = System.getenv().getOrDefault("SQS_DATA_DIR",
                System.getProperty("java.io.tmpdir", "."));
        return Path.of(dataDir, "sqs-command-results.wal");
    }

    @PreDestroy
    @Override
    public void close() {
        RandomAccessFile file = raf;
        if (file != null) {
            synchronized (lock) {
                try {
                    file.close();
                } catch (IOException e) {
                    log.debug("command-result-store: close failed: {}", e.getMessage());
                }
            }
        }
    }
}
