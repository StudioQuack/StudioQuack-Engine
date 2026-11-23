package com.studioquack.sqs.sandbox.queue;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Durable write-ahead log. Append-only + fsync after every commit so a job survives a host crash;
 * each record carries a CRC32C (Castagnoli) so a torn tail write is detected and tolerated on
 * replay (the in-flight record is lost, nothing before it).
 */
public final class WriteAheadLog implements AutoCloseable {

    private static final byte[] MAGIC = "SQWL".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_LEN = MAGIC.length + 4;
    private static final int MAX_RECORD = 1 << 20; // 1 MiB

    private final Path path;
    private final ObjectMapper mapper;
    private final RandomAccessFile raf;
    private final Object lock = new Object();

    public WriteAheadLog(Path path, ObjectMapper mapper) throws IOException {
        this.path = path;
        this.mapper = mapper;
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        this.raf = new RandomAccessFile(path.toFile(), "rw");
        if (raf.length() == 0) {
            writeHeader();
        } else {
            verifyHeader();
        }
        raf.seek(raf.length());
    }

    private void writeHeader() throws IOException {
        raf.seek(0);
        raf.write(MAGIC);
        ByteBuffer v = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(VERSION);
        raf.write(v.array());
        raf.getFD().sync();
    }

    private void verifyHeader() throws IOException {
        byte[] hdr = new byte[HEADER_LEN];
        raf.seek(0);
        raf.readFully(hdr);
        for (int i = 0; i < MAGIC.length; i++) {
            if (hdr[i] != MAGIC[i]) {
                throw new IOException("wal: bad magic; refusing to overwrite " + path);
            }
        }
        int v = ByteBuffer.wrap(hdr, MAGIC.length, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (v != VERSION) {
            throw new IOException("wal: unsupported format_version " + v + " (we speak " + VERSION + ")");
        }
    }

    /** Serialises + appends a record and fsyncs. */
    public void append(WalRecord record) throws IOException {
        byte[] body = mapper.writeValueAsBytes(record);
        if (body.length > MAX_RECORD) {
            throw new IOException("wal: record too large (" + body.length + " > " + MAX_RECORD + ")");
        }
        CRC32C crc = new CRC32C();
        crc.update(body, 0, body.length);
        ByteBuffer hdr = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        hdr.putInt(body.length);
        hdr.putInt((int) crc.getValue());
        synchronized (lock) {
            raf.seek(raf.length());
            raf.write(hdr.array());
            raf.write(body);
            raf.write('\n');
            raf.getFD().sync();
        }
    }

    /**
     * Replays the log, returning EVERY record in insertion order (the caller reduces them to the
     * latest state per {@code jobId} by merging fields — a {@code submit} carries the language +
     * source, a {@code finish} the terminal state + outputs, so both must be folded, not just the
     * last). A torn / bad-CRC tail record is reported via {@code warn} and stops the scan — everything
     * before it is intact.
     */
    public List<WalRecord> replay(Consumer<String> warn) throws IOException {
        List<WalRecord> records = new ArrayList<>();
        synchronized (lock) {
            long len = raf.length();
            raf.seek(HEADER_LEN);
            long pos = HEADER_LEN;
            while (pos + 8 <= len) {
                byte[] hdr = new byte[8];
                raf.readFully(hdr);
                ByteBuffer hb = ByteBuffer.wrap(hdr).order(ByteOrder.LITTLE_ENDIAN);
                int size = hb.getInt();
                int crcExpected = hb.getInt();
                if (size < 0 || size > MAX_RECORD || pos + 8 + size + 1 > len) {
                    warnTo(warn, "wal: torn/bogus record header at offset " + pos);
                    break;
                }
                byte[] body = new byte[size];
                raf.readFully(body);
                int nl = raf.read(); // trailing '\n'
                CRC32C crc = new CRC32C();
                crc.update(body, 0, body.length);
                if ((int) crc.getValue() != crcExpected) {
                    warnTo(warn, "wal: bad CRC at offset " + pos + "; stopping replay");
                    break;
                }
                if (nl != '\n') {
                    warnTo(warn, "wal: missing record terminator at offset " + pos);
                    break;
                }
                try {
                    WalRecord rec = mapper.readValue(body, WalRecord.class);
                    if (rec.jobId() != null) {
                        records.add(rec);
                    }
                } catch (Exception e) {
                    warnTo(warn, "wal: bad json at offset " + pos + ": " + e.getMessage());
                    // A single unparsable-but-CRC-valid record is skipped (Go parity: `continue`).
                }
                pos += 8 + size + 1;
            }
            raf.seek(raf.length());
        }
        return records;
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            raf.close();
        }
    }

    private static void warnTo(Consumer<String> warn, String msg) {
        if (warn != null) {
            warn.accept(msg);
        }
    }
}
