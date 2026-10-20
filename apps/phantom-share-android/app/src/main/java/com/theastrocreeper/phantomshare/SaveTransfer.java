package com.theastrocreeper.phantomshare;

import java.io.IOException;
import java.io.OutputStream;

/** A single ordered, bounded-chunk save. Android owns the destination URI. */
final class SaveTransfer {
    static final int CHUNK_SIZE = 64 * 1024;
    private final OutputStream output;
    private final long expected;
    private long written;
    private int sequence;
    private boolean closed;

    SaveTransfer(OutputStream output, long expected) {
        if (output == null || expected < 0) throw new IllegalArgumentException("Invalid save");
        this.output = output;
        this.expected = expected;
    }
    synchronized void append(int index, byte[] bytes) throws IOException {
        if (closed || index != sequence || bytes.length == 0 || bytes.length > CHUNK_SIZE
                || bytes.length > expected - written) throw new IOException("Invalid save chunk");
        output.write(bytes);
        written += bytes.length;
        sequence++;
    }
    synchronized void finish() throws IOException {
        if (closed || written != expected) throw new IOException("Incomplete save");
        output.flush();
        output.close();
        closed = true;
    }
    synchronized void abort() {
        if (closed) return;
        closed = true;
        try { output.close(); } catch (IOException ignored) { }
    }
}
