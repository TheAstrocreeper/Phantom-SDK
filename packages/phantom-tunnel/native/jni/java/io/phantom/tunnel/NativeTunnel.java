package io.phantom.tunnel;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Shared JNI binding: JVM/Android hosts call from worker threads, never a UI
 * thread. Only bounded setup/receipt JSON crosses JNI; the probe bytes remain
 * entirely inside the Rust transport. close() may cancel another worker call.
 * Explicit close is required; do not rely on finalizers for socket ownership.
 */
public final class NativeTunnel implements AutoCloseable {
    static { System.loadLibrary("phantom_tunnel_native"); System.loadLibrary("phantom_tunnel_jni"); }
    private final long handle;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final ReentrantReadWriteLock lifetime = new ReentrantReadWriteLock();
    private NativeTunnel(long handle) { this.handle = handle; }
    @FunctionalInterface private interface Call<T> { T run() throws IOException; }
    private <T> T use(Call<T> call) throws IOException {
        lifetime.readLock().lock();
        try { if (closing.get()) throw new IOException("Native tunnel closed"); return call.run(); }
        finally { lifetime.readLock().unlock(); }
    }
    public static NativeTunnel prepare(String configJson) throws IOException {
        if (configJson == null) throw new IllegalArgumentException("Config required");
        return new NativeTunnel(nPrepare(configJson.getBytes(StandardCharsets.UTF_8)));
    }
    /** Secret offer: forward only over the expected authenticated peer channel. */
    public String offer() throws IOException { return use(() -> nOffer(handle)); }
    public void connect(String remoteOffer, boolean initiator) throws IOException {
        if (remoteOffer == null) throw new IllegalArgumentException("Offer required");
        use(() -> { nConnect(handle, remoteOffer.getBytes(StandardCharsets.UTF_8), initiator); return null; });
    }
    /** Finite 1..32 MiB native probe; consumes the connected session. */
    public String runProbe(int bytes, boolean initiator) throws IOException {
        if (bytes < 1 || bytes > 32 * 1024 * 1024) throw new IllegalArgumentException("Invalid probe size");
        return use(() -> nProbe(handle, bytes, initiator));
    }
    @Override public void close() throws IOException {
        if (!closing.compareAndSet(false, true)) return;
        IOException failure = null;
        try { nClose(handle); } catch (IOException error) { failure = error; }
        lifetime.writeLock().lock();
        try { nFree(handle); } finally { lifetime.writeLock().unlock(); }
        if (failure != null) throw failure;
    }
    private static native long nPrepare(byte[] config) throws IOException;
    private static native String nOffer(long handle) throws IOException;
    private static native void nConnect(long handle, byte[] remote, boolean initiator) throws IOException;
    private static native String nProbe(long handle, int bytes, boolean initiator) throws IOException;
    private static native void nClose(long handle) throws IOException;
    private static native void nFree(long handle);
}
