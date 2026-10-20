package com.theastrocreeper.phantomshare;

import java.util.function.LongConsumer;

/** Bounded native renewal for the entire session, including idle time.
 * Closing invalidates queued callbacks before releasing the lock.
 */
final class SessionWakeLease implements AutoCloseable {
    interface Scheduler {
        void schedule(Runnable task, long delayMs);
        void cancel(Runnable task);
    }
    private final LongConsumer acquire;
    private final Runnable release;
    private final Scheduler scheduler;
    private boolean active;
    private long generation;
    private Runnable pending;

    SessionWakeLease(LongConsumer acquire, Runnable release, Scheduler scheduler) {
        this.acquire = acquire; this.release = release; this.scheduler = scheduler;
    }
    void start() {
        if (active) return;
        active = true;
        renew(++generation);
    }
    private void renew(long current) {
        if (!active || current != generation) return;
        acquire.accept(120000);
        pending = () -> renew(current);
        scheduler.schedule(pending, 60000);
    }
    @Override public void close() {
        active = false; generation++;
        if (pending != null) scheduler.cancel(pending);
        pending = null;
        release.run();
    }
}
