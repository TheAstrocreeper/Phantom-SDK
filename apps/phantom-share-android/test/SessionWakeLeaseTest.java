package com.theastrocreeper.phantomshare;

import java.util.HashMap;
import java.util.Map;

public final class SessionWakeLeaseTest {
    static final class Clock implements SessionWakeLease.Scheduler {
        long now, lockedUntil;
        int acquisitions, releases;
        final Map<Runnable, Long> tasks = new HashMap<>();
        public void schedule(Runnable task, long delay) { tasks.put(task, now + delay); }
        public void cancel(Runnable task) { tasks.remove(task); }
        void advance(long duration) {
            long end = now + duration;
            while (!tasks.isEmpty()) {
                var next = tasks.entrySet().stream().min(Map.Entry.comparingByValue()).orElseThrow();
                if (next.getValue() > end) break;
                now = next.getValue(); tasks.remove(next.getKey()); next.getKey().run();
                check(lockedUntil > now, "native renewal must keep the CPU lease alive");
            }
            now = end;
        }
    }
    static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }
    public static void main(String[] args) {
        Clock clock = new Clock();
        SessionWakeLease lease = new SessionWakeLease(timeout -> {
            clock.acquisitions++; clock.lockedUntil = clock.now + timeout;
        }, () -> { clock.releases++; clock.lockedUntil = 0; }, clock);
        lease.start(); lease.start();
        check(clock.acquisitions == 1 && clock.tasks.size() == 1, "duplicate start must not duplicate renewal");
        // No JavaScript calls, transfer events or foreground callbacks for an hour.
        clock.advance(3600000);
        check(clock.acquisitions == 61 && clock.lockedUntil > clock.now, "idle session must stay awake");
        Runnable late = clock.tasks.keySet().iterator().next();
        lease.close(); late.run();
        check(clock.lockedUntil == 0 && clock.tasks.isEmpty(), "close must release and fence queued renewal");
        lease.start(); int count = clock.acquisitions; late.run();
        check(clock.acquisitions == count && clock.tasks.size() == 1, "old callback must not renew a replacement session");
        lease.close(); clock.advance(600000);
        check(clock.lockedUntil == 0 && clock.tasks.isEmpty(), "closed session must remain asleep");
        System.out.println("Session wake lease: hour-long idle renewal, duplicate start, close and stale callback checks passed");
    }
}
