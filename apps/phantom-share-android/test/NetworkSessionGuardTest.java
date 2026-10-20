package com.theastrocreeper.phantomshare;

public final class NetworkSessionGuardTest {
    static void equal(Object expected, Object actual) {
        if (!java.util.Objects.equals(expected, actual)) throw new AssertionError(expected + " != " + actual);
    }
    public static void main(String[] args) {
        NetworkSessionGuard guard = new NetworkSessionGuard();
        equal(null, guard.changed("wifi-a"));
        guard.begin("first", "wifi-a");
        equal(null, guard.changed("wifi-a"));
        equal("first", guard.changed("cellular"));
        equal(null, guard.changed(null));
        equal(null, guard.changed("cellular"));
        guard.begin("second", "cellular");
        guard.end("first"); // An older session cannot detach a newer one.
        equal("second", guard.changed("wifi-a"));
        guard.begin("third", "wifi-a");
        equal("third", guard.changed("wifi-b"));
        guard.begin("fourth", "wifi-b");
        equal("fourth", guard.changed(null));
        guard.begin("fifth", null);
        equal("fifth", guard.changed("wifi-a"));
        guard.begin("closed", "wifi-a"); guard.end("closed");
        equal(null, guard.changed("cellular"));
        System.out.println("Network guard: baseline, same route, both switch directions, loss, stale owner and close passed");
    }
}
