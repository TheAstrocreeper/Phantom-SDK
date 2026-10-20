package com.theastrocreeper.phantomshare;

import java.util.Objects;

/** Network identity is local and ephemeral. Capability changes on the same
 * route do not end a session; replacement/loss ends its owner exactly once.
 */
final class NetworkSessionGuard {
    private String token, route;
    void begin(String owner, String network) { token = owner; route = network; }
    void end(String owner) { if (Objects.equals(token, owner)) token = null; }
    String changed(String network) {
        if (token == null || Objects.equals(route, network)) return null;
        String previous = token; token = null; return previous;
    }
}
