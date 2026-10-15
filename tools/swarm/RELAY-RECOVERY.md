Nostr relay coordination (v13)

Discovery-enabled Nostr doorways refresh alternatives on the first rejected
publication path, while another shared path may still be usable. They add
subscriptions without replacing healthy sockets. Signed peer announcements
carry the current relay coordinates; other discovery-enabled peers follow
those coordinates, subject to the eight-relay limit. Restricted networks
follow hints only from identities allowed by the admission checkpoint.

Explicit relay configurations without `discover` stay pinned. Relay hints
do not admit a peer, replace committed membership, or elect leaders.
Rejected sockets remain read-only; the SDK does not retry writes under new
identities to evade relay policy.

The deterministic regression uses production Nostr signing, verification,
doorway code, and relay-set code with simulated relay delivery. It verifies
sharing alternatives before asymmetric loss of the last original writable
relay, then bidirectional signaling over the new common relay. This is not
native browser or public-relay validation. If every common route is already
lost before coordinates can be shared, this change cannot guarantee reunion.

For live validation, run:

    bash tools/swarm/run.sh --nostr --stress --runs 1 --peers 4 --batch-size 8 --timeout 180000

Use eight chunks for this run to isolate relay recovery from batch tuning.
Inspect `relay_set_expanded` events, offer receipt, unanswered-offer timeouts,
and membership convergence alongside the stage results.
