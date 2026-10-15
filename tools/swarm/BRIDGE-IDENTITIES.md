v19 separates recovery identity bindings from presence/display metadata.

The current direct peer channel retains HELLO's full public key when its
sender and key prefix match that channel's peer ID. Recovery pins it only for
an accepted, ready, committed member. Conflicting bindings are rejected; the
room store is bounded to 256 keys and clears on retirement/disposal.

A forwarding peer verifies the original sender's signature using that pinned
key. It attaches its own signed identity attestation, tied to the original
envelope nonce, room, source, public key, and boot incarnation. A destination
missing the source key must verify the attestation against the intermediary's
already bound key, then verify the original signature and existing membership,
incarnation, attempt, and RTC ownership checks. This does not admit newcomers
or confer authority through an intermediary.

The full SDK integration regression reproduces sender_key_missing before the
binding fix. Afterward it verifies an intermediary offer, returned answer, and
restored direct SYS connection while doorway SIGNAL delivery is disabled.
Discovery remains available in that wiring test. The earlier in-memory
complete-blackout fixture did not converge, so native validation of that
broader scenario remains necessary.

Run the existing native sparse blackout test:

    bash tools/swarm/run.sh --scenario peer-bridge --peers 5 --bytes 4194304 --timeout 90000

Upload summary.json, events.jsonl, and final.json. Inspect sent/forwarded/received
counts, remaining rejection reasons, membership convergence, and the final
new-file integrity stage. Relay behavior and routing order are unchanged.
