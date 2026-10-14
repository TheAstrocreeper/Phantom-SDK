v16 adds one-intermediary SYS signaling recovery.

An admitted peer can send a signed `SYS_BRIDGE_RTC` envelope over an accepted,
ready peer pipe. The intermediary forwards it only to another directly
connected, admitted peer. The destination verifies the original sender's
signature against its full identity key, room, boot incarnation, and committed
membership. Existing offer ownership, attempt matching, admission policy,
and successor proof checks still run in the RTC handlers.

Responses use a short-lived return route over the same intermediary. The
bridge is limited to one forwarding hop, three outgoing candidates, 15-second
envelopes, 256 retained message IDs, 64 return routes, and 96 KiB envelopes.
Room reset and disposal clear retained state. Healthy direct peer paths remain
preferred; bridge signaling supplements the normal doorway send.

The browser SDK supplies signing and verification from its existing identity.
Core integrations can inject `recoverySignaling: {sign, verify}`: `sign(payload)`
returns a proof containing the original payload, full `peerPublicKey`,
`issuedAt`, and unique `nonce`; `verify(proof, payload, expectedPublicKey)` must
verify the signature, payload, and protocol/room scope synchronously. Without
that injection, the core retains its existing direct recovery behavior.

This covers previously admitted peers with surviving accepted connections.
It does not admit an unknown newcomer through an intermediary or create an
entry connection for a completely isolated peer. Forwarding establishes a
direct RTC path between the endpoints; the intermediary carries signaling.

Run the same sparse diagnostic that failed on v15:

    bash tools/swarm/run.sh --nostr --scenario peer-bridge --peers 5 --bytes 4194304 --timeout 90000

Success now requires forwarded bridge events, all survivors converging, and a
new verified transfer while relay signaling remains blacked out. Inspect
`bridgeCounts` and `SYS_SIGNAL_BRIDGE` events as well as final peer states.
Native/public-relay validation remains pending.
