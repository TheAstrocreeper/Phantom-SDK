Run after applying v13:

    bash tools/swarm/run.sh --nostr --scenario peer-recovery --peers 4 --bytes 4194304 --timeout 180000

This scenario first runs admission, distribution, publisher loss, and a late
peer download. It then uses the SDK's public invite/accept methods to establish
accepted peer connections among the future survivors. The harness verifies
both endpoints report open, ready peer pipes before dropping all doorway
signaling and closing the current leader.

Success requires surviving peers to agree on a replacement leader and mirror,
an observed existing-peer-pipe SYS recovery signal, and a new file verified by
all survivors while relay signaling remains blacked out. The blackout is
restored even if the scenario fails. Recovery signals appear as
`recovery.peer_pipe` events; normal stage timing and final diagnostics remain.

This tests the existing direct peer-pipe recovery path. It does not implement
third-peer forwarding, bootstrap a completely disconnected newcomer, or prove
cross-network NAT traversal. No SDK source changes are included in this patch.
