Diagnostic sparse topology, applied over v14:

    bash tools/swarm/run.sh --nostr --scenario peer-bridge --peers 5 --bytes 4194304 --timeout 90000

After publisher loss and late-peer admission, future survivors establish a
connected backbone. The late peer gets exactly one accepted peer connection,
to an ordinary bridge member. It has no direct peer pipe to the expected
replacement leader or mirror. The harness records `bridgeTopology` and rejects
an unintended open peer connection before cutting relay signaling.

The current leader then exits with all SDK relay frames blacked out. Success
requires all survivors to converge, peer-pipe recovery traffic to be observed,
and a new file to verify across the survivors before signaling is restored.
If recovery fails, inspect the waiting states and final peer pipes to determine
whether the backbone recovered while the late peer remained stranded.

This is a diagnostic experiment using existing SDK behavior. It adds no proxy
implementation. Existing direct recovery events do not establish third-peer
forwarding; sparse topology and the late peer's final state must also support
that conclusion. Five peers are required so the bridge can be an ordinary
member while separate survivors become leader and mirror. The 90-second
deadline bounds an expected gap without waiting the usual three minutes.
