# Continuous SYS outage credit

Apply over the v19 SDK and current swarm harness. This SDK change works with any doorway.

The SYS transport records the first disconnected observation on each native peer connection. A recovered connection clears that evidence. Only a confirmed failed transport can carry its continuous outage into the existing ten-second replacement window. Stale callbacks are still rejected by the engine's current-connection checks, and closed transports without failure evidence receive the original full window.

Membership ownership, incarnation checks, restored SYS pipes, doorway liveness corroboration, and leadership commitment remain required. Merely reaching disconnected does not remove a member or promote a leader.

The new SYS_RECOVERY_WINDOW event logs outageMs and remainingMs, together with the peer, attempt, incarnation and replacement mode. It contains no SDP or credentials.

Run the same native reference scenario:

```bash
bash tools/swarm/run.sh --cf wss://switchboard.primer187.workers.dev/ --scenario peer-bridge --peers 5 --bytes 4194304 --timeout 180000
```

Compare leader recovery with the previous 28.1 seconds. A ten-second continuous disconnected interval followed by failed should leave approximately zero additional replacement grace; native failure detection still takes time. Inspect summary.json and events.jsonl. Native browser validation is still required; deterministic SDK tests do not validate browser failure detection or NAT behavior.
