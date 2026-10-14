# Early successor offer retention

Apply over v21. This is a universal SYS recovery fix, independent of relay type.

A proposed successor can send its first SYS offer before the receiver loses its old leader pipe. Previously the existing successor guard rejected that early offer, leaving the sender to wait for its unanswered-offer timeout. The SDK now holds bounded offers and ICE only when the successor proof matches the committed room, roster, epoch, sequence, sender, receiver and incarnation. Retention grants no authority.

When the current old leader transport fails, the engine revalidates the retained offer and resumes the same attempt through the existing offer acceptance checks. Authority and incarnation changes invalidate pending offers. Expiration remains a failure bound, not a retry schedule.

The new SYS_SUCCESSOR_OFFER events show held (old_leader_pipe_open), resumed (old_leader_pipe_lost), dropped or rejected. They contain identifiers and candidate counts, not SDP. Existing SYS_RECOVERY_WINDOW events still show outage credit.

```bash
bash tools/swarm/run.sh --cf wss://switchboard.primer187.workers.dev/ --scenario peer-bridge --peers 5 --bytes 4194304 --timeout 180000
```

Check all eight stages, hash-verified transfers while signaling is blocked, recovery duration, and whether SYS_SIGNAL_TIMEOUT disappears from the leader-to-mirror transition. Native timing validation is pending; deterministic tests verify retention and existing bridge integration, not browser failure detection.
