Apply over v19, then run three independent sparse bridge scenarios:

    bash tools/swarm/run.sh --stress --scenario peer-bridge --runs 3 --peers 5 --bytes 4194304 --timeout 90000

Each iteration opens a fresh browser and room. Failed iterations remain in the
aggregate, subsequent iterations still run, and the command exits nonzero if
any run fails. Per-run bridgeCounts are included in stress-summary.json along
with stage times and transfer metrics. Each run retains its complete event log
and final diagnostics in run-001, run-002, and run-003.

This uses the local doorway for admission, then blacks out its signaling during
leader replacement. It checks repeatability of the network-side bridge with
native WebRTC on one host. SDK sources and routing behavior are unchanged.
