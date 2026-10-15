Run `bash tools/swarm/compare-batches.sh --nostr` from the repository root.
This runs the same four-peer, 4 MiB stress scenario in two fresh rooms,
first with eight chunks per job, then with 32. No SDK sources change.

Compare all stage results, duration, and `metrics` in each run's summary.json.
Metrics count job assignments, requested chunks (including retries), failures,
and connection opens/closes at each endpoint. A successful connection normally
produces two endpoint-open events. Per-stage totals are also included.

A faster run alone does not prove a better default: compare source diversity,
mid-transfer recovery, integrity, and repeated results. Public relay conditions
can vary between arms. This test does not evaluate persistent connection reuse
or connections across different physical networks.
