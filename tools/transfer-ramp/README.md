# Transfer ramp experiment

Build Phantom Share, connect two devices, finish active file transfers, then choose **Connection test** in the footer. The other device must accept. Both pages must remain visible; hiding a page cancels the run so timer throttling is not silently treated as network capacity.

The tool creates a separate reliable, ordered raw RTC channel, using admitted SDK peer messaging for SDP/ICE signaling. It does **not** change SDK transfer settings or claim that the benchmark and file channel selected the same route. Exported selected candidate types, protocol and relay transport permit checking that distinction without exporting IP addresses, SDP, ICE credentials or invitation secrets. The current default uses the SDK core's Google STUN configuration, with no provided TURN server; TURN scenarios need an explicit controlled configuration.

Each direction sweeps 256 KiB → 1 MiB → 4 MiB → 16 MiB → 4 MiB → 1 MiB → 256 KiB for three seconds per step. Send credit tracks receiver acknowledgments, and queued browser bytes are separately bounded by the same window. Chunks are 64 KiB. Each step has a 256 MiB traffic budget; hitting it marks the sample censored. Total generated payload is at most 3.5 GiB plus control traffic. The Extended profile adds a 64 MiB peak with a 1 GiB per-stage traffic budget (nine stages per direction, at most 18 GiB). The accepting device confirms the selected profile. These are disclosed experiment limits, not measured link limits or proposed SDK defaults. A growing result at the top of the sweep requires a wider experiment before declaring a ceiling.

Exports contain receiver-counted bytes and duration, active and drain/completion time, RTT samples summarized by median/max, peak queued and unacknowledged bytes, time waiting for browser buffer space versus receiver credit, test settings, both browser agents, selected route type, optional operator conditions and the local application's diagnostic history. Failed or cancelled runs are not capacity results; a partial direction report, when available, is labeled incomplete. The test discards received data instead of retaining a file. It measures throughput, not power use, thermal safety or a complete SDK memory audit.

## Compare multiple environments

Use the same settings, repeat each case at least three times and alternate which device initiates. Record conditions in the panel. Keep the raw report for each run and export regular Share logs for a normal transfer between the same peers. Run normal transfers separately; concurrent traffic cancels the local test when observed by Share.

| Cohort | Useful comparisons |
| --- | --- |
| Desktop LAN | Ethernet ↔ Ethernet; Ethernet ↔ Wi-Fi |
| Mobile LAN | Android ↔ desktop; iPhone ↔ desktop; older/lower-memory devices |
| Internet | Two independent home networks; Wi-Fi ↔ cellular; cellular ↔ cellular |
| Network variation | Strong/weak Wi-Fi, higher RTT, jitter/loss, constrained upstream |
| Browser variation | Chromium, Firefox, Safari where supported |
| Relayed paths | Explicitly configured TURN UDP/TCP, kept separate from direct paths |

Controlled bandwidth/RTT/loss emulation can expose controller defects; it does not replace real-device cohorts. Keep local loopback, simulations, direct WAN and TURN results distinguishable. There is no universal safe setting inferred from one pair or one successful run.

A *candidate* window is the smallest window measured twice (up/down) that sustains at least 90% of the observed peak while median RTT stays below max(50 ms, twice idle RTT). An 80% throughput pacing target is exported as a provisional candidate only. These thresholds are explicit heuristics for inspection, not validated guarantees. Censored samples, missing repeat samples, high latency, or a non-demonstrated plateau produce an inconclusive recommendation. The raw table remains available even when no candidate is selected.

Run `node tools/transfer-ramp/summarize.mjs path/to/phantom-ramp-*.json` for a compact per-run/direction comparison. It deliberately does not pool unlike routes or promote results to SDK defaults.


For a pacing comparison, choose Compare pacing on both-v9 endpoints. Group rows by `flowControl` and matching `windowStep`; mode order alternates by step. Receiver-credit bounds outstanding bytes, native-buffer bounds local payload buffering and deliberately omits application credits. Do not compare their outstanding-byte fields as if equivalent, mix their rows into a single recommendation, or interpret in-channel response time as pure network RTT. This isolates a sender-control difference on one dedicated RTC connection; it does not benchmark the actual Breach peer connection. Compare routes separately when following up with real file APIs.
