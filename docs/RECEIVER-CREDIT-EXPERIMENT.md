# Association layout and receiver credit experiments — 2026-10-29

These are isolated native Chrome lab experiments. No Phantom Share runtime
settings or deployment changed.

## Common method

Chrome 156.0.8078.47, Ubuntu GitHub Actions, two separate browser renderer
contexts sharing one host and Chrome network service. Two reliable ordered data
channels, each carrying 32 MiB of deterministic bytes (64 MiB total per trial).
Fresh connections each trial; SHA-256 verification of both full reconstructed
streams. Timings exclude connection setup and include final hashing.

256 KiB messages, 1 MiB application high watermark and 512 KiB low watermark
per data channel throughout. Sender resumes queue waits through drain events.
No pacing sleeps or recovery polling. A 45-second watchdog only bounds test
failure. Receiver gaps use receiver-local timestamps. Selected routes must remain
stable; counters must be present and monotonic.

This tests the local send-pressure mechanism, not internet/5G capacity. The
runner patch version differs from the user's traced Chrome 156.0.8078.73.

## A. Shared versus independent associations

Separate means two PeerConnections/SCTP associations; shared means two data
channels on one PeerConnection/SCTP association. Queue policy and payload unchanged.
Transport counters are collected once per association, avoiding double counting.

| Layout / trial | Verified MB/s (decimal) | Send discards | Maximum receive gap (ms) |
|---|---:|---:|---:|
| separate 1 | 20.54 | 732 | 881.8 |
| shared 2 | 24.51 | 361 | 830.5 |
| shared 3 | 18.63 | 352 | 1252.6 |
| separate 4 | 24.04 | 636 | 882.3 |

All four transfers verified. Sharing an association reduced discard counts in
these observations but did not eliminate them or the large gaps. Throughput
ranges overlap; there is no demonstrated consistent benefit to justify an app
architecture change from this experiment.

## B. Receiver receipt credits

The current feed uses application queue drain alone. The credit feed retains
two independent associations and the same message/watermark settings, but each
lane sends its next 256 KiB message only after the receiver acknowledges receipt
of the prior complete message. At most 256 KiB per lane (512 KiB total peer data)
is sent without a receipt credit. Credits acknowledge receipt after copying
bytes, not per-message hashing or disk persistence; full stream hashes are checked
at completion.

| Feed / trial | Verified MB/s (decimal) | Send discards | Maximum receive gap (ms) |
|---|---:|---:|---:|
| current 1 | 14.98 | 720 | 917.9 |
| credit 2 | 21.00 | 0 | 27.1 |
| credit 3 | 21.23 | 0 | 26.3 |
| current 4 | 13.16 | 742 | 901.1 |

Both credit trials eliminated observed send discards and long receive gaps and
exceeded both surrounding current-feed throughput observations. All streams
verified. This is a controlled local result, not a promise of internet speed.

The result supports bounding end-to-end outstanding data rather than treating
bufferedAmount as a count of all data still pending in Chromium. A fixed
one-message receipt window can limit high-RTT throughput: per-lane capacity is
approximately outstanding bytes divided by the effective receipt round trip.
Thus this experimental window is not a production constant or a universal fit.
Phantom's existing file-request window should be evaluated before adding a
separate credit protocol. The app remains on 0.1.77.

## Reproduction and raw data

```sh
npm ci --prefix tools/swarm
PHANTOM_CHROMIUM_EXECUTABLE="$(command -v google-chrome)" node tools/perf/association-layout-browser.mjs
PHANTOM_CHROMIUM_EXECUTABLE="$(command -v google-chrome)" node tools/perf/delivery-credit-browser.mjs
```

- [Association raw results](../tools/perf/results/association-layout-2026-10-29.json); workflow 40891707590.
- [Credit raw results](../tools/perf/results/delivery-credit-2026-10-29.json); workflow 40892109498.
