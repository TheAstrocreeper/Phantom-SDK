# Actual Breach transfer comparison

Build the recovered Breach application twice, with identical application source:

```sh
node tools/breach-comparison/build.mjs /tmp/phantom-breach-comparison
```

Requires the repository's root dependencies and `demos/phantom-share` dependencies.
The build reads committed Git objects into temporary isolated directories. It does
not modify Share or replace SDK files. `comparison.json` records both revisions.
Reference is preserved PR #2 source at `00dfcb1`, not a confirmed copy of the
previously fast deployed build. Current is the local committed HEAD.

Both applications run the real SDK: full-file preparation hash, `publishFile`
with 64 KiB chunks, `shareFile`, `requestFile`, per-chunk verification, receiver
progress, and full-file SHA-256 before Save. Both use eight outstanding requests
and the existing authenticated peer channel. Neither configures Share's live
membership authority or its separate distribution connection and scheduler.

Deploy the output as a separate test site. On both devices select the same build,
join using that build's invitation, and send the same file in the same direction.
Keep pages visible and compare completion plus downloaded reports. Do not mix
reference/current endpoints in the first comparison.

Interpretation:

| Result | What it isolates |
| --- | --- |
| Reference fast, current slow | SDK revision difference on Breach's actual path |
| Both fast, Share slow | Share configuration/distribution/membership or its selected path |
| Both slow | Preserved reference does not reproduce the reported fast condition; no universal device-cap conclusion |

These are isolation steps, not proofs of a specific faulty function. Different
RTC connections can select different paths and device/network conditions vary.
The existing deterministic `transfer-path-comparison.mjs` verifies exact bytes
through current direct and Share paths, but bypasses the native engine transport
and cannot establish physical throughput. The raw ramp is a third path and is
not a substitute for either actual application flow.
