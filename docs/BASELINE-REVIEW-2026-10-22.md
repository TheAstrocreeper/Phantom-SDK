# Phantom-SDK baseline review — 2026-10-22

Reviewed main: `dc70dd0d2766cc1961ccc7a549d9b55666a3dd46`. Its source tree matches the tested extraction commit `a9cc0d29c390b279cf299ef5e2e80e49511fc742`.

## Results

- No open PRs or unfinished feature work to merge.
- Nine old branch heads match exact merged PR heads, and all corresponding merge commits are retained in main. Eight appear diverged because of squash merges; ancestry alone would incorrectly classify those as unfinished work. Do not merge them again.
- The full 280-test suite passes after the test fixture adjustment below. Its targeted 15-test file-window suite passes. No production package file changes are needed.
- npm audit reports zero known advisories for the dependency lockfile at review time.
- The remote tracked tree contains no APKs, private key containers, signing keystores, dependency directories or generated build/report directories. A focused common-key/token pattern scan found no matches in the checked source tree. This is not a historical secret scan or independent security certification.
- Local encrypted doorways and PIN PAKE remain SDK APIs; HTTP serving, platform permissions, saving, app presentation and compatibility reporting remain application concerns. The proposed resource-adapter idea is future work and is not included in this cleanup.

## Test fixture adjustment

The file-resume test previously gave real hashing and writes a 20 ms deadline. In the full suite it could reject during chunk verification instead of reaching the intended missing-chunk retry failure (279/280 passed in the first run). The test now waits for the 31 verified chunks, advances a mock clock through the missing chunk's retries, then verifies that resuming requests only that missing chunk and returns identical bytes. Production transfer timeouts, retries and cache behavior are unchanged.

## Safe branch cleanup

| Branch | Merged PR |
| --- | --- |
| audit/sdk-contract-and-recovery | #5 |
| feat/local-connect-api | #14 |
| fix/admission-timeout-diagnostics | #10 |
| fix/permission-evidence | #3 |
| test/headless-nat | #12 |
| test/live-nostr-smoke | #7 |
| test/native-sdk-contracts | #6 |
| test/turn-failure-contracts | #9 |
| test/turn-only-connectivity | #8 |

Recheck exact branch heads before deletion and preserve any new commits. Keep main. The test/documentation housekeeping branch is removable once merged. Phantom Share can keep its existing SDK source pin because package contents have not changed.
