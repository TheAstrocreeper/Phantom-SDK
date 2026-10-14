# Local network permission

A public HTTPS app can connect two peers on the same Wi-Fi. Browsers may require
local-network permission for that route, even when signaling uses a public server.
Phantom does not change browser flags or bypass consent.

`getState().permissions.localNetwork` and `snapshot().permissions.localNetwork`
contain `state` (`granted`, `prompt`, `denied`, or `unknown`), queried `name`,
`reason`, and a conditional permission-specific `nextAction`. A denied state suggests
`review_local_network_if_connection_fails`; it is not an instruction to interrupt
a working connection. `NETWORK_PERMISSIONS` events
and state callbacks report changes. `refreshNetworkPermissions()` rechecks the
browser, including after returning from site settings. Close/dispose removes
permission listeners. The core uses the injected runtime's navigator.

The SDK queries `local-network` first and falls back to the older
`local-network-access` name. It never treats an unsupported query as denial,
requests permission through synthetic network probes, or blocks public-network
connections based on local permission. A grant is not proof of connectivity;
denial is not proof that a particular ICE failure was caused by permission.
The main transport state and retry policy are unchanged.

The connection demo shows permission evidence before startup. Invitation links
populate the join field and wait for an explicit Join click rather than starting
on page load. Browser-native WebRTC handles consent where supported; a Permissions
API query does not display a prompt. If the browser does not show a prompt,
review Local network access in that HTTPS site's settings. The demo can recheck
permission while startup is pending; it does not claim a grant restarts failed ICE.

## Real-device test

Deploy `demos/connection-test/dist` to a public HTTPS origin and use that same
origin on both devices. Keep mDNS flags at Default. Create on the PC and open
the invitation on the phone, then click Join.

1. With Local network access allowed on the PC, confirm admission and the peer
   data channel, then download both reports.
2. Leave both devices. Block Local network access on the PC, create a fresh room,
   and join on the phone. Confirm the PC reports `denied` with site-setting guidance.
   Actual failure depends on alternative routes; do not require ICE failure.
3. Change the PC setting back to Allow. Recheck permission and confirm `granted`.
   Leave/rejoin with a fresh room to verify a new native connection succeeds.
4. Restore Ask and start through Create/Join. Record whether Chrome displays a
   prompt and whether permission changes after accepting it. If no prompt appears,
   the report still exposes `prompt` and guidance; this is not proof of denial.
5. If a phone or another browser cannot query permission, confirm `unknown`,
   continued connection attempts, and no false claim of permission failure.

Reports retain SDK permission evidence plus browser origin, secure-context flag,
user agent, native RTC transitions, candidate trace, and source revision.
Automated permission/lifecycle tests do not replace this physical-device test.

References: https://developer.chrome.com/release-notes/145 and
https://github.com/GoogleChrome/modern-web-guidance/blob/main/skills/modern-web-guidance/guides/security/local-network-access.md

## Findings from the two-device investigation

The tested PC and phone connected with the PC setting at Allow, Ask, and Block,
including after restarting both devices. An earlier Allow change coincided with
recovery, but these results do not establish permission as the cause of the
original failed ICE checks. Preserve reports from both a failed and successful
attempt before changing settings; compare selected candidate pairs, candidate
receipt/application, and attempt identifiers.

The standard browser SDK creates fresh in-memory core storage for each instance.
It does not persist native peer connections, SDP, or ICE candidates. Browser
persistence includes the verified Nostr relay cache and configured administrative
control records; applications can also explicitly inject durable core storage.
Clearing all site storage is therefore not a justified transport fix and could
erase useful records. Native peer teardown closes both chat and system channels
and peer connections. These findings do not rule out a transient lifecycle or
browser fault, and no transport root cause has been confirmed.
