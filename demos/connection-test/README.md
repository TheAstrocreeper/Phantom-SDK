# Main SDK connection test

Minimal browser demo using the repository's createBrowserPhantom entry API, getState, snapshot, onEvent and onDoorwayStatus. No SDK changes or custom transport/runtime wrappers. No automatic app retry or recovery loop.

Build: npm install in this directory, then npm run build. Deploy only dist.

Create a room, open its generated invitation on another device. If the SDK admits both and a discovered peer has no chat channel, use Connect peer; incoming invitations are accepted through the SDK. Leave aborts pending startup or closes the returned network. Reports remain available afterward. Record SDK snapshot takes an explicit current observation without polling.

Reports include the SDK baseline commit, build, selected adapter, state, snapshots and full nested doorway events. Event history is bounded at 256 records, arrays at 256 items, depth at 12, strings at 16000 characters. Older events are counted as dropped. Credentials, invitation bodies and SDP fields are redacted. Reports contain peer IDs and diagnostic network metadata.

These observations do not include native RTC candidate-pair statistics: current main does not expose them. An open doorway is not an open peer connection. Successful compilation and app control checks do not prove cross-device connection or recovery.

SDK package baseline: 007a99affb99bb8055648fc5be3deb3a0408779e. All 38 package files were verified byte-for-byte against that commit when this demo was built.
