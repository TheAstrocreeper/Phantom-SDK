# SDK connection test

This demo uses createBrowserPhantom, state, snapshots, SDK events and doorway callbacks. Create on one device and open its invitation on the other. Connect peer invites a chat channel after admission; incoming invitations are accepted. Leave cancels startup or closes the network. Diagnostics remain available after failure and Leave.

Build with npm install and npm run build in this directory. Deploy only dist. Set SDK_SOURCE_COMMIT to the source commit being built to embed its exact revision.

Download full report or Copy full report includes native RTC state transitions, bounded getStats candidate, candidate-pair, transport, SCTP and data-channel rows, connection retirement reasons, ICE username fragments, SDK states and nested relay status. The SDK captures stats on events and invokes getStats before native close; completion is asynchronous and retrieval errors are recorded. No polling or app recovery loop is used.

Limits: 256 app events; 32 retained connections; 32 transitions and 128 stats per connection. Dropped records are counted. The SDK permits one live stats request and one terminal request per connection, coalescing other state changes. SDP, passwords, credentials and invitations are redacted; reports retain peer IDs and IP/port diagnostic metadata.

An open relay is not an open peer channel. Automated tests validate ownership, candidate queueing and evidence retention, not connectivity between physical devices.

See docs/six7-connection-lifecycle.md for the source comparison and known differences.
