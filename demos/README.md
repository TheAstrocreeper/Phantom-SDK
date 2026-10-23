# Phantom demos

Working applications built with the actual Phantom SDK. Source examples include setup instructions; separately maintained applications may offer a hosted demo without publishing their application source.

| Demo | What it demonstrates | Source and setup | Hosted demo |
| --- | --- | --- | --- |
| Phantom Share | Share files directly between devices using invitation codes, links or QR codes, with recipient selection and device removal. | Proprietary application; source is not included in this repository. | [Open Phantom Share](https://phantomshare.app/) |
| Shared canvas | Create a room on a laptop, scan its QR code on a phone, and draw together over peer data channels. Inspect real connection state and message counts. | [Shared canvas](shared-canvas/README.md) | [Open demo](https://phantom-canvas.pages.dev/) |
| Breach window comparison | Send the same file with 8 and 32 requests over one existing peer connection, verify bytes, and export measured timing. | [Breach comparison](phantom-breach/README.md) | Build and deploy this revision |

The hosted canvas is publicly accessible on Cloudflare Pages; no account login is required. Build and host the source to run your own instance.

## Adding a demo

For a separately maintained hosted application, add its public URL and describe the SDK capabilities it demonstrates. Clearly indicate when application source is not provided.

For a source example, create a folder under `demos/` with the application source and a README covering setup, SDK integration, and network requirements. Add a row above so every demo remains reachable from the main repository README. Use actual SDK events and traffic for any connection inspector.
